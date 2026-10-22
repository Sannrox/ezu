//! The live index: one R-tree per namespace and collection.
//!
//! Each `(namespace, collection)` is a partition behind its own lock, so
//! writers to different collections never wait on each other. Namespace
//! quotas (entity count, vertices per shape, updates per second) are checked
//! before a write lands. Expired entities are hidden from reads at once and
//! removed by [`Engine::sweep_expired`].

use crate::entity::{Entity, EntityError, Namespace};
use crate::geometry::{BoundingBox, query_boxes};
use crate::query::{
    EARTH_RADIUS_M, MAX_LIMIT, Query, QueryPage, QueryRequest, distance, invalid, matches,
    radius_windows, windows,
};
use rstar::primitives::{GeomWithData, Rectangle};
use rstar::{AABB, RTree};
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};

/// Per-namespace limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Quotas {
    pub max_entities: usize,
    /// Collections one namespace may hold; bounds empty partitions too.
    pub max_collections: usize,
    pub max_vertices: usize,
    pub max_updates_per_second: u32,
    pub max_fences: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub enum IndexError {
    Invalid(EntityError),
    EntityQuota {
        limit: usize,
    },
    CollectionQuota {
        limit: usize,
    },
    VertexQuota {
        limit: usize,
        vertices: usize,
    },
    RateQuota {
        limit: u32,
    },
    FenceQuota {
        limit: usize,
    },
    /// A query window outside WGS 84 or with `south > north`.
    InvalidWindow,
    InvalidQuery(&'static str),
    PartitionNotEmpty,
    /// A writer panicked while holding a lock; the partition is not trusted.
    Poisoned,
}

impl fmt::Display for IndexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(error) => write!(f, "invalid entity: {error}"),
            Self::EntityQuota { limit } => {
                write!(f, "namespace entity quota reached ({limit} entities)")
            }
            Self::CollectionQuota { limit } => {
                write!(
                    f,
                    "namespace collection quota reached ({limit} collections)"
                )
            }
            Self::VertexQuota { limit, vertices } => write!(
                f,
                "shape has {vertices} vertices; the namespace allows {limit}"
            ),
            Self::RateQuota { limit } => {
                write!(f, "namespace update rate exceeded ({limit} per second)")
            }
            Self::FenceQuota { limit } => {
                write!(f, "namespace fence quota reached ({limit} fences)")
            }
            Self::InvalidWindow => f.write_str(
                "query window needs finite lon in [-180, 180], lat in [-90, 90] and south <= north",
            ),
            Self::InvalidQuery(reason) => write!(f, "invalid query: {reason}"),
            Self::PartitionNotEmpty => f.write_str("bulk load needs an empty collection"),
            Self::Poisoned => f.write_str("index partition is unavailable after a failed write"),
        }
    }
}

impl std::error::Error for IndexError {}

pub(crate) type Node = GeomWithData<Rectangle<[f64; 2]>, String>;

pub(crate) fn node(bbox: &BoundingBox, id: &str) -> Node {
    GeomWithData::new(
        Rectangle::from_corners([bbox.min_lon, bbox.min_lat], [bbox.max_lon, bbox.max_lat]),
        id.to_string(),
    )
}

#[derive(Default)]
struct Partition {
    tree: RTree<Node>,
    entities: HashMap<String, Entity>,
}

impl Partition {
    fn remove(&mut self, id: &str) -> Option<Entity> {
        let entity = self.entities.remove(id)?;
        for bbox in entity.geometry.boxes() {
            self.tree.remove(&node(&bbox, id));
        }
        Some(entity)
    }

    fn insert(&mut self, entity: Entity) {
        for bbox in entity.geometry.boxes() {
            self.tree.insert(node(&bbox, &entity.id));
        }
        self.entities.insert(entity.id.clone(), entity);
    }
}

struct RateWindow {
    second: i64,
    count: u32,
}

struct NamespaceState {
    entities: AtomicUsize,
    rate: Mutex<RateWindow>,
    collections: RwLock<HashMap<String, Arc<RwLock<Partition>>>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Upsert {
    Inserted,
    Updated,
    /// Older than the stored entity; nothing changed.
    Stale,
}

/// The live index over every namespace.
pub struct Engine {
    quotas: Quotas,
    namespaces: RwLock<HashMap<Namespace, Arc<NamespaceState>>>,
}

impl Engine {
    pub fn new(quotas: Quotas) -> Self {
        Self {
            quotas,
            namespaces: RwLock::new(HashMap::new()),
        }
    }

    pub fn upsert(
        &self,
        ns: &Namespace,
        entity: Entity,
        now_ms: i64,
    ) -> Result<Upsert, IndexError> {
        self.admit(&entity)?;
        let state = self.namespace(ns)?;
        self.charge_rate(&state, now_ms)?;
        // A write into a collection that does not exist yet is an insert:
        // reserve first, so a refused write leaves no empty partition behind.
        let (partition, reserved) = match existing(&state, &entity.collection)? {
            Some(partition) => (partition, false),
            None => {
                self.reserve(&state, 1)?;
                match self.partition(&state, &entity.collection) {
                    Ok(partition) => (partition, true),
                    Err(error) => {
                        state.entities.fetch_sub(1, Ordering::AcqRel);
                        return Err(error);
                    }
                }
            }
        };
        let mut partition = partition.write().map_err(|_| IndexError::Poisoned)?;
        let outcome = match partition.entities.get(&entity.id) {
            Some(stored) if reserved => {
                // Another writer created the collection and this id first.
                state.entities.fetch_sub(1, Ordering::AcqRel);
                if stored.ts_ms > entity.ts_ms {
                    return Ok(Upsert::Stale);
                }
                partition.remove(&entity.id);
                Upsert::Updated
            }
            None if reserved => Upsert::Inserted,
            Some(stored) if stored.ts_ms > entity.ts_ms => return Ok(Upsert::Stale),
            Some(_) => {
                partition.remove(&entity.id);
                Upsert::Updated
            }
            None => {
                self.reserve(&state, 1)?;
                Upsert::Inserted
            }
        };
        partition.insert(entity);
        Ok(outcome)
    }

    /// Re-apply a write that was already admitted, during recovery. It is
    /// validated and keeps the newer-wins rule, but is not charged to
    /// quotas: data accepted before a restart is never refused after it.
    pub(crate) fn restore(&self, ns: &Namespace, entity: Entity) -> Result<Upsert, IndexError> {
        entity.validate().map_err(IndexError::Invalid)?;
        let state = self.namespace(ns)?;
        let partition = match existing(&state, &entity.collection)? {
            Some(partition) => partition,
            None => state
                .collections
                .write()
                .map_err(|_| IndexError::Poisoned)?
                .entry(entity.collection.clone())
                .or_default()
                .clone(),
        };
        let mut partition = partition.write().map_err(|_| IndexError::Poisoned)?;
        let outcome = match partition.entities.get(&entity.id) {
            Some(stored) if stored.ts_ms > entity.ts_ms => return Ok(Upsert::Stale),
            Some(_) => {
                partition.remove(&entity.id);
                Upsert::Updated
            }
            None => {
                state.entities.fetch_add(1, Ordering::AcqRel);
                Upsert::Inserted
            }
        };
        partition.insert(entity);
        Ok(outcome)
    }

    /// Load recovered entities into one collection in a single R-tree bulk
    /// pass, outside quotas like [`Self::restore`]. A collection that
    /// already holds entities takes them one by one instead.
    pub(crate) fn bulk_restore(
        &self,
        ns: &Namespace,
        collection: &str,
        entities: Vec<Entity>,
    ) -> Result<(), IndexError> {
        let mut latest: HashMap<String, Entity> = HashMap::new();
        for entity in entities {
            entity.validate().map_err(IndexError::Invalid)?;
            match latest.get(&entity.id) {
                Some(stored) if stored.ts_ms > entity.ts_ms => {}
                _ => {
                    latest.insert(entity.id.clone(), entity);
                }
            }
        }
        let state = self.namespace(ns)?;
        let partition = state
            .collections
            .write()
            .map_err(|_| IndexError::Poisoned)?
            .entry(collection.to_string())
            .or_default()
            .clone();
        let mut partition = partition.write().map_err(|_| IndexError::Poisoned)?;
        if !partition.entities.is_empty() {
            drop(partition);
            for entity in latest.into_values() {
                self.restore(ns, entity)?;
            }
            return Ok(());
        }
        let nodes = latest
            .values()
            .flat_map(|entity| {
                entity
                    .geometry
                    .boxes()
                    .into_iter()
                    .map(|bbox| node(&bbox, &entity.id))
            })
            .collect();
        partition.tree = build_tree(nodes);
        state.entities.fetch_add(latest.len(), Ordering::AcqRel);
        partition.entities = latest;
        Ok(())
    }

    /// Every stored entity with its namespace, expired ones included, for a
    /// snapshot.
    pub(crate) fn export(&self) -> Result<Vec<(Namespace, Entity)>, IndexError> {
        let namespaces: Vec<(Namespace, Arc<NamespaceState>)> = self
            .namespaces
            .read()
            .map_err(|_| IndexError::Poisoned)?
            .iter()
            .map(|(ns, state)| (ns.clone(), state.clone()))
            .collect();
        let mut all = Vec::new();
        for (ns, state) in namespaces {
            let partitions: Vec<Arc<RwLock<Partition>>> = state
                .collections
                .read()
                .map_err(|_| IndexError::Poisoned)?
                .values()
                .cloned()
                .collect();
            for partition in partitions {
                let partition = partition.read().map_err(|_| IndexError::Poisoned)?;
                all.extend(
                    partition
                        .entities
                        .values()
                        .map(|entity| (ns.clone(), entity.clone())),
                );
            }
        }
        Ok(all)
    }

    /// Remove a namespace with every entity in it, returning them. Its key
    /// was revoked: nothing of it stays readable in memory.
    pub(crate) fn drop_namespace(&self, ns: &Namespace) -> Result<Vec<Entity>, IndexError> {
        let Some(state) = self
            .namespaces
            .write()
            .map_err(|_| IndexError::Poisoned)?
            .remove(ns)
        else {
            return Ok(Vec::new());
        };
        let partitions: Vec<Arc<RwLock<Partition>>> = state
            .collections
            .read()
            .map_err(|_| IndexError::Poisoned)?
            .values()
            .cloned()
            .collect();
        let mut dropped = Vec::new();
        for partition in partitions {
            let mut partition = partition.write().map_err(|_| IndexError::Poisoned)?;
            dropped.extend(std::mem::take(&mut partition.entities).into_values());
            partition.tree = build_tree(Vec::new());
        }
        Ok(dropped)
    }

    /// Remove an entity; true when it existed.
    pub fn delete(&self, ns: &Namespace, collection: &str, id: &str) -> Result<bool, IndexError> {
        let Some(partition) = self.existing_partition(ns, collection)? else {
            return Ok(false);
        };
        let removed = partition
            .write()
            .map_err(|_| IndexError::Poisoned)?
            .remove(id)
            .is_some();
        if removed {
            self.namespace(ns)?.entities.fetch_sub(1, Ordering::AcqRel);
        }
        Ok(removed)
    }

    /// Load a collection on startup in one pass. The collection must be
    /// empty; quotas apply to the whole batch.
    pub fn bulk_load(
        &self,
        ns: &Namespace,
        collection: &str,
        entities: Vec<Entity>,
    ) -> Result<usize, IndexError> {
        if !crate::entity::valid_name(collection) {
            return Err(IndexError::Invalid(EntityError::Name));
        }
        let mut latest: HashMap<String, Entity> = HashMap::new();
        for entity in entities {
            if entity.collection != collection {
                return Err(IndexError::Invalid(EntityError::Name));
            }
            self.admit(&entity)?;
            match latest.get(&entity.id) {
                Some(stored) if stored.ts_ms > entity.ts_ms => {}
                _ => {
                    latest.insert(entity.id.clone(), entity);
                }
            }
        }
        let state = self.namespace(ns)?;
        // Reserve before publishing a new collection, so a refused load
        // leaves no empty partition holding a collection slot.
        self.reserve(&state, latest.len())?;
        let release = || state.entities.fetch_sub(latest.len(), Ordering::AcqRel);
        let partition = match self.partition(&state, collection) {
            Ok(partition) => partition,
            Err(error) => {
                release();
                return Err(error);
            }
        };
        let mut partition = match partition.write() {
            Ok(partition) => partition,
            Err(_) => {
                release();
                return Err(IndexError::Poisoned);
            }
        };
        if !partition.entities.is_empty() {
            release();
            return Err(IndexError::PartitionNotEmpty);
        }
        let nodes = latest
            .values()
            .flat_map(|entity| {
                entity
                    .geometry
                    .boxes()
                    .into_iter()
                    .map(|bbox| node(&bbox, &entity.id))
            })
            .collect();
        partition.tree = build_tree(nodes);
        let loaded = latest.len();
        partition.entities = latest;
        Ok(loaded)
    }

    pub fn get(
        &self,
        ns: &Namespace,
        collection: &str,
        id: &str,
        now_ms: i64,
    ) -> Result<Option<Entity>, IndexError> {
        let Some(partition) = self.existing_partition(ns, collection)? else {
            return Ok(None);
        };
        let partition = partition.read().map_err(|_| IndexError::Poisoned)?;
        Ok(partition
            .entities
            .get(id)
            .filter(|entity| !entity.expired(now_ms))
            .cloned())
    }

    /// Live entities whose bounding boxes meet the window from `west` to
    /// `east` (wrapping across the antimeridian when `west > east`).
    pub fn query_bbox(
        &self,
        ns: &Namespace,
        collection: &str,
        [west, south, east, north]: [f64; 4],
        now_ms: i64,
    ) -> Result<Vec<Entity>, IndexError> {
        let lon = |x: f64| x.is_finite() && (-180.0..=180.0).contains(&x);
        let lat = |y: f64| y.is_finite() && (-90.0..=90.0).contains(&y);
        if !(lon(west) && lon(east) && lat(south) && lat(north) && south <= north) {
            return Err(IndexError::InvalidWindow);
        }
        let Some(partition) = self.existing_partition(ns, collection)? else {
            return Ok(Vec::new());
        };
        let partition = partition.read().map_err(|_| IndexError::Poisoned)?;
        let mut seen = HashSet::new();
        let mut found = Vec::new();
        for window in query_boxes(west, south, east, north) {
            let envelope = AABB::from_corners(
                [window.min_lon, window.min_lat],
                [window.max_lon, window.max_lat],
            );
            for hit in partition.tree.locate_in_envelope_intersecting(envelope) {
                if seen.insert(hit.data.as_str())
                    && let Some(entity) = partition.entities.get(&hit.data)
                    && !entity.expired(now_ms)
                {
                    found.push(entity.clone());
                }
            }
        }
        found.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(found)
    }

    /// Run a spatial query. Results are live entities matching every filter,
    /// in id order with a cursor for the next page, or for `Nearest` the `k`
    /// closest by distance then id.
    pub fn query(
        &self,
        ns: &Namespace,
        request: &QueryRequest,
        now_ms: i64,
    ) -> Result<QueryPage, IndexError> {
        if let Some(reason) = invalid(&request.query) {
            return Err(IndexError::InvalidQuery(reason));
        }
        if request.limit == 0 || request.limit > MAX_LIMIT {
            return Err(IndexError::InvalidQuery("limit must be 1 to 10000"));
        }
        let empty = QueryPage {
            entities: Vec::new(),
            next_cursor: None,
        };
        let Some(partition) = self.existing_partition(ns, &request.collection)? else {
            return Ok(empty);
        };
        let partition = partition.read().map_err(|_| IndexError::Poisoned)?;
        let keep = |entity: &&Entity| {
            !entity.expired(now_ms) && request.filters.iter().all(|f| f.matches(entity))
        };
        if let Query::Nearest { center, k } = &request.query {
            let take = request.limit.min(*k);
            let world = std::f64::consts::PI * EARTH_RADIUS_M;
            // Grow the radius until it holds `take` entities: any entity
            // closer than the radius lies in its windows, so those are the
            // nearest.
            let mut meters: f64 = 1_000.0;
            loop {
                let mut found: Vec<(f64, &Entity)> =
                    candidates(&partition, &radius_windows(*center, meters))
                        .into_iter()
                        .filter(keep)
                        .map(|entity| (distance(&entity.geometry, *center), entity))
                        .filter(|(d, _)| *d <= meters)
                        .collect();
                if found.len() >= take || meters >= world {
                    found.sort_by(|a, b| a.0.total_cmp(&b.0).then_with(|| a.1.id.cmp(&b.1.id)));
                    return Ok(QueryPage {
                        entities: found
                            .into_iter()
                            .take(take)
                            .map(|(_, entity)| entity.clone())
                            .collect(),
                        next_cursor: None,
                    });
                }
                meters = (meters * 4.0).min(world);
            }
        }
        let mut found: Vec<&Entity> = candidates(&partition, &windows(&request.query))
            .into_iter()
            .filter(keep)
            .filter(|entity| request.cursor.as_ref().is_none_or(|c| entity.id > *c))
            .filter(|entity| matches(&request.query, &entity.geometry))
            .collect();
        found.sort_by(|a, b| a.id.cmp(&b.id));
        let more = found.len() > request.limit;
        found.truncate(request.limit);
        Ok(QueryPage {
            next_cursor: more
                .then(|| found.last().map(|entity| entity.id.clone()))
                .flatten(),
            entities: found.into_iter().cloned().collect(),
        })
    }

    /// Remove every expired entity; returns how many were removed.
    pub fn sweep_expired(&self, now_ms: i64) -> Result<usize, IndexError> {
        Ok(self.take_expired(now_ms)?.len())
    }

    /// Remove every expired entity and name each one removed, so a durable
    /// caller can log the removals.
    pub(crate) fn take_expired(
        &self,
        now_ms: i64,
    ) -> Result<Vec<(Namespace, String, String)>, IndexError> {
        let namespaces: Vec<(Namespace, Arc<NamespaceState>)> = self
            .namespaces
            .read()
            .map_err(|_| IndexError::Poisoned)?
            .iter()
            .map(|(ns, state)| (ns.clone(), state.clone()))
            .collect();
        let mut removed = Vec::new();
        for (ns, state) in namespaces {
            let partitions: Vec<(String, Arc<RwLock<Partition>>)> = state
                .collections
                .read()
                .map_err(|_| IndexError::Poisoned)?
                .iter()
                .map(|(name, partition)| (name.clone(), partition.clone()))
                .collect();
            for (collection, partition) in partitions {
                let mut partition = partition.write().map_err(|_| IndexError::Poisoned)?;
                let expired: Vec<String> = partition
                    .entities
                    .values()
                    .filter(|entity| entity.expired(now_ms))
                    .map(|entity| entity.id.clone())
                    .collect();
                for id in &expired {
                    partition.remove(id);
                }
                state.entities.fetch_sub(expired.len(), Ordering::AcqRel);
                removed.extend(
                    expired
                        .into_iter()
                        .map(|id| (ns.clone(), collection.clone(), id)),
                );
            }
        }
        Ok(removed)
    }

    /// Live and expired-but-unswept entities counted against the quota.
    /// Entities across every namespace.
    pub fn total_entities(&self) -> Result<usize, IndexError> {
        Ok(self
            .namespaces
            .read()
            .map_err(|_| IndexError::Poisoned)?
            .values()
            .map(|state| state.entities.load(Ordering::Acquire))
            .sum())
    }

    pub fn entity_count(&self, ns: &Namespace) -> Result<usize, IndexError> {
        Ok(self.namespace(ns)?.entities.load(Ordering::Acquire))
    }

    fn admit(&self, entity: &Entity) -> Result<(), IndexError> {
        entity.validate().map_err(IndexError::Invalid)?;
        let vertices = entity.geometry.vertex_count();
        if vertices > self.quotas.max_vertices {
            return Err(IndexError::VertexQuota {
                limit: self.quotas.max_vertices,
                vertices,
            });
        }
        Ok(())
    }

    fn reserve(&self, state: &NamespaceState, count: usize) -> Result<(), IndexError> {
        let limit = self.quotas.max_entities;
        state
            .entities
            .try_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current.checked_add(count).filter(|total| *total <= limit)
            })
            .map(|_| ())
            .map_err(|_| IndexError::EntityQuota { limit })
    }

    fn charge_rate(&self, state: &NamespaceState, now_ms: i64) -> Result<(), IndexError> {
        let limit = self.quotas.max_updates_per_second;
        let mut window = state.rate.lock().map_err(|_| IndexError::Poisoned)?;
        // The window only moves forward: a call stamped in an earlier second
        // that arrives late counts against the current window.
        let second = now_ms.div_euclid(1000);
        if second > window.second {
            *window = RateWindow { second, count: 0 };
        }
        if window.count >= limit {
            return Err(IndexError::RateQuota { limit });
        }
        window.count += 1;
        Ok(())
    }

    fn namespace(&self, ns: &Namespace) -> Result<Arc<NamespaceState>, IndexError> {
        if let Some(state) = self
            .namespaces
            .read()
            .map_err(|_| IndexError::Poisoned)?
            .get(ns)
        {
            return Ok(state.clone());
        }
        let mut namespaces = self.namespaces.write().map_err(|_| IndexError::Poisoned)?;
        Ok(namespaces
            .entry(ns.clone())
            .or_insert_with(|| {
                Arc::new(NamespaceState {
                    entities: AtomicUsize::new(0),
                    rate: Mutex::new(RateWindow {
                        second: i64::MIN,
                        count: 0,
                    }),
                    collections: RwLock::new(HashMap::new()),
                })
            })
            .clone())
    }

    /// The partition of `collection`, created within the collection quota.
    fn partition(
        &self,
        state: &NamespaceState,
        collection: &str,
    ) -> Result<Arc<RwLock<Partition>>, IndexError> {
        if let Some(partition) = existing(state, collection)? {
            return Ok(partition);
        }
        let mut collections = state
            .collections
            .write()
            .map_err(|_| IndexError::Poisoned)?;
        if let Some(partition) = collections.get(collection) {
            return Ok(partition.clone());
        }
        let limit = self.quotas.max_collections;
        if collections.len() >= limit {
            return Err(IndexError::CollectionQuota { limit });
        }
        Ok(collections
            .entry(collection.to_string())
            .or_default()
            .clone())
    }

    fn existing_partition(
        &self,
        ns: &Namespace,
        collection: &str,
    ) -> Result<Option<Arc<RwLock<Partition>>>, IndexError> {
        let namespaces = self.namespaces.read().map_err(|_| IndexError::Poisoned)?;
        let Some(state) = namespaces.get(ns) else {
            return Ok(None);
        };
        Ok(state
            .collections
            .read()
            .map_err(|_| IndexError::Poisoned)?
            .get(collection)
            .cloned())
    }
}

/// The partition of `collection` if it already exists.
fn existing(
    state: &NamespaceState,
    collection: &str,
) -> Result<Option<Arc<RwLock<Partition>>>, IndexError> {
    Ok(state
        .collections
        .read()
        .map_err(|_| IndexError::Poisoned)?
        .get(collection)
        .cloned())
}

/// A tree holding `nodes`, built by insertion. The R-tree crate's own bulk
/// loader can produce trees that a later insert panics on (seen with a few
/// dozen to a few hundred nodes), so it is not used.
pub(crate) fn build_tree(nodes: Vec<Node>) -> RTree<Node> {
    let mut tree = RTree::new();
    for node in nodes {
        tree.insert(node);
    }
    tree
}

/// Entities whose boxes meet any window, each once.
fn candidates<'a>(partition: &'a Partition, windows: &[BoundingBox]) -> Vec<&'a Entity> {
    let mut seen = HashSet::new();
    let mut found = Vec::new();
    for window in windows {
        let envelope = AABB::from_corners(
            [window.min_lon, window.min_lat],
            [window.max_lon, window.max_lat],
        );
        for hit in partition.tree.locate_in_envelope_intersecting(envelope) {
            if seen.insert(hit.data.as_str())
                && let Some(entity) = partition.entities.get(&hit.data)
            {
                found.push(entity);
            }
        }
    }
    found
}
