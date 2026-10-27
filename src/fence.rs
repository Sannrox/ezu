//! Static geofences and the events entity writes raise against them.
//!
//! A fence is a circle or polygon with a selector: the collections it
//! watches (all when empty) and field filters an entity must pass. Each
//! logged upsert is tested against the fences whose bounds it touches, found
//! through an R-tree over fence bounds, and raises:
//!
//! - `Enter` when the entity comes to intersect the fence;
//! - `Exit` when it no longer does, or is deleted or expires;
//! - `Dwell` once per stay, on the first update at least `dwell_ms` after
//!   entering;
//! - `Inside` on updates while inside, only for fences that ask for it and
//!   at most `inside_per_second` per fence. The window is the newest
//!   event-time second the fence has seen; an older observation counts
//!   against it rather than reopening its own second.
//!
//! Membership (which entity is inside which fence, since when) is state:
//! derived from logged writes in log order, replayed on recovery and kept in
//! snapshots, so a restart raises no spurious `Enter` and dwell timers carry
//! on. An event is identified by the LSN of the write that raised it and its
//! index among that write's events; ids only grow.
//!
//! The newest events of each namespace are kept in a bounded ring
//! ([`Recent`]) that snapshots hold with the fence state, so a reader can
//! resume from its last id across restarts. A reader whose id is older than
//! the newest dropped event has missed events and is told so.

use crate::codec::{
    CodecError, Reader, Writer, decode_fields, decode_labels, encode_fields, encode_labels,
};
use crate::entity::{
    Entity, EntityError, FieldValue, Labels, MAX_FIELDS, Namespace, valid_labels, valid_name,
    visible,
};
use crate::geometry::{BoundingBox, Geometry, Position};
use crate::index::{IndexError, Node, Quotas, build_tree, node};
use crate::query::{EARTH_RADIUS_M, Filter, Query, Relation, invalid, matches, windows};
use rstar::{AABB, RTree};
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};

#[derive(Debug, Clone, PartialEq)]
pub enum Shape {
    Circle {
        center: Position,
        meters: f64,
    },
    /// Exterior ring first, then holes; rings are closed.
    Polygon(Vec<Vec<Position>>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Fence {
    pub id: String,
    pub shape: Shape,
    /// Collections watched; empty watches every collection.
    pub collections: Vec<String>,
    /// Every filter must pass for an entity to count as inside.
    pub filters: Vec<Filter>,
    pub dwell_ms: Option<i64>,
    /// `Inside` events per second for this fence; `None` raises none.
    pub inside_per_second: Option<u32>,
    /// Opaque marking labels, carried into the fence's events.
    pub labels: Labels,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventKind {
    Enter,
    Exit,
    Inside,
    Dwell,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FenceEvent {
    /// The LSN of the write that raised the event.
    pub lsn: u64,
    /// Its place among that write's events.
    pub index: u32,
    pub fence: String,
    pub collection: String,
    pub entity: String,
    pub kind: EventKind,
    /// The entity's observation time.
    pub ts_ms: i64,
    pub position: Position,
    /// Both label sets, so a reader can filter or drop the event.
    pub entity_labels: Labels,
    pub fence_labels: Labels,
}

impl FenceEvent {
    /// Whether a reader allowed `readable` may see the event: it carries the
    /// labels of both the entity and the fence.
    fn visible(&self, readable: Option<&Labels>) -> bool {
        visible(&self.entity_labels, readable) && visible(&self.fence_labels, readable)
    }
}

impl Fence {
    /// Bounding boxes that hold the shape, split at the antimeridian.
    pub fn boxes(&self) -> Vec<BoundingBox> {
        windows(&self.query())
    }

    fn query(&self) -> Query {
        match &self.shape {
            Shape::Circle { center, meters } => Query::Radius {
                center: *center,
                meters: *meters,
            },
            Shape::Polygon(rings) => Query::Polygon {
                rings: rings.clone(),
                relation: Relation::Intersects,
            },
        }
    }

    /// Refuse a fence that is malformed or over the namespace's quotas.
    pub fn validate(&self, quotas: &Quotas) -> Result<(), IndexError> {
        if !valid_name(&self.id) || !self.collections.iter().all(|c| valid_name(c)) {
            return Err(IndexError::Invalid(EntityError::Name));
        }
        if !valid_labels(&self.labels) {
            return Err(IndexError::Invalid(EntityError::Labels));
        }
        match &self.shape {
            Shape::Circle { center, meters } => {
                Geometry::Point(*center)
                    .validate()
                    .map_err(|error| IndexError::Invalid(EntityError::Geometry(error)))?;
                if !meters.is_finite()
                    || *meters <= 0.0
                    || *meters > std::f64::consts::PI * EARTH_RADIUS_M
                {
                    return Err(IndexError::InvalidQuery("a fence radius is out of range"));
                }
            }
            Shape::Polygon(rings) => {
                let geometry = Geometry::Polygon(rings.clone());
                geometry
                    .validate()
                    .map_err(|error| IndexError::Invalid(EntityError::Geometry(error)))?;
                if geometry.vertex_count() > quotas.max_vertices {
                    return Err(IndexError::VertexQuota {
                        limit: quotas.max_vertices,
                        vertices: geometry.vertex_count(),
                    });
                }
            }
        }
        if let Some(reason) = invalid(&self.query()) {
            return Err(IndexError::InvalidQuery(reason));
        }
        if self.filters.len() > MAX_FIELDS {
            return Err(IndexError::InvalidQuery("a fence has too many filters"));
        }
        if self.dwell_ms.is_some_and(|dwell| dwell < 0) || self.inside_per_second == Some(0) {
            return Err(IndexError::InvalidQuery(
                "dwell must not be negative and inside rates must be positive",
            ));
        }
        Ok(())
    }

    fn selects(&self, entity: &Entity) -> bool {
        (self.collections.is_empty() || self.collections.contains(&entity.collection))
            && self.filters.iter().all(|filter| filter.matches(entity))
    }
}

/// One entity's place: where it was last seen and the fences it is inside.
#[derive(Debug, Clone, PartialEq)]
struct Tracked {
    ts_ms: i64,
    position: Position,
    /// The entity's labels as last seen, for the exits of its delete.
    labels: Labels,
    /// Fence id to entry time and whether its dwell was raised.
    inside: BTreeMap<String, (i64, bool)>,
}

struct Placed {
    fence: Fence,
    query: Query,
    boxes: Vec<BoundingBox>,
}

/// One namespace's fences and memberships.
#[derive(Default)]
pub(crate) struct Space {
    fences: BTreeMap<String, Placed>,
    tree: RTree<Node>,
    members: HashMap<(String, String), Tracked>,
    /// Per fence: the newest event-time second seen and the `Inside`
    /// events raised in it.
    rates: BTreeMap<String, (i64, u32)>,
}

/// A representative position: the point, or a shape's first vertex.
fn position(geometry: &Geometry) -> Position {
    match geometry {
        Geometry::Point(p) => *p,
        Geometry::Line(line) => line.first().copied().unwrap_or(Position::new(0.0, 0.0)),
        Geometry::Polygon(rings) => rings
            .first()
            .and_then(|ring| ring.first())
            .copied()
            .unwrap_or(Position::new(0.0, 0.0)),
    }
}

impl Space {
    pub(crate) fn is_empty(&self) -> bool {
        self.fences.is_empty() && self.members.is_empty()
    }

    pub(crate) fn len(&self) -> usize {
        self.fences.len()
    }

    pub(crate) fn get(&self, id: &str) -> Option<&Fence> {
        self.fences.get(id).map(|placed| &placed.fence)
    }

    pub(crate) fn list(&self) -> impl Iterator<Item = &Fence> {
        self.fences.values().map(|placed| &placed.fence)
    }

    /// Add or replace a fence. Memberships stay; the next update of each
    /// member decides whether it is still inside.
    pub(crate) fn put(&mut self, fence: Fence) {
        self.unplace(&fence.id);
        let query = fence.query();
        let boxes = windows(&query);
        for bbox in &boxes {
            self.tree.insert(node(bbox, &fence.id));
        }
        self.fences.insert(
            fence.id.clone(),
            Placed {
                fence,
                query,
                boxes,
            },
        );
    }

    fn unplace(&mut self, id: &str) -> bool {
        let Some(placed) = self.fences.remove(id) else {
            return false;
        };
        for bbox in &placed.boxes {
            self.tree.remove(&node(bbox, id));
        }
        true
    }

    /// Remove a fence and its memberships; it raises no events.
    pub(crate) fn remove(&mut self, id: &str) -> bool {
        if !self.unplace(id) {
            return false;
        }
        self.rates.remove(id);
        self.members.retain(|_, tracked| {
            tracked.inside.remove(id);
            !tracked.inside.is_empty()
        });
        true
    }

    /// Events for a logged upsert of `entity` at `lsn`.
    pub(crate) fn upserted(&mut self, entity: &Entity, lsn: u64) -> Vec<FenceEvent> {
        let mut now_inside = BTreeSet::new();
        for bbox in entity.geometry.boxes() {
            let envelope =
                AABB::from_corners([bbox.min_lon, bbox.min_lat], [bbox.max_lon, bbox.max_lat]);
            for candidate in self.tree.locate_in_envelope_intersecting(envelope) {
                now_inside.insert(candidate.data.clone());
            }
        }
        now_inside.retain(|id| {
            self.fences.get(id).is_some_and(|placed| {
                placed.fence.selects(entity) && matches(&placed.query, &entity.geometry)
            })
        });
        let key = (entity.collection.clone(), entity.id.clone());
        let at = position(&entity.geometry);
        let mut tracked = self.members.remove(&key).unwrap_or(Tracked {
            ts_ms: entity.ts_ms,
            position: at,
            labels: Labels::new(),
            inside: BTreeMap::new(),
        });
        tracked.ts_ms = entity.ts_ms;
        tracked.position = at;
        tracked.labels = entity.labels.clone();
        let mut events = Vec::new();
        let fence_labels = |fences: &BTreeMap<String, Placed>, id: &str| {
            fences
                .get(id)
                .map(|placed| placed.fence.labels.clone())
                .unwrap_or_default()
        };
        let mut raise = |fence: &str, labels: Labels, kind| {
            events.push(FenceEvent {
                lsn,
                index: 0,
                fence: fence.to_string(),
                collection: entity.collection.clone(),
                entity: entity.id.clone(),
                kind,
                ts_ms: entity.ts_ms,
                position: at,
                entity_labels: entity.labels.clone(),
                fence_labels: labels,
            });
        };
        let ids: BTreeSet<String> = tracked
            .inside
            .keys()
            .cloned()
            .chain(now_inside.iter().cloned())
            .collect();
        for id in ids {
            if !now_inside.contains(&id) {
                tracked.inside.remove(&id);
                raise(&id, fence_labels(&self.fences, &id), EventKind::Exit);
                continue;
            }
            let Some(placed) = self.fences.get(&id) else {
                continue;
            };
            let entered = !tracked.inside.contains_key(&id);
            let (since, dwelled) = tracked
                .inside
                .entry(id.clone())
                .or_insert((entity.ts_ms, false));
            let labels = placed.fence.labels.clone();
            if entered {
                raise(&id, labels.clone(), EventKind::Enter);
            } else if let Some(per_second) = placed.fence.inside_per_second {
                let second = entity.ts_ms.div_euclid(1000);
                let window = self.rates.entry(id.clone()).or_insert((second, 0));
                if second > window.0 {
                    *window = (second, 0);
                }
                if window.1 < per_second {
                    window.1 += 1;
                    raise(&id, labels.clone(), EventKind::Inside);
                }
            }
            if let Some(dwell) = placed.fence.dwell_ms
                && !*dwelled
                && entity.ts_ms.saturating_sub(*since) >= dwell
            {
                *dwelled = true;
                raise(&id, labels, EventKind::Dwell);
            }
        }
        if !tracked.inside.is_empty() {
            self.members.insert(key, tracked);
        }
        number(events)
    }

    /// Events for a logged delete at `lsn`: an exit from every fence the
    /// entity was inside, at its last observation.
    pub(crate) fn deleted(&mut self, collection: &str, id: &str, lsn: u64) -> Vec<FenceEvent> {
        let Some(tracked) = self
            .members
            .remove(&(collection.to_string(), id.to_string()))
        else {
            return Vec::new();
        };
        number(
            tracked
                .inside
                .into_keys()
                .map(|fence| FenceEvent {
                    lsn,
                    index: 0,
                    fence_labels: self
                        .fences
                        .get(&fence)
                        .map(|placed| placed.fence.labels.clone())
                        .unwrap_or_default(),
                    fence,
                    collection: collection.to_string(),
                    entity: id.to_string(),
                    kind: EventKind::Exit,
                    ts_ms: tracked.ts_ms,
                    position: tracked.position,
                    entity_labels: tracked.labels.clone(),
                })
                .collect(),
        )
    }

    /// The fences and memberships, for a snapshot.
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut out = Writer::default();
        out.u32(self.fences.len() as u32);
        for placed in self.fences.values() {
            encode_fence(&mut out, &placed.fence);
        }
        let mut members: Vec<_> = self.members.iter().collect();
        members.sort_by(|a, b| a.0.cmp(b.0));
        out.u32(members.len() as u32);
        for ((collection, id), tracked) in members {
            out.str(collection);
            out.str(id);
            out.i64(tracked.ts_ms);
            out.f64(tracked.position.lon);
            out.f64(tracked.position.lat);
            out.u32(tracked.inside.len() as u32);
            for (fence, (since, dwelled)) in &tracked.inside {
                out.str(fence);
                out.i64(*since);
                out.u8(u8::from(*dwelled));
            }
        }
        out.u32(self.rates.len() as u32);
        for (fence, (second, raised)) in &self.rates {
            out.str(fence);
            out.i64(*second);
            out.u32(*raised);
        }
        out.0
    }

    fn decode_from(input: &mut Reader<'_>) -> Result<Self, CodecError> {
        let mut space = Self::default();
        let mut nodes = Vec::new();
        for _ in 0..input.u32()? {
            let fence = decode_fence(input)?;
            let query = fence.query();
            let boxes = windows(&query);
            nodes.extend(boxes.iter().map(|bbox| node(bbox, &fence.id)));
            space.fences.insert(
                fence.id.clone(),
                Placed {
                    fence,
                    query,
                    boxes,
                },
            );
        }
        space.tree = build_tree(nodes);
        for _ in 0..input.u32()? {
            let key = (input.str()?, input.str()?);
            let ts_ms = input.i64()?;
            let position = Position::new(input.f64()?, input.f64()?);
            let mut inside = BTreeMap::new();
            for _ in 0..input.u32()? {
                let fence = input.str()?;
                let since = input.i64()?;
                inside.insert(fence, (since, input.u8()? != 0));
            }
            space.members.insert(
                key,
                Tracked {
                    ts_ms,
                    position,
                    labels: Labels::new(),
                    inside,
                },
            );
        }
        for _ in 0..input.u32()? {
            let fence = input.str()?;
            let window = (input.i64()?, input.u32()?);
            space.rates.insert(fence, window);
        }
        Ok(space)
    }
}

/// An event id: the raising write's LSN and the event's index in it.
pub type EventId = (u64, u32);

/// Events read, and the id read through: a reader resumes after `through`,
/// past events its fence filter skipped. `(0, 0)` lies before every event.
#[derive(Debug, Clone, PartialEq)]
pub struct EventPage {
    pub events: Vec<FenceEvent>,
    pub through: EventId,
}

/// A reader asked to resume from before events that are no longer kept.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Expired {
    /// The newest event dropped from the ring.
    pub dropped: EventId,
}

/// One namespace's newest events, oldest first, and the id of the newest
/// one dropped for space.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct Recent {
    kept: VecDeque<FenceEvent>,
    dropped: Option<EventId>,
}

impl Recent {
    pub(crate) fn is_empty(&self) -> bool {
        self.kept.is_empty() && self.dropped.is_none()
    }

    /// Keep `event`, dropping the oldest past `retained`; see [`Self::trim`].
    pub(crate) fn push(&mut self, event: FenceEvent, retained: usize, horizon: u64) {
        self.kept.push_back(event);
        self.trim(retained, horizon);
    }

    /// Drop the oldest events past `retained`, but none raised past the LSN
    /// `horizon`: an event no reader could be served yet is kept until it
    /// could, so the ring may hold more than `retained` meanwhile.
    pub(crate) fn trim(&mut self, retained: usize, horizon: u64) {
        while self.kept.len() > retained && self.kept.front().is_some_and(|old| old.lsn <= horizon)
        {
            if let Some(old) = self.kept.pop_front() {
                self.dropped = Some((old.lsn, old.index));
            }
        }
    }

    /// Events after `after` (from the oldest kept when `None`) of the given
    /// fences (all when empty) that `readable` may see, at most `limit`, none
    /// raised past the LSN `horizon`. Refused when events after `after` were
    /// already dropped.
    pub(crate) fn after(
        &self,
        after: Option<EventId>,
        fences: &[String],
        readable: Option<&Labels>,
        limit: usize,
        horizon: u64,
    ) -> Result<EventPage, Expired> {
        // From the oldest kept: just after the newest dropped.
        let start = match after {
            Some(after) => match self.dropped {
                Some(dropped) if after < dropped => return Err(Expired { dropped }),
                _ => after,
            },
            None => self.dropped.unwrap_or((0, 0)),
        };
        let mut page = EventPage {
            events: Vec::new(),
            through: start,
        };
        for event in &self.kept {
            let id = (event.lsn, event.index);
            if id <= start {
                continue;
            }
            if event.lsn > horizon || page.events.len() == limit {
                break;
            }
            page.through = id;
            if (fences.is_empty() || fences.contains(&event.fence)) && event.visible(readable) {
                page.events.push(event.clone());
            }
        }
        Ok(page)
    }

    fn encode(&self, out: &mut Writer) {
        match self.dropped {
            Some((lsn, index)) => {
                out.u8(1);
                out.u64(lsn);
                out.u32(index);
            }
            None => out.u8(0),
        }
        out.u32(self.kept.len() as u32);
        for event in &self.kept {
            out.u64(event.lsn);
            out.u32(event.index);
            out.str(&event.fence);
            out.str(&event.collection);
            out.str(&event.entity);
            out.u8(match event.kind {
                EventKind::Enter => 0,
                EventKind::Exit => 1,
                EventKind::Inside => 2,
                EventKind::Dwell => 3,
            });
            out.i64(event.ts_ms);
            encode_position(out, event.position);
        }
    }

    fn decode_from(input: &mut Reader<'_>) -> Result<Self, CodecError> {
        let dropped = match input.u8()? {
            0 => None,
            1 => Some((input.u64()?, input.u32()?)),
            _ => return Err(CodecError("option tag")),
        };
        let mut kept = VecDeque::new();
        for _ in 0..input.u32()? {
            kept.push_back(FenceEvent {
                lsn: input.u64()?,
                index: input.u32()?,
                fence: input.str()?,
                collection: input.str()?,
                entity: input.str()?,
                kind: match input.u8()? {
                    0 => EventKind::Enter,
                    1 => EventKind::Exit,
                    2 => EventKind::Inside,
                    3 => EventKind::Dwell,
                    _ => return Err(CodecError("event kind")),
                },
                ts_ms: input.i64()?,
                position: decode_position(input)?,
                entity_labels: Labels::new(),
                fence_labels: Labels::new(),
            });
        }
        Ok(Self { kept, dropped })
    }
}

/// A namespace's fence state as a snapshot stores it: fences, memberships
/// and rate windows, then its recent events, then, only when anything
/// carries labels, the labels of fences, members and events. A binary from
/// before labels then refuses the file instead of dropping them.
pub(crate) fn encode_state(space: &Space, recent: &Recent) -> Vec<u8> {
    let mut out = Writer(space.encode());
    recent.encode(&mut out);
    let fences: Vec<_> = space
        .fences
        .values()
        .filter(|placed| !placed.fence.labels.is_empty())
        .collect();
    let mut members: Vec<_> = space
        .members
        .iter()
        .filter(|(_, tracked)| !tracked.labels.is_empty())
        .collect();
    members.sort_by(|a, b| a.0.cmp(b.0));
    let events: Vec<_> = recent
        .kept
        .iter()
        .enumerate()
        .filter(|(_, event)| !event.entity_labels.is_empty() || !event.fence_labels.is_empty())
        .collect();
    if fences.is_empty() && members.is_empty() && events.is_empty() {
        return out.0;
    }
    out.u32(fences.len() as u32);
    for placed in fences {
        out.str(&placed.fence.id);
        encode_labels(&mut out, &placed.fence.labels);
    }
    out.u32(members.len() as u32);
    for ((collection, id), tracked) in members {
        out.str(collection);
        out.str(id);
        encode_labels(&mut out, &tracked.labels);
    }
    out.u32(events.len() as u32);
    for (at, event) in events {
        out.u32(at as u32);
        encode_labels(&mut out, &event.entity_labels);
        encode_labels(&mut out, &event.fence_labels);
    }
    out.0
}

/// Read [`encode_state`], or with `with_events` false a file from before
/// events were kept, which holds the fence state alone.
pub(crate) fn decode_state(bytes: &[u8], with_events: bool) -> Result<(Space, Recent), CodecError> {
    let mut input = Reader(bytes);
    let space = Space::decode_from(&mut input)?;
    let mut space = space;
    let mut recent = match with_events {
        true => Recent::decode_from(&mut input)?,
        false => Recent::default(),
    };
    if with_events && !input.0.is_empty() {
        for _ in 0..input.u32()? {
            let id = input.str()?;
            let labels = decode_labels(&mut input)?;
            let placed = space
                .fences
                .get_mut(&id)
                .ok_or(CodecError("labels of no fence"))?;
            placed.fence.labels = labels;
        }
        for _ in 0..input.u32()? {
            let key = (input.str()?, input.str()?);
            let labels = decode_labels(&mut input)?;
            let tracked = space
                .members
                .get_mut(&key)
                .ok_or(CodecError("labels of no member"))?;
            tracked.labels = labels;
        }
        for _ in 0..input.u32()? {
            let at = input.u32()? as usize;
            let (entity, fence) = (decode_labels(&mut input)?, decode_labels(&mut input)?);
            let event = recent
                .kept
                .get_mut(at)
                .ok_or(CodecError("labels of no event"))?;
            event.entity_labels = entity;
            event.fence_labels = fence;
        }
    }
    input.done()?;
    Ok((space, recent))
}

/// Number a write's events in order.
fn number(mut events: Vec<FenceEvent>) -> Vec<FenceEvent> {
    for (index, event) in events.iter_mut().enumerate() {
        event.index = index as u32;
    }
    events
}

/// Every namespace's fences.
#[derive(Default)]
pub(crate) struct Fences(HashMap<Namespace, Space>);

impl Fences {
    pub(crate) fn space(&self, ns: &Namespace) -> Option<&Space> {
        self.0.get(ns)
    }

    pub(crate) fn space_mut(&mut self, ns: &Namespace) -> &mut Space {
        self.0.entry(ns.clone()).or_default()
    }

    pub(crate) fn insert(&mut self, ns: Namespace, space: Space) {
        self.0.insert(ns, space);
    }

    pub(crate) fn take(&mut self, ns: &Namespace) -> Option<Space> {
        self.0.remove(ns)
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (&Namespace, &Space)> {
        self.0.iter()
    }

    /// Events of an entity upsert, if the namespace has fences or members.
    pub(crate) fn upserted(
        &mut self,
        ns: &Namespace,
        entity: &Entity,
        lsn: u64,
    ) -> Vec<FenceEvent> {
        match self.0.get_mut(ns) {
            Some(space) => space.upserted(entity, lsn),
            None => Vec::new(),
        }
    }

    pub(crate) fn deleted(
        &mut self,
        ns: &Namespace,
        collection: &str,
        id: &str,
        lsn: u64,
    ) -> Vec<FenceEvent> {
        match self.0.get_mut(ns) {
            Some(space) => space.deleted(collection, id, lsn),
            None => Vec::new(),
        }
    }
}

fn encode_position(out: &mut Writer, p: Position) {
    out.f64(p.lon);
    out.f64(p.lat);
}

fn decode_position(input: &mut Reader<'_>) -> Result<Position, CodecError> {
    Ok(Position::new(input.f64()?, input.f64()?))
}

fn encode_option_i64(out: &mut Writer, value: Option<i64>) {
    match value {
        Some(v) => {
            out.u8(1);
            out.i64(v);
        }
        None => out.u8(0),
    }
}

fn decode_option_i64(input: &mut Reader<'_>) -> Result<Option<i64>, CodecError> {
    match input.u8()? {
        0 => Ok(None),
        1 => Ok(Some(input.i64()?)),
        _ => Err(CodecError("option tag")),
    }
}

/// A fence as the log and snapshots store it.
pub(crate) fn encode_fence(out: &mut Writer, fence: &Fence) {
    out.str(&fence.id);
    match &fence.shape {
        Shape::Circle { center, meters } => {
            out.u8(0);
            encode_position(out, *center);
            out.f64(*meters);
        }
        Shape::Polygon(rings) => {
            out.u8(1);
            out.u32(rings.len() as u32);
            for ring in rings {
                out.u32(ring.len() as u32);
                for p in ring {
                    encode_position(out, *p);
                }
            }
        }
    }
    out.u32(fence.collections.len() as u32);
    for collection in &fence.collections {
        out.str(collection);
    }
    out.u32(fence.filters.len() as u32);
    for filter in &fence.filters {
        match filter {
            Filter::Eq(field, value) => {
                out.u8(0);
                out.bytes(&encode_fields(&BTreeMap::from([(
                    field.clone(),
                    value.clone(),
                )])));
            }
            Filter::Range { field, min, max } => {
                out.u8(1);
                out.str(field);
                for bound in [min, max] {
                    match bound {
                        Some(v) => {
                            out.u8(1);
                            out.f64(*v);
                        }
                        None => out.u8(0),
                    }
                }
            }
        }
    }
    encode_option_i64(out, fence.dwell_ms);
    encode_option_i64(out, fence.inside_per_second.map(i64::from));
}

pub(crate) fn decode_fence(input: &mut Reader<'_>) -> Result<Fence, CodecError> {
    let id = input.str()?;
    let shape = match input.u8()? {
        0 => Shape::Circle {
            center: decode_position(input)?,
            meters: input.f64()?,
        },
        1 => {
            let mut rings = Vec::new();
            for _ in 0..input.u32()? {
                let mut ring = Vec::new();
                for _ in 0..input.u32()? {
                    ring.push(decode_position(input)?);
                }
                rings.push(ring);
            }
            Shape::Polygon(rings)
        }
        _ => return Err(CodecError("fence shape")),
    };
    let mut collections = Vec::new();
    for _ in 0..input.u32()? {
        collections.push(input.str()?);
    }
    let mut filters = Vec::new();
    for _ in 0..input.u32()? {
        filters.push(match input.u8()? {
            0 => {
                let (field, value): (String, FieldValue) = decode_fields(input.bytes()?)?
                    .into_iter()
                    .next()
                    .ok_or(CodecError("filter"))?;
                Filter::Eq(field, value)
            }
            1 => {
                let field = input.str()?;
                let mut bounds = [None, None];
                for bound in &mut bounds {
                    *bound = match input.u8()? {
                        0 => None,
                        1 => Some(input.f64()?),
                        _ => return Err(CodecError("option tag")),
                    };
                }
                Filter::Range {
                    field,
                    min: bounds[0],
                    max: bounds[1],
                }
            }
            _ => return Err(CodecError("filter")),
        });
    }
    Ok(Fence {
        id,
        shape,
        collections,
        filters,
        dwell_ms: decode_option_i64(input)?,
        inside_per_second: decode_option_i64(input)?
            .map(|v| u32::try_from(v).map_err(|_| CodecError("inside rate")))
            .transpose()?,
        labels: Default::default(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fence(id: &str, lon: f64) -> Fence {
        Fence {
            id: id.into(),
            shape: Shape::Circle {
                center: Position::new(lon, 0.0),
                meters: 1_000.0,
            },
            collections: vec!["cars".into()],
            filters: vec![Filter::Eq("kind".into(), FieldValue::Text("bus".into()))],
            dwell_ms: Some(5_000),
            inside_per_second: Some(2),
            labels: Default::default(),
        }
    }

    fn at(lon: f64, ts_ms: i64) -> Entity {
        Entity {
            collection: "cars".into(),
            id: "c1".into(),
            geometry: Geometry::Point(Position::new(lon, 0.0)),
            ts_ms,
            speed: None,
            heading: None,
            ttl_ms: None,
            fields: BTreeMap::from([("kind".into(), FieldValue::Text("bus".into()))]),
            labels: Default::default(),
        }
    }

    fn kinds(events: &[FenceEvent]) -> Vec<(String, EventKind)> {
        events
            .iter()
            .map(|event| (event.fence.clone(), event.kind))
            .collect()
    }

    #[test]
    fn scripted_moves_raise_exactly_the_expected_events() {
        let mut space = Space::default();
        space.put(fence("a", 0.0));
        space.put(fence("b", 0.005));
        // Outside both.
        assert!(space.upserted(&at(1.0, 0), 1).is_empty());
        // Into a only, then into the overlap of a and b.
        assert_eq!(
            kinds(&space.upserted(&at(-0.005, 1_000), 2)),
            [("a".into(), EventKind::Enter)]
        );
        let both = space.upserted(&at(0.003, 2_000), 3);
        assert_eq!(
            kinds(&both),
            [
                ("a".into(), EventKind::Inside),
                ("b".into(), EventKind::Enter)
            ]
        );
        assert_eq!((both[0].index, both[1].index, both[1].lsn), (0, 1, 3));
        // Inside events are capped per fence per second, and an older
        // second does not reopen its allowance.
        space.upserted(&at(0.003, 2_100), 4);
        assert!(
            space
                .upserted(&at(0.003, 2_200), 5)
                .iter()
                .all(|e| e.fence != "a")
        );
        let mut other = at(0.003, 1_500);
        other.id = "c2".into();
        assert_eq!(
            kinds(&space.upserted(&other, 5)),
            [
                ("a".into(), EventKind::Enter),
                ("b".into(), EventKind::Enter)
            ]
        );
        other.ts_ms = 1_600;
        assert!(space.upserted(&other, 5).is_empty());
        // Dwell once, five seconds after entering a.
        let dwell = space.upserted(&at(0.003, 6_000), 6);
        assert!(kinds(&dwell).contains(&("a".into(), EventKind::Dwell)));
        assert!(
            !kinds(&space.upserted(&at(0.003, 9_000), 7)).contains(&("a".into(), EventKind::Dwell))
        );
        // An entity the selector no longer picks exits.
        let mut car = at(0.003, 10_000);
        car.fields.clear();
        assert_eq!(
            kinds(&space.upserted(&car, 8)),
            [("a".into(), EventKind::Exit), ("b".into(), EventKind::Exit)]
        );
        // Re-entering, then a delete exits at the last observation.
        space.upserted(&at(-0.005, 11_000), 9);
        let gone = space.deleted("cars", "c1", 10);
        assert_eq!(kinds(&gone), [("a".into(), EventKind::Exit)]);
        assert_eq!((gone[0].ts_ms, gone[0].lsn), (11_000, 10));
        // A removed fence takes its members with it, silently.
        space.upserted(&at(-0.005, 12_000), 11);
        assert!(space.remove("a"));
        assert!(space.upserted(&at(5.0, 13_000), 12).is_empty());
        assert_eq!(space.len(), 1);
    }

    #[test]
    fn fences_and_memberships_round_trip_through_a_snapshot() {
        let mut space = Space::default();
        space.put(fence("a", 0.0));
        let mut ring_fence = fence("p", 10.0);
        ring_fence.shape = Shape::Polygon(vec![vec![
            Position::new(9.0, -1.0),
            Position::new(11.0, -1.0),
            Position::new(11.0, 1.0),
            Position::new(9.0, -1.0),
        ]]);
        ring_fence.filters = vec![Filter::Range {
            field: "n".into(),
            min: Some(1.0),
            max: None,
        }];
        ring_fence.inside_per_second = None;
        space.put(ring_fence);
        let mut recent = Recent::default();
        for (lsn, ts) in [(1, 1_000), (2, 1_100), (3, 1_200)] {
            for event in space.upserted(&at(0.0, ts), lsn) {
                recent.push(event, 2, u64::MAX);
            }
        }
        let bytes = encode_state(&space, &recent);
        let (mut restored, kept) = decode_state(&bytes, true).unwrap();
        assert_eq!(encode_state(&restored, &kept), bytes);
        assert_eq!(restored.list().count(), 2);
        // The ring kept the newest two and remembers what it dropped.
        assert_eq!(kept, recent);
        assert_eq!(
            kept.after(None, &[], None, 10, u64::MAX)
                .unwrap()
                .events
                .len(),
            2
        );
        assert_eq!(
            kept.after(Some((1, 0)), &[], None, 10, u64::MAX)
                .unwrap()
                .events
                .len(),
            2
        );
        assert_eq!(
            kept.after(Some((0, 0)), &[], None, 10, u64::MAX),
            Err(Expired { dropped: (1, 0) })
        );
        // A filter that matches nothing still moves the reader along.
        let skipped = kept.after(None, &["p".into()], None, 10, u64::MAX).unwrap();
        assert_eq!((skipped.events.len(), skipped.through), (0, (3, 0)));
        // Nothing past the horizon is read, nor read through.
        let held = kept.after(None, &[], None, 10, 2).unwrap();
        assert_eq!((held.events.len(), held.through), (1, (2, 0)));
        // A fences file from before events were kept still reads.
        let (old, none) = decode_state(&restored.encode(), false).unwrap();
        assert_eq!((old.list().count(), none), (2, Recent::default()));
        // The inside allowance of that second stays spent.
        assert!(restored.upserted(&at(0.0, 1_300), 4).is_empty());
        // Still inside after the round trip: no second enter, and the dwell
        // timer kept its start.
        let events = restored.upserted(&at(0.0, 6_000), 5);
        assert_eq!(
            kinds(&events),
            [
                ("a".into(), EventKind::Inside),
                ("a".into(), EventKind::Dwell)
            ]
        );
        for cut in 0..bytes.len() {
            assert!(decode_state(&bytes[..cut], true).is_err(), "cut {cut}");
        }
    }
}
