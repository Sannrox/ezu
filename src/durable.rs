//! Durability: the live index behind a write-ahead log and snapshots.
//!
//! A write is applied to the index, appended to the WAL and synced before it
//! is acknowledged; a batch shares one sync (group commit). Every N records
//! or T milliseconds a snapshot of all namespaces is written and the WAL it
//! covers is dropped, so the log stays bounded. Recovery loads the newest
//! snapshot, bulk-loads the index and replays the WAL after it. When the
//! snapshot is lost or damaged, a [`RebuildSource`] re-syncs the entities.
//!
//! One process owns a directory at a time: `LOCK` is held for the engine's
//! life, so a second instance fails to open instead of writing alongside.

use crate::entity::{Entity, Labels, Namespace, visible};
use crate::fence::{
    EventId, EventPage, Expired, Fence, FenceEvent, Fences, Recent, Space, decode_state,
    encode_state,
};
use crate::index::{Engine, IndexError, Quotas, Upsert};
use crate::keys::sekai_keys::{Dek, KeyId};
use crate::keys::{self, KeyConfig, Keyring};
use crate::metrics::{Gauges, Metrics};
use crate::snapshot::{self, DataFile, Found, Part, SealInfo, SnapshotError};
use crate::wal::{Entry, Op, Wal, WalError};
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::path::PathBuf;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Debug, Clone)]
pub struct DurabilityConfig {
    pub dir: PathBuf,
    /// Sync every batch before acknowledging it. Off trades durability of
    /// the last moments before a crash for throughput.
    pub fsync: bool,
    /// A segment rolls over after this many bytes.
    pub segment_bytes: u64,
    pub snapshot_every_records: u64,
    pub snapshot_every_ms: i64,
    /// Fence events kept per namespace, in memory and in snapshots, for
    /// readers to resume from; older ones are dropped.
    pub events_retained: usize,
    /// Namespaces encrypted with keys from an external provider; `None`
    /// keeps every namespace in plaintext.
    pub keys: Option<KeyConfig>,
}

/// Where entities and fences come from again when no usable snapshot
/// exists. The rebuilt state replaces everything, so a source must return
/// the fences too (none when it keeps none).
pub trait RebuildSource {
    fn entities(&self) -> Result<Vec<(Namespace, Entity)>, String>;
    fn fences(&self) -> Result<Vec<(Namespace, Fence)>, String>;
}

#[derive(Debug)]
pub enum DurableError {
    Index(IndexError),
    Wal(WalError),
    Snapshot(SnapshotError),
    /// Another process holds the directory.
    Locked,
    /// No usable snapshot and no rebuild source, while the WAL alone no
    /// longer starts at the beginning.
    NeedsRebuild(String),
    /// A write would not fit in one WAL record.
    TooLarge,
    /// The namespace's key is revoked or missing; it refuses reads and
    /// writes until the key returns. Other namespaces are unaffected.
    Sealed(String),
    /// The key store could not be read or written.
    Keys(String),
    /// A reader resumed from before events that are no longer kept.
    Expired(Expired),
    /// A map package could not be written.
    Package(String),
    /// A write could not be made durable after it reached the index; the
    /// engine refuses further writes until it is reopened.
    Failed,
}

impl std::fmt::Display for DurableError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Index(error) => write!(f, "{error}"),
            Self::Wal(error) => write!(f, "{error}"),
            Self::Snapshot(error) => write!(f, "{error}"),
            Self::Locked => f.write_str("the data directory is held by another process"),
            Self::TooLarge => f.write_str("the write is larger than one log record"),
            Self::Sealed(why) => write!(f, "namespace unavailable: {why}"),
            Self::Keys(why) => write!(f, "key store: {why}"),
            Self::Package(why) => write!(f, "package: {why}"),
            Self::Expired(Expired {
                dropped: (lsn, index),
            }) => write!(
                f,
                "events up to {lsn}/{index} are no longer kept; resume from a later id"
            ),
            Self::NeedsRebuild(why) => write!(f, "recovery needs a rebuild source: {why}"),
            Self::Failed => {
                f.write_str("durability was lost after a failed write; reopen to recover")
            }
        }
    }
}

impl std::error::Error for DurableError {}

impl From<IndexError> for DurableError {
    fn from(error: IndexError) -> Self {
        Self::Index(error)
    }
}
impl From<WalError> for DurableError {
    fn from(error: WalError) -> Self {
        Self::Wal(error)
    }
}
impl From<SnapshotError> for DurableError {
    fn from(error: SnapshotError) -> Self {
        Self::Snapshot(error)
    }
}

struct Writer {
    fences: Fences,
    /// Sealed namespaces' data, as stored, carried into every snapshot
    /// until their keys return.
    carried: HashMap<String, Part>,
    wal: Wal,
    since_snapshot: u64,
    snapshot_at_ms: i64,
    /// The LSN the newest snapshot covers; every record after it is read
    /// again on recovery.
    snapshot_lsn: u64,
}

pub struct DurableEngine {
    engine: Engine,
    writer: Mutex<Writer>,
    events: Mutex<HashMap<Namespace, Recent>>,
    /// Event ids below this are unknown to every namespace; fixed at open.
    events_floor: Option<EventId>,
    /// Counts publishes, so a subscriber waits for new events.
    published: tokio::sync::watch::Sender<u64>,
    quotas: Quotas,
    config: DurabilityConfig,
    keyring: Keyring,
    failed: AtomicBool,
    metrics: Metrics,
    _lock: File,
}

impl DurableEngine {
    /// Open or recover the engine in `config.dir`.
    pub fn open(
        config: DurabilityConfig,
        quotas: Quotas,
        rebuild: Option<&dyn RebuildSource>,
        now_ms: i64,
    ) -> Result<Self, DurableError> {
        let (wal_dir, snapshot_dir) = (config.dir.join("wal"), config.dir.join("snapshots"));
        create_dir_durably(&wal_dir).map_err(WalError::Io)?;
        create_dir_durably(&snapshot_dir).map_err(WalError::Io)?;
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(config.dir.join("LOCK"))
            .map_err(WalError::Io)?;
        lock.try_lock().map_err(|_| DurableError::Locked)?;

        create_dir_durably(&config.dir.join("keys")).map_err(WalError::Io)?;
        let keyring = Keyring::open(&config.dir.join("keys"), config.keys.clone())
            .map_err(DurableError::Keys)?;
        // Every granted namespace has its key before anything is restored or
        // written: sealing a logged change then never has to mint (and
        // possibly fail) after the index changed. One that cannot get a key
        // opens sealed, alone, and its stored data is carried.
        for namespace in keyring.granted() {
            if keyring.check(&namespace).is_ok()
                && let Err(sealed) = keyring.current(&namespace)
            {
                keyring
                    .seal_namespace(&namespace, &sealed.0)
                    .map_err(DurableError::Keys)?;
            }
        }
        let engine = Engine::new(quotas);
        let newest = snapshot::newest(&snapshot_dir)?;
        let highest_snapshot = newest.as_ref().map_or(0, |(lsn, _)| *lsn);
        // When the newest snapshot was taken, so the time trigger carries on
        // across restarts instead of starting over.
        let snapshot_at_ms = newest
            .as_ref()
            .and_then(|(_, Found::V1(path) | Found::V2(path))| {
                fs::metadata(path).ok()?.modified().ok()
            })
            .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
            .map_or(0, |since| {
                i64::try_from(since.as_millis()).unwrap_or(i64::MAX)
            });
        let mut loaded = None;
        let mut fences = Fences::default();
        // Event ids below this are unknown: history the store did not keep.
        let mut events_floor = None;
        let (base_lsn, rebuilt) = match newest.map(|(_, found)| load_snapshot(&keyring, found)) {
            Some(Ok(snapshot)) => {
                let lsn = snapshot.lsn;
                events_floor = snapshot.events_floor;
                loaded = Some(snapshot);
                (lsn, false)
            }
            damaged_or_missing => {
                let why = match damaged_or_missing {
                    Some(Err(error)) => Some(error.to_string()),
                    _ => None,
                };
                let rebuilt = Self::rebuild_if_needed(
                    &engine,
                    &mut fences,
                    &keyring,
                    &wal_dir,
                    rebuild,
                    why,
                )?;
                (0, rebuilt)
            }
        };
        // A rebuilt state is published above every existing snapshot, so a
        // damaged one is superseded rather than chosen again.
        let floor = if rebuilt {
            highest_snapshot + 1
        } else {
            base_lsn
        };
        let (wal, records) = Wal::open(
            &wal_dir,
            floor,
            config.segment_bytes,
            config.fsync,
            !rebuilt,
        )?;
        if rebuilt {
            // Every event before the rebuild is gone; new writes take LSNs
            // past every discarded record, so every id before them is.
            events_floor = Some((wal.last_lsn(), u32::MAX));
        }
        // Records the snapshot already holds are skipped; the rest replay in
        // order with the same newer-wins rule as live writes. The first one
        // must follow the snapshot directly, or writes in between are lost.
        if !rebuilt
            && let Some(first) = records.iter().find(|record| record.lsn > base_lsn)
            && first.lsn != base_lsn + 1
        {
            return Err(DurableError::Wal(WalError::Corrupt(format!(
                "log resumes at {}, after snapshot {base_lsn}",
                first.lsn
            ))));
        }
        // Every namespace the log shows to be unreadable is sealed before
        // anything of it is restored.
        for record in records.iter().filter(|record| record.lsn > base_lsn) {
            if let Entry::Sealed {
                namespace, dek_id, ..
            } = &record.entry
            {
                seal_if_unreadable(&keyring, namespace.as_str(), dek_id)?;
            }
        }
        let mut carried = HashMap::new();
        // Replay raises the events it raised before, with the same ids,
        // kept within the retention as it goes.
        let mut events = HashMap::new();
        let retained = config.events_retained;
        if let Some(mut loaded) = loaded {
            loaded.carry_sealed(&keyring)?;
            restore_all(&engine, loaded.entities)?;
            for (namespace, (space, mut recent)) in loaded.spaces {
                fences.insert(Namespace::stored(namespace.clone()), space);
                // The retention may have been lowered since the snapshot.
                recent.trim(retained);
                events.insert(Namespace::stored(namespace), recent);
            }
            for (lsn, entry) in loaded.tail {
                let raised = apply_entry(&engine, &mut fences, &keyring, lsn, entry)?;
                keep(&mut events, raised, retained);
            }
            carried = loaded.carried;
        }
        let mut replayed = 0;
        for record in records {
            if record.lsn <= base_lsn {
                continue;
            }
            let namespace = record.entry.namespace().as_str();
            // A sealed namespace's records are carried as written.
            if keyring.check(namespace).is_err() {
                carried
                    .entry(namespace.to_string())
                    .or_insert_with(|| Part {
                        namespace: namespace.to_string(),
                        ..Part::default()
                    })
                    .tail
                    .push(record.payload);
                continue;
            }
            let raised = apply_entry(&engine, &mut fences, &keyring, record.lsn, record.entry)?;
            keep(&mut events, raised, retained);
            replayed += 1;
        }

        let durable = Self {
            engine,
            // Replayed records still count toward the next snapshot.
            writer: Mutex::new(Writer {
                fences,
                carried,
                wal,
                since_snapshot: replayed,
                snapshot_at_ms,
                snapshot_lsn: base_lsn,
            }),
            events: Mutex::new(events),
            published: tokio::sync::watch::Sender::new(0),
            events_floor,
            quotas,
            config,
            keyring,
            failed: AtomicBool::new(false),
            metrics: Metrics::default(),
            _lock: lock,
        };
        // Entities that expired while the engine was down release their
        // quota now, through the log like any other removal.
        durable.sweep_expired(now_ms)?;
        if rebuilt {
            // Pin the re-synced state so the next recovery does not need it.
            durable.snapshot(now_ms)?;
        }
        Ok(durable)
    }

    /// No snapshot to load: the WAL alone suffices only if it still starts
    /// at the first record. Otherwise the rebuild source re-syncs.
    fn rebuild_if_needed(
        engine: &Engine,
        fences: &mut Fences,
        keyring: &Keyring,
        wal_dir: &std::path::Path,
        rebuild: Option<&dyn RebuildSource>,
        damaged: Option<String>,
    ) -> Result<bool, DurableError> {
        let first = fs::read_dir(wal_dir)
            .map_err(WalError::Io)?
            .filter_map(|entry| entry.ok())
            .filter_map(|entry| {
                entry
                    .path()
                    .file_stem()
                    .and_then(|stem| stem.to_str())
                    .and_then(|stem| stem.parse::<u64>().ok())
            })
            .min();
        let complete_log = first.is_none_or(|first| first <= 1);
        if damaged.is_none() && complete_log {
            return Ok(false);
        }
        let Some(source) = rebuild else {
            return Err(DurableError::NeedsRebuild(
                damaged.unwrap_or_else(|| "the snapshot is missing".into()),
            ));
        };
        // The rebuilt snapshot replaces everything; a sealed namespace's
        // data could not be stored in it and would be lost.
        let sealed = keyring.sealed();
        if !sealed.is_empty() {
            return Err(DurableError::NeedsRebuild(format!(
                "namespaces {sealed:?} are sealed; restore their keys before a rebuild"
            )));
        }
        let entities = source.entities().map_err(DurableError::NeedsRebuild)?;
        for (ns, fence) in source.fences().map_err(DurableError::NeedsRebuild)? {
            fences.space_mut(&ns).put(fence);
        }
        // Memberships are derived from where entities are now, without
        // events: those were raised before the loss. Dwell timers restart
        // at each entity's last observation.
        for (ns, entity) in &entities {
            fences.upserted(ns, entity, 0);
        }
        restore_all(engine, entities)?;
        Ok(true)
    }

    /// Reads go to the live index; writes only through this engine, so
    /// every change is logged.
    /// The entity, if it is live and `readable` may see it (`None`: no
    /// label filter).
    pub fn get(
        &self,
        ns: &Namespace,
        collection: &str,
        id: &str,
        readable: Option<&Labels>,
        now_ms: i64,
    ) -> Result<Option<Entity>, DurableError> {
        self.open_namespace(ns)?;
        Ok(self
            .engine
            .get(ns, collection, id, now_ms)?
            .filter(|entity| visible(&entity.labels, readable)))
    }

    /// The collections of `ns`, in name order.
    pub fn collections(&self, ns: &Namespace) -> Result<Vec<String>, DurableError> {
        self.open_namespace(ns)?;
        Ok(self.engine.collections(ns)?)
    }

    /// Visit the live entities of `collection` in `window` that `readable`
    /// may see; see [`Engine::visit_region`].
    pub fn visit_region(
        &self,
        ns: &Namespace,
        collection: &str,
        window: [f64; 4],
        readable: Option<&Labels>,
        now_ms: i64,
        visit: &mut dyn FnMut(&Entity) -> bool,
    ) -> Result<bool, DurableError> {
        self.open_namespace(ns)?;
        Ok(self
            .engine
            .visit_region(ns, collection, window, readable, now_ms, visit)?)
    }

    /// The LSN the last logged write took.
    pub fn last_lsn(&self) -> Result<u64, DurableError> {
        let writer = self.writer.lock().map_err(|_| DurableError::Failed)?;
        Ok(writer.wal.last_lsn())
    }

    /// Entities counted against `ns`'s quota.
    pub fn entity_count(&self, ns: &Namespace) -> Result<usize, DurableError> {
        self.open_namespace(ns)?;
        Ok(self.engine.entity_count(ns)?)
    }

    /// Refuse a sealed namespace.
    fn open_namespace(&self, ns: &Namespace) -> Result<(), DurableError> {
        self.keyring
            .check(ns.as_str())
            .map_err(|sealed| DurableError::Sealed(sealed.0))
    }

    pub fn query(
        &self,
        ns: &Namespace,
        request: &crate::query::QueryRequest,
        now_ms: i64,
    ) -> Result<crate::query::QueryPage, DurableError> {
        self.open_namespace(ns)?;
        let started = std::time::Instant::now();
        let page = self.engine.query(ns, request, now_ms)?;
        self.metrics.queried(started.elapsed());
        Ok(page)
    }

    pub fn upsert(
        &self,
        ns: &Namespace,
        entity: Entity,
        now_ms: i64,
    ) -> Result<Upsert, DurableError> {
        Ok(self
            .upsert_batch(ns, vec![entity], now_ms)?
            .into_iter()
            .next()
            .unwrap_or(Ok(Upsert::Stale))?)
    }

    /// Apply a batch and make every applied write durable with one sync.
    /// Each entry reports its own outcome; refused entries are not logged.
    pub fn upsert_batch(
        &self,
        ns: &Namespace,
        entities: Vec<Entity>,
        now_ms: i64,
    ) -> Result<Vec<Result<Upsert, IndexError>>, DurableError> {
        // Checked before anything is applied: a record recovery would refuse
        // must never be acknowledged.
        if entities
            .iter()
            .any(|entity| !crate::wal::fits(&Op::Upsert(ns.clone(), entity.clone())))
        {
            return Err(DurableError::TooLarge);
        }
        self.write(Some(ns), now_ms, |engine, _, ops| {
            entities
                .into_iter()
                .map(|entity| {
                    let logged = entity.clone();
                    let outcome = engine.upsert(ns, entity, now_ms);
                    if matches!(outcome, Ok(Upsert::Inserted | Upsert::Updated)) {
                        ops.push(Op::Upsert(ns.clone(), logged));
                    }
                    outcome
                })
                .collect()
        })
    }

    pub fn delete(
        &self,
        ns: &Namespace,
        collection: &str,
        id: &str,
        now_ms: i64,
    ) -> Result<bool, DurableError> {
        self.write(Some(ns), now_ms, |engine, _, ops| {
            let removed = engine.delete(ns, collection, id);
            if matches!(removed, Ok(true)) {
                ops.push(Op::Delete {
                    namespace: ns.clone(),
                    collection: collection.into(),
                    id: id.into(),
                });
            }
            removed
        })?
        .map_err(DurableError::Index)
    }

    /// Run `change` under the writer lock, then log and sync what it
    /// applied. A logging failure poisons the engine: the index is ahead of
    /// the log, so further writes are refused until a reopen recovers.
    /// `ns`, the namespace written, must be open and have its key before
    /// anything changes; sealing state only changes under this lock.
    fn write<T>(
        &self,
        ns: Option<&Namespace>,
        now_ms: i64,
        change: impl FnOnce(&Engine, &Fences, &mut Vec<Op>) -> T,
    ) -> Result<T, DurableError> {
        let mut writer = self.writer.lock().map_err(|_| DurableError::Failed)?;
        // Under the lock: a writer that waited behind a failed append must
        // not log after the damaged frame.
        if self.failed.load(Ordering::Acquire) {
            return Err(DurableError::Failed);
        }
        if let Some(ns) = ns {
            self.keyring
                .current(ns.as_str())
                .map_err(|sealed| DurableError::Sealed(sealed.0))?;
        }
        let mut ops = Vec::new();
        let result = change(&self.engine, &writer.fences, &mut ops);
        if !ops.is_empty() {
            let mut seal = |lsn: u64, op: &Op, plain: &[u8]| {
                let ns = op.namespace();
                let Some((id, dek)) = self.keyring.current(ns.as_str()).map_err(|s| s.0)? else {
                    return Ok(None);
                };
                let aad = crate::wal::sealed_aad(lsn, ns.as_str(), id.as_str());
                keys::seal(&dek, &aad, plain).map(|body| Some((id.as_str().to_string(), body)))
            };
            let last = match writer.wal.append(&ops, &mut seal) {
                Ok(last) => last,
                Err(error) => {
                    self.failed.store(true, Ordering::Release);
                    return Err(error.into());
                }
            };
            writer.since_snapshot += ops.len() as u64;
            self.metrics.wrote(
                ops.iter()
                    .filter(|op| matches!(op, Op::Upsert(..) | Op::Delete { .. }))
                    .count(),
            );
            // Fences change, and raise events, only once the write is
            // durable, in log order, as replay will do it again.
            let first = last + 1 - ops.len() as u64;
            let mut raised: Raised = Vec::new();
            for (lsn, op) in (first..).zip(&ops) {
                raised.extend(apply_fences(&mut writer.fences, op, lsn));
            }
            self.publish(raised)?;
        }
        if writer.since_snapshot >= self.config.snapshot_every_records
            || (writer.since_snapshot > 0
                && now_ms - writer.snapshot_at_ms >= self.config.snapshot_every_ms)
        {
            self.snapshot_locked(&mut writer, now_ms)?;
        }
        Ok(result)
    }

    /// Remove expired entities, releasing their quota. Each removal is
    /// logged as a delete, so replay keeps it in order with later writes (a
    /// later, older observation of the same key is then admitted again).
    pub fn sweep_expired(&self, now_ms: i64) -> Result<usize, DurableError> {
        self.write(None, now_ms, |engine, _, ops| {
            let removed = engine.take_expired(now_ms)?;
            let count = removed.len();
            ops.extend(
                removed
                    .into_iter()
                    .map(|(namespace, collection, id)| Op::Delete {
                        namespace,
                        collection,
                        id,
                    }),
            );
            Ok(count)
        })?
    }

    /// Write a snapshot now and drop the WAL it covers.
    pub fn snapshot(&self, now_ms: i64) -> Result<u64, DurableError> {
        let mut writer = self.writer.lock().map_err(|_| DurableError::Failed)?;
        // After a failed append the index is ahead of the log; a snapshot
        // would certify changes that never reached it.
        if self.failed.load(Ordering::Acquire) {
            return Err(DurableError::Failed);
        }
        self.snapshot_locked(&mut writer, now_ms)
    }

    fn snapshot_locked(&self, writer: &mut Writer, now_ms: i64) -> Result<u64, DurableError> {
        let lsn = writer.wal.last_lsn();
        let mut by_namespace: HashMap<Namespace, Vec<(Namespace, Entity)>> = HashMap::new();
        for (ns, entity) in self.engine.export()? {
            by_namespace
                .entry(ns.clone())
                .or_default()
                .push((ns, entity));
        }
        let mut parts: Vec<Part> = writer.carried.values().cloned().collect();
        let mut namespaces: Vec<Namespace> = by_namespace.keys().cloned().collect();
        // A copy, so readers are not held up by the snapshot's disk work.
        // Events only change under the writer lock, which this holds.
        let events = self
            .events
            .lock()
            .map_err(|_| DurableError::Failed)?
            .clone();
        let mut with_state: Vec<Namespace> = writer
            .fences
            .iter()
            .filter(|(_, space)| !space.is_empty())
            .map(|(ns, _)| ns.clone())
            .chain(
                events
                    .iter()
                    .filter(|(_, recent)| !recent.is_empty())
                    .map(|(ns, _)| ns.clone()),
            )
            .collect();
        with_state.sort();
        with_state.dedup();
        namespaces.extend(
            with_state
                .iter()
                .filter(|ns| !by_namespace.contains_key(*ns))
                .cloned(),
        );
        for ns in namespaces {
            let key = self
                .keyring
                .current(ns.as_str())
                .map_err(|sealed| DurableError::Sealed(sealed.0))?;
            let store =
                |bytes: Vec<u8>, aad: Aad| stored_file(key.as_ref(), ns.as_str(), lsn, bytes, aad);
            let file = match by_namespace.get(&ns) {
                Some(entities) => Some(store(
                    snapshot::encode_arrow(lsn, entities)?,
                    snapshot::sealed_aad,
                )?),
                None => None,
            };
            let fences = match with_state.binary_search(&ns) {
                Ok(_) => Some(store(
                    encode_state(
                        writer.fences.space(&ns).unwrap_or(&Space::default()),
                        events.get(&ns).unwrap_or(&Recent::default()),
                    ),
                    snapshot::fences_aad,
                )?),
                Err(_) => None,
            };
            parts.push(Part {
                namespace: ns.as_str().to_string(),
                file,
                fences_with_events: fences.is_some(),
                fences,
                tail: Vec::new(),
            });
        }
        let published = snapshot::write_set(
            &self.config.dir.join("snapshots"),
            lsn,
            &parts,
            self.events_floor,
        )?;
        // Only after the snapshot is durable may the log it covers go. A
        // failure here leaves the segment layout unknown, so writes stop.
        if let Err(error) = writer
            .wal
            .rotate()
            .and_then(|()| writer.wal.drop_through(lsn))
        {
            self.failed.store(true, Ordering::Release);
            return Err(error.into());
        }
        // Everything is now sealed under the current keys (an emptied
        // namespace holds nothing at all), so older keys are no longer
        // needed. A set kept from before (no write since) may still use them.
        if published {
            for (namespace, id) in self.keyring.current_ids() {
                self.keyring
                    .retire_older(&namespace, &id)
                    .map_err(DurableError::Keys)?;
            }
        }
        writer.since_snapshot = 0;
        writer.snapshot_at_ms = now_ms;
        writer.snapshot_lsn = lsn;
        Ok(lsn)
    }

    /// Check every open namespace's keys with the provider again. A
    /// namespace whose key was revoked or whose grant expired is sealed: its
    /// entities leave memory, sealed one last time under the key it had,
    /// and are carried unread until a restart finds the key again. Returns
    /// the namespaces sealed now.
    pub fn refresh_keys(&self) -> Result<Vec<String>, DurableError> {
        let mut writer = self.writer.lock().map_err(|_| DurableError::Failed)?;
        if self.failed.load(Ordering::Acquire) {
            return Err(DurableError::Failed);
        }
        let lsn = writer.wal.last_lsn();
        let mut sealed = Vec::new();
        for (namespace, last) in self.keyring.refresh() {
            // The namespace is sealed already; its data must reach the
            // carried parts, or the next snapshot would drop it with the log.
            // On failure writes and snapshots stop until a reopen recovers it
            // from disk.
            let ns = Namespace::stored(namespace.clone());
            let space = writer.fences.take(&ns).unwrap_or_default();
            let recent = match self.events.lock() {
                Ok(mut events) => events.remove(&ns).unwrap_or_default(),
                Err(_) => {
                    self.failed.store(true, Ordering::Release);
                    return Err(DurableError::Failed);
                }
            };
            match self.carry(&namespace, space, recent, last, lsn) {
                Ok(Some(part)) => {
                    writer.carried.insert(namespace.clone(), part);
                }
                Ok(None) => {}
                Err(error) => {
                    self.failed.store(true, Ordering::Release);
                    return Err(error);
                }
            }
            sealed.push(namespace);
        }
        // Wake subscribers, so those of a sealed namespace end now.
        if !sealed.is_empty() {
            self.published
                .send_modify(|count| *count = count.wrapping_add(1));
        }
        Ok(sealed)
    }

    /// Take `namespace` out of the index and, with its fences and recent
    /// events, seal it under `last`, its final key, as a part to carry.
    /// `None` when it held nothing.
    fn carry(
        &self,
        namespace: &str,
        space: Space,
        recent: Recent,
        last: Option<(KeyId, Dek)>,
        lsn: u64,
    ) -> Result<Option<Part>, DurableError> {
        let ns = Namespace::stored(namespace.to_string());
        let entities: Vec<(Namespace, Entity)> = self
            .engine
            .drop_namespace(&ns)?
            .into_iter()
            .map(|entity| (ns.clone(), entity))
            .collect();
        let state = !(space.is_empty() && recent.is_empty());
        if entities.is_empty() && !state {
            return Ok(None);
        }
        let Some(key) = last else {
            return Err(DurableError::Keys(format!(
                "{namespace}: no key to carry it under"
            )));
        };
        let store = |bytes: Vec<u8>, aad: Aad| stored_file(Some(&key), namespace, lsn, bytes, aad);
        Ok(Some(Part {
            namespace: namespace.to_string(),
            fences_with_events: state,
            file: match entities.is_empty() {
                true => None,
                false => Some(store(
                    snapshot::encode_arrow(lsn, &entities)?,
                    snapshot::sealed_aad,
                )?),
            },
            fences: match state {
                true => Some(store(encode_state(&space, &recent), snapshot::fences_aad)?),
                false => None,
            },
            tail: Vec::new(),
        }))
    }

    /// Keep `raised` for readers, oldest dropped past the retention.
    fn publish(&self, raised: Raised) -> Result<(), DurableError> {
        if raised.is_empty() {
            return Ok(());
        }
        self.metrics.raised(raised.len());
        let mut events = self.events.lock().map_err(|_| DurableError::Failed)?;
        keep(&mut events, raised, self.config.events_retained);
        drop(events);
        self.published
            .send_modify(|count| *count = count.wrapping_add(1));
        Ok(())
    }

    /// Fence events of `ns` after `after` (from the oldest kept when
    /// `None`), of the given fences (all when empty), oldest first, at most
    /// `limit`, and the id read through. A reader whose `after` is older
    /// than the newest dropped event, or than history the store kept, gets
    /// [`DurableError::Expired`]: it has missed events.
    pub fn events(
        &self,
        ns: &Namespace,
        after: Option<EventId>,
        fences: &[String],
        readable: Option<&Labels>,
        limit: usize,
    ) -> Result<EventPage, DurableError> {
        self.open_namespace(ns)?;
        let floor = self.events_floor.unwrap_or((0, 0));
        if let Some(after) = after
            && after < floor
        {
            return Err(DurableError::Expired(Expired { dropped: floor }));
        }
        let events = self.events.lock().map_err(|_| DurableError::Failed)?;
        let mut page = match events.get(ns) {
            Some(recent) => recent
                .after(after, fences, readable, limit)
                .map_err(DurableError::Expired)?,
            None => EventPage {
                events: Vec::new(),
                through: after.unwrap_or((0, 0)),
            },
        };
        // A reader that started from the oldest kept resumes past the floor.
        page.through = page.through.max(floor);
        Ok(page)
    }

    /// Changes whenever events are published; a subscriber marks it seen,
    /// reads [`Self::events`], and waits on it when there were none.
    pub fn watch_events(&self) -> tokio::sync::watch::Receiver<u64> {
        self.published.subscribe()
    }

    /// Add or replace a fence; true when it replaced one.
    pub fn put_fence(
        &self,
        ns: &Namespace,
        fence: Fence,
        now_ms: i64,
    ) -> Result<bool, DurableError> {
        fence.validate(&self.quotas)?;
        let id = fence.id.clone();
        let op = Op::FencePut(ns.clone(), fence);
        if !crate::wal::fits(&op) {
            return Err(DurableError::TooLarge);
        }
        self.write(Some(ns), now_ms, |_, fences, ops| {
            let existing = fences
                .space(ns)
                .is_some_and(|space| space.get(&id).is_some());
            let count = fences.space(ns).map_or(0, Space::len);
            if !existing && count >= self.quotas.max_fences {
                return Err(IndexError::FenceQuota {
                    limit: self.quotas.max_fences,
                });
            }
            ops.push(op);
            Ok(existing)
        })?
        .map_err(DurableError::Index)
    }

    /// Remove a fence; true when it existed. Its members raise no exits.
    pub fn delete_fence(
        &self,
        ns: &Namespace,
        id: &str,
        now_ms: i64,
    ) -> Result<bool, DurableError> {
        self.write(Some(ns), now_ms, |_, fences, ops| {
            let existing = fences
                .space(ns)
                .is_some_and(|space| space.get(id).is_some());
            if existing {
                ops.push(Op::FenceDelete {
                    namespace: ns.clone(),
                    id: id.to_string(),
                });
            }
            existing
        })
    }

    /// The fence, if `readable` may see it.
    pub fn get_fence(
        &self,
        ns: &Namespace,
        id: &str,
        readable: Option<&Labels>,
    ) -> Result<Option<Fence>, DurableError> {
        self.open_namespace(ns)?;
        let writer = self.writer.lock().map_err(|_| DurableError::Failed)?;
        Ok(writer
            .fences
            .space(ns)
            .and_then(|space| space.get(id))
            .filter(|fence| visible(&fence.labels, readable))
            .cloned())
    }

    /// Fences of `ns` after the id `after` that `readable` may see, in id
    /// order, at most `limit`.
    pub fn list_fences(
        &self,
        ns: &Namespace,
        after: Option<&str>,
        readable: Option<&Labels>,
        limit: usize,
    ) -> Result<Vec<Fence>, DurableError> {
        self.open_namespace(ns)?;
        let writer = self.writer.lock().map_err(|_| DurableError::Failed)?;
        Ok(writer
            .fences
            .space(ns)
            .map(|space| {
                space
                    .list()
                    .filter(|fence| after.is_none_or(|after| fence.id.as_str() > after))
                    .filter(|fence| visible(&fence.labels, readable))
                    .take(limit)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default())
    }

    /// Start a new current key for `ns`. New writes use it at once; the
    /// next snapshot re-seals older data under it and retires the old key.
    pub fn rotate_key(&self, ns: &Namespace) -> Result<(), DurableError> {
        let _writer = self.writer.lock().map_err(|_| DurableError::Failed)?;
        self.keyring
            .rotate(ns.as_str())
            .map(|_| ())
            .map_err(|sealed| DurableError::Sealed(sealed.0))
    }

    /// Bytes the WAL holds now.
    pub fn wal_bytes(&self) -> Result<u64, DurableError> {
        let writer = self.writer.lock().map_err(|_| DurableError::Failed)?;
        Ok(writer.wal.size()?)
    }

    /// Whether the engine can still take writes: no durability failure
    /// since it opened.
    pub fn healthy(&self) -> bool {
        !self.failed.load(Ordering::Acquire) && !self.writer.is_poisoned()
    }

    /// Metrics in the Prometheus text format, aggregated over namespaces.
    pub fn render_metrics(&self, now_ms: i64) -> Result<String, DurableError> {
        let (wal_bytes, since_snapshot, snapshot_at_ms) = {
            let writer = self.writer.lock().map_err(|_| DurableError::Failed)?;
            (
                writer.wal.size()?,
                // Sealed namespaces' records count too: recovery reads them.
                writer.wal.last_lsn().saturating_sub(writer.snapshot_lsn),
                writer.snapshot_at_ms,
            )
        };
        Ok(self.metrics.render(&Gauges {
            entities: self.engine.total_entities()? as u64,
            wal_bytes,
            wal_records_since_snapshot: since_snapshot,
            snapshot_age_seconds: now_ms.saturating_sub(snapshot_at_ms).max(0) as f64 / 1000.0,
            sealed_namespaces: self.keyring.sealed().len() as u64,
            healthy: self.healthy(),
        }))
    }
}

/// Fence events raised by writes, with their namespaces.
type Raised = Vec<(Namespace, FenceEvent)>;

/// Add `raised` to the kept events, dropping the oldest past `retained` per
/// namespace.
fn keep(events: &mut HashMap<Namespace, Recent>, raised: Raised, retained: usize) {
    for (ns, event) in raised {
        events.entry(ns).or_default().push(event, retained);
    }
}

/// Authenticated data for one kind of sealed snapshot file.
type Aad = fn(u64, &str, &str) -> Vec<u8>;

/// `bytes` of `namespace` as a snapshot stores them: sealed under `key`, or
/// plaintext for a namespace without one.
fn stored_file(
    key: Option<&(KeyId, Dek)>,
    namespace: &str,
    lsn: u64,
    bytes: Vec<u8>,
    aad: Aad,
) -> Result<DataFile, DurableError> {
    let Some((id, dek)) = key else {
        return Ok(DataFile {
            sealed: None,
            bytes,
        });
    };
    Ok(DataFile {
        bytes: keys::seal(dek, &aad(lsn, namespace, id.as_str()), &bytes)
            .map_err(DurableError::Keys)?,
        sealed: Some(SealInfo {
            dek_id: id.as_str().to_string(),
            sealed_lsn: lsn,
        }),
    })
}

/// A loaded snapshot: entities and fences to restore, records carried in its
/// parts to apply after them, the parts of sealed namespaces as stored, and
/// every part as stored, to carry a namespace the log turns out to seal.
struct Loaded {
    lsn: u64,
    events_floor: Option<EventId>,
    entities: Vec<(Namespace, Entity)>,
    spaces: HashMap<String, (Space, Recent)>,
    tail: Vec<(u64, Entry)>,
    carried: HashMap<String, Part>,
    stored: HashMap<String, Part>,
}

impl Loaded {
    /// Move every namespace sealed since loading to the carried parts.
    fn carry_sealed(&mut self, keyring: &Keyring) -> Result<(), DurableError> {
        let mut sealed: HashMap<String, Vec<(Namespace, Entity)>> = HashMap::new();
        let entities = std::mem::take(&mut self.entities);
        for (ns, entity) in entities {
            if keyring.check(ns.as_str()).is_ok() {
                self.entities.push((ns, entity));
            } else {
                sealed
                    .entry(ns.as_str().to_string())
                    .or_default()
                    .push((ns, entity));
            }
        }
        self.tail
            .retain(|(_, entry)| keyring.check(entry.namespace().as_str()).is_ok());
        self.spaces
            .retain(|namespace, _| keyring.check(namespace).is_ok());
        for (namespace, part) in std::mem::take(&mut self.stored) {
            if keyring.check(&namespace).is_err() {
                self.carried.entry(namespace).or_insert(part);
            }
        }
        // Without a stored part (a version 1 snapshot) the data was
        // plaintext on disk already, and is carried as such.
        for (namespace, entities) in sealed {
            if !self.carried.contains_key(&namespace) {
                let bytes = snapshot::encode_arrow(self.lsn, &entities)?;
                self.carried.insert(
                    namespace.clone(),
                    Part {
                        namespace,
                        file: Some(DataFile {
                            sealed: None,
                            bytes,
                        }),
                        fences: None,
                        fences_with_events: false,
                        tail: Vec::new(),
                    },
                );
            }
        }
        Ok(())
    }
}

/// Seal `namespace` when sealed data of it names a key the keyring cannot
/// give, so one lost key isolates its namespace instead of failing the open.
fn seal_if_unreadable(
    keyring: &Keyring,
    namespace: &str,
    dek_id: &str,
) -> Result<(), DurableError> {
    if keyring.check(namespace).is_ok() && keyring.key(namespace, dek_id).is_err() {
        keyring
            .seal_namespace(namespace, &format!("its key {dek_id} is unavailable"))
            .map_err(DurableError::Keys)?;
    }
    Ok(())
}

/// Read the snapshot `found`, opening each namespace's data with its key.
/// Any unreadable part makes the whole snapshot damaged.
fn load_snapshot(keyring: &Keyring, found: Found) -> Result<Loaded, DurableError> {
    let damaged = |why: String| DurableError::Snapshot(SnapshotError(why));
    let set = match found {
        Found::V1(path) => {
            let (lsn, entities) = snapshot::read_v1(&path)?;
            let mut loaded = Loaded {
                lsn,
                // Version 1 kept no events.
                events_floor: Some((lsn, u32::MAX)),
                entities,
                spaces: HashMap::new(),
                tail: Vec::new(),
                carried: HashMap::new(),
                stored: HashMap::new(),
            };
            loaded.carry_sealed(keyring)?;
            return Ok(loaded);
        }
        Found::V2(dir) => snapshot::read_set(&dir)?,
    };
    let mut loaded = Loaded {
        lsn: set.lsn,
        events_floor: set.events_floor,
        entities: Vec::new(),
        spaces: HashMap::new(),
        tail: Vec::new(),
        carried: HashMap::new(),
        stored: HashMap::new(),
    };
    for part in set.parts {
        let mut tail = Vec::new();
        for payload in &part.tail {
            let (lsn, entry) =
                crate::wal::decode_payload(payload).map_err(|error| damaged(error.to_string()))?;
            if entry.namespace().as_str() != part.namespace {
                return Err(damaged(format!("{}: foreign record", part.namespace)));
            }
            if let Entry::Sealed { dek_id, .. } = &entry {
                seal_if_unreadable(keyring, &part.namespace, dek_id)?;
            }
            tail.push((lsn, entry));
        }
        for info in [&part.file, &part.fences]
            .into_iter()
            .flatten()
            .filter_map(|file| file.sealed.as_ref())
        {
            seal_if_unreadable(keyring, &part.namespace, &info.dek_id)?;
        }
        if keyring.check(&part.namespace).is_err() {
            loaded.carried.insert(part.namespace.clone(), part);
            continue;
        }
        // A file's plaintext, opened with its key when sealed.
        let open = |file: &DataFile, aad: Aad| match &file.sealed {
            None => Ok(file.bytes.clone()),
            Some(info) => {
                let dek = keyring
                    .key(&part.namespace, &info.dek_id)
                    .map_err(|sealed| damaged(sealed.0))?;
                let aad = aad(info.sealed_lsn, &part.namespace, &info.dek_id);
                keys::open(&dek, &aad, &file.bytes).map_err(damaged)
            }
        };
        if let Some(file) = &part.fences {
            let state = decode_state(&open(file, snapshot::fences_aad)?, part.fences_with_events)
                .map_err(|error| damaged(error.to_string()))?;
            loaded.spaces.insert(part.namespace.clone(), state);
        }
        if let Some(file) = &part.file {
            let entities = match &file.sealed {
                None => snapshot::plain_entities(file)?,
                Some(_) => snapshot::current_entities(open(file, snapshot::sealed_aad)?)?,
            };
            // A part holds only its own namespace.
            if entities.iter().any(|(ns, _)| ns.as_str() != part.namespace) {
                return Err(damaged(format!("{}: foreign entities", part.namespace)));
            }
            loaded.entities.extend(entities);
        }
        loaded.tail.extend(tail);
        loaded.stored.insert(part.namespace.clone(), part);
    }
    Ok(loaded)
}

/// Apply one logged entry, opening a sealed one with its namespace's key,
/// and return the fence events it raises.
fn apply_entry(
    engine: &Engine,
    fences: &mut Fences,
    keyring: &Keyring,
    lsn: u64,
    entry: Entry,
) -> Result<Raised, DurableError> {
    let op = match entry {
        Entry::Op(op) => op,
        Entry::Sealed {
            namespace,
            dek_id,
            body,
        } => {
            let corrupt = |why: String| {
                DurableError::Wal(WalError::Corrupt(format!(
                    "record {lsn} of {}: {why}",
                    namespace.as_str()
                )))
            };
            let dek = keyring
                .key(namespace.as_str(), &dek_id)
                .map_err(|sealed| corrupt(sealed.0))?;
            let aad = crate::wal::sealed_aad(lsn, namespace.as_str(), &dek_id);
            let plain = keys::open(&dek, &aad, &body).map_err(corrupt)?;
            match crate::wal::decode_payload(&plain).map_err(|error| corrupt(error.to_string()))? {
                (inner, Entry::Op(op))
                    if inner == lsn && op.namespace().as_str() == namespace.as_str() =>
                {
                    op
                }
                _ => return Err(corrupt("sealed record does not match its frame".into())),
            }
        }
    };
    match &op {
        Op::Upsert(ns, entity) => {
            engine.restore(ns, entity.clone())?;
        }
        Op::Delete {
            namespace,
            collection,
            id,
        } => {
            engine.delete(namespace, collection, id)?;
        }
        Op::FencePut(..) | Op::FenceDelete { .. } => {}
    }
    Ok(apply_fences(fences, &op, lsn))
}

/// Apply a logged write at `lsn` to the fences: a fence change, or the
/// events an entity write raises.
fn apply_fences(fences: &mut Fences, op: &Op, lsn: u64) -> Raised {
    let ns = op.namespace();
    let events = match op {
        Op::Upsert(ns, entity) => fences.upserted(ns, entity, lsn),
        Op::Delete {
            namespace,
            collection,
            id,
        } => fences.deleted(namespace, collection, id, lsn),
        Op::FencePut(ns, fence) => {
            fences.space_mut(ns).put(fence.clone());
            Vec::new()
        }
        Op::FenceDelete { namespace, id } => {
            fences.space_mut(namespace).remove(id);
            Vec::new()
        }
    };
    events
        .into_iter()
        .map(|event| (ns.clone(), event))
        .collect()
}

/// Load recovered entities per collection in one bulk pass each.
fn restore_all(engine: &Engine, entities: Vec<(Namespace, Entity)>) -> Result<(), DurableError> {
    let mut groups: HashMap<(Namespace, String), Vec<Entity>> = HashMap::new();
    for (ns, entity) in entities {
        groups
            .entry((ns, entity.collection.clone()))
            .or_default()
            .push(entity);
    }
    for ((ns, collection), entities) in groups {
        engine.bulk_restore(&ns, &collection, entities)?;
    }
    Ok(())
}

/// Create `dir` and any missing ancestors, syncing each new entry's parent so
/// a power cut cannot drop the directory and the log inside it.
fn create_dir_durably(dir: &std::path::Path) -> std::io::Result<()> {
    let mut missing = Vec::new();
    let mut at = Some(dir);
    while let Some(path) = at
        && !path.exists()
    {
        missing.push(path.to_path_buf());
        at = path.parent();
    }
    fs::create_dir_all(dir)?;
    for created in missing.iter().rev() {
        // A relative path's top component has the empty parent: the working
        // directory.
        let parent = match created.parent() {
            Some(parent) if parent.as_os_str().is_empty() => std::path::Path::new("."),
            Some(parent) => parent,
            None => continue,
        };
        crate::wal::sync_dir(parent)?;
    }
    Ok(())
}
