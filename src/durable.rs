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

use crate::entity::{Entity, Namespace};
use crate::index::{Engine, IndexError, Quotas, Upsert};
use crate::snapshot::{self, SnapshotError};
use crate::wal::{Op, Wal, WalError};
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
}

/// Where entities come from again when no usable snapshot exists.
pub trait RebuildSource {
    fn entities(&self) -> Result<Vec<(Namespace, Entity)>, String>;
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
    wal: Wal,
    since_snapshot: u64,
    snapshot_at_ms: i64,
}

pub struct DurableEngine {
    engine: Engine,
    writer: Mutex<Writer>,
    config: DurabilityConfig,
    failed: AtomicBool,
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

        let engine = Engine::new(quotas);
        let newest = snapshot::list(&snapshot_dir)?.into_iter().next();
        let highest_snapshot = newest.as_ref().map_or(0, |(lsn, _)| *lsn);
        // When the newest snapshot was taken, so the time trigger carries on
        // across restarts instead of starting over.
        let snapshot_at_ms = newest
            .as_ref()
            .and_then(|(_, path)| fs::metadata(path).ok()?.modified().ok())
            .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
            .map_or(0, |since| {
                i64::try_from(since.as_millis()).unwrap_or(i64::MAX)
            });
        let (base_lsn, rebuilt) = match newest.map(|(_, path)| snapshot::read(&path)) {
            Some(Ok((lsn, entities))) => {
                restore_all(&engine, entities)?;
                (lsn, false)
            }
            damaged_or_missing => {
                let why = match damaged_or_missing {
                    Some(Err(error)) => Some(error.to_string()),
                    _ => None,
                };
                (0, Self::rebuild_if_needed(&engine, &wal_dir, rebuild, why)?)
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
        // Records the snapshot already holds are skipped; the rest replay in
        // order with the same newer-wins rule as live writes. The first one
        // must follow the snapshot directly, or writes in between are lost.
        if !rebuilt
            && let Some((first, _)) = records.iter().find(|(lsn, _)| *lsn > base_lsn)
            && *first != base_lsn + 1
        {
            return Err(DurableError::Wal(WalError::Corrupt(format!(
                "log resumes at {first}, after snapshot {base_lsn}"
            ))));
        }
        let mut replayed = 0;
        for (lsn, op) in records {
            if lsn > base_lsn {
                apply(&engine, op)?;
                replayed += 1;
            }
        }

        let durable = Self {
            engine,
            // Replayed records still count toward the next snapshot.
            writer: Mutex::new(Writer {
                wal,
                since_snapshot: replayed,
                snapshot_at_ms,
            }),
            config,
            failed: AtomicBool::new(false),
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
        restore_all(
            engine,
            source.entities().map_err(DurableError::NeedsRebuild)?,
        )?;
        Ok(true)
    }

    /// Reads go to the live index; writes only through this engine, so
    /// every change is logged.
    pub fn get(
        &self,
        ns: &Namespace,
        collection: &str,
        id: &str,
        now_ms: i64,
    ) -> Result<Option<Entity>, IndexError> {
        self.engine.get(ns, collection, id, now_ms)
    }

    /// Entities counted against `ns`'s quota.
    pub fn entity_count(&self, ns: &Namespace) -> Result<usize, IndexError> {
        self.engine.entity_count(ns)
    }

    pub fn query(
        &self,
        ns: &Namespace,
        request: &crate::query::QueryRequest,
        now_ms: i64,
    ) -> Result<crate::query::QueryPage, IndexError> {
        self.engine.query(ns, request, now_ms)
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
        self.write(now_ms, |engine, ops| {
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
        self.write(now_ms, |engine, ops| {
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
    fn write<T>(
        &self,
        now_ms: i64,
        change: impl FnOnce(&Engine, &mut Vec<Op>) -> T,
    ) -> Result<T, DurableError> {
        let mut writer = self.writer.lock().map_err(|_| DurableError::Failed)?;
        // Under the lock: a writer that waited behind a failed append must
        // not log after the damaged frame.
        if self.failed.load(Ordering::Acquire) {
            return Err(DurableError::Failed);
        }
        let mut ops = Vec::new();
        let result = change(&self.engine, &mut ops);
        if !ops.is_empty() {
            if let Err(error) = writer.wal.append(&ops) {
                self.failed.store(true, Ordering::Release);
                return Err(error.into());
            }
            writer.since_snapshot += ops.len() as u64;
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
        self.write(now_ms, |engine, ops| {
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
        snapshot::write(
            &self.config.dir.join("snapshots"),
            lsn,
            &self.engine.export()?,
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
        writer.since_snapshot = 0;
        writer.snapshot_at_ms = now_ms;
        Ok(lsn)
    }

    /// Bytes the WAL holds now.
    pub fn wal_bytes(&self) -> Result<u64, DurableError> {
        let writer = self.writer.lock().map_err(|_| DurableError::Failed)?;
        Ok(writer.wal.size()?)
    }
}

fn apply(engine: &Engine, op: Op) -> Result<(), DurableError> {
    match op {
        Op::Upsert(ns, entity) => {
            engine.restore(&ns, entity)?;
        }
        Op::Delete {
            namespace,
            collection,
            id,
        } => {
            engine.delete(&namespace, &collection, &id)?;
        }
    }
    Ok(())
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
