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
use crate::keys::sekai_keys::{Dek, KeyId};
use crate::keys::{self, KeyConfig, Keyring};
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
    /// Namespaces encrypted with keys from an external provider; `None`
    /// keeps every namespace in plaintext.
    pub keys: Option<KeyConfig>,
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
    /// The namespace's key is revoked or missing; it refuses reads and
    /// writes until the key returns. Other namespaces are unaffected.
    Sealed(String),
    /// The key store could not be read or written.
    Keys(String),
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
    /// Sealed namespaces' data, as stored, carried into every snapshot
    /// until their keys return.
    carried: HashMap<String, Part>,
    wal: Wal,
    since_snapshot: u64,
    snapshot_at_ms: i64,
}

pub struct DurableEngine {
    engine: Engine,
    writer: Mutex<Writer>,
    config: DurabilityConfig,
    keyring: Keyring,
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
        let (base_lsn, rebuilt) = match newest.map(|(_, found)| load_snapshot(&keyring, found)) {
            Some(Ok(snapshot)) => {
                let lsn = snapshot.lsn;
                loaded = Some(snapshot);
                (lsn, false)
            }
            damaged_or_missing => {
                let why = match damaged_or_missing {
                    Some(Err(error)) => Some(error.to_string()),
                    _ => None,
                };
                let rebuilt = Self::rebuild_if_needed(&engine, &keyring, &wal_dir, rebuild, why)?;
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
        if let Some(mut loaded) = loaded {
            loaded.carry_sealed(&keyring)?;
            restore_all(&engine, loaded.entities)?;
            for (lsn, entry) in loaded.tail {
                apply_entry(&engine, &keyring, lsn, entry)?;
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
            apply_entry(&engine, &keyring, record.lsn, record.entry)?;
            replayed += 1;
        }

        let durable = Self {
            engine,
            // Replayed records still count toward the next snapshot.
            writer: Mutex::new(Writer {
                carried,
                wal,
                since_snapshot: replayed,
                snapshot_at_ms,
            }),
            config,
            keyring,
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
    ) -> Result<Option<Entity>, DurableError> {
        self.open_namespace(ns)?;
        Ok(self.engine.get(ns, collection, id, now_ms)?)
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
        Ok(self.engine.query(ns, request, now_ms)?)
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
        self.write(Some(ns), now_ms, |engine, ops| {
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
        self.write(Some(ns), now_ms, |engine, ops| {
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
        change: impl FnOnce(&Engine, &mut Vec<Op>) -> T,
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
        let result = change(&self.engine, &mut ops);
        if !ops.is_empty() {
            let mut seal = |lsn: u64, op: &Op, plain: &[u8]| {
                let ns = op_namespace(op);
                let Some((id, dek)) = self.keyring.current(ns.as_str()).map_err(|s| s.0)? else {
                    return Ok(None);
                };
                let aad = crate::wal::sealed_aad(lsn, ns.as_str(), id.as_str());
                keys::seal(&dek, &aad, plain).map(|body| Some((id.as_str().to_string(), body)))
            };
            if let Err(error) = writer.wal.append(&ops, &mut seal) {
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
        self.write(None, now_ms, |engine, ops| {
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
        for (ns, entities) in by_namespace {
            let bytes = snapshot::encode_arrow(lsn, &entities)?;
            let file = match self
                .keyring
                .current(ns.as_str())
                .map_err(|sealed| DurableError::Sealed(sealed.0))?
            {
                Some((id, dek)) => {
                    let aad = snapshot::sealed_aad(lsn, ns.as_str(), id.as_str());
                    DataFile {
                        bytes: keys::seal(&dek, &aad, &bytes).map_err(DurableError::Keys)?,
                        sealed: Some(SealInfo {
                            dek_id: id.as_str().to_string(),
                            sealed_lsn: lsn,
                        }),
                    }
                }
                None => DataFile {
                    sealed: None,
                    bytes,
                },
            };
            parts.push(Part {
                namespace: ns.as_str().to_string(),
                file: Some(file),
                tail: Vec::new(),
            });
        }
        let published = snapshot::write_set(&self.config.dir.join("snapshots"), lsn, &parts)?;
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
            match self.carry(&namespace, last, lsn) {
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
        Ok(sealed)
    }

    /// Take `namespace` out of the index, sealed under `last`, its final
    /// key, as a part to carry. `None` when it held nothing.
    fn carry(
        &self,
        namespace: &str,
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
        if entities.is_empty() {
            return Ok(None);
        }
        let Some((id, dek)) = last else {
            return Err(DurableError::Keys(format!(
                "{namespace}: no key to carry it under"
            )));
        };
        let aad = snapshot::sealed_aad(lsn, namespace, id.as_str());
        let bytes = snapshot::encode_arrow(lsn, &entities)?;
        Ok(Some(Part {
            namespace: namespace.to_string(),
            file: Some(DataFile {
                bytes: keys::seal(&dek, &aad, &bytes).map_err(DurableError::Keys)?,
                sealed: Some(SealInfo {
                    dek_id: id.as_str().to_string(),
                    sealed_lsn: lsn,
                }),
            }),
            tail: Vec::new(),
        }))
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
}

/// A loaded snapshot: entities to restore, records carried in its parts to
/// apply after them, the parts of sealed namespaces as stored, and every
/// part as stored, to carry a namespace the log turns out to seal.
struct Loaded {
    lsn: u64,
    entities: Vec<(Namespace, Entity)>,
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
    let (lsn, parts) = match found {
        Found::V1(path) => {
            let (lsn, entities) = snapshot::read_v1(&path)?;
            let mut loaded = Loaded {
                lsn,
                entities,
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
        lsn,
        entities: Vec::new(),
        tail: Vec::new(),
        carried: HashMap::new(),
        stored: HashMap::new(),
    };
    for part in parts {
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
        if let Some(info) = part.file.as_ref().and_then(|file| file.sealed.as_ref()) {
            seal_if_unreadable(keyring, &part.namespace, &info.dek_id)?;
        }
        if keyring.check(&part.namespace).is_err() {
            loaded.carried.insert(part.namespace.clone(), part);
            continue;
        }
        if let Some(file) = &part.file {
            let entities = match &file.sealed {
                None => snapshot::plain_entities(file)?,
                Some(info) => {
                    let dek = keyring
                        .key(&part.namespace, &info.dek_id)
                        .map_err(|sealed| damaged(sealed.0))?;
                    let aad = snapshot::sealed_aad(info.sealed_lsn, &part.namespace, &info.dek_id);
                    snapshot::current_entities(
                        keys::open(&dek, &aad, &file.bytes).map_err(damaged)?,
                    )?
                }
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

/// Apply one logged entry, opening a sealed one with its namespace's key.
fn apply_entry(
    engine: &Engine,
    keyring: &Keyring,
    lsn: u64,
    entry: Entry,
) -> Result<(), DurableError> {
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
                    if inner == lsn && op_namespace(&op).as_str() == namespace.as_str() =>
                {
                    op
                }
                _ => return Err(corrupt("sealed record does not match its frame".into())),
            }
        }
    };
    apply(engine, op)
}

fn op_namespace(op: &Op) -> &Namespace {
    match op {
        Op::Upsert(ns, _) => ns,
        Op::Delete { namespace, .. } => namespace,
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
