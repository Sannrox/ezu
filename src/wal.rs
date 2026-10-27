//! The write-ahead log: numbered segments of checksummed records.
//!
//! A segment is `wal/<first lsn>.seg`: an 8-byte header (`EZUW`, format
//! version) and frames `[len u32][!len u32][crc32 u32][payload]`. A batch is
//! its record frames, synced, then a commit frame naming its last LSN,
//! synced again; only then is the batch acknowledged. On open, frames after
//! the last commit of the last segment are an unfinished batch and are cut
//! off. Damage anywhere before a later commit is refused, since that data was
//! acknowledged. One ambiguity remains by design: a damaged final commit
//! frame reads as unfinished, so media damage to exactly that frame loses its
//! batch. Segments a snapshot fully covers are skipped, and segment names
//! must continue one another, so a lost segment is a refused gap.
//!
//! The log has an epoch, held durably in `wal/EPOCH`. [`Wal::promote`] raises
//! it, and a writer opened at an older epoch is refused (fenced) from then
//! on: every append and rotation checks the file first, under a shared lock
//! a promotion takes exclusively. A write holding the lock (from the check
//! through its append, see [`Wal::hold_fence`]) finishes before the raise,
//! and none starts after it. A writer past epoch 0 begins each segment
//! it starts with a promotion frame `[last lsn u64][0xE0][epoch u64]`, so the
//! log records the raise from the next open on, and keeps the epoch however
//! much of it a snapshot drops. The frame takes no LSN, so LSNs, and the
//! event ids made of them, run on unbroken.
//!
//! An owner that honors epochs holds a shared lock on `wal/OWNER` for its
//! life, next to its lock on the directory, so a promotion can tell it from
//! an owner of a release without epochs, which it cannot fence and refuses
//! to promote past. Releases without epochs read only `.seg` files in `wal/`
//! and ignore `EPOCH` and `OWNER`.

use crate::codec::{
    CodecError, Reader, Writer, decode_entity, decode_labels, encode_entity, encode_labels,
};
use crate::entity::{Entity, Namespace};
use crate::fence::{Fence, decode_fence, encode_fence};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

const MAGIC: &[u8; 4] = b"EZUW";
/// Version 2 adds sealed records, version 3 promotion frames; older segments
/// are still read. Only a segment that begins with a promotion frame (epoch
/// past 0) is written as version 3, so a log that was never promoted stays
/// readable by a version 2 binary and rollback keeps working. Once promoted,
/// a binary from before version 3 refuses the log, as it cannot honor the
/// epoch. Fence records came without a version change: a binary from before
/// them refuses a log that holds them, and only then.
const VERSION: u32 = 3;
/// Header version of a segment without a promotion frame.
const UNPROMOTED_VERSION: u32 = 2;
const HEADER: u64 = 8;
/// Largest record payload; a frame claiming more is damage, not data.
const MAX_RECORD: u32 = 64 << 20;

/// What one logged record holds: an operation, or an encrypted namespace's
/// operation still sealed under its key.
#[derive(Debug, Clone, PartialEq)]
pub enum Entry {
    Op(Op),
    Sealed {
        namespace: Namespace,
        dek_id: String,
        /// Nonce and ciphertext of the record's plaintext payload.
        body: Vec<u8>,
    },
}

impl Entry {
    pub fn namespace(&self) -> &Namespace {
        match self {
            Self::Op(op) => op.namespace(),
            Self::Sealed { namespace, .. } => namespace,
        }
    }
}

/// One committed record with its exact payload bytes, which a sealed
/// namespace's data is carried forward as.
#[derive(Debug, Clone, PartialEq)]
pub struct Record {
    pub lsn: u64,
    pub entry: Entry,
    pub payload: Vec<u8>,
}

/// Authenticated data binding a sealed record to its LSN, namespace and key.
pub fn sealed_aad(lsn: u64, namespace: &str, dek_id: &str) -> Vec<u8> {
    let mut out = Writer::default();
    out.str("ezu.wal");
    out.u64(lsn);
    out.str(namespace);
    out.str(dek_id);
    out.0
}

#[derive(Debug, Clone, PartialEq)]
pub enum Op {
    Upsert(Namespace, Entity),
    Delete {
        namespace: Namespace,
        collection: String,
        id: String,
    },
    FencePut(Namespace, Fence),
    FenceDelete {
        namespace: Namespace,
        id: String,
    },
}

impl Op {
    pub fn namespace(&self) -> &Namespace {
        match self {
            Self::Upsert(ns, _) | Self::FencePut(ns, _) => ns,
            Self::Delete { namespace, .. } | Self::FenceDelete { namespace, .. } => namespace,
        }
    }
}

#[derive(Debug)]
pub enum WalError {
    Io(io::Error),
    /// A complete segment is damaged; recovery would lose acknowledged data.
    Corrupt(String),
    /// A record could not be sealed under its namespace's key.
    Seal(String),
    /// An owner that ignores epochs holds the directory; a promotion could
    /// not fence it.
    LegacyOwner,
    /// The directory was promoted past this writer's epoch; it may not write.
    Fenced {
        epoch: u64,
        directory: u64,
    },
}

impl std::fmt::Display for WalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(f, "wal io: {error}"),
            Self::Corrupt(what) => write!(f, "wal corrupt: {what}"),
            Self::Seal(why) => write!(f, "wal seal: {why}"),
            Self::Fenced { epoch, directory } => write!(
                f,
                "wal fenced: this writer is at epoch {epoch}, the log at {directory}"
            ),
            Self::LegacyOwner => {
                f.write_str("wal promotion refused: the directory's owner does not honor epochs")
            }
        }
    }
}

impl From<io::Error> for WalError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

fn encode(lsn: u64, op: &Op) -> Vec<u8> {
    let mut out = Writer::default();
    out.u64(lsn);
    match op {
        Op::Upsert(ns, entity) => {
            out.u8(1);
            out.str(ns.as_str());
            encode_entity(&mut out, entity);
        }
        Op::Delete {
            namespace,
            collection,
            id,
        } => {
            out.u8(2);
            out.str(namespace.as_str());
            out.str(collection);
            out.str(id);
        }
        Op::FencePut(ns, fence) => {
            out.u8(FENCE_PUT);
            out.str(ns.as_str());
            encode_fence(&mut out, fence);
            // Last and only when present, as for entities.
            if !fence.labels.is_empty() {
                encode_labels(&mut out, &fence.labels);
            }
        }
        Op::FenceDelete { namespace, id } => {
            out.u8(FENCE_DELETE);
            out.str(namespace.as_str());
            out.str(id);
        }
    }
    out.0
}

const COMMIT: u8 = 0xC0;
const PROMOTE: u8 = 0xE0;
/// The smallest frame: its 12-byte header and a commit payload (LSN, tag).
const MIN_FRAME: u64 = 12 + 9;
const SEALED: u8 = 3;
const FENCE_PUT: u8 = 4;
const FENCE_DELETE: u8 = 5;

enum Frame {
    Record(u64, Box<Entry>),
    /// The batch ending at this LSN is complete and acknowledged.
    Commit(u64),
    /// The log is at this epoch from here on.
    Promote(u64),
}

fn commit_payload(lsn: u64) -> Vec<u8> {
    let mut out = Writer::default();
    out.u64(lsn);
    out.u8(COMMIT);
    out.0
}

fn promote_payload(last_lsn: u64, epoch: u64) -> Vec<u8> {
    let mut out = Writer::default();
    out.u64(last_lsn);
    out.u8(PROMOTE);
    out.u64(epoch);
    out.0
}

fn frame(payload: &[u8], into: &mut Vec<u8>) {
    let len = payload.len() as u32;
    into.extend_from_slice(&len.to_le_bytes());
    into.extend_from_slice(&(!len).to_le_bytes());
    into.extend_from_slice(&crc32fast::hash(payload).to_le_bytes());
    into.extend_from_slice(payload);
}

/// A record payload as written: its LSN and entry. Sealed entries stay
/// sealed; plaintext ones decode fully.
pub fn decode_payload(payload: &[u8]) -> Result<(u64, Entry), CodecError> {
    match decode(payload)? {
        Frame::Record(lsn, entry) => Ok((lsn, *entry)),
        Frame::Commit(_) => Err(CodecError("a commit is not a record")),
        Frame::Promote(_) => Err(CodecError("a promotion is not a record")),
    }
}

fn decode(payload: &[u8]) -> Result<Frame, CodecError> {
    let mut input = Reader(payload);
    let lsn = input.u64()?;
    let op = match input.u8()? {
        COMMIT => {
            input.done()?;
            return Ok(Frame::Commit(lsn));
        }
        PROMOTE => {
            let epoch = input.u64()?;
            input.done()?;
            return Ok(Frame::Promote(epoch));
        }
        SEALED => {
            let entry = Entry::Sealed {
                namespace: Namespace::stored(input.str()?),
                dek_id: input.str()?,
                body: input.bytes()?.to_vec(),
            };
            input.done()?;
            return Ok(Frame::Record(lsn, Box::new(entry)));
        }
        1 => {
            let ns = Namespace::stored(input.str()?);
            Op::Upsert(ns, decode_entity(&mut input)?)
        }
        2 => Op::Delete {
            namespace: Namespace::stored(input.str()?),
            collection: input.str()?,
            id: input.str()?,
        },
        FENCE_PUT => {
            let ns = Namespace::stored(input.str()?);
            let mut fence = decode_fence(&mut input)?;
            if !input.0.is_empty() {
                fence.labels = decode_labels(&mut input)?;
            }
            Op::FencePut(ns, fence)
        }
        FENCE_DELETE => Op::FenceDelete {
            namespace: Namespace::stored(input.str()?),
            id: input.str()?,
        },
        _ => return Err(CodecError("op")),
    };
    input.done()?;
    Ok(Frame::Record(lsn, Box::new(Entry::Op(op))))
}

/// Whether `op` encodes within the record size recovery accepts, with room
/// for sealing (namespace, key id, nonce and tag).
pub fn fits(op: &Op) -> bool {
    encode(0, op).len() + 1024 <= MAX_RECORD as usize
}

/// The payload of a sealed record.
fn sealed_payload(lsn: u64, namespace: &str, dek_id: &str, body: &[u8]) -> Vec<u8> {
    let mut out = Writer::default();
    out.u64(lsn);
    out.u8(SEALED);
    out.str(namespace);
    out.str(dek_id);
    out.bytes(body);
    out.0
}

/// Seals one operation, given its LSN and plaintext payload: the key id and
/// sealed body, or `None` to log it in plaintext.
pub type Sealer<'a> = dyn FnMut(u64, &Op, &[u8]) -> Result<Option<(String, Vec<u8>)>, String> + 'a;

/// Sync a directory so a created, renamed or removed entry is durable.
pub(crate) fn sync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

fn segment_path(dir: &Path, first_lsn: u64) -> PathBuf {
    dir.join(format!("{first_lsn:020}.seg"))
}

/// Segments in log order with their first LSN.
fn segments(dir: &Path) -> io::Result<Vec<(u64, PathBuf)>> {
    let mut found = Vec::new();
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        if path.extension().is_some_and(|ext| ext == "seg")
            && let Some(lsn) = path
                .file_stem()
                .and_then(|stem| stem.to_str())
                .and_then(|stem| stem.parse::<u64>().ok())
        {
            found.push((lsn, path));
        }
    }
    found.sort();
    Ok(found)
}

/// `wal/EPOCH`: the log's epoch in two slots `[epoch u64][crc32 u32]`, epoch
/// `e` in slot `e % 2`, so a torn raise leaves the epoch before it readable.
/// Written in place and never replaced, so every open handle sees a raise.
/// Its lock orders writes against promotions: shared to write, exclusive to
/// open or promote.
struct EpochFile(File);

const EPOCH_SLOT: usize = 12;

impl EpochFile {
    /// Open, creating, the epoch file of `dir` and take its exclusive lock.
    /// Dropping the handle on an error releases the lock.
    fn lock(dir: &Path) -> Result<Self, WalError> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(dir.join("EPOCH"))?;
        file.lock()?;
        let epochs = Self(file);
        // Until a first slot is whole, no raise has happened: the epoch is 0,
        // and slot 0 says so before a raise writes slot 1.
        if epochs.0.metadata()?.len() < EPOCH_SLOT as u64 {
            epochs.write(0)?;
            sync_dir(dir)?;
        }
        Ok(epochs)
    }

    /// The newest epoch a whole slot holds.
    fn read(&self) -> Result<u64, WalError> {
        let mut bytes = Vec::new();
        (&self.0).seek(SeekFrom::Start(0))?;
        (&self.0).read_to_end(&mut bytes)?;
        let newest = bytes
            .as_chunks::<EPOCH_SLOT>()
            .0
            .iter()
            .filter_map(|slot| {
                let (epoch, crc) = slot.split_first_chunk::<8>()?;
                (crc32fast::hash(epoch).to_le_bytes() == crc).then(|| u64::from_le_bytes(*epoch))
            })
            .max();
        match newest {
            Some(epoch) => Ok(epoch),
            // Only the first write, of epoch 0, can have torn this short.
            None if bytes.len() <= EPOCH_SLOT => Ok(0),
            None => Err(WalError::Corrupt("EPOCH: no readable slot".into())),
        }
    }

    /// Record `epoch` durably, whatever the log's fsync setting.
    fn write(&self, epoch: u64) -> Result<(), WalError> {
        let bytes = epoch.to_le_bytes();
        let slot = [&bytes[..], &crc32fast::hash(&bytes).to_le_bytes()].concat();
        (&self.0).seek(SeekFrom::Start(epoch % 2 * EPOCH_SLOT as u64))?;
        (&self.0).write_all(&slot)?;
        self.0.sync_data()?;
        Ok(())
    }
}

pub struct Wal {
    dir: PathBuf,
    file: File,
    segment_bytes: u64,
    written: u64,
    fsync: bool,
    next_lsn: u64,
    /// The epoch this writer was opened at; it writes only while the log's
    /// is the same.
    epoch: u64,
    epochs: EpochFile,
    /// Whether this writer holds the epoch file's shared lock, checked.
    fence_held: bool,
}

impl Wal {
    /// Read every committed record (see the module docs for what is cut off
    /// or refused), then open a fresh segment for appends after the last LSN
    /// or after `floor`, the LSN a snapshot covers, if that is later. A
    /// superseded log (`read == false`, after an authoritative rebuild) is
    /// not read: appends start past every LSN the existing segments could
    /// hold (bounded by their sizes), so no discarded LSN is reused, and the
    /// next snapshot's drop removes the old ones. The writer takes the log's
    /// epoch, or the highest a promotion frame records if the epoch file
    /// lost it.
    pub fn open(
        dir: &Path,
        floor: u64,
        segment_bytes: u64,
        fsync: bool,
        read: bool,
    ) -> Result<(Self, Vec<Record>), WalError> {
        fs::create_dir_all(dir)?;
        // Held while the log is read and the new segment started, so no
        // other writer appends or rotates in between.
        let epochs = EpochFile::lock(dir)?;
        let found = segments(dir)?;
        let mut records = Vec::new();
        // The highest epoch a promotion frame read records.
        let mut logged = 0;
        let next_lsn = if read {
            read_log(&found, floor, &mut records, &mut logged)?;
            records
                .last()
                .map_or(0, |record| record.lsn)
                .max(floor)
                .saturating_add(1)
        } else {
            let mut last = floor;
            for (first, path) in &found {
                // Every frame takes at least MIN_FRAME bytes, so a segment
                // holds no LSN past this, however damaged it is.
                let frames = fs::metadata(path)?.len() / MIN_FRAME;
                last = last.max(first.saturating_add(frames));
                // Records are not replayed, but the promotion history still
                // counts: a rebuilt directory keeps its epoch.
                logged = logged.max(segment_epoch(path)?);
            }
            last.saturating_add(1)
        };
        let mut epoch = epochs.read()?;
        if logged > epoch {
            // The epoch file was lost or replaced; the log still knows.
            epochs.write(logged)?;
            epoch = logged;
        }
        let (file, written) = new_segment(dir, next_lsn, epoch)?;
        let wal = Self {
            dir: dir.to_path_buf(),
            file,
            segment_bytes,
            written,
            fsync,
            next_lsn,
            epoch,
            epochs,
            fence_held: false,
        };
        wal.epochs.0.unlock()?;
        Ok((wal, records))
    }

    /// Raise the epoch of the log in `dir` by one and return it. Every
    /// writer opened before is fenced: its next write is refused. A write in
    /// flight finishes first. The next open records the raise in the log.
    ///
    /// `owner_lock` is the directory lock an owner holds. When that owner
    /// does not hold `OWNER` (see the module docs), it ignores epochs and the
    /// promotion is refused, before anything changes if it already runs. One
    /// that starts before the log is marked is caught by a second check
    /// after marking: the epoch is then not raised, and the marked log stays
    /// refused by that release from its next open. One that starts after the
    /// marking refuses to open the log at all. So no such owner writes past
    /// a raise.
    pub fn promote(dir: &Path, owner_lock: &Path) -> Result<u64, WalError> {
        fs::create_dir_all(dir)?;
        let epochs = EpochFile::lock(dir)?;
        // From the highest the log records too, as an open would restore it
        // should the epoch file have lost it.
        let mut current = epochs.read()?;
        for (_, path) in segments(dir)? {
            current = current.max(segment_epoch(&path)?);
        }
        let epoch = current
            .checked_add(1)
            .ok_or_else(|| WalError::Corrupt("EPOCH: no epoch after the last".into()))?;
        refuse_legacy_owner(dir, owner_lock)?;
        // The log is marked as the epoch format before the epoch rises, so a
        // binary that ignores the epoch refuses it from the raise on; a crash
        // in between leaves a marked log at the old epoch.
        mark_promoted(dir)?;
        refuse_legacy_owner(dir, owner_lock)?;
        epochs.write(epoch)?;
        epochs.0.unlock()?;
        Ok(epoch)
    }

    /// Mark the log in `dir` as held, for its life, by an owner that honors
    /// epochs; the lock is released when the returned file is dropped.
    pub fn hold_owner(dir: &Path) -> Result<File, WalError> {
        let owner = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(dir.join("OWNER"))?;
        owner.lock_shared()?;
        Ok(owner)
    }

    /// Refuse, as an append would, when the log was promoted past this
    /// writer's epoch; for writes that log nothing.
    pub fn check_fence(&mut self) -> Result<(), WalError> {
        self.fenced(|_| Ok(()))
    }

    /// Run a durable change kept outside the log (a key rotation) under the
    /// same fence as an append, so a promotion cannot slip in between the
    /// check and the change.
    pub fn under_fence<T>(&mut self, change: impl FnOnce() -> T) -> Result<T, WalError> {
        self.fenced(|_| Ok(change()))
    }

    /// The epoch this writer holds.
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// The LSN the last appended record took.
    pub fn last_lsn(&self) -> u64 {
        self.next_lsn - 1
    }

    /// Take the epoch file's shared lock once the log's epoch is checked to
    /// still be this writer's, and keep it until [`Self::release_fence`]: a
    /// promotion waits for the release, so everything done in between, in
    /// memory or on disk, comes before the raise. Appends and rotations
    /// while held do not lock again.
    pub fn hold_fence(&mut self) -> Result<(), WalError> {
        if self.fence_held {
            return Ok(());
        }
        self.epochs.0.lock_shared()?;
        let checked = match self.epochs.read() {
            Ok(directory) if directory == self.epoch => Ok(()),
            Ok(directory) if directory > self.epoch => Err(WalError::Fenced {
                epoch: self.epoch,
                directory,
            }),
            Ok(directory) => Err(WalError::Corrupt(format!(
                "EPOCH: went back from {} to {directory}",
                self.epoch
            ))),
            Err(error) => Err(error),
        };
        match checked {
            Ok(()) => {
                self.fence_held = true;
                Ok(())
            }
            Err(error) => {
                self.epochs.0.unlock()?;
                Err(error)
            }
        }
    }

    /// Release what [`Self::hold_fence`] took.
    pub fn release_fence(&mut self) -> Result<(), WalError> {
        if self.fence_held {
            self.fence_held = false;
            self.epochs.0.unlock()?;
        }
        Ok(())
    }

    /// Run `write` under the fence: within a held one as it is, otherwise
    /// holding it just for `write`.
    fn fenced<T>(
        &mut self,
        write: impl FnOnce(&mut Self) -> Result<T, WalError>,
    ) -> Result<T, WalError> {
        if self.fence_held {
            return write(self);
        }
        self.hold_fence()?;
        let result = write(self);
        self.release_fence()?;
        result
    }

    /// Append a batch and sync it once; returns the batch's last LSN.
    pub fn append(&mut self, ops: &[Op], seal: &mut Sealer<'_>) -> Result<u64, WalError> {
        self.fenced(|wal| wal.append_unfenced(ops, seal))
    }

    fn append_unfenced(&mut self, ops: &[Op], seal: &mut Sealer<'_>) -> Result<u64, WalError> {
        if self.written >= self.segment_bytes {
            self.rotate_unfenced()?;
        }
        let mut frames = Vec::new();
        let mut lsn = self.next_lsn;
        for op in ops {
            let plain = encode(lsn, op);
            let payload = match seal(lsn, op, &plain).map_err(WalError::Seal)? {
                Some((dek_id, body)) => {
                    let namespace = op.namespace();
                    sealed_payload(lsn, namespace.as_str(), &dek_id, &body)
                }
                None => plain,
            };
            frame(&payload, &mut frames);
            lsn += 1;
        }
        // Sealing happened before any byte was written, so a refusal leaves
        // the log and its LSNs untouched.
        self.next_lsn = lsn;
        self.file.write_all(&frames)?;
        if self.fsync {
            self.file.sync_data()?;
        }
        // The commit frame goes out only once the records are durable, so a
        // commit on disk always vouches for complete records before it.
        let mut commit = Vec::new();
        frame(&commit_payload(self.last_lsn()), &mut commit);
        self.file.write_all(&commit)?;
        if self.fsync {
            self.file.sync_data()?;
        }
        let frame = [frames, commit].concat();
        self.written += frame.len() as u64;
        Ok(self.last_lsn())
    }

    /// Start a new segment, so earlier ones can be dropped whole.
    pub fn rotate(&mut self) -> Result<(), WalError> {
        self.fenced(Self::rotate_unfenced)
    }

    fn rotate_unfenced(&mut self) -> Result<(), WalError> {
        self.file.sync_data()?;
        let (file, written) = new_segment(&self.dir, self.next_lsn, self.epoch)?;
        self.file = file;
        self.written = written;
        Ok(())
    }

    /// Remove segments whose records all have LSN at most `lsn`. The current
    /// segment is kept.
    pub fn drop_through(&mut self, lsn: u64) -> Result<(), WalError> {
        let found = segments(&self.dir)?;
        for pair in found.windows(2) {
            let ((_, path), (next_first, _)) = (&pair[0], &pair[1]);
            if *next_first <= lsn + 1 {
                fs::remove_file(path)?;
            }
        }
        sync_dir(&self.dir)?;
        Ok(())
    }

    /// Bytes held by all segments.
    pub fn size(&self) -> Result<u64, WalError> {
        let mut total = 0;
        for (_, path) in segments(&self.dir)? {
            total += fs::metadata(path)?.len();
        }
        Ok(total)
    }
}

/// Create the segment for records from `first_lsn`, begun with a promotion
/// frame and a version 3 header when `epoch` is past 0, otherwise a version 2
/// header; returns it and its length.
fn new_segment(dir: &Path, first_lsn: u64, epoch: u64) -> Result<(File, u64), WalError> {
    let path = segment_path(dir, first_lsn);
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&path)?;
    let version = if epoch > 0 {
        VERSION
    } else {
        UNPROMOTED_VERSION
    };
    let mut start = [&MAGIC[..], &version.to_le_bytes()].concat();
    if epoch > 0 {
        frame(
            &promote_payload(first_lsn.saturating_sub(1), epoch),
            &mut start,
        );
    }
    file.write_all(&start)?;
    file.sync_all()?;
    sync_dir(dir)?;
    Ok((file, start.len() as u64))
}

/// The epoch the promotion frame starting the segment at `path` records, or
/// 0 without one; only its first frame is read.
fn segment_epoch(path: &Path) -> Result<u64, WalError> {
    let mut start = Vec::new();
    // The header, then a frame header and a promotion payload.
    File::open(path)?
        .take(HEADER + 12 + 17)
        .read_to_end(&mut start)?;
    Ok(match parse_frame(&start, HEADER as usize) {
        Some((Frame::Promote(epoch), _)) => epoch,
        _ => 0,
    })
}

/// Refuse when `owner_lock` is held by an owner without a shared lock on
/// `OWNER`: one of a release that ignores epochs.
fn refuse_legacy_owner(dir: &Path, owner_lock: &Path) -> Result<(), WalError> {
    let try_exclusive = |path: &Path| -> Result<bool, WalError> {
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(path)?;
        match file.try_lock() {
            Ok(()) => {
                file.unlock()?;
                Ok(true)
            }
            Err(fs::TryLockError::WouldBlock) => Ok(false),
            Err(fs::TryLockError::Error(error)) => Err(error.into()),
        }
    };
    if !try_exclusive(owner_lock)? && try_exclusive(&dir.join("OWNER"))? {
        return Err(WalError::LegacyOwner);
    }
    Ok(())
}

/// Set every segment's header to [`VERSION`], creating an empty first
/// segment when there is none, so no binary from before epochs opens the
/// log. Only header bytes change: a writer appending through its own handle
/// writes past them, at its own offset.
fn mark_promoted(dir: &Path) -> Result<(), WalError> {
    let found = segments(dir)?;
    if found.is_empty() {
        // Read as a segment holding nothing, which a snapshot may cover.
        let mut file = File::create_new(segment_path(dir, 1))?;
        file.write_all(&[&MAGIC[..], &VERSION.to_le_bytes()].concat())?;
        file.sync_all()?;
    }
    for (_, path) in &found {
        let mut file = OpenOptions::new().read(true).write(true).open(path)?;
        let mut header = Vec::new();
        (&mut file).take(HEADER).read_to_end(&mut header)?;
        let at = match header.len() as u64 {
            // A header that never reached the disk: no frames follow it.
            short if short < HEADER => 0,
            _ if &header[..4] != MAGIC => {
                return Err(WalError::Corrupt(format!(
                    "{}: not a segment",
                    path.display()
                )));
            }
            _ if u32::from_le_bytes([header[4], header[5], header[6], header[7]]) >= VERSION => {
                continue;
            }
            _ => 4,
        };
        let full = [&MAGIC[..], &VERSION.to_le_bytes()].concat();
        file.seek(SeekFrom::Start(at))?;
        file.write_all(&full[at as usize..])?;
        file.sync_all()?;
    }
    sync_dir(dir)?;
    Ok(())
}

/// Read the committed records of the segments `found` (see [`Wal::open`])
/// into `records`, and the highest epoch their promotion frames record into
/// `logged`.
fn read_log(
    found: &[(u64, PathBuf)],
    floor: u64,
    records: &mut Vec<Record>,
    logged: &mut u64,
) -> Result<(), WalError> {
    let last = found.len().saturating_sub(1);
    // The LSN the next kept segment must start at: right after the
    // snapshot, then right after the previous segment's records.
    let mut expected: Option<u64> = None;
    for (index, (first, path)) in found.iter().enumerate() {
        // Wholly covered by the snapshot: the next segment starts at or
        // before the first LSN after `floor`. Such a segment may be a
        // leftover of an interrupted drop and is not read.
        if found
            .get(index + 1)
            .is_some_and(|(next_first, _)| *next_first <= floor + 1)
        {
            continue;
        }
        let fits = match expected {
            Some(expected) => *first == expected,
            None => *first <= floor + 1,
        };
        if !fits {
            return Err(WalError::Corrupt(format!(
                "{}: segment does not continue the log",
                path.display()
            )));
        }
        let before = records.len();
        read_segment(path, index == last, records, logged)?;
        if let Some(record) = records.get(before)
            && record.lsn != *first
        {
            return Err(WalError::Corrupt(format!(
                "{}: first record is not the segment's",
                path.display()
            )));
        }
        expected = Some(
            records
                .last()
                .map_or(*first, |record| record.lsn + 1)
                .max(*first),
        );
    }
    Ok(())
}

fn read_segment(
    path: &Path,
    last: bool,
    records: &mut Vec<Record>,
    logged: &mut u64,
) -> Result<(), WalError> {
    let mut bytes = Vec::new();
    File::open(path)?.read_to_end(&mut bytes)?;
    if bytes.len() < HEADER as usize {
        // A segment whose header never reached the disk.
        if last {
            return truncate(path, 0);
        }
        return Err(WalError::Corrupt(format!(
            "{}: short header",
            path.display()
        )));
    }
    if &bytes[..4] != MAGIC {
        return Err(WalError::Corrupt(format!(
            "{}: not a segment",
            path.display()
        )));
    }
    let version = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
    if !(1..=VERSION).contains(&version) {
        return Err(WalError::Corrupt(format!(
            "{}: format version {version}",
            path.display()
        )));
    }
    // Records of the batch being read; they count once its commit is seen.
    let mut pending: Vec<Record> = Vec::new();
    let mut committed_end = HEADER as usize;
    let mut offset = HEADER as usize;
    while offset < bytes.len() {
        let parsed = parse_frame(&bytes, offset);
        let Some((frame, next)) = parsed else {
            // Unreadable from here on. If a sound commit frame still follows,
            // this was acknowledged data; otherwise it is an unfinished batch.
            if !last || commit_after(&bytes, offset) {
                return Err(WalError::Corrupt(format!(
                    "{}: damaged record",
                    path.display()
                )));
            }
            return truncate(path, committed_end);
        };
        match frame {
            Frame::Record(lsn, entry) => {
                let previous = pending.last().or(records.last()).map(|record| record.lsn);
                // LSNs are contiguous; a gap is a lost segment, not a skip.
                if previous.is_some_and(|previous| lsn != previous + 1) {
                    return Err(WalError::Corrupt(format!("{}: lsn gap", path.display())));
                }
                pending.push(Record {
                    lsn,
                    entry: *entry,
                    payload: bytes[offset + 12..next].to_vec(),
                });
            }
            Frame::Commit(lsn) => {
                if pending.last().map(|record| record.lsn) != Some(lsn) {
                    return Err(WalError::Corrupt(format!(
                        "{}: commit does not close its batch",
                        path.display()
                    )));
                }
                records.append(&mut pending);
                committed_end = next;
            }
            Frame::Promote(epoch) => {
                // Written between batches, never inside one.
                if !pending.is_empty() {
                    return Err(WalError::Corrupt(format!(
                        "{}: promotion inside a batch",
                        path.display()
                    )));
                }
                *logged = (*logged).max(epoch);
                committed_end = next;
            }
        }
        offset = next;
    }
    if !pending.is_empty() {
        // Records with no commit: an unfinished batch, only possible last.
        if !last {
            return Err(WalError::Corrupt(format!(
                "{}: uncommitted batch",
                path.display()
            )));
        }
        return truncate(path, committed_end);
    }
    Ok(())
}

/// Cut an unfinished batch off the end of a segment.
fn truncate(path: &Path, len: usize) -> Result<(), WalError> {
    let file = OpenOptions::new().write(true).open(path)?;
    file.set_len(len as u64)?;
    file.sync_all()?;
    Ok(())
}

/// The frame at `offset` and where the next starts, if it is sound.
fn parse_frame(bytes: &[u8], offset: usize) -> Option<(Frame, usize)> {
    let header = bytes.get(offset..offset + 12)?;
    let word = |at: usize| {
        u32::from_le_bytes([header[at], header[at + 1], header[at + 2], header[at + 3]])
    };
    let (len, check, crc) = (word(0), word(4), word(8));
    if check != !len || len > MAX_RECORD {
        return None;
    }
    let end = offset + 12 + len as usize;
    let payload = bytes.get(offset + 12..end)?;
    if crc32fast::hash(payload) != crc {
        return None;
    }
    Some((decode(payload).ok()?, end))
}

/// Whether a sound commit or promotion frame starts anywhere after `offset`.
fn commit_after(bytes: &[u8], offset: usize) -> bool {
    (offset + 1..bytes.len()).any(|at| {
        matches!(
            parse_frame(bytes, at),
            Some((Frame::Commit(_) | Frame::Promote(_), _))
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;
    use std::time::Duration;

    #[test]
    fn a_promotion_waits_for_a_held_fence_and_then_fences_its_holder() {
        let dir = std::env::temp_dir().join(format!(
            "ezu-wal-fence-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = fs::remove_dir_all(&dir);
        let (mut wal, _) = Wal::open(&dir, 0, 1 << 20, false, true).unwrap();
        wal.hold_fence().unwrap();
        let (done, promoted) = mpsc::channel();
        let promoting = dir.clone();
        let promoter = std::thread::spawn(move || {
            let lock = promoting.join("LOCK");
            done.send(Wal::promote(&promoting, &lock).map_err(|e| e.to_string()))
                .unwrap();
        });
        // Whatever the holder does now comes before the raise.
        assert!(promoted.recv_timeout(Duration::from_millis(200)).is_err());
        wal.release_fence().unwrap();
        assert_eq!(
            promoted.recv_timeout(Duration::from_secs(30)).unwrap(),
            Ok(1)
        );
        promoter.join().unwrap();
        assert!(matches!(
            wal.hold_fence(),
            Err(WalError::Fenced {
                epoch: 0,
                directory: 1
            })
        ));
        let _ = fs::remove_dir_all(&dir);
    }
}
