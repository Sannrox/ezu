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

use crate::codec::{CodecError, Reader, Writer, decode_entity, encode_entity};
use crate::entity::{Entity, Namespace};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

const MAGIC: &[u8; 4] = b"EZUW";
const VERSION: u32 = 1;
const HEADER: u64 = 8;
/// Largest record payload; a frame claiming more is damage, not data.
const MAX_RECORD: u32 = 64 << 20;

#[derive(Debug, Clone, PartialEq)]
pub enum Op {
    Upsert(Namespace, Entity),
    Delete {
        namespace: Namespace,
        collection: String,
        id: String,
    },
}

#[derive(Debug)]
pub enum WalError {
    Io(io::Error),
    /// A complete segment is damaged; recovery would lose acknowledged data.
    Corrupt(String),
}

impl std::fmt::Display for WalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(f, "wal io: {error}"),
            Self::Corrupt(what) => write!(f, "wal corrupt: {what}"),
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
    }
    out.0
}

const COMMIT: u8 = 0xC0;

enum Frame {
    Record(u64, Op),
    /// The batch ending at this LSN is complete and acknowledged.
    Commit(u64),
}

fn commit_payload(lsn: u64) -> Vec<u8> {
    let mut out = Writer::default();
    out.u64(lsn);
    out.u8(COMMIT);
    out.0
}

fn frame(payload: &[u8], into: &mut Vec<u8>) {
    let len = payload.len() as u32;
    into.extend_from_slice(&len.to_le_bytes());
    into.extend_from_slice(&(!len).to_le_bytes());
    into.extend_from_slice(&crc32fast::hash(payload).to_le_bytes());
    into.extend_from_slice(payload);
}

fn decode(payload: &[u8]) -> Result<Frame, CodecError> {
    let mut input = Reader(payload);
    let lsn = input.u64()?;
    let op = match input.u8()? {
        COMMIT => {
            input.done()?;
            return Ok(Frame::Commit(lsn));
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
        _ => return Err(CodecError("op")),
    };
    input.done()?;
    Ok(Frame::Record(lsn, op))
}

/// Whether `op` encodes within the record size recovery accepts.
pub fn fits(op: &Op) -> bool {
    encode(0, op).len() <= MAX_RECORD as usize
}

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

pub struct Wal {
    dir: PathBuf,
    file: File,
    segment_bytes: u64,
    written: u64,
    fsync: bool,
    next_lsn: u64,
}

impl Wal {
    /// Read every intact record, cutting a torn tail off the last segment,
    /// then open a fresh segment for appends after the last LSN (or after
    /// `floor`, the LSN a snapshot already covers, if that is later).
    /// Read every committed record (see the module docs for what is cut off
    /// or refused), then open a fresh segment for appends after the last LSN
    /// or after `floor`, the LSN a snapshot covers, if that is later. A
    /// superseded log (`read == false`, after an authoritative rebuild) is
    /// not read: appends start past every existing segment, and the next
    /// snapshot's drop removes the old ones.
    pub fn open(
        dir: &Path,
        floor: u64,
        segment_bytes: u64,
        fsync: bool,
        read: bool,
    ) -> Result<(Self, Vec<(u64, Op)>), WalError> {
        fs::create_dir_all(dir)?;
        let found = segments(dir)?;
        if !read {
            let next_lsn = found
                .last()
                .map_or(floor, |(first, _)| (*first).max(floor))
                .saturating_add(1);
            let (file, written) = new_segment(dir, next_lsn)?;
            return Ok((
                Self {
                    dir: dir.to_path_buf(),
                    file,
                    segment_bytes,
                    written,
                    fsync,
                    next_lsn,
                },
                Vec::new(),
            ));
        }
        let mut records = Vec::new();
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
            read_segment(path, index == last, &mut records)?;
            if let Some((lsn, _)) = records.get(before)
                && lsn != first
            {
                return Err(WalError::Corrupt(format!(
                    "{}: first record is not the segment's",
                    path.display()
                )));
            }
            expected = Some(
                records
                    .last()
                    .map_or(*first, |(lsn, _)| lsn + 1)
                    .max(*first),
            );
        }
        let next_lsn = records
            .last()
            .map_or(0, |(lsn, _)| *lsn)
            .max(floor)
            .saturating_add(1);
        let (file, written) = new_segment(dir, next_lsn)?;
        Ok((
            Self {
                dir: dir.to_path_buf(),
                file,
                segment_bytes,
                written,
                fsync,
                next_lsn,
            },
            records,
        ))
    }

    /// The LSN the last appended record took.
    pub fn last_lsn(&self) -> u64 {
        self.next_lsn - 1
    }

    /// Append a batch and sync it once; returns the batch's last LSN.
    pub fn append(&mut self, ops: &[Op]) -> Result<u64, WalError> {
        if self.written >= self.segment_bytes {
            self.rotate()?;
        }
        let mut frames = Vec::new();
        for op in ops {
            frame(&encode(self.next_lsn, op), &mut frames);
            self.next_lsn += 1;
        }
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
        self.file.sync_data()?;
        let (file, written) = new_segment(&self.dir, self.next_lsn)?;
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

fn new_segment(dir: &Path, first_lsn: u64) -> Result<(File, u64), WalError> {
    let path = segment_path(dir, first_lsn);
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&path)?;
    file.write_all(MAGIC)?;
    file.write_all(&VERSION.to_le_bytes())?;
    file.sync_all()?;
    sync_dir(dir)?;
    Ok((file, HEADER))
}

fn read_segment(path: &Path, last: bool, records: &mut Vec<(u64, Op)>) -> Result<(), WalError> {
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
    if version != VERSION {
        return Err(WalError::Corrupt(format!(
            "{}: format version {version}",
            path.display()
        )));
    }
    // Records of the batch being read; they count once its commit is seen.
    let mut pending: Vec<(u64, Op)> = Vec::new();
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
            Frame::Record(lsn, op) => {
                let previous = pending.last().or(records.last()).map(|(lsn, _)| *lsn);
                // LSNs are contiguous; a gap is a lost segment, not a skip.
                if previous.is_some_and(|previous| lsn != previous + 1) {
                    return Err(WalError::Corrupt(format!("{}: lsn gap", path.display())));
                }
                pending.push((lsn, op));
            }
            Frame::Commit(lsn) => {
                if pending.last().map(|(last, _)| *last) != Some(lsn) {
                    return Err(WalError::Corrupt(format!(
                        "{}: commit does not close its batch",
                        path.display()
                    )));
                }
                records.append(&mut pending);
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

/// Whether a sound commit frame starts anywhere after `offset`.
fn commit_after(bytes: &[u8], offset: usize) -> bool {
    (offset + 1..bytes.len())
        .any(|at| matches!(parse_frame(bytes, at), Some((Frame::Commit(_), _))))
}
