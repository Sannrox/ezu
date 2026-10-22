//! Snapshots, as Arrow IPC with geometry as WKB.
//!
//! Version 2 (written now) is a set per LSN: the directory
//! `snapshots/<lsn>/` holds per namespace a data file and a fences file
//! (fences and memberships), each plaintext or sealed under that
//! namespace's key, an optional tail of raw log payloads carried for a
//! sealed namespace, and a `MANIFEST.json` naming each file with its CRC32. The set is written under `<lsn>.tmp/`, synced, and published by
//! renaming the directory, so a crash leaves the old set or the new one.
//! Version 1 (one file per LSN with a CRC32 sidecar) is still read.

use crate::codec::{decode_fields, encode_fields, geometry_from_wkb, geometry_to_wkb};
use crate::entity::{Entity, Namespace};
use crate::wal::sync_dir;
use arrow_array::builder::{BinaryBuilder, Float64Builder, Int64Builder, StringBuilder};
use arrow_array::{
    Array, ArrayRef, BinaryArray, Float64Array, Int64Array, RecordBatch, StringArray,
};
use arrow_ipc::reader::FileReader;
use arrow_ipc::writer::FileWriter;
use arrow_schema::{DataType, Field, Schema};
use std::collections::HashMap;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Format stamped into the Arrow metadata of data files written now: "3"
/// when any entity carries labels (an extra `labels` column), so a binary
/// from before labels refuses the file rather than drop them; else "2".
const VERSION: &str = "2";
const LABELED: &str = "3";
const BATCH_ROWS: usize = 65_536;

#[derive(Debug)]
pub struct SnapshotError(pub String);

impl std::fmt::Display for SnapshotError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "snapshot: {}", self.0)
    }
}

fn err(error: impl std::fmt::Display) -> SnapshotError {
    SnapshotError(error.to_string())
}

fn schema(lsn: u64, labeled: bool) -> Schema {
    let mut fields = vec![
        Field::new("namespace", DataType::Utf8, false),
        Field::new("collection", DataType::Utf8, false),
        Field::new("id", DataType::Utf8, false),
        Field::new("geometry", DataType::Binary, false),
        Field::new("ts_ms", DataType::Int64, false),
        Field::new("speed", DataType::Float64, true),
        Field::new("heading", DataType::Float64, true),
        Field::new("ttl_ms", DataType::Int64, true),
        Field::new("fields", DataType::Binary, false),
    ];
    if labeled {
        fields.push(Field::new("labels", DataType::Binary, false));
    }
    let version = if labeled { LABELED } else { VERSION };
    Schema::new(fields).with_metadata(HashMap::from([
        ("ezu.snapshot.version".to_string(), version.to_string()),
        ("ezu.snapshot.lsn".to_string(), lsn.to_string()),
    ]))
}

fn sum_path(dir: &Path, lsn: u64) -> PathBuf {
    dir.join(format!("{lsn:020}.crc32"))
}

/// Snapshot files, newest first.
pub fn list(dir: &Path) -> Result<Vec<(u64, PathBuf)>, SnapshotError> {
    let mut found = Vec::new();
    for entry in fs::read_dir(dir).map_err(err)? {
        let path = entry.map_err(err)?.path();
        if path.extension().is_some_and(|ext| ext == "arrow")
            && let Some(lsn) = path
                .file_stem()
                .and_then(|stem| stem.to_str())
                .and_then(|stem| stem.parse::<u64>().ok())
        {
            found.push((lsn, path));
        }
    }
    found.sort_by_key(|(lsn, _)| std::cmp::Reverse(*lsn));
    Ok(found)
}

/// Entities as Arrow IPC file bytes, stamped with the format and `lsn`.
pub fn encode_arrow(lsn: u64, entities: &[(Namespace, Entity)]) -> Result<Vec<u8>, SnapshotError> {
    let labeled = entities.iter().any(|(_, entity)| !entity.labels.is_empty());
    let schema = Arc::new(schema(lsn, labeled));
    let mut writer = FileWriter::try_new(Vec::new(), &schema).map_err(err)?;
    for chunk in entities.chunks(BATCH_ROWS) {
        let mut namespace = StringBuilder::new();
        let mut collection = StringBuilder::new();
        let mut id = StringBuilder::new();
        let mut geometry = BinaryBuilder::new();
        let mut ts = Int64Builder::new();
        let mut speed = Float64Builder::new();
        let mut heading = Float64Builder::new();
        let mut ttl = Int64Builder::new();
        let mut fields = BinaryBuilder::new();
        let mut labels = BinaryBuilder::new();
        for (ns, entity) in chunk {
            namespace.append_value(ns.as_str());
            collection.append_value(&entity.collection);
            id.append_value(&entity.id);
            geometry.append_value(geometry_to_wkb(&entity.geometry));
            ts.append_value(entity.ts_ms);
            speed.append_option(entity.speed);
            heading.append_option(entity.heading);
            ttl.append_option(entity.ttl_ms);
            fields.append_value(encode_fields(&entity.fields));
            let mut encoded = crate::codec::Writer::default();
            crate::codec::encode_labels(&mut encoded, &entity.labels);
            labels.append_value(encoded.0);
        }
        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(namespace.finish()),
            Arc::new(collection.finish()),
            Arc::new(id.finish()),
            Arc::new(geometry.finish()),
            Arc::new(ts.finish()),
            Arc::new(speed.finish()),
            Arc::new(heading.finish()),
            Arc::new(ttl.finish()),
            Arc::new(fields.finish()),
        ];
        if labeled {
            columns.push(Arc::new(labels.finish()));
        }
        let batch = RecordBatch::try_new(schema.clone(), columns).map_err(err)?;
        writer.write(&batch).map_err(err)?;
    }
    writer.into_inner().map_err(err)
}

/// Read a version 1 snapshot file: its LSN and every entity.
pub fn read_v1(path: &Path) -> Result<(u64, Vec<(Namespace, Entity)>), SnapshotError> {
    let bytes = fs::read(path).map_err(err)?;
    let expected: Option<u32> = fs::read_to_string(path.with_extension("crc32"))
        .ok()
        .and_then(|sum| sum.trim().parse().ok());
    if expected != Some(crc32fast::hash(&bytes)) {
        return Err(SnapshotError("checksum missing or mismatched".into()));
    }
    decode_arrow(bytes, &["1"])
}

/// Entities from Arrow IPC bytes of one of the formats `versions`, with
/// their LSN.
pub fn decode_arrow(
    bytes: Vec<u8>,
    versions: &[&str],
) -> Result<(u64, Vec<(Namespace, Entity)>), SnapshotError> {
    let reader = FileReader::try_new(std::io::Cursor::new(bytes), None).map_err(err)?;
    let metadata = reader.schema().metadata().clone();
    let version = metadata
        .get("ezu.snapshot.version")
        .map(String::as_str)
        .filter(|version| versions.contains(version))
        .ok_or_else(|| SnapshotError("unknown snapshot format version".into()))?;
    let labeled = version == LABELED;
    let lsn: u64 = metadata
        .get("ezu.snapshot.lsn")
        .and_then(|lsn| lsn.parse().ok())
        .ok_or_else(|| SnapshotError("snapshot lsn missing".into()))?;
    let mut entities = Vec::new();
    for batch in reader {
        let batch = batch.map_err(err)?;
        let column = |i: usize| batch.column(i).clone();
        let text = |i: usize| -> Result<StringArray, SnapshotError> {
            column(i)
                .as_any()
                .downcast_ref::<StringArray>()
                .cloned()
                .ok_or_else(|| SnapshotError("column type".into()))
        };
        let binary = |i: usize| -> Result<BinaryArray, SnapshotError> {
            column(i)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .cloned()
                .ok_or_else(|| SnapshotError("column type".into()))
        };
        let int = |i: usize| -> Result<Int64Array, SnapshotError> {
            column(i)
                .as_any()
                .downcast_ref::<Int64Array>()
                .cloned()
                .ok_or_else(|| SnapshotError("column type".into()))
        };
        let float = |i: usize| -> Result<Float64Array, SnapshotError> {
            column(i)
                .as_any()
                .downcast_ref::<Float64Array>()
                .cloned()
                .ok_or_else(|| SnapshotError("column type".into()))
        };
        let (namespace, collection, id) = (text(0)?, text(1)?, text(2)?);
        let (geometry, ts, speed, heading, ttl, fields) = (
            binary(3)?,
            int(4)?,
            float(5)?,
            float(6)?,
            int(7)?,
            binary(8)?,
        );
        let labels = labeled.then(|| binary(9)).transpose()?;
        for row in 0..batch.num_rows() {
            let maybe = |array: &Float64Array| (!array.is_null(row)).then(|| array.value(row));
            let entity = Entity {
                collection: collection.value(row).to_string(),
                id: id.value(row).to_string(),
                geometry: geometry_from_wkb(geometry.value(row)).map_err(err)?,
                ts_ms: ts.value(row),
                speed: maybe(&speed),
                heading: maybe(&heading),
                ttl_ms: (!ttl.is_null(row)).then(|| ttl.value(row)),
                fields: decode_fields(fields.value(row)).map_err(err)?,
                labels: match &labels {
                    Some(labels) => {
                        let mut input = crate::codec::Reader(labels.value(row));
                        let decoded = crate::codec::decode_labels(&mut input).map_err(err)?;
                        input.done().map_err(err)?;
                        decoded
                    }
                    None => Default::default(),
                },
            };
            // A readable file can still hold damaged values; those make the
            // whole snapshot unusable rather than failing later in the index.
            entity.validate().map_err(err)?;
            entities.push((Namespace::stored(namespace.value(row).to_string()), entity));
        }
    }
    Ok((lsn, entities))
}

/// How a sealed data file was sealed: under which key, and at which LSN (the
/// LSN is bound into the seal, so a carried file keeps its own).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SealInfo {
    pub dek_id: String,
    pub sealed_lsn: u64,
}

/// A namespace's data file: Arrow IPC bytes, or those bytes sealed.
#[derive(Debug, Clone, PartialEq)]
pub struct DataFile {
    pub sealed: Option<SealInfo>,
    pub bytes: Vec<u8>,
}

/// One namespace's part of a snapshot set: its data file, its fences and
/// memberships, and for a sealed namespace the raw log payloads written
/// after those, carried unread.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Part {
    pub namespace: String,
    pub file: Option<DataFile>,
    pub fences: Option<DataFile>,
    /// Whether the fences file also holds the namespace's recent events
    /// (written since events were kept); older ones hold fence state only.
    pub fences_with_events: bool,
    pub tail: Vec<Vec<u8>>,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct Manifest {
    version: u32,
    lsn: u64,
    /// Written by binaries that keep events in snapshots; a set without it
    /// kept none, so history before it is unknown.
    #[serde(default)]
    events_kept: bool,
    /// Event ids before this are unknown to every namespace.
    #[serde(default)]
    events_floor: Option<(u64, u32)>,
    parts: Vec<ManifestPart>,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct ManifestPart {
    namespace: String,
    file: Option<ManifestFile>,
    /// Absent in sets written before fences.
    #[serde(default)]
    fences: Option<ManifestFile>,
    #[serde(default)]
    fences_with_events: bool,
    tail: Option<ManifestFile>,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct ManifestFile {
    name: String,
    crc32: u32,
    sealed: Option<SealInfo>,
}

/// Authenticated data binding a sealed data file to its LSN, namespace and
/// key.
pub fn sealed_aad(lsn: u64, namespace: &str, dek_id: &str) -> Vec<u8> {
    let mut out = crate::codec::Writer::default();
    out.str("ezu.snapshot");
    out.u64(lsn);
    out.str(namespace);
    out.str(dek_id);
    out.0
}

/// Authenticated data binding a sealed fences file to its LSN, namespace
/// and key.
pub fn fences_aad(lsn: u64, namespace: &str, dek_id: &str) -> Vec<u8> {
    let mut out = crate::codec::Writer::default();
    out.str("ezu.snapshot.fences");
    out.u64(lsn);
    out.str(namespace);
    out.str(dek_id);
    out.0
}

/// The newest snapshot of either format.
pub enum Found {
    V1(PathBuf),
    V2(PathBuf),
}

fn set_lsn(path: &Path) -> Option<u64> {
    path.is_dir()
        .then(|| path.file_name()?.to_str()?.parse::<u64>().ok())
        .flatten()
}

/// The newest published snapshot, set or single file.
pub fn newest(dir: &Path) -> Result<Option<(u64, Found)>, SnapshotError> {
    let mut best: Option<(u64, Found)> = list(dir)?
        .into_iter()
        .next()
        .map(|(lsn, path)| (lsn, Found::V1(path)));
    for entry in fs::read_dir(dir).map_err(err)? {
        let path = entry.map_err(err)?.path();
        if let Some(lsn) = set_lsn(&path)
            && best.as_ref().is_none_or(|(best, _)| lsn > *best)
        {
            best = Some((lsn, Found::V2(path)));
        }
    }
    Ok(best)
}

fn write_synced(path: &Path, bytes: &[u8]) -> Result<u32, SnapshotError> {
    fs::write(path, bytes).map_err(err)?;
    File::open(path).map_err(err)?.sync_all().map_err(err)?;
    Ok(crc32fast::hash(bytes))
}

/// Publish the set at `lsn`, then remove older snapshots of both formats.
/// False when a set at `lsn` already existed and was kept as it is.
pub fn write_set(
    dir: &Path,
    lsn: u64,
    parts: &[Part],
    events_floor: Option<(u64, u32)>,
) -> Result<bool, SnapshotError> {
    fs::create_dir_all(dir).map_err(err)?;
    let target = dir.join(format!("{lsn:020}"));
    // Every change takes an LSN, so a set already published at this LSN
    // holds the same state. The directory is synced again first: an earlier
    // attempt may have renamed it and failed before its sync.
    if target.is_dir() {
        return sync_dir(dir).map(|()| false).map_err(err);
    }
    let temp = dir.join(format!("{lsn:020}.tmp"));
    if temp.exists() {
        fs::remove_dir_all(&temp).map_err(err)?;
    }
    fs::create_dir(&temp).map_err(err)?;
    // Version 3 marks a set with fences and 4 one whose fences files hold
    // events, so a binary from before either refuses the set rather than
    // dropping or misreading them; others stay readable by it.
    let mut manifest = Manifest {
        version: if parts.iter().any(|part| part.fences_with_events) {
            4
        } else if parts.iter().any(|part| part.fences.is_some()) {
            3
        } else {
            2
        },
        lsn,
        events_kept: true,
        events_floor,
        parts: Vec::new(),
    };
    // Files are numbered; the manifest names each part's namespace, so any
    // namespace fits.
    for (index, part) in parts.iter().enumerate() {
        let base = format!("{index:06}");
        let write_file = |file: &Option<DataFile>, suffix: &str| match file {
            Some(file) => {
                let name = format!("{base}.{suffix}");
                Ok::<_, SnapshotError>(Some(ManifestFile {
                    crc32: write_synced(&temp.join(&name), &file.bytes)?,
                    name,
                    sealed: file.sealed.clone(),
                }))
            }
            None => Ok(None),
        };
        let file = write_file(&part.file, "data")?;
        let fences = write_file(&part.fences, "fences")?;
        let tail = if part.tail.is_empty() {
            None
        } else {
            let mut bytes = Vec::new();
            for payload in &part.tail {
                bytes.extend_from_slice(&(payload.len() as u32).to_le_bytes());
                bytes.extend_from_slice(payload);
            }
            let name = format!("{base}.tail");
            Some(ManifestFile {
                crc32: write_synced(&temp.join(&name), &bytes)?,
                name,
                sealed: None,
            })
        };
        manifest.parts.push(ManifestPart {
            namespace: part.namespace.clone(),
            file,
            fences,
            fences_with_events: part.fences_with_events,
            tail,
        });
    }
    let manifest_bytes = serde_json::to_vec(&manifest).map_err(err)?;
    write_synced(&temp.join("MANIFEST.json"), &manifest_bytes)?;
    sync_dir(&temp).map_err(err)?;
    fs::rename(&temp, &target).map_err(err)?;
    sync_dir(dir).map_err(err)?;
    // Older snapshots, and anything an unfinished write left behind.
    for entry in fs::read_dir(dir).map_err(err)? {
        let stale = entry.map_err(err)?.path();
        let older_set = set_lsn(&stale).is_some_and(|older| older < lsn);
        let leftover = stale.extension().is_some_and(|ext| ext == "tmp");
        if older_set || (leftover && stale.is_dir()) {
            fs::remove_dir_all(&stale).map_err(err)?;
        } else if leftover {
            fs::remove_file(&stale).map_err(err)?;
        }
    }
    for (older, stale) in list(dir)? {
        if older < lsn {
            fs::remove_file(stale).map_err(err)?;
            let _ = fs::remove_file(sum_path(dir, older));
        }
    }
    sync_dir(dir).map(|()| true).map_err(err)
}

fn read_checked(dir: &Path, file: &ManifestFile) -> Result<Vec<u8>, SnapshotError> {
    if file.name.contains('/') || file.name.contains("..") {
        return Err(SnapshotError(
            "manifest names a file outside the set".into(),
        ));
    }
    let bytes = fs::read(dir.join(&file.name)).map_err(err)?;
    if crc32fast::hash(&bytes) != file.crc32 {
        return Err(SnapshotError(format!("{}: checksum mismatched", file.name)));
    }
    Ok(bytes)
}

/// A read snapshot set.
pub struct Set {
    pub lsn: u64,
    pub parts: Vec<Part>,
    /// Event ids before this are unknown: the set's own floor, or for a set
    /// from before events were kept, everything up to its LSN.
    pub events_floor: Option<(u64, u32)>,
}

/// Read a set: its LSN and each namespace's part, checksums verified.
pub fn read_set(dir: &Path) -> Result<Set, SnapshotError> {
    let manifest: Manifest =
        serde_json::from_slice(&fs::read(dir.join("MANIFEST.json")).map_err(err)?).map_err(err)?;
    if !(2..=4).contains(&manifest.version) || set_lsn(dir) != Some(manifest.lsn) {
        return Err(SnapshotError("manifest does not match its set".into()));
    }
    let mut parts = Vec::new();
    for part in manifest.parts {
        let read_file = |file: &Option<ManifestFile>| match file {
            Some(file) => Ok::<_, SnapshotError>(Some(DataFile {
                sealed: file.sealed.clone(),
                bytes: read_checked(dir, file)?,
            })),
            None => Ok(None),
        };
        let file = read_file(&part.file)?;
        let fences = read_file(&part.fences)?;
        let mut tail = Vec::new();
        if let Some(tail_file) = &part.tail {
            let bytes = read_checked(dir, tail_file)?;
            let mut offset = 0;
            while offset < bytes.len() {
                let len = bytes
                    .get(offset..offset + 4)
                    .map(|b| u32::from_le_bytes([b[0], b[1], b[2], b[3]]) as usize)
                    .ok_or_else(|| SnapshotError("torn tail".into()))?;
                let payload = bytes
                    .get(offset + 4..offset + 4 + len)
                    .ok_or_else(|| SnapshotError("torn tail".into()))?;
                tail.push(payload.to_vec());
                offset += 4 + len;
            }
        }
        parts.push(Part {
            namespace: part.namespace,
            file,
            fences,
            fences_with_events: part.fences_with_events,
            tail,
        });
    }
    Ok(Set {
        lsn: manifest.lsn,
        parts,
        events_floor: match manifest.events_kept {
            true => manifest.events_floor,
            false => Some((manifest.lsn, u32::MAX)),
        },
    })
}

/// Plaintext entities from a data file, which must not be sealed.
pub fn plain_entities(file: &DataFile) -> Result<Vec<(Namespace, Entity)>, SnapshotError> {
    if file.sealed.is_some() {
        return Err(SnapshotError("the data file is sealed".into()));
    }
    decode_arrow(file.bytes.clone(), &[VERSION, LABELED]).map(|(_, entities)| entities)
}

/// Entities from Arrow bytes written now (after any unsealing).
pub fn current_entities(bytes: Vec<u8>) -> Result<Vec<(Namespace, Entity)>, SnapshotError> {
    decode_arrow(bytes, &[VERSION, LABELED]).map(|(_, entities)| entities)
}
