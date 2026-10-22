//! Snapshots of every namespace as one Arrow IPC file per LSN, geometry as
//! WKB. A snapshot is written to a temporary file, synced, renamed into
//! place and the directory synced, so a crash leaves either the old snapshot
//! or the new one. The format version and LSN live in the schema metadata.

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

const VERSION: &str = "1";
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

fn schema(lsn: u64) -> Schema {
    Schema::new(vec![
        Field::new("namespace", DataType::Utf8, false),
        Field::new("collection", DataType::Utf8, false),
        Field::new("id", DataType::Utf8, false),
        Field::new("geometry", DataType::Binary, false),
        Field::new("ts_ms", DataType::Int64, false),
        Field::new("speed", DataType::Float64, true),
        Field::new("heading", DataType::Float64, true),
        Field::new("ttl_ms", DataType::Int64, true),
        Field::new("fields", DataType::Binary, false),
    ])
    .with_metadata(HashMap::from([
        ("ezu.snapshot.version".to_string(), VERSION.to_string()),
        ("ezu.snapshot.lsn".to_string(), lsn.to_string()),
    ]))
}

fn path(dir: &Path, lsn: u64) -> PathBuf {
    dir.join(format!("{lsn:020}.arrow"))
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

/// Write every entity as the snapshot at `lsn`, then remove older ones.
pub fn write(dir: &Path, lsn: u64, entities: &[(Namespace, Entity)]) -> Result<(), SnapshotError> {
    fs::create_dir_all(dir).map_err(err)?;
    // Every change takes an LSN, so a snapshot already published at this
    // LSN holds the same state; it is kept rather than replaced in place.
    // The directory is synced again first: a previous attempt may have
    // renamed both files and then failed before its sync.
    if path(dir, lsn).exists() && sum_path(dir, lsn).exists() {
        return sync_dir(dir).map_err(err);
    }
    let schema = Arc::new(schema(lsn));
    let temp = dir.join(format!("{lsn:020}.arrow.tmp"));
    let file = File::create(&temp).map_err(err)?;
    let mut writer = FileWriter::try_new(file, &schema).map_err(err)?;
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
        }
        let columns: Vec<ArrayRef> = vec![
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
        let batch = RecordBatch::try_new(schema.clone(), columns).map_err(err)?;
        writer.write(&batch).map_err(err)?;
    }
    let file = writer.into_inner().map_err(err)?;
    file.sync_all().map_err(err)?;
    // Arrow carries no checksum of its buffers: a sidecar CRC, durable
    // before the snapshot goes live, lets recovery tell damage from data.
    let sum = crc32fast::hash(&fs::read(&temp).map_err(err)?);
    let sum_temp = dir.join(format!("{lsn:020}.crc32.tmp"));
    fs::write(&sum_temp, sum.to_string()).map_err(err)?;
    File::open(&sum_temp)
        .map_err(err)?
        .sync_all()
        .map_err(err)?;
    fs::rename(&sum_temp, sum_path(dir, lsn)).map_err(err)?;
    fs::rename(&temp, path(dir, lsn)).map_err(err)?;
    sync_dir(dir).map_err(err)?;
    for (older, stale) in list(dir)? {
        if older < lsn {
            fs::remove_file(stale).map_err(err)?;
            let _ = fs::remove_file(sum_path(dir, older));
        }
    }
    // Left-over temporary files are writes that never finished.
    for entry in fs::read_dir(dir).map_err(err)? {
        let stale = entry.map_err(err)?.path();
        if stale.extension().is_some_and(|ext| ext == "tmp") {
            fs::remove_file(stale).map_err(err)?;
        }
    }
    sync_dir(dir).map_err(err)
}

/// Read the snapshot at `path`: its LSN and every entity.
pub fn read(path: &Path) -> Result<(u64, Vec<(Namespace, Entity)>), SnapshotError> {
    let bytes = fs::read(path).map_err(err)?;
    let expected: Option<u32> = fs::read_to_string(path.with_extension("crc32"))
        .ok()
        .and_then(|sum| sum.trim().parse().ok());
    if expected != Some(crc32fast::hash(&bytes)) {
        return Err(SnapshotError("checksum missing or mismatched".into()));
    }
    let reader = FileReader::try_new(std::io::Cursor::new(bytes), None).map_err(err)?;
    let metadata = reader.schema().metadata().clone();
    if metadata.get("ezu.snapshot.version").map(String::as_str) != Some(VERSION) {
        return Err(SnapshotError("unknown snapshot format version".into()));
    }
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
            };
            // A readable file can still hold damaged values; those make the
            // whole snapshot unusable rather than failing later in the index.
            entity.validate().map_err(err)?;
            entities.push((Namespace::stored(namespace.value(row).to_string()), entity));
        }
    }
    Ok((lsn, entities))
}
