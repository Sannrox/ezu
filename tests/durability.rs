//! Durability (#5): acknowledged writes survive crashes at random points,
//! the WAL stays bounded under steady load, a lost snapshot is rebuilt from
//! the source hook, and one process owns a directory.

use ezu::{
    DurabilityConfig, DurableEngine, DurableError, Entity, FieldValue, Geometry, Identity,
    Namespace, Position, Query, QueryRequest, Quotas, RebuildSource,
};
use std::collections::BTreeMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

static SEQ: AtomicU64 = AtomicU64::new(0);

fn scratch(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "ezu-durability-{name}-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = fs::remove_dir_all(&dir);
    dir
}

fn config(dir: &Path, snapshot_every_records: u64) -> DurabilityConfig {
    DurabilityConfig {
        dir: dir.to_path_buf(),
        fsync: true,
        segment_bytes: 4_096,
        snapshot_every_records,
        snapshot_every_ms: i64::MAX,
        events_retained: 1_000,
        keys: None,
    }
}

fn quotas() -> Quotas {
    Quotas {
        max_entities: 100_000,
        max_collections: 8,
        max_vertices: 64,
        max_updates_per_second: u32::MAX,
        max_fences: 1_000,
    }
}

fn ns() -> Namespace {
    Namespace::of(&Identity::authenticated("svc", "t"))
}

fn entity(id: u64, ts_ms: i64, lon: f64) -> Entity {
    Entity {
        collection: "c".into(),
        id: format!("e{id:04}"),
        geometry: if id.is_multiple_of(3) {
            Geometry::Line(vec![Position::new(lon, 1.0), Position::new(lon + 0.5, 2.0)])
        } else {
            Geometry::Point(Position::new(lon, 0.0))
        },
        ts_ms,
        speed: Some(1.0),
        heading: None,
        ttl_ms: None,
        fields: BTreeMap::from([("n".into(), FieldValue::Int(id as i64))]),
        labels: Default::default(),
    }
}

/// Everything the engine holds in collection `c`, by id.
fn contents(engine: &DurableEngine) -> BTreeMap<String, Entity> {
    let page = engine
        .query(
            &ns(),
            &QueryRequest {
                collection: "c".into(),
                query: Query::Bbox([-180.0, -90.0, 180.0, 90.0]),
                filters: Vec::new(),
                limit: 10_000,
                cursor: None,
                readable: None,
            },
            0,
        )
        .unwrap();
    page.entities
        .into_iter()
        .map(|e| (e.id.clone(), e))
        .collect()
}

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

fn segments(dir: &Path) -> Vec<PathBuf> {
    let mut found: Vec<PathBuf> = fs::read_dir(dir.join("wal"))
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|x| x == "seg"))
        .collect();
    found.sort();
    found
}

#[test]
fn acknowledged_writes_survive_crashes_at_random_points() {
    for seed in 1..=12u64 {
        let dir = scratch("crash");
        let mut rng = Rng(seed * 0x9E37_79B9);
        let mut model: BTreeMap<String, Entity> = BTreeMap::new();
        let mut ts = 1;
        for round in 0..4 {
            let engine = DurableEngine::open(config(&dir, 40), quotas(), None, 0).unwrap();
            assert_eq!(
                contents(&engine),
                model,
                "seed {seed} round {round}: recovered state"
            );
            let ops = 20 + rng.next() % 80;
            for _ in 0..ops {
                ts += 1;
                let id = rng.next() % 60;
                if rng.next().is_multiple_of(5) {
                    let removed = engine.delete(&ns(), "c", &format!("e{id:04}"), ts).unwrap();
                    assert_eq!(removed, model.remove(&format!("e{id:04}")).is_some());
                } else if rng.next().is_multiple_of(4) {
                    let batch: Vec<Entity> = (0..1 + rng.next() % 5)
                        .map(|k| entity((id + k) % 60, ts, (id + k) as f64))
                        .collect();
                    for (result, entity) in engine
                        .upsert_batch(&ns(), batch.clone(), ts)
                        .unwrap()
                        .into_iter()
                        .zip(batch)
                    {
                        result.unwrap();
                        model.insert(entity.id.clone(), entity);
                    }
                } else {
                    let e = entity(id, ts, rng.next() as f64 % 170.0);
                    engine.upsert(&ns(), e.clone(), ts).unwrap();
                    model.insert(e.id.clone(), e);
                }
            }
            // Crash: keep segments a snapshot might be about to drop, then
            // drop the engine without a clean shutdown.
            let kept: Vec<(PathBuf, Vec<u8>)> = segments(&dir)
                .into_iter()
                .map(|p| (p.clone(), fs::read(&p).unwrap()))
                .collect();
            drop(engine);
            match rng.next() % 3 {
                // A write that never finished: a frame header claiming more
                // bytes than made it to disk.
                0 => {
                    let last = segments(&dir).pop().unwrap();
                    let mut torn = 200u32.to_le_bytes().to_vec();
                    torn.extend_from_slice(&(!200u32).to_le_bytes());
                    torn.extend_from_slice(&(rng.next() as u32).to_le_bytes());
                    torn.extend((0..rng.next() % 150).map(|_| rng.next() as u8));
                    fs::OpenOptions::new()
                        .append(true)
                        .open(last)
                        .unwrap()
                        .write_all(&torn)
                        .unwrap();
                }
                // A snapshot that never finished.
                1 => fs::write(
                    dir.join("snapshots").join("99999999999999999999.arrow.tmp"),
                    b"half",
                )
                .unwrap(),
                // A crash between a snapshot and dropping the log it covers.
                _ => {
                    for (path, bytes) in kept {
                        if !path.exists() {
                            fs::write(path, bytes).unwrap();
                        }
                    }
                }
            }
        }
        let engine = DurableEngine::open(config(&dir, 40), quotas(), None, 0).unwrap();
        assert_eq!(contents(&engine), model, "seed {seed}: final state");
        drop(engine);
        let _ = fs::remove_dir_all(&dir);
    }
}

#[test]
fn the_wal_stays_bounded_under_steady_load() {
    let dir = scratch("bounded");
    let engine = DurableEngine::open(config(&dir, 200), quotas(), None, 0).unwrap();
    let mut peak = 0;
    for i in 0..3_000u64 {
        engine
            .upsert(
                &ns(),
                entity(i % 500, i as i64 + 1, (i % 170) as f64),
                i as i64,
            )
            .unwrap();
        peak = peak.max(engine.wal_bytes().unwrap());
    }
    // About one snapshot interval of records, not three thousand.
    assert!(peak < 64 * 1024, "peak WAL {peak} bytes");
    drop(engine);
    let reopened = DurableEngine::open(config(&dir, 200), quotas(), None, 0).unwrap();
    assert_eq!(contents(&reopened).len(), 500);
    drop(reopened);
    let _ = fs::remove_dir_all(&dir);
}

struct Source(Vec<Entity>);

impl RebuildSource for Source {
    fn entities(&self) -> Result<Vec<(Namespace, Entity)>, String> {
        Ok(self.0.iter().map(|e| (ns(), e.clone())).collect())
    }

    fn fences(&self) -> Result<Vec<(Namespace, ezu::fence::Fence)>, String> {
        Ok(Vec::new())
    }
}

#[test]
fn a_lost_snapshot_is_rebuilt_from_the_source_and_one_process_owns_the_directory() {
    let dir = scratch("rebuild");
    let engine = DurableEngine::open(config(&dir, 10), quotas(), None, 0).unwrap();
    // A second instance cannot open the same directory.
    assert!(matches!(
        DurableEngine::open(config(&dir, 10), quotas(), None, 0),
        Err(DurableError::Locked)
    ));
    for i in 0..25 {
        engine
            .upsert(&ns(), entity(i, i as i64 + 1, i as f64), 0)
            .unwrap();
    }
    drop(engine);
    fs::remove_dir_all(dir.join("snapshots")).unwrap();
    // The log no longer starts at the beginning, so it alone is not enough.
    assert!(matches!(
        DurableEngine::open(config(&dir, 10), quotas(), None, 0),
        Err(DurableError::NeedsRebuild(_))
    ));
    let source = Source((0..25).map(|i| entity(i, i as i64 + 1, i as f64)).collect());
    let engine = DurableEngine::open(config(&dir, 10), quotas(), Some(&source), 0).unwrap();
    assert_eq!(contents(&engine).len(), 25);
    drop(engine);
    // The rebuild was pinned by a snapshot, so the source is not needed again.
    let engine = DurableEngine::open(config(&dir, 10), quotas(), None, 0).unwrap();
    assert_eq!(contents(&engine).len(), 25);
    drop(engine);
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn a_damaged_complete_segment_is_refused() {
    let dir = scratch("damaged");
    let engine = DurableEngine::open(
        DurabilityConfig {
            segment_bytes: 256,
            ..config(&dir, u64::MAX)
        },
        quotas(),
        None,
        0,
    )
    .unwrap();
    for i in 0..30 {
        engine
            .upsert(&ns(), entity(i, i as i64 + 1, i as f64), 0)
            .unwrap();
    }
    drop(engine);
    let first = segments(&dir).remove(0);
    let mut bytes = fs::read(&first).unwrap();
    let middle = bytes.len() / 2;
    bytes[middle] ^= 0xFF;
    fs::write(&first, bytes).unwrap();
    let error = DurableEngine::open(config(&dir, u64::MAX), quotas(), None, 0)
        .err()
        .expect("a damaged complete segment must fail recovery");
    assert!(error.to_string().contains("corrupt"), "{error}");
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn damage_before_later_records_in_the_active_segment_is_refused() {
    let dir = scratch("active");
    let engine = DurableEngine::open(config(&dir, u64::MAX), quotas(), None, 0).unwrap();
    for i in 0..10 {
        engine
            .upsert(&ns(), entity(i, i as i64 + 1, i as f64), 0)
            .unwrap();
    }
    drop(engine);
    // The newest segment holds all ten; damage the second record's payload.
    let last = segments(&dir).pop().unwrap();
    let mut bytes = fs::read(&last).unwrap();
    let first_len = u32::from_le_bytes(bytes[8..12].try_into().unwrap()) as usize;
    // Each write is its record frame, then a 21-byte commit frame.
    let second = 8 + 12 + first_len + 21;
    bytes[second + 12 + 4] ^= 0xFF;
    fs::write(&last, bytes).unwrap();
    let error = DurableEngine::open(config(&dir, u64::MAX), quotas(), None, 0)
        .err()
        .expect("acknowledged records after the damage must not be dropped");
    assert!(error.to_string().contains("corrupt"), "{error}");
    // A damaged length is refused too, rather than read as a torn tail.
    let mut bytes = fs::read(&last).unwrap();
    bytes[second + 12 + 4] ^= 0xFF;
    bytes[second] ^= 0x01;
    fs::write(&last, bytes).unwrap();
    let error = DurableEngine::open(config(&dir, u64::MAX), quotas(), None, 0)
        .err()
        .expect("a damaged frame length must fail closed");
    assert!(error.to_string().contains("corrupt"), "{error}");
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn a_flipped_snapshot_or_final_record_fails_closed_and_expiry_is_swept_on_open() {
    // A bit flip in a snapshot is caught by its checksum and sends recovery
    // to the rebuild source.
    let dir = scratch("integrity");
    let engine = DurableEngine::open(config(&dir, 5), quotas(), None, 0).unwrap();
    for i in 0..5 {
        engine
            .upsert(&ns(), entity(i, i as i64 + 1, i as f64), 0)
            .unwrap();
    }
    drop(engine);
    // The namespace's data file inside the newest snapshot set.
    let set = fs::read_dir(dir.join("snapshots"))
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| p.is_dir())
        .unwrap();
    let snapshot = fs::read_dir(set)
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| p.extension().is_some_and(|x| x == "data"))
        .unwrap();
    let mut bytes = fs::read(&snapshot).unwrap();
    let middle = bytes.len() / 2;
    bytes[middle] ^= 0x01;
    fs::write(&snapshot, bytes).unwrap();
    let error = DurableEngine::open(config(&dir, 5), quotas(), None, 0)
        .err()
        .expect("a damaged snapshot must not be trusted");
    assert!(matches!(error, DurableError::NeedsRebuild(_)), "{error}");
    // The rebuilt state supersedes the damaged snapshot for good.
    let source = Source((0..5).map(|i| entity(i, i as i64 + 1, i as f64)).collect());
    drop(DurableEngine::open(config(&dir, 5), quotas(), Some(&source), 0).unwrap());
    let engine = DurableEngine::open(config(&dir, 5), quotas(), None, 0).unwrap();
    assert_eq!(contents(&engine).len(), 5);
    drop(engine);
    let _ = fs::remove_dir_all(&dir);

    // A flipped payload byte in the last, complete record is damage, not an
    // unfinished write.
    let dir = scratch("final");
    let engine = DurableEngine::open(config(&dir, u64::MAX), quotas(), None, 0).unwrap();
    for i in 0..3 {
        engine
            .upsert(&ns(), entity(i, i as i64 + 1, i as f64), 0)
            .unwrap();
    }
    drop(engine);
    let last = segments(&dir).pop().unwrap();
    let mut bytes = fs::read(&last).unwrap();
    // The last byte of the last record, before its 21-byte commit frame.
    let end = bytes.len() - 21 - 1;
    bytes[end] ^= 0x01;
    fs::write(&last, bytes).unwrap();
    assert!(DurableEngine::open(config(&dir, u64::MAX), quotas(), None, 0).is_err());
    let _ = fs::remove_dir_all(&dir);

    // Expired entities release their quota, also after a restart.
    let dir = scratch("expiry");
    let small = Quotas {
        max_entities: 3,
        ..quotas()
    };
    let engine = DurableEngine::open(config(&dir, u64::MAX), small, None, 0).unwrap();
    for i in 0..3 {
        let mut brief = entity(i, 1, i as f64);
        brief.ttl_ms = Some(10);
        engine.upsert(&ns(), brief, 1).unwrap();
    }
    drop(engine);
    let engine = DurableEngine::open(config(&dir, u64::MAX), small, None, 100).unwrap();
    engine.upsert(&ns(), entity(9, 100, 9.0), 100).unwrap();
    assert_eq!(contents(&engine).len(), 1);
    // After a sweep, an older observation of an expired key is admitted, and
    // a restart keeps it: the sweep was logged in order.
    let mut brief = entity(9, 300, 9.0);
    brief.ttl_ms = Some(10);
    engine.upsert(&ns(), brief, 300).unwrap();
    assert_eq!(engine.sweep_expired(400).unwrap(), 1);
    let older = entity(9, 200, 1.0);
    engine.upsert(&ns(), older.clone(), 400).unwrap();
    drop(engine);
    let engine = DurableEngine::open(config(&dir, u64::MAX), small, None, 400).unwrap();
    assert_eq!(
        engine.get(&ns(), "c", "e0009", None, 400).unwrap(),
        Some(older)
    );
    drop(engine);
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn a_missing_segment_between_survivors_is_refused() {
    let dir = scratch("gap");
    let engine = DurableEngine::open(
        DurabilityConfig {
            segment_bytes: 128,
            ..config(&dir, u64::MAX)
        },
        quotas(),
        None,
        0,
    )
    .unwrap();
    for i in 0..20 {
        engine
            .upsert(&ns(), entity(i, i as i64 + 1, i as f64), 0)
            .unwrap();
    }
    drop(engine);
    let found = segments(&dir);
    assert!(found.len() > 3);
    fs::remove_file(&found[found.len() / 2]).unwrap();
    let error = DurableEngine::open(config(&dir, u64::MAX), quotas(), None, 0)
        .err()
        .expect("a gap in the log must fail recovery");
    assert!(error.to_string().contains("corrupt"), "{error}");
    let _ = fs::remove_dir_all(&dir);
}
