//! Geofences (#3): scripted movements raise exactly the expected events,
//! memberships and event ids survive snapshots and restarts, and an update
//! is evaluated quickly against 10^5 fences.

use ezu::fence::{EventKind, Fence, Shape};
use ezu::{
    DurabilityConfig, DurableEngine, DurableError, Entity, FieldValue, Filter, Geometry, Identity,
    IndexError, Namespace, Position, Quotas,
};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

static SEQ: AtomicU64 = AtomicU64::new(0);

fn scratch(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "ezu-fences-{name}-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = fs::remove_dir_all(&dir);
    dir
}

fn open(dir: &Path, fsync: bool, max_fences: usize) -> DurableEngine {
    open_keeping(dir, fsync, max_fences, 1_000)
}

fn open_keeping(dir: &Path, fsync: bool, max_fences: usize, retained: usize) -> DurableEngine {
    DurableEngine::open(
        DurabilityConfig {
            dir: dir.to_path_buf(),
            fsync,
            segment_bytes: 1 << 20,
            snapshot_every_records: u64::MAX,
            snapshot_every_ms: i64::MAX,
            events_retained: retained,
            keys: None,
        },
        Quotas {
            max_entities: 10_000,
            max_collections: 8,
            max_vertices: 64,
            max_updates_per_second: u32::MAX,
            max_fences,
        },
        None,
        0,
    )
    .unwrap()
}

fn ns() -> Namespace {
    Namespace::of(&Identity::authenticated("svc", "t"))
}

fn circle(id: &str, lon: f64, lat: f64, meters: f64) -> Fence {
    Fence {
        id: id.into(),
        shape: Shape::Circle {
            center: Position::new(lon, lat),
            meters,
        },
        collections: vec!["trucks".into()],
        filters: Vec::new(),
        dwell_ms: None,
        inside_per_second: None,
    }
}

fn truck(id: &str, lon: f64, ts_ms: i64) -> Entity {
    Entity {
        collection: "trucks".into(),
        id: id.into(),
        geometry: Geometry::Point(Position::new(lon, 50.0)),
        ts_ms,
        speed: None,
        heading: None,
        ttl_ms: None,
        fields: BTreeMap::from([("fleet".into(), FieldValue::Text("north".into()))]),
    }
}

/// Every kept event as (fence, entity, kind, ts).
fn log(engine: &DurableEngine) -> Vec<(String, String, EventKind, i64)> {
    engine
        .events(&ns(), None, &[], 1_000)
        .unwrap()
        .events
        .into_iter()
        .map(|e| (e.fence, e.entity, e.kind, e.ts_ms))
        .collect()
}

fn ev(fence: &str, entity: &str, kind: EventKind, ts: i64) -> (String, String, EventKind, i64) {
    (fence.into(), entity.into(), kind, ts)
}

#[test]
fn scripted_moves_raise_exact_events_across_snapshots_and_restarts() {
    use EventKind::*;
    let dir = scratch("script");
    let engine = open(&dir, true, 100);
    let mut depot = circle("depot", 10.0, 50.0, 500.0);
    depot.dwell_ms = Some(2_000);
    depot.filters = vec![Filter::Eq("fleet".into(), FieldValue::Text("north".into()))];
    assert!(!engine.put_fence(&ns(), depot, 0).unwrap());
    let yard = Fence {
        shape: Shape::Polygon(vec![vec![
            Position::new(10.05, 49.99),
            Position::new(10.07, 49.99),
            Position::new(10.07, 50.01),
            Position::new(10.05, 49.99),
        ]]),
        ..circle("yard", 0.0, 0.0, 1.0)
    };
    engine.put_fence(&ns(), yard, 0).unwrap();

    engine.upsert(&ns(), truck("t1", 9.0, 1_000), 0).unwrap();
    engine.upsert(&ns(), truck("t1", 10.0, 2_000), 0).unwrap();
    engine.upsert(&ns(), truck("t1", 10.001, 3_000), 0).unwrap();
    // A stale observation is not logged and raises nothing.
    engine.upsert(&ns(), truck("t1", 12.0, 2_500), 0).unwrap();
    engine.snapshot(0).unwrap();
    engine.upsert(&ns(), truck("t1", 10.0, 4_500), 0).unwrap();
    let before = engine.events(&ns(), None, &[], 100).unwrap().events;
    assert_eq!(
        log(&engine),
        [
            ev("depot", "t1", Enter, 2_000),
            ev("depot", "t1", Dwell, 4_500)
        ]
    );
    // A crash: memberships and the enter come from the snapshot, the dwell
    // is raised again by replay with the same id, and nothing enters twice.
    drop(engine);
    let engine = open(&dir, true, 100);
    let replayed = engine.events(&ns(), None, &[], 100).unwrap().events;
    assert_eq!(replayed, before);
    engine.upsert(&ns(), truck("t1", 10.0, 5_000), 0).unwrap();
    engine.upsert(&ns(), truck("t1", 10.06, 6_000), 0).unwrap();
    // Resuming after the last seen id returns only what came later.
    let last = replayed.last().map(|e| (e.lsn, e.index));
    let later: Vec<_> = engine
        .events(&ns(), last, &[], 100)
        .unwrap()
        .events
        .into_iter()
        .map(|e| (e.fence, e.kind))
        .collect();
    assert_eq!(later, [("depot".into(), Exit), ("yard".into(), Enter)]);
    // Deletes exit at the last observation; a removed fence is silent.
    engine.upsert(&ns(), truck("t2", 10.0, 7_000), 0).unwrap();
    assert!(engine.delete_fence(&ns(), "yard", 0).unwrap());
    engine.delete(&ns(), "trucks", "t1", 8_000).unwrap();
    engine.delete(&ns(), "trucks", "t2", 8_000).unwrap();
    let tail: Vec<_> = log(&engine).into_iter().skip(4).collect();
    assert_eq!(
        tail,
        [
            ev("depot", "t2", Enter, 7_000),
            ev("depot", "t2", Exit, 7_000)
        ]
    );
    assert_eq!(engine.list_fences(&ns(), None, 10).unwrap().len(), 1);
    drop(engine);
    // Fences come back from the log alone too.
    let engine = open(&dir, true, 100);
    assert!(engine.get_fence(&ns(), "depot").unwrap().is_some());
    assert!(engine.get_fence(&ns(), "yard").unwrap().is_none());
    drop(engine);
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn expiry_exits_and_quotas_and_bad_fences_are_refused() {
    let dir = scratch("quota");
    let engine = open(&dir, false, 1);
    engine
        .put_fence(&ns(), circle("a", 10.0, 50.0, 500.0), 0)
        .unwrap();
    assert!(matches!(
        engine.put_fence(&ns(), circle("b", 10.0, 50.0, 500.0), 0),
        Err(DurableError::Index(IndexError::FenceQuota { limit: 1 }))
    ));
    // Replacing does not count against the quota.
    assert!(
        engine
            .put_fence(&ns(), circle("a", 10.0, 50.0, 800.0), 0)
            .unwrap()
    );
    assert!(matches!(
        engine.put_fence(&ns(), circle("a", 10.0, 50.0, -1.0), 0),
        Err(DurableError::Index(IndexError::InvalidQuery(_)))
    ));
    let mut brief = truck("t1", 10.0, 1_000);
    brief.ttl_ms = Some(500);
    engine.upsert(&ns(), brief, 0).unwrap();
    engine.sweep_expired(2_000).unwrap();
    let kinds: Vec<_> = log(&engine).into_iter().map(|e| e.2).collect();
    assert_eq!(kinds, [EventKind::Enter, EventKind::Exit]);
    drop(engine);
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn an_update_is_evaluated_quickly_against_a_hundred_thousand_fences() {
    let dir = scratch("load");
    let engine = open(&dir, false, 100_000);
    // A grid of 2 km circles over Europe.
    for i in 0..100_000u32 {
        let (x, y) = (i % 400, i / 400);
        let fence = circle(
            &format!("f{i}"),
            -10.0 + f64::from(x) * 0.1,
            40.0 + f64::from(y) * 0.08,
            2_000.0,
        );
        engine.put_fence(&ns(), fence, 0).unwrap();
    }
    let mut latencies = Vec::new();
    for i in 0..2_000i64 {
        let lon = -10.0 + (i % 400) as f64 * 0.1 + 0.01;
        let mut moved = truck("t", lon, 1_000 + i);
        moved.geometry = Geometry::Point(Position::new(lon, 40.0 + (i % 250) as f64 * 0.08));
        let start = Instant::now();
        engine.upsert(&ns(), moved, 0).unwrap();
        latencies.push(start.elapsed());
    }
    latencies.sort();
    let p99 = latencies[latencies.len() * 99 / 100];
    assert!(!log(&engine).is_empty());
    assert!(p99.as_millis() < 50, "p99 {p99:?}");
    drop(engine);
    let _ = fs::remove_dir_all(&dir);
}

struct Source(Vec<Entity>, Vec<Fence>);

impl ezu::RebuildSource for Source {
    fn entities(&self) -> Result<Vec<(Namespace, Entity)>, String> {
        Ok(self.0.iter().map(|e| (ns(), e.clone())).collect())
    }

    fn fences(&self) -> Result<Vec<(Namespace, Fence)>, String> {
        Ok(self.1.iter().map(|f| (ns(), f.clone())).collect())
    }
}

#[test]
fn a_rebuild_restores_fences_and_memberships_without_events() {
    let dir = scratch("rebuild");
    let engine = open(&dir, false, 10);
    let depot = circle("depot", 10.0, 50.0, 500.0);
    engine.put_fence(&ns(), depot.clone(), 0).unwrap();
    engine.upsert(&ns(), truck("t1", 10.0, 1_000), 0).unwrap();
    engine.snapshot(0).unwrap();
    engine.upsert(&ns(), truck("t1", 10.0, 2_000), 0).unwrap();
    drop(engine);
    fs::remove_dir_all(dir.join("snapshots")).unwrap();
    let source = Source(vec![truck("t1", 10.0, 2_000)], vec![depot]);
    let engine = DurableEngine::open(
        DurabilityConfig {
            dir: dir.clone(),
            fsync: false,
            segment_bytes: 1 << 20,
            snapshot_every_records: u64::MAX,
            snapshot_every_ms: i64::MAX,
            events_retained: 1_000,
            keys: None,
        },
        Quotas {
            max_entities: 10,
            max_collections: 8,
            max_vertices: 64,
            max_updates_per_second: u32::MAX,
            max_fences: 10,
        },
        Some(&source),
        0,
    )
    .unwrap();
    assert!(engine.get_fence(&ns(), "depot").unwrap().is_some());
    assert!(log(&engine).is_empty());
    // Event history before the rebuild is gone, and a reader there is told.
    assert!(matches!(
        engine.events(&ns(), Some((1, 0)), &[], 10),
        Err(DurableError::Expired(_))
    ));
    // Still inside: no second enter, then a clean exit.
    engine.upsert(&ns(), truck("t1", 10.0, 3_000), 0).unwrap();
    engine.upsert(&ns(), truck("t1", 12.0, 4_000), 0).unwrap();
    let kinds: Vec<_> = log(&engine).into_iter().map(|e| e.2).collect();
    assert_eq!(kinds, [EventKind::Exit]);
    drop(engine);
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn a_reader_resumes_without_gaps_across_restarts_and_an_expired_point_is_refused() {
    let dir = scratch("resume");
    let engine = open_keeping(&dir, true, 10, 4);
    engine
        .put_fence(&ns(), circle("depot", 10.0, 50.0, 500.0), 0)
        .unwrap();
    // Enter and exit, three times over: six events.
    let mut ts = 0;
    let mut step = |engine: &DurableEngine, lon: f64| {
        ts += 1_000;
        engine.upsert(&ns(), truck("t1", lon, ts), 0).unwrap();
    };
    step(&engine, 10.0);
    step(&engine, 12.0);
    let read = engine.events(&ns(), None, &[], 100).unwrap().events;
    let seen = read.last().map(|e| (e.lsn, e.index));
    engine.snapshot(0).unwrap();
    step(&engine, 10.0);
    step(&engine, 12.0);
    let expected = engine.events(&ns(), seen, &[], 100).unwrap().events;
    assert_eq!(expected.len(), 2);
    // A crash after the snapshot: the kept events come back from it and
    // from replay, with the same ids, so the reader continues gap-free.
    drop(engine);
    let engine = open_keeping(&dir, true, 10, 4);
    assert_eq!(
        engine.events(&ns(), seen, &[], 100).unwrap().events,
        expected
    );
    // Past the retention, a reader at the first event has missed some and
    // is told so, before and after a restart.
    step(&engine, 10.0);
    step(&engine, 12.0);
    let first = read.first().map(|e| (e.lsn, e.index));
    assert!(matches!(
        engine.events(&ns(), first, &[], 100),
        Err(DurableError::Expired(_))
    ));
    assert_eq!(
        engine.events(&ns(), None, &[], 100).unwrap().events.len(),
        4
    );
    drop(engine);
    let engine = open_keeping(&dir, true, 10, 4);
    assert!(matches!(
        engine.events(&ns(), first, &[], 100),
        Err(DurableError::Expired(_))
    ));
    assert!(engine.events(&ns(), seen, &[], 100).is_ok());
    drop(engine);
    let _ = fs::remove_dir_all(&dir);
}
