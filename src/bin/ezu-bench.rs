//! Benchmark harness: one node, in process, through the durable engine with
//! fsync on, as the server runs it.
//!
//! `ezu-bench [--entities N] [--fences N] [--dir PATH] [--baseline FILE]`
//!
//! The data goes into a new directory of its own under `--dir` (default:
//! the system temp directory), which is removed afterwards; nothing else
//! there is touched.
//!
//! Measures update throughput (load, then a move of every entity, each batch
//! synced), query latency (bbox, radius, nearest), fence evaluation (the
//! latency of single updates against `--fences` fences, without the disk
//! sync, whose cost the throughput already shows), memory per entity
//! (resident set, Linux only), recovery time (reopen from a snapshot plus
//! a WAL of one move) and follower convergence (a follower tailing a load
//! of at most 10^6 entities in process: its lag during the load and the
//! time it takes to catch up after) and the sync standby (the same load and
//! single writes against a leader that waits for a standby's
//! acknowledgement: throughput and acknowledgement latency). Prints the
//! results as JSON. With
//! `--baseline`, compares against an earlier result and exits non-zero when
//! any measure is more than 10% worse.

use ezu::fence::{Fence, Shape};
use ezu::{
    DurabilityConfig, DurableEngine, Entity, FieldValue, Geometry, Identity, Namespace, Position,
    Query, QueryRequest, Quotas, SyncStandby, Upsert,
};
use serde_json::{Map, Value, json};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

const BATCH: usize = 10_000;
/// A measure this much worse than the baseline is a regression.
const TOLERANCE: f64 = 0.10;

struct Options {
    entities: u64,
    fences: u32,
    dir: PathBuf,
    baseline: Option<PathBuf>,
}

fn options() -> Result<Options, String> {
    let mut options = Options {
        entities: 1_000_000,
        fences: 100_000,
        dir: std::env::temp_dir(),
        baseline: None,
    };
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        let mut value = || args.next().ok_or(format!("{arg} needs a value"));
        match arg.as_str() {
            "--entities" => options.entities = value()?.parse().map_err(|_| "--entities")?,
            "--fences" => options.fences = value()?.parse().map_err(|_| "--fences")?,
            "--dir" => options.dir = value()?.into(),
            "--baseline" => options.baseline = Some(value()?.into()),
            other => return Err(format!("unknown argument {other}")),
        }
    }
    if options.entities == 0 {
        return Err("--entities must be positive".into());
    }
    Ok(options)
}

fn config(dir: &Path, fsync: bool) -> DurabilityConfig {
    DurabilityConfig {
        dir: dir.to_path_buf(),
        fsync,
        segment_bytes: 256 << 20,
        snapshot_every_records: u64::MAX,
        snapshot_every_ms: i64::MAX,
        events_retained: 10_000,
        keys: None,
    }
}

fn quotas() -> Quotas {
    Quotas {
        max_entities: usize::MAX,
        max_collections: 16,
        max_vertices: 64,
        max_updates_per_second: u32::MAX,
        max_fences: usize::MAX,
    }
}

fn open(dir: &Path, fsync: bool) -> Result<DurableEngine, String> {
    DurableEngine::open(config(dir, fsync), quotas(), None, 0).map_err(|error| error.to_string())
}

/// A follower tails a leader in process (ship, then apply, as over the
/// API without the network) while the leader loads `count` entities.
/// Returns the leader's load rate, the follower's lag in records sampled
/// after each batch it applied, and the seconds from the leader's last
/// write until the follower has applied it.
fn replication(dir: &Path, count: u64) -> Result<Value, String> {
    let leader = Arc::new(open(&dir.join("leader"), true)?);
    let follower = DurableEngine::open_follower(config(&dir.join("follower"), true), quotas(), 0)
        .map_err(|error| error.to_string())?;
    let done = Arc::new(AtomicBool::new(false));
    let tailing = {
        let (leader, done) = (leader.clone(), done.clone());
        std::thread::spawn(move || -> Result<(Vec<u64>, Instant), String> {
            let mut from = leader
                .shipping(0, None)
                .map_err(|error| error.to_string())?;
            let mut lags = Vec::new();
            let mut applied = 0;
            loop {
                let shipped = leader
                    .ship(&mut from, 1 << 20)
                    .map_err(|error| error.to_string())?;
                if shipped.records.is_empty() {
                    // Caught up once the load is done and the follower holds
                    // the leader's last record: the empty read may predate
                    // the final batch.
                    if done.load(Ordering::Acquire)
                        && applied >= leader.last_lsn().map_err(|error| error.to_string())?
                    {
                        return Ok((lags, Instant::now()));
                    }
                    std::thread::sleep(Duration::from_millis(1));
                    continue;
                }
                applied = follower
                    .apply_shipped(shipped, 0)
                    .map_err(|error| error.to_string())?;
                let last = leader.last_lsn().map_err(|error| error.to_string())?;
                lags.push(last.saturating_sub(applied));
            }
        })
    };
    let load = write_all(&leader, count, 0, None);
    let finished = Instant::now();
    done.store(true, Ordering::Release);
    let (mut lags, caught_up) = tailing
        .join()
        .map_err(|_| "the follower thread panicked".to_string())??;
    let load = load?;
    lags.sort_unstable();
    let at = |q: f64| {
        lags.get(((lags.len().max(1) - 1) as f64 * q) as usize)
            .copied()
    };
    Ok(json!({
        "entities": count,
        "load_updates_per_s": load,
        "lag_records": { "p50": at(0.5), "p99": at(0.99), "max": lags.last() },
        "catchup_s": caught_up.saturating_duration_since(finished).as_secs_f64(),
    }))
}

/// A leader with a sync standby, the standby tailing it in process (ship,
/// apply with its own sync, then acknowledge, as over `Replication.Sync`
/// without the network). Returns the leader's load rate of `count`
/// entities in batches of [`BATCH`], the latency until each batch was
/// acknowledged, and the same for single writes.
fn sync_standby(dir: &Path, count: u64) -> Result<Value, String> {
    const NODE: &str = "bench-standby";
    let leader = DurableEngine::open_sync(
        config(&dir.join("leader"), true),
        quotas(),
        None,
        0,
        SyncStandby {
            node_id: NODE.into(),
            timeout: Duration::from_secs(60),
            lease: Duration::from_secs(60),
        },
    )
    .map_err(|error| error.to_string())?;
    let leader = Arc::new(leader);
    let standby = DurableEngine::open_follower(config(&dir.join("standby"), true), quotas(), 0)
        .map_err(|error| error.to_string())?;
    // The lease starts with the standby's first acknowledgement.
    leader.acknowledge(NODE, 0, 0, leader.stamp());
    let done = Arc::new(AtomicBool::new(false));
    let tailing = {
        let (leader, done) = (leader.clone(), done.clone());
        std::thread::spawn(move || -> Result<(), String> {
            let waiting = tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .build()
                .map_err(|error| error.to_string())?;
            let mut durable = leader.watch_durable();
            let mut from = leader
                .shipping(0, None)
                .map_err(|error| error.to_string())?;
            loop {
                // Marked seen before the read, so a write after it wakes us.
                durable.borrow_and_update();
                let shipped = leader
                    .ship(&mut from, 1 << 20)
                    .map_err(|error| error.to_string())?;
                if shipped.records.is_empty() {
                    if done.load(Ordering::Acquire) {
                        return Ok(());
                    }
                    // The timer is made inside the runtime that drives it.
                    let _ = waiting.block_on(async {
                        tokio::time::timeout(Duration::from_millis(10), durable.changed()).await
                    });
                    continue;
                }
                let stamp = shipped.stamp;
                let applied = standby
                    .apply_shipped(shipped, 0)
                    .map_err(|error| error.to_string())?;
                let epoch = standby.epoch().map_err(|error| error.to_string())?;
                leader.acknowledge(NODE, applied, epoch, stamp);
            }
        })
    };
    let mut batches = Vec::new();
    let load = write_all(&leader, count, 0, Some(&mut batches));
    let mut singles = Vec::new();
    let single = (|| {
        for i in 0..2_000 {
            let started = Instant::now();
            leader
                .upsert(&ns(), entity(i % count, 1), 0)
                .map_err(|error| error.to_string())?;
            singles.push(started.elapsed());
        }
        Ok::<_, String>(())
    })();
    done.store(true, Ordering::Release);
    tailing
        .join()
        .map_err(|_| "the standby thread panicked".to_string())??;
    let load = load?;
    single?;
    Ok(json!({
        "entities": count,
        "load_updates_per_s": load,
        "batch_ack": percentiles(batches),
        "write_ack": percentiles(singles),
    }))
}

fn ns() -> Namespace {
    Namespace::of(&Identity::authenticated("bench", "bench"))
}

/// Entity `i` of a grid over most of the world; `round` moves it a little.
fn entity(i: u64, round: i64) -> Entity {
    let (x, y) = (i % 3_000, i / 3_000);
    Entity {
        collection: "vehicles".into(),
        id: format!("v{i}"),
        geometry: Geometry::Point(Position::new(
            -170.0 + x as f64 * 0.1 + round as f64 * 0.001,
            -60.0 + (y % 1_200) as f64 * 0.1,
        )),
        ts_ms: 1 + round,
        speed: Some(10.0),
        heading: Some(90.0),
        ttl_ms: None,
        fields: BTreeMap::from([("fleet".into(), FieldValue::Int((i % 16) as i64))]),
        labels: Default::default(),
    }
}

/// Updates per second writing every entity once at `round`; with
/// `timed`, the time each batch took until it was acknowledged.
fn write_all(
    engine: &DurableEngine,
    count: u64,
    round: i64,
    mut timed: Option<&mut Vec<Duration>>,
) -> Result<f64, String> {
    let started = Instant::now();
    let mut next = 0;
    while next < count {
        let end = (next + BATCH as u64).min(count);
        let batch = (next..end).map(|i| entity(i, round)).collect();
        let batch_started = Instant::now();
        // Every write must land, or the rest measures a partial dataset.
        for outcome in engine
            .upsert_batch(&ns(), batch, 0)
            .map_err(|error| error.to_string())?
        {
            match outcome {
                Ok(Upsert::Inserted | Upsert::Updated) => {}
                other => return Err(format!("a write did not land: {other:?}")),
            }
        }
        if let Some(timed) = timed.as_deref_mut() {
            timed.push(batch_started.elapsed());
        }
        next = end;
    }
    Ok(count as f64 / started.elapsed().as_secs_f64())
}

/// The p50 and p99 of `samples`, in microseconds.
fn percentiles(mut samples: Vec<Duration>) -> Value {
    samples.sort();
    let at = |q: f64| samples[((samples.len() - 1) as f64 * q) as usize].as_secs_f64() * 1e6;
    json!({ "p50_us": at(0.5), "p99_us": at(0.99) })
}

fn query_latency(engine: &DurableEngine, query: impl Fn(u64) -> Query) -> Result<Value, String> {
    let mut samples = Vec::new();
    for i in 0..1_000 {
        let request = QueryRequest {
            collection: "vehicles".into(),
            query: query(i * 7_919),
            filters: Vec::new(),
            limit: 1_000,
            cursor: None,
            readable: None,
        };
        let started = Instant::now();
        engine
            .query(&ns(), &request, 0)
            .map_err(|error| error.to_string())?;
        samples.push(started.elapsed());
    }
    Ok(percentiles(samples))
}

fn place(i: u64) -> Position {
    let (x, y) = (i % 3_000, (i / 3_000) % 1_200);
    Position::new(-170.0 + x as f64 * 0.1, -60.0 + y as f64 * 0.1)
}

/// Resident memory in bytes, where the platform reports it.
fn resident_bytes() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let line = status.lines().find(|line| line.starts_with("VmRSS:"))?;
    let kib: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
    Some(kib * 1024)
}

fn run(options: &Options) -> Result<Value, String> {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| since.as_nanos());
    let dir = options
        .dir
        .join(format!("ezu-bench-{}-{nanos}", std::process::id()));
    // Created here, so it is ours alone to remove.
    std::fs::create_dir(&dir).map_err(|error| format!("{}: {error}", dir.display()))?;
    let result = measure(options, &dir);
    let _ = std::fs::remove_dir_all(&dir);
    result
}

fn measure(options: &Options, dir: &Path) -> Result<Value, String> {
    let engine = open(&dir.join("main"), true)?;
    let before = resident_bytes();
    let load = write_all(&engine, options.entities, 0, None)?;
    let memory = before
        .zip(resident_bytes())
        .map(|(before, after)| after.saturating_sub(before) as f64 / options.entities as f64);
    engine.snapshot(0).map_err(|error| error.to_string())?;
    let moved = write_all(&engine, options.entities, 1, None)?;
    let count = options.entities;
    let bbox = query_latency(&engine, |i| {
        let p = place(i % count);
        Query::Bbox([p.lon - 0.5, p.lat - 0.5, p.lon + 0.5, p.lat + 0.5])
    })?;
    let radius = query_latency(&engine, |i| Query::Radius {
        center: place(i % count),
        meters: 5_000.0,
    })?;
    let nearest = query_latency(&engine, |i| Query::Nearest {
        center: place(i % count),
        k: 10,
    })?;

    // Recovery reads the snapshot taken after the load and replays the
    // move from the log.
    drop(engine);
    let started = Instant::now();
    let reopened = open(&dir.join("main"), true)?;
    let recovery = started.elapsed().as_secs_f64();
    drop(reopened);

    let engine = open(&dir.join("fences"), false)?;
    for f in 0..u64::from(options.fences) {
        let fence = Fence {
            id: format!("f{f}"),
            shape: Shape::Circle {
                center: place(f * 37),
                meters: 2_000.0,
            },
            collections: vec!["tracked".into()],
            filters: Vec::new(),
            dwell_ms: Some(60_000),
            inside_per_second: None,
            labels: Default::default(),
        };
        engine
            .put_fence(&ns(), fence, 0)
            .map_err(|error| error.to_string())?;
    }
    let mut fenced = Vec::new();
    for i in 0..10_000u64 {
        let mut tracked = entity(i % 100, i as i64);
        tracked.collection = "tracked".into();
        tracked.geometry = Geometry::Point(place(i * 37));
        let started = Instant::now();
        engine
            .upsert(&ns(), tracked, 0)
            .map_err(|error| error.to_string())?;
        fenced.push(started.elapsed());
    }

    drop(engine);

    let follower = replication(&dir.join("replication"), options.entities.min(1_000_000))?;
    let standby = sync_standby(&dir.join("sync"), options.entities.min(1_000_000))?;

    Ok(json!({
        "entities": options.entities,
        "fences": options.fences,
        "load_updates_per_s": load,
        "move_updates_per_s": moved,
        "query_bbox": bbox,
        "query_radius": radius,
        "query_nearest": nearest,
        "fence_evaluation": percentiles(fenced),
        "memory_bytes_per_entity": memory,
        "recovery_s": recovery,
        "follower": follower,
        "sync_standby": standby,
    }))
}

/// Measures more than [`TOLERANCE`] worse than `baseline`, by name. Rates
/// are better higher; times and memory lower.
fn regressions(result: &Value, baseline: &Value) -> Vec<String> {
    let mut flat = Map::new();
    let flatten = |value: &Value, into: &mut Map<String, Value>| {
        fn walk(prefix: &str, value: &Value, into: &mut Map<String, Value>) {
            match value {
                Value::Object(object) => {
                    for (key, inner) in object {
                        let name = match prefix {
                            "" => key.clone(),
                            _ => format!("{prefix}.{key}"),
                        };
                        walk(&name, inner, into);
                    }
                }
                other => {
                    into.insert(prefix.to_string(), other.clone());
                }
            }
        }
        walk("", value, into);
    };
    let mut old = Map::new();
    flatten(result, &mut flat);
    flatten(baseline, &mut old);
    flat.iter()
        .filter_map(|(name, value)| {
            let (now, then) = (value.as_f64()?, old.get(name)?.as_f64()?);
            if name.ends_with("entities") || name == "fences" || then <= 0.0 {
                return None;
            }
            let worse = match name.ends_with("_per_s") {
                true => now < then * (1.0 - TOLERANCE),
                false => now > then * (1.0 + TOLERANCE),
            };
            worse.then(|| format!("{name}: {now:.1} against {then:.1}"))
        })
        .collect()
}

fn main() -> ExitCode {
    let result = options().and_then(|options| {
        let result = run(&options)?;
        let baseline = match &options.baseline {
            Some(path) => Some(
                std::fs::read(path)
                    .map_err(|error| format!("{}: {error}", path.display()))
                    .and_then(|bytes| {
                        serde_json::from_slice::<Value>(&bytes).map_err(|error| error.to_string())
                    })?,
            ),
            None => None,
        };
        Ok((result, baseline))
    });
    match result {
        Ok((result, baseline)) => {
            println!("{result:#}");
            let Some(baseline) = baseline else {
                return ExitCode::SUCCESS;
            };
            if baseline["entities"] != result["entities"] || baseline["fences"] != result["fences"]
            {
                eprintln!("ezu-bench: the baseline measured another size; not compared");
                return ExitCode::SUCCESS;
            }
            let worse = regressions(&result, &baseline);
            for line in &worse {
                eprintln!("ezu-bench: regression: {line}");
            }
            if worse.is_empty() {
                ExitCode::SUCCESS
            } else {
                ExitCode::FAILURE
            }
        }
        Err(error) => {
            eprintln!("ezu-bench: {error}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rates_regress_downward_and_times_upward_past_the_tolerance() {
        let baseline = json!({
            "entities": 10, "load_updates_per_s": 1000.0,
            "query_bbox": {"p99_us": 100.0}, "recovery_s": 1.0,
        });
        let fine = json!({
            "entities": 10, "load_updates_per_s": 950.0,
            "query_bbox": {"p99_us": 105.0}, "recovery_s": 0.5,
        });
        assert!(regressions(&fine, &baseline).is_empty());
        let worse = json!({
            "entities": 10, "load_updates_per_s": 850.0,
            "query_bbox": {"p99_us": 120.0}, "recovery_s": 1.0,
        });
        let found = regressions(&worse, &baseline);
        assert_eq!(found.len(), 2, "{found:?}");
        assert!(
            found
                .iter()
                .any(|line| line.starts_with("load_updates_per_s"))
        );
        assert!(
            found
                .iter()
                .any(|line| line.starts_with("query_bbox.p99_us"))
        );
    }
}
