//! Kill-leader evidence (#42): the leader runs as the `ezu` binary with a
//! sync standby and an async follower (both in process), under concurrent
//! writers and an event subscriber. It is SIGKILLed mid-load and the
//! standby is promoted. Every acknowledged write is on the standby, the
//! subscriber resumes there from its last id without a gap, and the async
//! follower switches to the standby through the lineage check and
//! converges. Prints the numbers recorded in `docs/benchmarks.md`.

use ezu::api::proto::ezu_client::EzuClient;
use ezu::api::proto::replication_client::ReplicationClient;
use ezu::api::proto::{self};
use ezu::api::{Api, serve};
use ezu::auth::{Authenticator, StaticToken};
use ezu::{DurabilityConfig, DurableEngine, Identity, Namespace, Quotas};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const TOKEN: &str = "tenant-token";
const REPLICATION: &str = "replication-token";
const STANDBY: &str = "standby-1";
const WRITERS: usize = 4;
/// Entities each writer moves per batch.
const BATCH: usize = 1_000;

fn scratch(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("ezu-kill-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn config(dir: &Path) -> DurabilityConfig {
    DurabilityConfig {
        dir: dir.to_path_buf(),
        fsync: true,
        segment_bytes: 64 << 20,
        snapshot_every_records: 1_000_000,
        snapshot_every_ms: i64::MAX,
        events_retained: 1_000_000,
        keys: None,
    }
}

fn quotas() -> Quotas {
    Quotas {
        max_entities: 1_000_000,
        max_collections: 8,
        max_vertices: 64,
        max_updates_per_second: u32::MAX,
        max_fences: 100,
    }
}

fn ns() -> Namespace {
    Namespace::of(&Identity::authenticated("static-token", "t"))
}

/// The leader process; killed (SIGKILL) and reaped when dropped, so a
/// failing test leaves nothing running.
struct Leader(Child);

impl Drop for Leader {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Start the `ezu` binary as a leader whose sync standby is `STANDBY`, on
/// an ephemeral port; returns it and its URL.
fn spawn_leader(dir: &Path) -> (Leader, String) {
    std::fs::write(dir.join("token"), TOKEN).unwrap();
    std::fs::write(dir.join("replication"), REPLICATION).unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_ezu"))
        .env_clear()
        .env("EZU_DATA_DIR", dir.join("data"))
        .env("EZU_LISTEN", "127.0.0.1:0")
        .env("EZU_STATIC_TOKEN_FILE", dir.join("token"))
        .env("EZU_STATIC_NAMESPACE", "t")
        .env("EZU_REPLICATION_TOKEN_FILE", dir.join("replication"))
        .env("EZU_SYNC_STANDBY", STANDBY)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let stderr = child.stderr.take().unwrap();
    let leader = Leader(child);
    let (found, address) = std::sync::mpsc::channel();
    // Drained to the end, so the leader never blocks on a full pipe.
    std::thread::spawn(move || {
        for line in BufReader::new(stderr).lines().map_while(Result::ok) {
            if let Some((_, address)) = line.split_once(" listening on ") {
                let _ = found.send(address.to_string());
            }
        }
    });
    let address = address.recv_timeout(Duration::from_secs(30)).unwrap();
    (leader, format!("http://{address}"))
}

/// Serve `engine` as `node_id`; returns its URL.
async fn start(engine: Arc<DurableEngine>, node_id: &str) -> String {
    let auth: Arc<dyn Authenticator> = Arc::new(StaticToken::new(TOKEN, "t"));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = format!("http://{}", listener.local_addr().unwrap());
    let api = Api::new(engine, auth)
        .replication_token(Some(REPLICATION))
        .promotion(Some(node_id), ezu::durable::LEASE);
    tokio::spawn(serve(api, listener, None, std::future::pending()));
    address
}

fn endpoint(url: &str) -> tonic::transport::Endpoint {
    tonic::transport::Endpoint::from_shared(url.to_string()).unwrap()
}

fn bearer<T>(message: T, token: &str) -> tonic::Request<T> {
    let mut request = tonic::Request::new(message);
    request
        .metadata_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
    request
}

/// Wait, bounded, until `condition` holds.
async fn eventually(what: &str, condition: impl Fn() -> bool) {
    let started = Instant::now();
    while !condition() {
        assert!(started.elapsed() < Duration::from_secs(60), "never: {what}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Writer `w`'s batch for round `round`: its entities, inside the fence on
/// even rounds and outside on odd ones, observed at `round + 1`.
fn batch(w: usize, round: i64) -> proto::UpsertRequest {
    let lon = if round % 2 == 0 { 0.0 } else { 1.0 };
    let entities = (0..BATCH)
        .map(|i| proto::Entity {
            collection: "v".into(),
            id: format!("w{w}-{i}"),
            geometry: Some(proto::Geometry {
                shape: Some(proto::geometry::Shape::Point(proto::Position {
                    lon,
                    lat: 0.0,
                })),
            }),
            ts_ms: round + 1,
            ..Default::default()
        })
        .collect();
    proto::UpsertRequest { entities }
}

fn percentile(samples: &mut [u64], p: f64) -> u64 {
    samples.sort_unstable();
    samples
        .get(((samples.len() as f64 - 1.0) * p).round() as usize)
        .copied()
        .unwrap_or(0)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_killed_leaders_standby_is_promoted_with_every_acknowledged_write_and_event() {
    let root = scratch("leader");
    let (mut leader, leader_url) = spawn_leader(&root);
    let standby_dir = scratch("standby");
    let standby =
        Arc::new(DurableEngine::open_follower(config(&standby_dir), quotas(), 0).unwrap());
    tokio::spawn(ezu::follow::standby(
        standby.clone(),
        endpoint(&leader_url),
        REPLICATION.into(),
        STANDBY.into(),
        |_| {},
    ));
    let standby_url = start(standby.clone(), STANDBY).await;
    let other_dir = scratch("other");
    let other = Arc::new(DurableEngine::open_follower(config(&other_dir), quotas(), 0).unwrap());
    let other_following = tokio::spawn(ezu::follow::follow(
        other.clone(),
        endpoint(&leader_url),
        REPLICATION.into(),
        |_| {},
    ));

    // The leader serves once its standby acknowledged.
    let mut client = EzuClient::connect(leader_url.clone()).await.unwrap();
    let fence = proto::PutFenceRequest {
        fence: Some(proto::Fence {
            id: "zone".into(),
            shape: Some(proto::fence::Shape::Circle(proto::Radius {
                center: Some(proto::Position { lon: 0.0, lat: 0.0 }),
                meters: 20_000.0,
            })),
            ..Default::default()
        }),
    };
    let started = Instant::now();
    while client
        .put_fence(bearer(fence.clone(), TOKEN))
        .await
        .is_err()
    {
        assert!(started.elapsed() < Duration::from_secs(60), "no lease");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // A subscriber reads the leader's events from the start.
    let seen: Arc<Mutex<Vec<(u64, u32)>>> = Arc::default();
    let mut events = client
        .subscribe(bearer(proto::SubscribeRequest::default(), TOKEN))
        .await
        .unwrap()
        .into_inner();
    let subscriber = {
        let seen = seen.clone();
        tokio::spawn(async move {
            while let Ok(Some(event)) = events.message().await {
                seen.lock().unwrap().push((event.lsn, event.index));
            }
        })
    };

    // Concurrent writers; each keeps the last round acknowledged.
    let acknowledged: Arc<Mutex<Vec<Option<i64>>>> = Arc::new(Mutex::new(vec![None; WRITERS]));
    let writers: Vec<_> = (0..WRITERS)
        .map(|w| {
            let (mut client, acknowledged) = (client.clone(), acknowledged.clone());
            tokio::spawn(async move {
                for round in 0.. {
                    if client.upsert(bearer(batch(w, round), TOKEN)).await.is_err() {
                        return;
                    }
                    acknowledged.lock().unwrap()[w] = Some(round);
                }
            })
        })
        .collect();
    // Follower lag, in records, sampled during the load.
    let (mut standby_lag, mut other_lag) = (Vec::new(), Vec::new());
    let load = Instant::now();
    while load.elapsed() < Duration::from_secs(2) {
        standby_lag.extend(standby.replication().map(|follower| follower.lag()));
        other_lag.extend(other.replication().map(|follower| follower.lag()));
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // SIGKILL mid-load.
    leader.0.kill().unwrap();
    leader.0.wait().unwrap();
    let killed = load.elapsed();
    for writer in writers {
        tokio::time::timeout(Duration::from_secs(30), writer)
            .await
            .unwrap()
            .unwrap();
    }
    let _ = tokio::time::timeout(Duration::from_secs(30), subscriber).await;
    let acknowledged: Vec<i64> = acknowledged
        .lock()
        .unwrap()
        .iter()
        .map(|round| round.unwrap())
        .collect();
    let acknowledged_writes: i64 = acknowledged
        .iter()
        .map(|round| (round + 1) * BATCH as i64)
        .sum();

    // Promote the standby.
    let promoting = Instant::now();
    let mut replication = ReplicationClient::connect(standby_url.clone())
        .await
        .unwrap();
    let epoch = replication
        .promote(bearer(proto::PromoteRequest { force: false }, REPLICATION))
        .await
        .unwrap()
        .into_inner()
        .epoch;
    let promotion = promoting.elapsed();
    assert_eq!(epoch, 1);
    let start_lsn = standby.last_lsn().unwrap();

    // Every acknowledged write is on the promoted standby.
    for (w, round) in acknowledged.iter().enumerate() {
        for i in 0..BATCH {
            let entity = standby
                .get(&ns(), "v", &format!("w{w}-{i}"), None, 0)
                .unwrap()
                .unwrap();
            assert!(
                entity.ts_ms > *round,
                "w{w}-{i}: {} <= {round}",
                entity.ts_ms
            );
        }
    }

    // The subscriber resumes on the standby from its last id: what the
    // leader served and what follows are every event the standby holds, and
    // a write after the promotion raises the next ids.
    let seen = seen.lock().unwrap().clone();
    let cursor = *seen.last().unwrap();
    let mut promoted = EzuClient::connect(standby_url.clone()).await.unwrap();
    let mut resumed = promoted
        .subscribe(bearer(
            proto::SubscribeRequest {
                after_lsn: Some(cursor.0),
                after_index: cursor.1,
                ..Default::default()
            },
            TOKEN,
        ))
        .await
        .unwrap()
        .into_inner();
    // New entities, entering the fence: one event each.
    promoted
        .upsert(bearer(batch(WRITERS, 0), TOKEN))
        .await
        .unwrap();
    assert_eq!(standby.last_lsn().unwrap(), start_lsn + BATCH as u64);
    let mut read = seen.clone();
    while read.last().is_none_or(|last| last.0 <= start_lsn) {
        let event = tokio::time::timeout(Duration::from_secs(30), resumed.message())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        read.push((event.lsn, event.index));
    }
    while let Ok(Ok(Some(event))) =
        tokio::time::timeout(Duration::from_millis(200), resumed.message()).await
    {
        read.push((event.lsn, event.index));
    }
    let held: Vec<(u64, u32)> = standby
        .events(&ns(), None, &[], None, usize::MAX)
        .unwrap()
        .events
        .iter()
        .map(|event| (event.lsn, event.index))
        .collect();
    assert_eq!(read, held);
    assert_eq!(
        held.iter().filter(|event| event.0 > start_lsn).count(),
        BATCH,
        "a write after the promotion raises its events at the next LSNs"
    );

    // The async follower switches to the promoted standby: it was never
    // ahead of it, its lineage passes, and it converges.
    let ahead = other.last_lsn().unwrap();
    assert!(ahead <= start_lsn, "{ahead} > {start_lsn}");
    other_following.abort();
    let _ = other_following.await;
    eventually("the follower is released", || {
        Arc::strong_count(&other) == 1
    })
    .await;
    drop(other);
    let other = Arc::new(DurableEngine::open_follower(config(&other_dir), quotas(), 0).unwrap());
    tokio::spawn(ezu::follow::follow(
        other.clone(),
        endpoint(&standby_url),
        REPLICATION.into(),
        |_| {},
    ));
    let switching = Instant::now();
    let last = standby.last_lsn().unwrap();
    eventually("the follower converges", || {
        other.last_lsn().unwrap() == last
    })
    .await;
    let converged = switching.elapsed();
    assert!(other.healthy());
    assert_eq!(other.lineage(), standby.lineage());
    assert_eq!(
        other.entity_count(&ns()).unwrap(),
        standby.entity_count(&ns()).unwrap()
    );

    eprintln!(
        "kill-leader evidence: {acknowledged_writes} acknowledged writes in {:.1} s ({:.0}/s) \
         from {WRITERS} writers in batches of {BATCH}; standby lag p50 {} p99 {} records; \
         async follower lag p50 {} p99 {} records; {} events read before the kill; \
         promotion {:.1} s; the async follower was {} records behind the promotion point \
         and converged on the new leader in {:.2} s",
        killed.as_secs_f64(),
        acknowledged_writes as f64 / killed.as_secs_f64(),
        percentile(&mut standby_lag, 0.5),
        percentile(&mut standby_lag, 0.99),
        percentile(&mut other_lag, 0.5),
        percentile(&mut other_lag, 0.99),
        seen.len(),
        promotion.as_secs_f64(),
        start_lsn - ahead,
        converged.as_secs_f64(),
    );
}
