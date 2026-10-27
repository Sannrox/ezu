//! Follower replicas (#41): a follower tails its leader's committed log
//! over the API, converges on the same query results and event ids, and
//! resumes after a restart without a gap, also over mutual TLS. It refuses
//! writes, its directory refuses to open as a leader's, and it stops fail
//! closed on a gap, an epoch going back, a sealed record, a last record
//! that differs from the leader's, or a leader that no longer holds what
//! comes next.

use ezu::api::proto::ezu_client::EzuClient;
use ezu::api::proto::{self, query_request};
use ezu::api::{Api, serve};
use ezu::auth::{Authenticator, StaticToken};
use ezu::durable::{ReplicaError, Shipped};
use ezu::fence::{Fence, Shape};
use ezu::keys::KeyConfig;
use ezu::keys::sekai_keys::{KeyPurpose, MemoryKeyProvider, RuntimeProfile, TenantId, UnwrapGrant};
use ezu::{
    DurabilityConfig, DurableEngine, DurableError, Entity, Geometry, Identity, Namespace, Position,
    Quotas,
};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

static SEQ: AtomicU64 = AtomicU64::new(0);
const TOKEN: &str = "tenant-token";
const REPLICATION: &str = "replication-token";

fn scratch(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "ezu-replication-{name}-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn config(dir: &Path) -> DurabilityConfig {
    DurabilityConfig {
        dir: dir.to_path_buf(),
        fsync: true,
        // Small segments and snapshots, so the leader rotates and drops
        // its log while the follower tails it.
        segment_bytes: 64 << 10,
        snapshot_every_records: 2_000,
        snapshot_every_ms: i64::MAX,
        events_retained: 100_000,
        keys: None,
    }
}

fn quotas() -> Quotas {
    Quotas {
        max_entities: 100_000,
        max_collections: 8,
        max_vertices: 64,
        max_updates_per_second: u32::MAX,
        max_fences: 100,
    }
}

fn ns() -> Namespace {
    Namespace::of(&Identity::authenticated("static-token", "t"))
}

fn leader(dir: &Path) -> DurableEngine {
    DurableEngine::open(config(dir), quotas(), None, 0).unwrap()
}

fn follower(dir: &Path) -> DurableEngine {
    DurableEngine::open_follower(config(dir), quotas(), 0).unwrap()
}

/// Entity `i` at `round`: rounds move it east through the fence at 0.
fn vehicle(i: u64, round: i64) -> Entity {
    Entity {
        collection: "v".into(),
        id: format!("v{i}"),
        geometry: Geometry::Point(Position::new(
            -0.5 + round as f64 * 0.25 + (i % 10) as f64 * 0.001,
            (i / 10) as f64 * 0.001,
        )),
        ts_ms: 1 + round,
        speed: None,
        heading: None,
        ttl_ms: None,
        fields: Default::default(),
        labels: Default::default(),
    }
}

fn write_round(engine: &DurableEngine, count: u64, round: i64) {
    for start in (0..count).step_by(250) {
        let batch = (start..(start + 250).min(count))
            .map(|i| vehicle(i, round))
            .collect();
        for outcome in engine.upsert_batch(&ns(), batch, 0).unwrap() {
            outcome.unwrap();
        }
    }
}

fn fence() -> Fence {
    Fence {
        id: "zone".into(),
        shape: Shape::Circle {
            center: Position::new(0.0, 0.0),
            meters: 20_000.0,
        },
        collections: Vec::new(),
        filters: Vec::new(),
        dwell_ms: None,
        inside_per_second: None,
        labels: Default::default(),
    }
}

/// Serve `engine` on an ephemeral port; returns its address and a stop.
async fn start(engine: Arc<DurableEngine>) -> (String, tokio::sync::oneshot::Sender<()>) {
    let auth: Arc<dyn Authenticator> = Arc::new(StaticToken::new(TOKEN, "t"));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = format!("http://{}", listener.local_addr().unwrap());
    let (stop, stopped) = tokio::sync::oneshot::channel();
    tokio::spawn(serve(
        Api::new(engine, auth).replication_token(Some(REPLICATION)),
        listener,
        None,
        async {
            let _ = stopped.await;
        },
    ));
    (address, stop)
}

fn authed<T>(message: T) -> tonic::Request<T> {
    let mut request = tonic::Request::new(message);
    request
        .metadata_mut()
        .insert("authorization", format!("Bearer {TOKEN}").parse().unwrap());
    request
}

/// Every entity of `v` as the API answers, and every event.
async fn observe(address: &str) -> (Vec<proto::Entity>, Vec<proto::FenceEvent>) {
    let mut client = EzuClient::connect(address.to_string()).await.unwrap();
    let mut entities = Vec::new();
    let mut cursor = String::new();
    loop {
        let page = client
            .query(authed(proto::QueryRequest {
                collection: "v".into(),
                query: Some(query_request::Query::Bbox(proto::Bbox {
                    west: -180.0,
                    south: -90.0,
                    east: 180.0,
                    north: 90.0,
                })),
                limit: 1_000,
                cursor: cursor.clone(),
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        entities.extend(page.entities);
        if page.next_cursor.is_empty() {
            break;
        }
        cursor = page.next_cursor;
    }
    entities.sort_by(|a, b| a.id.cmp(&b.id));
    let events = client
        .list_events(authed(proto::ListEventsRequest::default()))
        .await
        .unwrap()
        .into_inner()
        .events;
    (entities, events)
}

/// Follow `leader` until the follower has applied `lsn`.
async fn converge(follower: &DurableEngine, lsn: u64) {
    let started = Instant::now();
    while follower.replication().unwrap().applied_lsn < lsn {
        assert!(
            started.elapsed() < Duration::from_secs(60),
            "the follower did not converge: {:?}",
            follower.replication()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Run a follower of `leader_address` on `dir`, serving on its own port;
/// returns the engine, its address, and what stops both.
async fn run_follower(
    dir: &Path,
    leader_address: &str,
) -> (
    Arc<DurableEngine>,
    String,
    tokio::sync::oneshot::Sender<()>,
    tokio::task::JoinHandle<DurableError>,
) {
    let engine = Arc::new(follower(dir));
    let endpoint = tonic::transport::Endpoint::from_shared(leader_address.to_string()).unwrap();
    let following = tokio::spawn(ezu::follow::follow(
        engine.clone(),
        endpoint,
        REPLICATION.into(),
        |_| {},
    ));
    let (address, stop) = start(engine.clone()).await;
    (engine, address, stop, following)
}

/// Stop a follower and wait until its directory is released.
async fn shut_down(
    engine: Arc<DurableEngine>,
    stop: tokio::sync::oneshot::Sender<()>,
    following: tokio::task::JoinHandle<DurableError>,
) {
    following.abort();
    let _ = following.await;
    let _ = stop.send(());
    let started = Instant::now();
    while Arc::strong_count(&engine) > 1 {
        assert!(started.elapsed() < Duration::from_secs(30), "still held");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_converges_with_the_same_results_and_event_ids_and_resumes_after_a_restart() {
    let (leader_dir, follower_dir) = (scratch("leader"), scratch("follower"));
    let leader = Arc::new(leader(&leader_dir));
    let (leader_address, _stop_leader) = start(leader.clone()).await;
    let (engine, address, stop, following) = run_follower(&follower_dir, &leader_address).await;

    // Written while the follower tails: segments rotate, snapshots drop
    // the log behind it, and moves raise enter and exit events.
    leader.put_fence(&ns(), fence(), 0).unwrap();
    for round in 0..3 {
        write_round(&leader, 3_000, round);
    }
    converge(&engine, leader.last_lsn().unwrap()).await;
    let on_leader = observe(&leader_address).await;
    assert_eq!(on_leader.0.len(), 3_000);
    assert!(!on_leader.1.is_empty(), "no events were raised");
    assert_eq!(observe(&address).await, on_leader);
    let replication = engine.replication().unwrap();
    assert_eq!(replication.lag(), 0);
    assert!(replication.connected);
    let metrics = engine.render_metrics(0).unwrap();
    assert!(
        metrics.contains("ezu_replication_lag_records 0"),
        "{metrics}"
    );
    assert!(metrics.contains("ezu_up 1"), "{metrics}");

    // Writes are refused over the API, with a typed error.
    let mut client = EzuClient::connect(address.clone()).await.unwrap();
    let refused = client
        .upsert(authed(proto::UpsertRequest {
            entities: Vec::new(),
        }))
        .await
        .unwrap_err();
    assert_eq!(refused.code(), tonic::Code::FailedPrecondition);
    assert!(matches!(
        engine.upsert(&ns(), vehicle(0, 9), 0),
        Err(DurableError::ReadOnly)
    ));

    // A restart resumes after what the follower logged, while the leader
    // goes on writing, with no gap and no record applied twice. (Writes
    // past the leader's next snapshot would drop the log the follower
    // needs, and it would stop instead.)
    let applied = engine.replication().unwrap().applied_lsn;
    shut_down(engine, stop, following).await;
    write_round(&leader, 500, 3);
    let (engine, address, _stop, _following) = run_follower(&follower_dir, &leader_address).await;
    assert_eq!(engine.replication().unwrap().applied_lsn, applied);
    write_round(&leader, 3_000, 4);
    converge(&engine, leader.last_lsn().unwrap()).await;
    assert_eq!(observe(&address).await, observe(&leader_address).await);
    assert!(engine.healthy());
}

#[test]
fn a_gap_an_older_epoch_and_a_sealed_record_stop_the_follower_fail_closed() {
    let source = leader(&scratch("source"));
    write_round(&source, 10, 0);
    let mut from = source.shipping(0, None).unwrap();
    let shipped = source.ship(&mut from, usize::MAX).unwrap();
    assert_eq!(shipped.records.len(), 10);

    // A gap: the first record is missing.
    let gapped = follower(&scratch("gap"));
    let mut skipping = shipped.clone();
    skipping.records.remove(0);
    assert!(matches!(
        gapped.apply_shipped(skipping, 0),
        Err(DurableError::Replica(ReplicaError::Gap {
            applied: 0,
            next: 2
        }))
    ));
    assert!(!gapped.healthy());
    assert!(gapped.replication().unwrap().stopped.is_some());
    // Stopped for good: even what would continue is refused.
    assert!(gapped.apply_shipped(shipped.clone(), 0).is_err());

    // An epoch going back, after a resend of what was applied (a no-op).
    let dir = scratch("epoch");
    let epochs = follower(&dir);
    let (first, rest) = shipped.records.split_at(5);
    let at = |epoch, records: &[Vec<u8>]| Shipped {
        epoch,
        records: records.to_vec(),
        leader_lsn: 10,
        lineage: Vec::new(),
        stamp: 0,
    };
    assert_eq!(epochs.apply_shipped(at(2, first), 0).unwrap(), 5);
    assert_eq!(epochs.apply_shipped(at(2, first), 0).unwrap(), 5);
    assert_eq!(epochs.epoch().unwrap(), 2);
    assert!(matches!(
        epochs.apply_shipped(at(1, rest), 0),
        Err(DurableError::Replica(ReplicaError::EpochBack {
            seen: 2,
            shipped: 1
        }))
    ));
    drop(epochs);
    // The epoch followed is durable.
    assert_eq!(follower(&dir).epoch().unwrap(), 2);

    // A sealed record: followers hold no keys.
    let provider = Arc::new(MemoryKeyProvider::new(RuntimeProfile::Development).unwrap());
    let grant = UnwrapGrant {
        tenant_id: TenantId::new(ns().as_str()).unwrap(),
        purpose: KeyPurpose::Store,
        expires_unix_secs: i64::MAX,
    };
    let encrypted_dir = scratch("encrypted");
    let mut encrypted = config(&encrypted_dir);
    encrypted.keys = Some(KeyConfig {
        provider,
        grants: HashMap::from([(ns().as_str().to_string(), grant)]),
    });
    let sealed_source = DurableEngine::open(encrypted, quotas(), None, 0).unwrap();
    write_round(&sealed_source, 1, 0);
    let sealed = sealed_source
        .ship(&mut sealed_source.shipping(0, None).unwrap(), usize::MAX)
        .unwrap();
    let refusing = follower(&scratch("sealed"));
    assert!(matches!(
        refusing.apply_shipped(sealed, 0),
        Err(DurableError::Replica(ReplicaError::Sealed(_)))
    ));

    // Seeding from a snapshot that holds a sealed namespace is refused too,
    // instead of following without it.
    sealed_source.snapshot(0).unwrap();
    let seeded = scratch("sealed-seed");
    fs::create_dir_all(seeded.join("snapshots")).unwrap();
    for set in fs::read_dir(encrypted_dir.join("snapshots")).unwrap() {
        let set = set.unwrap().path();
        let target = seeded.join("snapshots").join(set.file_name().unwrap());
        fs::create_dir_all(&target).unwrap();
        for file in fs::read_dir(&set).unwrap() {
            let file = file.unwrap().path();
            fs::copy(&file, target.join(file.file_name().unwrap())).unwrap();
        }
    }
    assert!(matches!(
        DurableEngine::open_follower(config(&seeded), quotas(), 0),
        Err(DurableError::Replica(ReplicaError::Sealed(_)))
    ));
}

#[test]
fn a_promoted_leader_ships_its_new_epoch_and_a_dropped_log_is_gone() {
    let dir = scratch("promoted");
    let first = leader(&dir);
    write_round(&first, 10, 0);
    drop(first);
    DurableEngine::promote(&dir).unwrap();
    let promoted = leader(&dir);
    write_round(&promoted, 10, 1);

    // Records keep the epoch they were logged at.
    let replica = follower(&scratch("promoted-follower"));
    let mut from = promoted.shipping(0, None).unwrap();
    let old = promoted.ship(&mut from, usize::MAX).unwrap();
    assert_eq!((old.epoch, old.records.len()), (0, 10));
    replica.apply_shipped(old, 0).unwrap();
    let new = promoted.ship(&mut from, usize::MAX).unwrap();
    assert_eq!((new.epoch, new.records.len()), (1, 10));
    replica.apply_shipped(new, 0).unwrap();
    assert_eq!(replica.epoch().unwrap(), 1);
    assert_eq!(replica.last_lsn().unwrap(), 20);

    // Once a snapshot drops the log, a follower from the start needs a
    // rebuild, and one ahead of the log is refused.
    promoted.snapshot(0).unwrap();
    write_round(&promoted, 10, 2);
    assert!(matches!(
        promoted.ship(&mut promoted.shipping(0, None).unwrap(), usize::MAX),
        Err(DurableError::Replica(ReplicaError::Gone(0)))
    ));
    assert!(matches!(
        promoted.shipping(1_000, None),
        Err(DurableError::Replica(ReplicaError::Ahead { .. }))
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_stopped_inside_a_leader_batch_restarts_and_resumes_from_it() {
    let leader = Arc::new(leader(&scratch("split")));
    // One committed batch of 250 records, shipped one record at a time as
    // a budget smaller than the batch splits it.
    write_round(&leader, 250, 0);
    let dir = scratch("split-follower");
    let replica = follower(&dir);
    let mut from = leader.shipping(0, None).unwrap();
    for _ in 0..3 {
        replica
            .apply_shipped(leader.ship(&mut from, 1).unwrap(), 0)
            .unwrap();
    }
    drop(replica);

    // Its own log framed each part as a complete batch: a restart keeps
    // the prefix, and the leader accepts its last record inside the batch.
    let replica = follower(&dir);
    assert_eq!(replica.last_lsn().unwrap(), 3);
    let last = replica.fingerprint(3).unwrap();
    assert!(last.is_some());
    let mut resumed = leader.shipping(3, last).unwrap();
    let rest = leader.ship(&mut resumed, usize::MAX).unwrap();
    assert_eq!(rest.records.len(), 247);
    drop(replica);

    // Over the API, the same restart converges.
    let (leader_address, _stop) = start(leader.clone()).await;
    let (engine, address, _stop_follower, _following) = run_follower(&dir, &leader_address).await;
    converge(&engine, 250).await;
    assert!(engine.healthy());
    assert_eq!(observe(&address).await, observe(&leader_address).await);
}

#[test]
fn a_follower_directory_is_refused_as_a_leader() {
    let dir = scratch("marked");
    drop(follower(&dir));
    assert!(dir.join("FOLLOWER").exists());
    assert!(matches!(
        DurableEngine::open(config(&dir), quotas(), None, 0),
        Err(DurableError::FollowerDirectory)
    ));
    // It still opens as the follower it is.
    drop(follower(&dir));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_whose_last_record_differs_from_the_leaders_is_refused_and_unhealthy() {
    let leader = Arc::new(leader(&scratch("history")));
    write_round(&leader, 10, 0);
    let (leader_address, _stop) = start(leader.clone()).await;

    // Another history at the same LSNs: the same ids, other positions.
    let other = other_history(&scratch("other"));
    let shipped = other
        .ship(&mut other.shipping(0, None).unwrap(), usize::MAX)
        .unwrap();
    let dir = scratch("diverged");
    let diverged = follower(&dir);
    assert_eq!(diverged.apply_shipped(shipped, 0).unwrap(), 10);
    let fingerprint = diverged.fingerprint(10).unwrap().unwrap();
    assert!(matches!(
        leader.shipping(10, Some(fingerprint)),
        Err(DurableError::Replica(ReplicaError::Diverged(10)))
    ));
    drop(diverged);

    let (engine, _address, _stop_follower, following) = run_follower(&dir, &leader_address).await;
    let stopped = tokio::time::timeout(Duration::from_secs(30), following)
        .await
        .unwrap()
        .unwrap();
    assert!(stopped.to_string().contains("differs"), "{stopped}");
    assert!(!engine.healthy());
    assert_eq!(engine.replication().unwrap().applied_lsn, 10);

    // The same history passes the check.
    let same = follower(&scratch("same"));
    same.apply_shipped(
        leader
            .ship(&mut leader.shipping(0, None).unwrap(), usize::MAX)
            .unwrap(),
        0,
    )
    .unwrap();
    assert!(leader.shipping(10, same.fingerprint(10).unwrap()).is_ok());
}

/// A leader whose entities sit elsewhere than [`leader`]'s.
fn other_history(dir: &Path) -> DurableEngine {
    let engine = leader(dir);
    for start in (0..10).step_by(5) {
        let batch = (start..start + 5)
            .map(|i| {
                let mut moved = vehicle(i, 0);
                moved.geometry = Geometry::Point(Position::new(40.0, 40.0));
                moved
            })
            .collect();
        engine.upsert_batch(&ns(), batch, 0).unwrap();
    }
    engine
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_tails_a_leader_over_mutual_tls() {
    let server = rcgen::generate_simple_self_signed(vec!["127.0.0.1".into()]).unwrap();
    let client = rcgen::generate_simple_self_signed(vec!["follower".into()]).unwrap();
    let tls = ezu::api::tls_config(
        &server.cert.pem(),
        &server.signing_key.serialize_pem(),
        Some(&client.cert.pem()),
    )
    .unwrap();
    let leader = Arc::new(leader(&scratch("tls-leader")));
    write_round(&leader, 100, 0);
    let auth: Arc<dyn Authenticator> = Arc::new(StaticToken::new(TOKEN, "t"));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("https://{}", listener.local_addr().unwrap());
    let (_stop, stopped) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(serve(
        Api::new(leader.clone(), auth).replication_token(Some(REPLICATION)),
        listener,
        Some(tls),
        async {
            let _ = stopped.await;
        },
    ));

    let engine = Arc::new(follower(&scratch("tls-follower")));
    let identity = (client.cert.pem(), client.signing_key.serialize_pem());
    let endpoint = ezu::follow::endpoint(&url, Some(server.cert.pem()), Some(identity)).unwrap();
    tokio::spawn(ezu::follow::follow(
        engine.clone(),
        endpoint,
        REPLICATION.into(),
        |_| {},
    ));
    converge(&engine, leader.last_lsn().unwrap()).await;

    // Without a client certificate the leader's mutual TLS refuses it.
    let anonymous = Arc::new(follower(&scratch("tls-anonymous")));
    let endpoint = ezu::follow::endpoint(&url, Some(server.cert.pem()), None).unwrap();
    let retries = Arc::new(AtomicU64::new(0));
    let counted = retries.clone();
    tokio::spawn(ezu::follow::follow(
        anonymous.clone(),
        endpoint,
        REPLICATION.into(),
        move |_| {
            counted.fetch_add(1, Ordering::Relaxed);
        },
    ));
    let started = Instant::now();
    while retries.load(Ordering::Relaxed) == 0 {
        assert!(started.elapsed() < Duration::from_secs(30));
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(anonymous.replication().unwrap().applied_lsn, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_follower_that_cannot_follow_reports_why_and_unhealthy() {
    let engine = Arc::new(follower(&scratch("bad-token")));
    let endpoint = tonic::transport::Endpoint::from_static("http://127.0.0.1:9");
    // A token that is no valid header value ends following at once.
    let stopped = ezu::follow::follow(engine.clone(), endpoint, "bad\ntoken".into(), |_| {}).await;
    assert!(stopped.to_string().contains("token"), "{stopped}");
    assert!(!engine.healthy());
    assert!(engine.replication().unwrap().stopped.is_some());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_refused_token_stops_the_follower_and_followers_ship_nothing() {
    let leader = Arc::new(leader(&scratch("refused-leader")));
    let (leader_address, _stop_leader) = start(leader.clone()).await;
    let engine = Arc::new(follower(&scratch("refused-follower")));
    let endpoint = tonic::transport::Endpoint::from_shared(leader_address).unwrap();
    // A wrong token cannot get better by retrying: it ends following.
    let stopped = tokio::time::timeout(
        Duration::from_secs(30),
        ezu::follow::follow(engine.clone(), endpoint, "wrong".into(), |_| {}),
    )
    .await
    .unwrap();
    assert!(!engine.healthy(), "{stopped}");

    // A follower does not ship its log on.
    let (follower_address, _stop_follower) = start(engine.clone()).await;
    let mut client = proto::replication_client::ReplicationClient::connect(follower_address)
        .await
        .unwrap();
    let mut request = tonic::Request::new(proto::FollowRequest::default());
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {REPLICATION}").parse().unwrap(),
    );
    let refused = client.follow(request).await.unwrap_err();
    assert_eq!(refused.code(), tonic::Code::FailedPrecondition);
}
