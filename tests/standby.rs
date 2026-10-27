//! Sync standby (#47): a leader opened with `open_sync` acknowledges a write
//! only once its standby appended and synced it, at the leader's epoch. A
//! standby outage times writes out and, after the lease, makes the leader
//! refuse reads and writes until acknowledgements resume; a restarted
//! leader serves nothing until its standby acknowledges. A standby at a
//! higher epoch refuses an old leader, which then acknowledges nothing.
//! Fence events are served only up to the acknowledged LSN.

use ezu::api::proto::ezu_client::EzuClient;
use ezu::api::proto::{self, ezu_server::EzuServer};
use ezu::api::{Api, serve};
use ezu::auth::{Authenticator, StaticToken};
use ezu::fence::{Fence, Shape};
use ezu::{
    DurabilityConfig, DurableEngine, DurableError, Entity, Geometry, Identity, Namespace, Position,
    Quotas, SyncStandby,
};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

static SEQ: AtomicU64 = AtomicU64::new(0);
const TOKEN: &str = "tenant-token";
const REPLICATION: &str = "replication-token";
const STANDBY: &str = "standby-1";

fn scratch(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "ezu-standby-{name}-{}-{}",
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
        segment_bytes: 64 << 10,
        snapshot_every_records: 100_000,
        snapshot_every_ms: i64::MAX,
        events_retained: 10_000,
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

fn sync(timeout: Duration, lease: Duration) -> SyncStandby {
    SyncStandby {
        node_id: STANDBY.into(),
        timeout,
        lease,
    }
}

fn leader(dir: &Path, standby: SyncStandby) -> DurableEngine {
    DurableEngine::open_sync(config(dir), quotas(), None, 0, standby).unwrap()
}

fn follower(dir: &Path) -> DurableEngine {
    DurableEngine::open_follower(config(dir), quotas(), 0).unwrap()
}

/// Entity `i`; `x` places it, the fence at 0 holds `x` near 0.
fn vehicle(i: u64, x: f64) -> Entity {
    Entity {
        collection: "v".into(),
        id: format!("v{i}"),
        geometry: Geometry::Point(Position::new(x, 0.0)),
        ts_ms: 1 + i as i64,
        speed: None,
        heading: None,
        ttl_ms: None,
        fields: Default::default(),
        labels: Default::default(),
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

/// Serve `engine`; returns its URL and a stop.
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

/// Run `dir` as the sync standby of `leader`.
fn run_standby(
    dir: &Path,
    leader: &str,
) -> (Arc<DurableEngine>, tokio::task::JoinHandle<DurableError>) {
    let engine = Arc::new(follower(dir));
    let endpoint = tonic::transport::Endpoint::from_shared(leader.to_string()).unwrap();
    let following = tokio::spawn(ezu::follow::standby(
        engine.clone(),
        endpoint,
        REPLICATION.into(),
        STANDBY.into(),
        |_| {},
    ));
    (engine, following)
}

/// Stop a standby, as a crash would, and wait until its directory is free.
async fn kill(engine: Arc<DurableEngine>, following: tokio::task::JoinHandle<DurableError>) {
    following.abort();
    let _ = following.await;
    let started = Instant::now();
    while Arc::strong_count(&engine) > 1 {
        assert!(started.elapsed() < Duration::from_secs(30), "still held");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Wait, bounded, until `condition` holds.
async fn eventually(what: &str, condition: impl Fn() -> bool) {
    let started = Instant::now();
    while !condition() {
        assert!(started.elapsed() < Duration::from_secs(30), "never: {what}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn authed<T>(message: T) -> tonic::Request<T> {
    let mut request = tonic::Request::new(message);
    request
        .metadata_mut()
        .insert("authorization", format!("Bearer {TOKEN}").parse().unwrap());
    request
}

fn upsert(i: u64) -> tonic::Request<proto::UpsertRequest> {
    let entity = proto::Entity {
        collection: "v".into(),
        id: format!("v{i}"),
        geometry: Some(proto::Geometry {
            shape: Some(proto::geometry::Shape::Point(proto::Position {
                lon: 0.0,
                lat: 0.0,
            })),
        }),
        ts_ms: 1 + i as i64,
        ..Default::default()
    };
    authed(proto::UpsertRequest {
        entities: vec![entity],
    })
}

/// `GET /healthz`: the status code.
async fn health(url: &str) -> u16 {
    let address = url.trim_start_matches("http://");
    let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
    stream
        .write_all(b"GET /healthz HTTP/1.1\r\nhost: ezu\r\nconnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    response[9..12].parse().unwrap()
}

/// Copy the directory tree `from` to `to`.
fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let path = entry.unwrap().path();
        let target = to.join(path.file_name().unwrap());
        match path.is_dir() {
            true => copy_dir(&path, &target),
            false => {
                std::fs::copy(&path, &target).unwrap();
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_write_is_acknowledged_only_once_the_standby_synced_it() {
    let leader = Arc::new(leader(
        &scratch("leader"),
        sync(Duration::from_secs(30), Duration::from_secs(30)),
    ));
    let (url, _stop) = start(leader.clone()).await;
    let (standby, _following) = run_standby(&scratch("standby"), &url);
    // The standby's first heartbeat acknowledgement grants the lease.
    eventually("the lease", || leader.healthy()).await;
    assert_eq!(health(&url).await, 200);

    let mut client = EzuClient::connect(url.clone()).await.unwrap();
    for i in 0..20 {
        client.upsert(upsert(i)).await.unwrap();
        // Acknowledged: the standby holds it, in its own synced log.
        assert!(standby.last_lsn().unwrap() >= leader.last_lsn().unwrap());
    }
    // Concurrent writes share acknowledgements; each still waits for its own.
    let writes: Vec<_> = (100..132)
        .map(|i| {
            let mut client = client.clone();
            tokio::spawn(async move { client.upsert(upsert(i)).await.map(|_| ()) })
        })
        .collect();
    for write in writes {
        write.await.unwrap().unwrap();
    }
    assert!(standby.last_lsn().unwrap() >= leader.last_lsn().unwrap());
    assert_eq!(leader.standby().unwrap().acked_lsn, 52);
    let metrics = leader.render_metrics(0).unwrap();
    assert!(metrics.contains("ezu_sync_acked_lsn 52"), "{metrics}");
    assert!(metrics.contains("ezu_sync_lease_held 1"), "{metrics}");
}

#[test]
fn only_the_standbys_acknowledgement_at_the_leaders_epoch_releases_a_write() {
    let leader = Arc::new(leader(
        &scratch("ack"),
        sync(Duration::from_secs(30), Duration::from_secs(30)),
    ));
    // No acknowledgement yet: the leader serves nothing on its own.
    assert!(!leader.healthy());
    assert!(matches!(
        leader.upsert(&ns(), vehicle(0, 0.0), 0),
        Err(DurableError::NoStandby)
    ));
    assert!(leader.acknowledge(STANDBY, 0, 0));
    assert!(leader.healthy());

    let (done, outcome) = std::sync::mpsc::channel();
    let writing = leader.clone();
    std::thread::spawn(move || {
        let _ = done.send(writing.upsert(&ns(), vehicle(0, 0.0), 0).map(|_| ()));
    });
    let waiting = Duration::from_millis(100);
    assert!(outcome.recv_timeout(waiting).is_err(), "acknowledged early");
    // Another node, another epoch, or past the log: not counted.
    eventually_blocking(|| leader.last_lsn().unwrap() == 1);
    assert!(!leader.acknowledge("other", 1, 0));
    assert!(!leader.acknowledge(STANDBY, 1, 1));
    assert!(!leader.acknowledge(STANDBY, 2, 0));
    assert!(outcome.recv_timeout(waiting).is_err(), "acknowledged early");
    assert!(leader.acknowledge(STANDBY, 1, 0));
    outcome
        .recv_timeout(Duration::from_secs(30))
        .unwrap()
        .unwrap();
}

fn eventually_blocking(condition: impl Fn() -> bool) {
    let started = Instant::now();
    while !condition() {
        assert!(started.elapsed() < Duration::from_secs(30));
        std::thread::sleep(Duration::from_millis(5));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_standby_outage_times_writes_out_and_lapses_the_lease_until_it_restarts() {
    let leader = Arc::new(leader(
        &scratch("outage"),
        sync(Duration::from_millis(300), Duration::from_secs(1)),
    ));
    let (url, _stop) = start(leader.clone()).await;
    let standby_dir = scratch("outage-standby");
    let (standby, following) = run_standby(&standby_dir, &url);
    eventually("the lease", || leader.healthy()).await;
    let mut client = EzuClient::connect(url.clone()).await.unwrap();
    client.upsert(upsert(0)).await.unwrap();

    kill(standby, following).await;
    // Logged here, never acknowledged: the outcome is unknown.
    let refused = client.upsert(upsert(1)).await.unwrap_err();
    assert_eq!(refused.code(), tonic::Code::Unavailable);
    assert!(refused.message().contains("unknown"), "{refused:?}");
    assert_eq!(leader.last_lsn().unwrap(), 2);

    // Past the lease, reads and writes are refused and health says so.
    eventually("the lease lapses", || !leader.healthy()).await;
    assert_eq!(health(&url).await, 503);
    let read = client
        .get(authed(proto::GetRequest {
            collection: "v".into(),
            id: "v0".into(),
            label_filter: None,
        }))
        .await
        .unwrap_err();
    assert_eq!(read.code(), tonic::Code::Unavailable);
    let metrics = leader.render_metrics(0).unwrap();
    assert!(metrics.contains("ezu_sync_lease_held 0"), "{metrics}");

    // The standby restarts from its own log, receives the unacknowledged
    // write, and its acknowledgements bring the leader back.
    let (standby, _following) = run_standby(&standby_dir, &url);
    eventually("the lease returns", || leader.healthy()).await;
    assert_eq!(health(&url).await, 200);
    client.upsert(upsert(2)).await.unwrap();
    assert_eq!(standby.last_lsn().unwrap(), 3);
    let read = client
        .get(authed(proto::GetRequest {
            collection: "v".into(),
            id: "v1".into(),
            label_filter: None,
        }))
        .await
        .unwrap();
    assert!(read.into_inner().entity.is_some());
}

#[test]
fn a_restarted_leader_serves_nothing_until_the_standby_acknowledges() {
    let dir = scratch("restart");
    let standby = sync(Duration::from_millis(200), Duration::from_secs(30));
    let first = Arc::new(leader(&dir, standby.clone()));
    first.acknowledge(STANDBY, 0, 0);
    first.put_fence(&ns(), fence(), 0).unwrap_err();
    // The fence was logged; its acknowledgement never came.
    assert_eq!(first.last_lsn().unwrap(), 1);
    drop(first);

    // A crash and restart keeps the log; nothing is served until the
    // standby acknowledges again, and then only up to what it holds.
    let restarted = leader(&dir, standby);
    assert!(!restarted.healthy());
    assert!(matches!(
        restarted.events(&ns(), None, &[], None, 10),
        Err(DurableError::NoStandby)
    ));
    assert!(restarted.acknowledge(STANDBY, 1, 0));
    assert!(restarted.healthy());
    assert!(restarted.get_fence(&ns(), "zone", None).unwrap().is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn events_are_served_only_up_to_the_acknowledged_lsn() {
    let leader = Arc::new(leader(
        &scratch("events"),
        sync(Duration::from_secs(30), Duration::from_secs(30)),
    ));
    assert!(leader.acknowledge(STANDBY, 0, 0));
    let fenced = leader.clone();
    let fencing = tokio::task::spawn_blocking(move || fenced.put_fence(&ns(), fence(), 0));
    eventually("the fence is logged", || leader.last_lsn().unwrap() == 1).await;
    leader.acknowledge(STANDBY, 1, 0);
    fencing.await.unwrap().unwrap();

    let auth: Arc<dyn Authenticator> = Arc::new(StaticToken::new(TOKEN, "t"));
    let api = Api::new(leader.clone(), auth);
    let bearer = format!("Bearer {TOKEN}");
    let mut subscribed = api
        .subscribe(Some(&bearer), proto::SubscribeRequest::default())
        .await
        .unwrap();
    // An entering write is logged, its event raised, but not acknowledged.
    let writing = leader.clone();
    let write = tokio::task::spawn_blocking(move || writing.upsert(&ns(), vehicle(0, 0.0), 0));
    eventually("the write is logged", || leader.last_lsn().unwrap() == 2).await;
    let page = leader.events(&ns(), None, &[], None, 10).unwrap();
    assert!(page.events.is_empty());
    assert!(page.through.0 < 2, "{page:?}");
    assert!(
        tokio::time::timeout(Duration::from_millis(100), subscribed.recv())
            .await
            .is_err(),
        "an unacknowledged event was streamed"
    );

    // Acknowledged: served, and the subscriber is woken.
    assert!(leader.acknowledge(STANDBY, 2, 0));
    write.await.unwrap().unwrap();
    let event = tokio::time::timeout(Duration::from_secs(30), subscribed.recv())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!((event.lsn, event.index), (2, 0));
    assert_eq!(
        leader
            .events(&ns(), None, &[], None, 10)
            .unwrap()
            .events
            .len(),
        1
    );

    // A resume point past this leader's log came from another history.
    let ahead = api
        .list_events(
            Some(&bearer),
            proto::ListEventsRequest {
                after_lsn: Some(3),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    assert_eq!(tonic::Status::from(ahead).code(), tonic::Code::OutOfRange);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_standby_at_a_higher_epoch_refuses_the_old_leader_which_acknowledges_nothing() {
    // One history of 10 records, then two leaders of it: `old` at epoch 0
    // and `new`, a copy promoted to epoch 1.
    let old_dir = scratch("old");
    let base = DurableEngine::open(config(&old_dir), quotas(), None, 0).unwrap();
    for i in 0..10 {
        base.upsert(&ns(), vehicle(i, 1.0), 0).unwrap();
    }
    drop(base);
    let new_dir = scratch("new");
    copy_dir(&old_dir, &new_dir);
    assert_eq!(DurableEngine::promote(&new_dir).unwrap(), 1);

    // The standby follows the new leader and takes its epoch from a
    // heartbeat, before any record of it: the new leader counts its
    // acknowledgements.
    let standby = sync(Duration::from_millis(300), Duration::from_secs(30));
    let new = Arc::new(leader(&new_dir, standby.clone()));
    let (new_url, stop_new) = start(new.clone()).await;
    let standby_dir = scratch("epoch-standby");
    let (engine, following) = run_standby(&standby_dir, &new_url);
    eventually("the new leader's lease", || new.healthy()).await;
    assert_eq!(engine.epoch().unwrap(), 1);
    assert_eq!(engine.last_lsn().unwrap(), 10);
    kill(engine, following).await;
    let _ = stop_new.send(());

    // The old leader goes on at epoch 0 with records the standby lacks.
    let old = DurableEngine::open(config(&old_dir), quotas(), None, 0).unwrap();
    old.upsert(&ns(), vehicle(10, 1.0), 0).unwrap();
    drop(old);
    let old = Arc::new(leader(&old_dir, standby));
    let (old_url, _stop_old) = start(old.clone()).await;
    let (engine, following) = run_standby(&standby_dir, &old_url);
    let stopped = tokio::time::timeout(Duration::from_secs(30), following)
        .await
        .unwrap()
        .unwrap();
    assert!(stopped.to_string().contains("epoch"), "{stopped}");
    assert_eq!(engine.last_lsn().unwrap(), 10);
    // Even an acknowledgement it sent would carry the higher epoch.
    assert!(!old.acknowledge(STANDBY, 10, 1));
    assert!(!old.healthy());
    assert_eq!(health(&old_url).await, 503);
    let mut client = EzuClient::connect(old_url).await.unwrap();
    let refused = client.upsert(upsert(11)).await.unwrap_err();
    assert_eq!(refused.code(), tonic::Code::Unavailable);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_standby_against_a_leader_without_sync_retries_visibly() {
    // A release from before sync standbys serves no `Replication.Sync`.
    let leader =
        Arc::new(DurableEngine::open(config(&scratch("legacy")), quotas(), None, 0).unwrap());
    let auth: Arc<dyn Authenticator> = Arc::new(StaticToken::new(TOKEN, "t"));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(EzuServer::new(Api::new(leader, auth)))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );
    let engine = Arc::new(follower(&scratch("legacy-standby")));
    let (retried, mut retries) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(ezu::follow::standby(
        engine.clone(),
        tonic::transport::Endpoint::from_shared(url).unwrap(),
        REPLICATION.into(),
        STANDBY.into(),
        move |why| {
            let _ = retried.send(why.to_string());
        },
    ));
    for _ in 0..2 {
        let why = tokio::time::timeout(Duration::from_secs(30), retries.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(why.contains("Unimplemented"), "{why}");
    }
    assert!(engine.healthy());
}

#[test]
fn a_snapshot_keeps_the_log_the_standby_has_not_acknowledged() {
    let leader = Arc::new(leader(
        &scratch("retain"),
        sync(Duration::from_secs(30), Duration::from_secs(30)),
    ));
    assert!(leader.acknowledge(STANDBY, 0, 0));
    // Two unacknowledged batches, the second in a segment of its own (the
    // first fills the 64 KiB segment), then a snapshot.
    let write = |from: u64| {
        let writing = leader.clone();
        std::thread::spawn(move || {
            let batch = (from..from + 1_000).map(|i| vehicle(i, 1.0)).collect();
            writing.upsert_batch(&ns(), batch, 0).map(|_| ())
        })
    };
    let first = write(0);
    eventually_blocking(|| leader.last_lsn().unwrap() == 1_000);
    let second = write(1_000);
    eventually_blocking(|| leader.last_lsn().unwrap() == 2_000);
    assert_eq!(leader.snapshot(0).unwrap(), 2_000);

    // The standby still receives all of it, and both writes are acknowledged.
    let standby = follower(&scratch("retain-standby"));
    let mut from = leader.shipping(0, None).unwrap();
    while standby.last_lsn().unwrap() < 2_000 {
        let applied = standby
            .apply_shipped(leader.ship(&mut from, 1 << 20).unwrap(), 0)
            .unwrap();
        leader.acknowledge(STANDBY, applied, 0);
    }
    first.join().unwrap().unwrap();
    second.join().unwrap().unwrap();
    // Once acknowledged, the next snapshot drops it as usual.
    leader.snapshot(0).unwrap();
    assert!(
        leader
            .shipping(0, None)
            .and_then(|mut from| leader.ship(&mut from, 1))
            .is_err()
    );
}

#[test]
fn a_resume_point_never_passes_the_acknowledged_lsn() {
    let dir = scratch("cursor");
    let mut retaining = config(&dir);
    retaining.events_retained = 3;
    let open = || {
        DurableEngine::open_sync(
            retaining.clone(),
            quotas(),
            None,
            0,
            sync(Duration::from_secs(2), Duration::from_secs(30)),
        )
        .unwrap()
    };
    let leader = Arc::new(open());
    assert!(leader.acknowledge(STANDBY, 0, 0));
    let fenced = leader.clone();
    let fencing = std::thread::spawn(move || fenced.put_fence(&ns(), fence(), 0));
    eventually_blocking(|| leader.last_lsn().unwrap() == 1);
    leader.acknowledge(STANDBY, 1, 0);
    fencing.join().unwrap().unwrap();

    // One batch raising more events than are retained, not acknowledged.
    let writing = leader.clone();
    let batch = std::thread::spawn(move || {
        let entering = (0..10).map(|i| vehicle(i, 0.0)).collect();
        writing.upsert_batch(&ns(), entering, 0).map(|_| ())
    });
    eventually_blocking(|| leader.last_lsn().unwrap() == 11);
    let page = leader.events(&ns(), None, &[], None, 10).unwrap();
    assert!(page.events.is_empty());
    assert!(page.through.0 <= 1, "{page:?}");

    // Acknowledged, a reader pages forward from there through every event.
    leader.acknowledge(STANDBY, 11, 0);
    batch.join().unwrap().unwrap();
    let (mut cursor, mut read) = (page.through, Vec::new());
    loop {
        let page = leader.events(&ns(), Some(cursor), &[], None, 2).unwrap();
        if page.events.is_empty() {
            break;
        }
        read.extend(page.events.iter().map(|event| event.lsn));
        cursor = page.through;
    }
    assert_eq!(read, (2..=11).collect::<Vec<_>>());
}

#[test]
fn unacknowledged_events_outlive_a_leader_restart() {
    let dir = scratch("restart-events");
    let mut retaining = config(&dir);
    retaining.events_retained = 3;
    let open = |timeout| {
        DurableEngine::open_sync(
            retaining.clone(),
            quotas(),
            None,
            0,
            sync(timeout, Duration::from_secs(30)),
        )
        .unwrap()
    };
    let leader = open(Duration::from_millis(200));
    assert!(leader.acknowledge(STANDBY, 0, 0));
    assert!(matches!(
        leader.put_fence(&ns(), fence(), 0),
        Err(DurableError::Unacknowledged(1))
    ));
    leader.acknowledge(STANDBY, 1, 0);
    // A batch raising more events than are retained, never acknowledged
    // before the leader goes down.
    let entering = (0..10).map(|i| vehicle(i, 0.0)).collect();
    assert!(matches!(
        leader.upsert_batch(&ns(), entering, 0),
        Err(DurableError::Unacknowledged(11))
    ));
    let cursor = leader.events(&ns(), None, &[], None, 10).unwrap().through;
    assert!(cursor.0 <= 1, "{cursor:?}");
    drop(leader);

    // Restarted, the standby catches up, and a reader resuming from its
    // cursor gets every event.
    let restarted = open(Duration::from_secs(30));
    assert!(restarted.acknowledge(STANDBY, 11, 0));
    let page = restarted
        .events(&ns(), Some(cursor), &[], None, 100)
        .unwrap();
    let read: Vec<u64> = page.events.iter().map(|event| event.lsn).collect();
    assert_eq!(read, (2..=11).collect::<Vec<_>>());
    // Acknowledged, the next snapshot trims them to the retention.
    restarted.snapshot(0).unwrap();
    let kept = restarted.events(&ns(), None, &[], None, 100).unwrap();
    assert_eq!(kept.events.len(), 3);
}

/// More concurrent API writes than blocking threads, against a standby
/// shipped and applied on the same small pool: waits for acknowledgements
/// must not hold the threads the shipping needs.
#[test]
fn waiting_writes_do_not_starve_the_shipping_that_acknowledges_them() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(2)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let leader = Arc::new(leader(
            &scratch("pool"),
            sync(Duration::from_secs(5), Duration::from_secs(30)),
        ));
        let (url, _stop) = start(leader.clone()).await;
        let (_standby, _following) = run_standby(&scratch("pool-standby"), &url);
        eventually("the lease", || leader.healthy()).await;
        let client = EzuClient::connect(url).await.unwrap();
        let writes: Vec<_> = (0..16)
            .map(|i| {
                let mut client = client.clone();
                tokio::spawn(async move { client.upsert(upsert(i)).await.map(|_| ()) })
            })
            .collect();
        for write in writes {
            write.await.unwrap().unwrap();
        }
        assert_eq!(leader.standby().unwrap().acked_lsn, 16);
    });
}
