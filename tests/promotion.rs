//! Promotion (#42): `Replication.Promote` turns the sync standby into a
//! leader in place at the next epoch, continuing LSNs and event ids, once
//! and idempotently. An old leader acknowledges nothing after it and is
//! refused as the new leader's follower; a forced promotion elsewhere is
//! refused by lineage even when its last record is the same; a crash
//! mid-promotion is completed by a leader open and refused by a follower
//! open; other followers are shipped only what the standby acknowledged.

use ezu::api::proto::ezu_client::EzuClient;
use ezu::api::proto::replication_client::ReplicationClient;
use ezu::api::proto::{self};
use ezu::api::{Api, serve};
use ezu::auth::{Authenticator, StaticToken};
use ezu::fence::{Fence, Shape};
use ezu::{
    DurabilityConfig, DurableEngine, DurableError, Entity, EpochStart, Geometry, Identity,
    Namespace, Position, Quotas, ReplicaError, SyncStandby,
};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

static SEQ: AtomicU64 = AtomicU64::new(0);
const TOKEN: &str = "tenant-token";
const REPLICATION: &str = "replication-token";
const STANDBY: &str = "standby-1";

fn scratch(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "ezu-promotion-{name}-{}-{}",
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

fn follower(dir: &Path) -> DurableEngine {
    DurableEngine::open_follower(config(dir), quotas(), 0).unwrap()
}

/// Entity `i` at `x`; the fence at 0 holds `x` near 0.
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

/// Serve `engine` as the node `node_id`, whose promotion waits `lease`;
/// returns its URL and a stop.
async fn start(
    engine: Arc<DurableEngine>,
    node_id: &str,
    lease: Duration,
) -> (String, tokio::sync::oneshot::Sender<()>) {
    let auth: Arc<dyn Authenticator> = Arc::new(StaticToken::new(TOKEN, "t"));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = format!("http://{}", listener.local_addr().unwrap());
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let api = Api::new(engine, auth)
        .replication_token(Some(REPLICATION))
        .promotion(Some(node_id), lease);
    tokio::spawn(serve(api, listener, None, async {
        let _ = stopped.await;
    }));
    (address, stop)
}

fn endpoint(url: &str) -> tonic::transport::Endpoint {
    tonic::transport::Endpoint::from_shared(url.to_string()).unwrap()
}

/// Wait, bounded, until `condition` holds.
async fn eventually(what: &str, condition: impl Fn() -> bool) {
    let started = Instant::now();
    while !condition() {
        assert!(started.elapsed() < Duration::from_secs(30), "never: {what}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Wait until nothing but `engine` holds the engine, so its directory can
/// be opened again.
async fn released(engine: Arc<DurableEngine>) {
    eventually("the engine is released", || Arc::strong_count(&engine) == 1).await;
}

fn bearer<T>(message: T, token: &str) -> tonic::Request<T> {
    let mut request = tonic::Request::new(message);
    request
        .metadata_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
    request
}

async fn promote(url: &str, force: bool) -> Result<u64, tonic::Status> {
    let mut client = ReplicationClient::connect(url.to_string()).await.unwrap();
    client
        .promote(bearer(proto::PromoteRequest { force }, REPLICATION))
        .await
        .map(|response| response.into_inner().epoch)
}

fn upsert(i: u64, x: f64) -> tonic::Request<proto::UpsertRequest> {
    let entity = proto::Entity {
        collection: "v".into(),
        id: format!("v{i}"),
        geometry: Some(proto::Geometry {
            shape: Some(proto::geometry::Shape::Point(proto::Position {
                lon: x,
                lat: 0.0,
            })),
        }),
        ts_ms: 1 + i as i64,
        ..Default::default()
    };
    bearer(
        proto::UpsertRequest {
            entities: vec![entity],
        },
        TOKEN,
    )
}

/// The ids of every event `engine` keeps for the test namespace.
fn event_ids(engine: &DurableEngine) -> Vec<(u64, u32)> {
    engine
        .events(&ns(), None, &[], None, usize::MAX)
        .unwrap()
        .events
        .iter()
        .map(|event| (event.lsn, event.index))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_sync_standby_promotes_once_and_the_log_and_event_ids_go_on() {
    let leader = Arc::new(
        DurableEngine::open_sync(
            config(&scratch("leader")),
            quotas(),
            None,
            0,
            sync(Duration::from_secs(30), Duration::from_secs(30)),
        )
        .unwrap(),
    );
    let (leader_url, stop_leader) = start(leader.clone(), "leader", Duration::ZERO).await;
    let standby_dir = scratch("standby");
    let standby = Arc::new(follower(&standby_dir));
    let following = tokio::spawn(ezu::follow::standby(
        standby.clone(),
        endpoint(&leader_url),
        REPLICATION.into(),
        STANDBY.into(),
        |_| {},
    ));
    let lease = Duration::from_millis(300);
    let (standby_url, _stop_standby) = start(standby.clone(), STANDBY, lease).await;
    let other_dir = scratch("other");
    let other = Arc::new(follower(&other_dir));
    let other_following = tokio::spawn(ezu::follow::follow(
        other.clone(),
        endpoint(&leader_url),
        REPLICATION.into(),
        |_| {},
    ));
    let (other_url, stop_other) = start(other.clone(), "other", lease).await;
    eventually("the lease", || leader.healthy()).await;

    let fenced = leader.clone();
    tokio::task::spawn_blocking(move || fenced.put_fence(&ns(), fence(), 0))
        .await
        .unwrap()
        .unwrap();
    let mut client = EzuClient::connect(leader_url.clone()).await.unwrap();
    for i in 0..20 {
        client.upsert(upsert(i, 0.0)).await.unwrap();
    }
    let start_lsn = leader.last_lsn().unwrap();
    // The leader told the standby it is its standby, and it kept that.
    eventually("the designation is kept", || designated(&standby_dir)).await;
    eventually("the other follower converges", || {
        other.last_lsn().unwrap() == start_lsn
    })
    .await;

    // Without force, only the standby promotes.
    let refused = promote(&other_url, false).await.unwrap_err();
    assert_eq!(refused.code(), tonic::Code::FailedPrecondition);
    assert!(other.replication().is_some());

    // Asked twice at once, one promotion and one epoch.
    let (first, second) = tokio::join!(promote(&standby_url, false), promote(&standby_url, false));
    assert_eq!((first.unwrap(), second.unwrap()), (1, 1));
    assert_eq!(
        following.await.unwrap().to_string(),
        "this node stopped following to be promoted"
    );
    assert!(standby.replication().is_none());
    assert!(standby.healthy());
    assert!(!standby_dir.join("FOLLOWER").exists());
    assert_eq!(standby.last_lsn().unwrap(), start_lsn);
    assert_eq!(
        standby.lineage(),
        [EpochStart {
            epoch: 1,
            start_lsn,
            node_id: STANDBY.into(),
        }]
    );

    // The promoted standby takes writes after the applied LSN, and their
    // events take the next ids.
    let before = event_ids(&standby);
    assert_eq!(before, event_ids(&leader));
    let mut promoted = EzuClient::connect(standby_url.clone()).await.unwrap();
    promoted.upsert(upsert(100, 0.0)).await.unwrap();
    assert_eq!(standby.last_lsn().unwrap(), start_lsn + 1);
    let after = event_ids(&standby);
    assert_eq!(after[..before.len()], before[..]);
    assert_eq!(after[before.len()..], [(start_lsn + 1, 0)]);
    assert_eq!(promote(&standby_url, false).await.unwrap(), 1);

    // A resume point past its log came from another history.
    let ahead = promoted
        .list_events(bearer(
            proto::ListEventsRequest {
                after_lsn: Some(start_lsn + 10),
                ..Default::default()
            },
            TOKEN,
        ))
        .await
        .unwrap_err();
    assert_eq!(ahead.code(), tonic::Code::OutOfRange);

    // The other follower switches to the promoted standby: its lineage is a
    // prefix of the new leader's, and it takes the new epoch's start.
    let _ = stop_leader.send(());
    other_following.abort();
    let _ = other_following.await;
    let _ = stop_other.send(());
    released(other).await;
    let other = Arc::new(follower(&other_dir));
    tokio::spawn(ezu::follow::follow(
        other.clone(),
        endpoint(&standby_url),
        REPLICATION.into(),
        |_| {},
    ));
    eventually("the other follower converges", || {
        other.last_lsn().unwrap() == start_lsn + 1
    })
    .await;
    assert!(other.healthy());
    assert_eq!(other.epoch().unwrap(), 1);
    assert_eq!(other.lineage(), standby.lineage());
    assert_eq!(event_ids(&other), after);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_old_leader_acknowledges_nothing_after_a_promotion_and_is_refused_as_a_follower() {
    let lease = Duration::from_secs(1);
    let leader_dir = scratch("old");
    let leader = Arc::new(
        DurableEngine::open_sync(
            config(&leader_dir),
            quotas(),
            None,
            0,
            sync(Duration::from_millis(300), lease),
        )
        .unwrap(),
    );
    let (leader_url, stop_leader) = start(leader.clone(), "old", lease).await;
    let standby_dir = scratch("new");
    let standby = Arc::new(follower(&standby_dir));
    tokio::spawn(ezu::follow::standby(
        standby.clone(),
        endpoint(&leader_url),
        REPLICATION.into(),
        STANDBY.into(),
        |_| {},
    ));
    let (standby_url, _stop_standby) = start(standby.clone(), STANDBY, lease).await;
    eventually("the lease", || leader.healthy()).await;
    let mut client = EzuClient::connect(leader_url.clone()).await.unwrap();
    for i in 0..5 {
        client.upsert(upsert(i, 1.0)).await.unwrap();
    }
    eventually("the designation", || designated(&standby_dir)).await;

    // The standby is promoted while the old leader runs on, cut off from it.
    // It waited out the old leader's lease since its last acknowledgement,
    // so the old leader serves nothing by the time it takes writes.
    assert_eq!(promote(&standby_url, false).await.unwrap(), 1);
    assert!(!leader.healthy());
    let refused = client.upsert(upsert(5, 1.0)).await.unwrap_err();
    assert_eq!(refused.code(), tonic::Code::Unavailable);
    let mut promoted = EzuClient::connect(standby_url.clone()).await.unwrap();
    promoted.upsert(upsert(6, 1.0)).await.unwrap();
    assert_eq!(standby.last_lsn().unwrap(), 6);

    // The old leader logged a write past the promotion point on its own.
    let _ = stop_leader.send(());
    released(leader).await;
    let alone = DurableEngine::open(config(&leader_dir), quotas(), None, 0).unwrap();
    alone.upsert(&ns(), vehicle(7, 1.0), 0).unwrap();
    assert_eq!(alone.last_lsn().unwrap(), 6);
    drop(alone);

    // Rejoining as the new leader's follower, it is refused, its log left
    // as it is.
    let rejoining = Arc::new(follower(&leader_dir));
    let stopped = tokio::time::timeout(
        Duration::from_secs(30),
        ezu::follow::follow(
            rejoining.clone(),
            endpoint(&standby_url),
            REPLICATION.into(),
            |_| {},
        ),
    )
    .await
    .unwrap();
    assert!(stopped.to_string().contains("epoch history"), "{stopped}");
    assert_eq!(rejoining.last_lsn().unwrap(), 6);
    assert!(!rejoining.healthy());
}

/// Whether the follower in `dir` kept that its leader named it the sync
/// standby, so a promotion without force is accepted.
fn designated(dir: &Path) -> bool {
    std::fs::read_to_string(dir.join("FOLLOWER"))
        .is_ok_and(|marker| marker.lines().any(|line| line == "standby"))
}

/// Ten records shipped from `leader` into a new follower in `dir`.
fn replica(leader: &DurableEngine, dir: &Path) -> DurableEngine {
    let replica = follower(dir);
    let mut from = leader.shipping(0, None).unwrap();
    while replica.last_lsn().unwrap() < leader.last_lsn().unwrap() {
        replica
            .apply_shipped(leader.ship(&mut from, usize::MAX).unwrap(), 0)
            .unwrap();
    }
    replica
}

#[test]
fn a_forced_promotion_elsewhere_is_refused_by_lineage_even_with_the_same_last_record() {
    let leader = DurableEngine::open(config(&scratch("source")), quotas(), None, 0).unwrap();
    for i in 0..10 {
        leader.upsert(&ns(), vehicle(i, 1.0), 0).unwrap();
    }
    let a = Arc::new(replica(&leader, &scratch("a")));
    let b_dir = scratch("b");
    let b = replica(&leader, &b_dir);
    let c = replica(&leader, &scratch("c"));

    // Neither is a sync standby: only force promotes them.
    assert!(matches!(
        b.promote_follower("b", false, Duration::ZERO),
        Err(DurableError::NotPromotable(_))
    ));
    // Racing promotions of one node give one epoch.
    let racing: Vec<_> = (0..2)
        .map(|_| {
            let a = a.clone();
            std::thread::spawn(move || a.promote_follower("a", true, Duration::ZERO))
        })
        .collect();
    for promotion in racing {
        assert_eq!(promotion.join().unwrap().unwrap(), 1);
    }
    assert_eq!(b.promote_follower("b", true, Duration::ZERO).unwrap(), 1);

    // Both started epoch 1 after the same record 10: the last record alone
    // would pass, the lineage does not.
    drop(b);
    let b = follower(&b_dir);
    let last = b.fingerprint(10).unwrap();
    assert!(a.shipping(10, last).is_ok());
    assert!(matches!(
        a.check_lineage(10, &b.lineage()),
        Err(DurableError::Replica(ReplicaError::Lineage(10)))
    ));

    // A follower that was never promoted follows the new leader, and takes
    // its epoch start with its first record.
    assert!(a.check_lineage(10, &c.lineage()).is_ok());
    a.upsert(&ns(), vehicle(10, 1.0), 0).unwrap();
    let mut from = a.shipping(10, c.fingerprint(10).unwrap()).unwrap();
    c.apply_shipped(a.ship(&mut from, usize::MAX).unwrap(), 0)
        .unwrap();
    assert_eq!(c.lineage(), a.lineage());
    assert!(a.check_lineage(11, &c.lineage()).is_ok());
}

#[test]
fn a_crash_mid_promotion_is_completed_by_a_leader_open_and_refused_by_a_follower_open() {
    let leader = DurableEngine::open(config(&scratch("crash-source")), quotas(), None, 0).unwrap();
    for i in 0..10 {
        leader.upsert(&ns(), vehicle(i, 1.0), 0).unwrap();
    }
    let dir = scratch("crash");
    drop(replica(&leader, &dir));
    let copy = scratch("crash-copy");
    copy_dir(&dir, &copy);
    let intent = EpochStart {
        epoch: 1,
        start_lsn: 10,
        node_id: "node-f".into(),
    };
    // The intent is synced, then the process dies.
    std::fs::write(
        dir.join("FOLLOWER"),
        "A follower's data directory; a leader refuses to open it.\nstandby\npromoting 1 10 node-f\n",
    )
    .unwrap();
    assert!(matches!(
        DurableEngine::open_follower(config(&dir), quotas(), 0),
        Err(DurableError::PromotionPending)
    ));
    let promoted = DurableEngine::open(config(&dir), quotas(), None, 0).unwrap();
    assert_eq!(promoted.epoch().unwrap(), 1);
    assert_eq!(promoted.lineage(), std::slice::from_ref(&intent));
    assert!(!dir.join("FOLLOWER").exists());
    promoted.upsert(&ns(), vehicle(10, 1.0), 0).unwrap();
    assert_eq!(promoted.last_lsn().unwrap(), 11);
    // Asked again after the restart, the same epoch; another node is not
    // this node's promotion.
    assert_eq!(
        promoted
            .promote_follower("node-f", false, Duration::ZERO)
            .unwrap(),
        1
    );
    assert!(matches!(
        promoted.promote_follower("node-g", true, Duration::ZERO),
        Err(DurableError::NotPromotable(_))
    ));
    drop(promoted);
    assert_eq!(
        DurableEngine::open(config(&dir), quotas(), None, 0)
            .unwrap()
            .last_lsn()
            .unwrap(),
        11
    );

    // A crash after the lineage was written, before the epoch: the open
    // completes the raise from the lineage.
    std::fs::copy(dir.join("wal/LINEAGE"), copy.join("wal/LINEAGE")).unwrap();
    std::fs::write(
        copy.join("FOLLOWER"),
        "A follower's data directory; a leader refuses to open it.\npromoting 1 10 node-f\n",
    )
    .unwrap();
    let completed = DurableEngine::open(config(&copy), quotas(), None, 0).unwrap();
    assert_eq!(completed.epoch().unwrap(), 1);
    assert_eq!(completed.lineage(), [intent]);
    assert!(!copy.join("FOLLOWER").exists());
}

#[test]
fn other_followers_are_shipped_only_what_the_standby_acknowledged() {
    let leader = Arc::new(
        DurableEngine::open_sync(
            config(&scratch("capped")),
            quotas(),
            None,
            0,
            sync(Duration::from_secs(30), Duration::from_secs(30)),
        )
        .unwrap(),
    );
    assert!(leader.acknowledge(STANDBY, 0, 0, leader.stamp()));
    let writing = leader.clone();
    let write = std::thread::spawn(move || writing.upsert(&ns(), vehicle(0, 1.0), 0));
    let started = Instant::now();
    while leader.last_lsn().unwrap() < 1 {
        assert!(started.elapsed() < Duration::from_secs(30));
        std::thread::sleep(Duration::from_millis(5));
    }
    let mut capped = leader.shipping(0, None).unwrap().acknowledged_only();
    let mut standby = leader.shipping(0, None).unwrap();
    assert!(
        leader
            .ship(&mut capped, usize::MAX)
            .unwrap()
            .records
            .is_empty()
    );
    assert_eq!(
        leader.ship(&mut standby, usize::MAX).unwrap().records.len(),
        1
    );
    assert!(leader.acknowledge(STANDBY, 1, 0, leader.stamp()));
    write.join().unwrap().unwrap();
    assert_eq!(
        leader.ship(&mut capped, usize::MAX).unwrap().records.len(),
        1
    );
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

/// A follower in a new directory that its leader named the sync standby,
/// reopened as a restarted standby would be.
fn restarted_standby(dir: &Path) -> DurableEngine {
    drop(follower(dir));
    std::fs::write(
        dir.join("FOLLOWER"),
        "A follower's data directory; a leader refuses to open it.\nstandby\n",
    )
    .unwrap();
    follower(dir)
}

#[test]
fn a_restarted_standby_waits_a_full_lease_before_it_is_promoted() {
    // It may have acknowledged its old leader just before the restart, and
    // that is not known any more: the wait counts from the open.
    let standby = restarted_standby(&scratch("restarted"));
    let opened = Instant::now();
    let lease = Duration::from_millis(500);
    assert_eq!(standby.promote_follower(STANDBY, true, lease).unwrap(), 1);
    assert!(opened.elapsed() >= lease, "{:?}", opened.elapsed());
}

#[test]
fn a_late_acknowledgement_does_not_renew_the_old_leaders_lease() {
    let lease = Duration::from_millis(300);
    let leader = DurableEngine::open_sync(
        config(&scratch("late")),
        quotas(),
        None,
        0,
        sync(Duration::from_secs(30), lease),
    )
    .unwrap();
    let sent = leader.stamp();
    assert!(leader.acknowledge(STANDBY, 0, 0, sent));
    assert!(leader.healthy());
    std::thread::sleep(lease);
    assert!(!leader.healthy());
    // The acknowledgement of what was sent then arrives only now (held in
    // transit): it counts, but the lease runs from the stamp it echoes, so
    // reads and writes stay refused.
    assert!(leader.acknowledge(STANDBY, 0, 0, sent));
    assert!(!leader.healthy());
    assert!(matches!(
        leader.get(&ns(), "v", "v0", None, 0),
        Err(DurableError::NoStandby)
    ));
    // One echoing what was sent now renews it.
    assert!(leader.acknowledge(STANDBY, 0, 0, leader.stamp()));
    assert!(leader.healthy());
}

#[test]
fn a_failure_while_a_promotion_waits_refuses_it_and_stays_failed() {
    let dir = scratch("failing");
    let standby = Arc::new(restarted_standby(&dir));
    let promoting = standby.clone();
    let promotion = std::thread::spawn(move || {
        promoting.promote_follower(STANDBY, true, Duration::from_millis(500))
    });
    // An apply under way when the promotion began fails during its wait, as
    // the following task reports it.
    std::thread::sleep(Duration::from_millis(100));
    standby.end_following(DurableError::Failed);
    assert!(matches!(
        promotion.join().unwrap(),
        Err(DurableError::NotPromotable(_))
    ));
    assert!(standby.failed());
    assert!(!standby.healthy());
    assert!(standby.replication().is_some());
    assert!(dir.join("FOLLOWER").exists());
    assert_eq!(standby.epoch().unwrap(), 0);
}

#[test]
fn an_unstamped_acknowledgement_from_an_earlier_standby_renews_the_lease_from_receipt() {
    let lease = Duration::from_millis(300);
    let leader = DurableEngine::open_sync(
        config(&scratch("unstamped")),
        quotas(),
        None,
        0,
        sync(Duration::from_secs(30), lease),
    )
    .unwrap();
    // A standby of the release before promotion echoes no stamp (0): its
    // acknowledgements keep the lease, renewed when they arrive.
    for _ in 0..3 {
        assert!(leader.acknowledge(STANDBY, 0, 0, 0));
        assert!(leader.healthy());
        std::thread::sleep(lease / 2);
    }
    assert!(leader.standby().unwrap().unstamped);
    assert!(
        leader
            .render_metrics(0)
            .unwrap()
            .contains("ezu_sync_unstamped 1")
    );
    // A stamped one still counts from its stamp, however late it arrives.
    let sent = leader.stamp();
    std::thread::sleep(lease);
    assert!(leader.acknowledge(STANDBY, 0, 0, sent));
    assert!(!leader.healthy());
    assert!(!leader.standby().unwrap().unstamped);
}

#[test]
fn a_standby_process_without_a_stamped_acknowledgement_is_not_promoted_without_force() {
    // Its process before may have sent unstamped acknowledgements, which an
    // old leader renews its lease from on receipt, however late.
    let standby = restarted_standby(&scratch("unstamped-standby"));
    assert!(matches!(
        standby.promote_follower(STANDBY, false, Duration::ZERO),
        Err(DurableError::NotPromotable(why)) if why.contains("stamp")
    ));
    assert!(standby.replication().is_some());
    assert!(standby.may_acknowledge(false));
}

#[test]
fn after_a_stamped_acknowledgement_unstamped_ones_are_refused_and_the_lease_follows_the_stamp() {
    let lease = Duration::from_millis(300);
    let leader = DurableEngine::open_sync(
        config(&scratch("barrier")),
        quotas(),
        None,
        0,
        sync(Duration::from_secs(30), lease),
    )
    .unwrap();
    assert!(leader.acknowledge(STANDBY, 0, 0, 0));
    let sent = leader.stamp();
    assert!(leader.acknowledge(STANDBY, 0, 0, sent));
    std::thread::sleep(lease);
    // A late unstamped acknowledgement of the standby's earlier process.
    assert!(!leader.acknowledge(STANDBY, 0, 0, 0));
    assert!(!leader.healthy());
    let standby = leader.standby().unwrap();
    assert_eq!((standby.unstamped, standby.refused_unstamped), (false, 1));
    assert!(
        leader
            .render_metrics(0)
            .unwrap()
            .contains("ezu_sync_unstamped_refused 1")
    );
}

#[test]
fn a_newer_sync_stream_supersedes_the_older_whose_acknowledgements_are_not_counted() {
    let leader = DurableEngine::open_sync(
        config(&scratch("sessions")),
        quotas(),
        None,
        0,
        sync(Duration::from_secs(30), Duration::from_secs(30)),
    )
    .unwrap();
    assert_eq!(leader.sync_session("other"), None);
    let older = leader.sync_session(STANDBY).unwrap();
    assert!(leader.acknowledge_on(older, STANDBY, 0, 0, leader.stamp()));
    let newer = leader.sync_session(STANDBY).unwrap();
    assert!(!leader.session_current(older));
    assert!(!leader.acknowledge_on(older, STANDBY, 0, 0, leader.stamp()));
    assert!(leader.acknowledge_on(newer, STANDBY, 0, 0, leader.stamp()));
}

#[test]
fn a_follower_behind_the_promotion_point_takes_the_new_epoch_once_it_holds_its_start() {
    let source = DurableEngine::open(config(&scratch("behind-source")), quotas(), None, 0).unwrap();
    for i in 0..10 {
        source.upsert(&ns(), vehicle(i, 1.0), 0).unwrap();
    }
    let promoted = replica(&source, &scratch("behind-promoted"));
    assert_eq!(
        promoted
            .promote_follower("p", true, Duration::ZERO)
            .unwrap(),
        1
    );
    // A follower that holds only the first five records.
    let behind = follower(&scratch("behind"));
    let mut from = source.shipping(0, None).unwrap();
    while behind.last_lsn().unwrap() < 5 {
        behind
            .apply_shipped(source.ship(&mut from, 1).unwrap(), 0)
            .unwrap();
    }
    // A heartbeat of the new leader's epoch, which starts after record 10:
    // left for later, and the follower goes on.
    behind.follow_leader_epoch(1, &promoted.lineage()).unwrap();
    assert!(behind.healthy());
    assert_eq!(behind.epoch().unwrap(), 0);
    assert!(behind.lineage().is_empty());
    // With the records up to the start and the first after it, it takes it.
    promoted.upsert(&ns(), vehicle(10, 1.0), 0).unwrap();
    let mut from = promoted
        .shipping(5, behind.fingerprint(5).unwrap())
        .unwrap();
    while behind.last_lsn().unwrap() < 11 {
        behind
            .apply_shipped(promoted.ship(&mut from, usize::MAX).unwrap(), 0)
            .unwrap();
    }
    assert!(behind.healthy());
    assert_eq!(behind.epoch().unwrap(), 1);
    assert_eq!(behind.lineage(), promoted.lineage());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_promoted_elsewhere_is_refused_without_a_last_record() {
    // Two empty followers, promoted independently: both at epoch 1 after
    // LSN 0, with no record to fingerprint.
    let a = Arc::new(follower(&scratch("empty-a")));
    assert_eq!(a.promote_follower("a", true, Duration::ZERO).unwrap(), 1);
    let b_dir = scratch("empty-b");
    let b = follower(&b_dir);
    assert_eq!(b.promote_follower("b", true, Duration::ZERO).unwrap(), 1);
    drop(b);
    a.upsert(&ns(), vehicle(0, 1.0), 0).unwrap();
    let (url, _stop) = start(a.clone(), "a", Duration::ZERO).await;

    // The leader checks its lineage although it sends no last record.
    let b = Arc::new(follower(&b_dir));
    let stopped = tokio::time::timeout(
        Duration::from_secs(30),
        ezu::follow::follow(b.clone(), endpoint(&url), REPLICATION.into(), |_| {}),
    )
    .await
    .unwrap();
    assert!(stopped.to_string().contains("epoch history"), "{stopped}");
    assert_eq!(b.last_lsn().unwrap(), 0);
    // So does the follower, shipped the other history directly.
    let shipped = a
        .ship(&mut a.shipping(0, None).unwrap(), usize::MAX)
        .unwrap();
    let other_dir = scratch("empty-b2");
    let other = follower(&other_dir);
    assert_eq!(
        other.promote_follower("b", true, Duration::ZERO).unwrap(),
        1
    );
    drop(other);
    let other = follower(&other_dir);
    assert!(matches!(
        other.apply_shipped(shipped, 0),
        Err(DurableError::Replica(ReplicaError::Lineage(0)))
    ));
    assert_eq!(other.last_lsn().unwrap(), 0);

    // A follower with no lineage of its own follows and takes the leader's.
    let fresh = Arc::new(follower(&scratch("empty-fresh")));
    tokio::spawn(ezu::follow::follow(
        fresh.clone(),
        endpoint(&url),
        REPLICATION.into(),
        |_| {},
    ));
    eventually("the fresh follower converges", || {
        fresh.last_lsn().unwrap() == 1
    })
    .await;
    assert_eq!(fresh.lineage(), a.lineage());
}
