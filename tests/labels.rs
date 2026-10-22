//! Labels (#12): entities and fences carry opaque labels; a reader sees only
//! what carries no label outside its filter; labels survive the log,
//! snapshots and restarts; governed mode refuses reads without a filter.

use ezu::api::proto::{self, ezu_client::EzuClient};
use ezu::api::{Api, serve};
use ezu::auth::{Authenticator, StaticToken};
use ezu::entity::Labels;
use ezu::fence::{EventKind, Fence, Shape};
use ezu::{
    DurabilityConfig, DurableEngine, DurableError, Entity, EntityError, Geometry, Identity,
    IndexError, Namespace, Position, Query, QueryRequest, Quotas,
};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

static SEQ: AtomicU64 = AtomicU64::new(0);

fn scratch() -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "ezu-labels-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn open(dir: &Path) -> DurableEngine {
    DurableEngine::open(
        DurabilityConfig {
            dir: dir.to_path_buf(),
            fsync: false,
            segment_bytes: 1 << 20,
            snapshot_every_records: u64::MAX,
            snapshot_every_ms: i64::MAX,
            events_retained: 100,
            keys: None,
        },
        Quotas {
            max_entities: 1_000,
            max_collections: 4,
            max_vertices: 64,
            max_updates_per_second: u32::MAX,
            max_fences: 10,
        },
        None,
        0,
    )
    .unwrap()
}

fn ns() -> Namespace {
    Namespace::of(&Identity::authenticated("svc", "t"))
}

fn labels(names: &[&str]) -> Labels {
    names.iter().map(|name| name.to_string()).collect()
}

fn ship(id: &str, lon: f64, marks: &[&str]) -> Entity {
    Entity {
        collection: "ships".into(),
        id: id.into(),
        geometry: Geometry::Point(Position::new(lon, 50.0)),
        ts_ms: 1,
        speed: None,
        heading: None,
        ttl_ms: None,
        fields: BTreeMap::new(),
        labels: labels(marks),
    }
}

fn world(readable: Option<Labels>, limit: usize) -> QueryRequest {
    QueryRequest {
        collection: "ships".into(),
        query: Query::Bbox([-180.0, -90.0, 180.0, 90.0]),
        filters: Vec::new(),
        limit,
        cursor: None,
        readable,
    }
}

fn ids(engine: &DurableEngine, readable: Option<Labels>) -> Vec<String> {
    let mut found = Vec::new();
    let mut request = world(readable, 2);
    loop {
        let page = engine.query(&ns(), &request, 0).unwrap();
        found.extend(page.entities.into_iter().map(|e| e.id));
        match page.next_cursor {
            Some(cursor) => request.cursor = Some(cursor),
            None => return found,
        }
    }
}

#[test]
fn a_reader_sees_only_what_carries_no_label_outside_its_filter() {
    let dir = scratch();
    let engine = open(&dir);
    for entity in [
        ship("a", 10.0, &[]),
        ship("b", 10.1, &["secret"]),
        ship("c", 10.2, &["secret", "eu"]),
        ship("d", 10.3, &["eu"]),
        ship("e", 10.4, &[]),
    ] {
        engine.upsert(&ns(), entity, 0).unwrap();
    }
    // Paged in twos, so the filter applies before the limit.
    assert_eq!(ids(&engine, Some(labels(&[]))), ["a", "e"]);
    assert_eq!(ids(&engine, Some(labels(&["eu"]))), ["a", "d", "e"]);
    assert_eq!(
        ids(&engine, Some(labels(&["eu", "secret"]))),
        ["a", "b", "c", "d", "e"]
    );
    assert_eq!(ids(&engine, None).len(), 5);
    let only_eu = labels(&["eu"]);
    assert!(
        engine
            .get(&ns(), "ships", "c", Some(&only_eu), 0)
            .unwrap()
            .is_none()
    );
    assert!(
        engine
            .get(&ns(), "ships", "d", Some(&only_eu), 0)
            .unwrap()
            .is_some()
    );

    // Labels survive the log, a snapshot and restarts.
    engine.snapshot(0).unwrap();
    engine
        .upsert(&ns(), ship("f", 10.5, &["secret"]), 0)
        .unwrap();
    drop(engine);
    let engine = open(&dir);
    assert_eq!(ids(&engine, Some(labels(&[]))), ["a", "e"]);
    let c = engine.get(&ns(), "ships", "c", None, 0).unwrap().unwrap();
    assert_eq!(c.labels, labels(&["eu", "secret"]));
    let f = engine.get(&ns(), "ships", "f", None, 0).unwrap().unwrap();
    assert_eq!(f.labels, labels(&["secret"]));
    drop(engine);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn events_carry_both_label_sets_and_are_filtered_by_them() {
    let dir = scratch();
    let engine = open(&dir);
    let harbour = Fence {
        id: "harbour".into(),
        shape: Shape::Circle {
            center: Position::new(10.0, 50.0),
            meters: 1_000.0,
        },
        collections: Vec::new(),
        filters: Vec::new(),
        dwell_ms: None,
        inside_per_second: None,
        labels: labels(&["zone"]),
    };
    engine.put_fence(&ns(), harbour, 0).unwrap();
    engine.upsert(&ns(), ship("a", 10.0, &[]), 0).unwrap();
    engine
        .upsert(&ns(), ship("b", 10.0, &["secret"]), 0)
        .unwrap();
    let seen = |engine: &DurableEngine, readable: &[&str]| -> Vec<String> {
        let readable = labels(readable);
        engine
            .events(&ns(), None, &[], Some(&readable), 10)
            .unwrap()
            .events
            .into_iter()
            .map(|e| e.entity)
            .collect()
    };
    assert!(seen(&engine, &[]).is_empty());
    assert_eq!(seen(&engine, &["zone"]), ["a"]);
    assert_eq!(seen(&engine, &["zone", "secret"]), ["a", "b"]);
    let all = engine.events(&ns(), None, &[], None, 10).unwrap().events;
    assert_eq!(
        (all[1].kind, &all[1].entity_labels, &all[1].fence_labels),
        (EventKind::Enter, &labels(&["secret"]), &labels(&["zone"]))
    );
    // The fence filter applies to fences too, and everything persists.
    assert!(
        engine
            .get_fence(&ns(), "harbour", Some(&labels(&[])))
            .unwrap()
            .is_none()
    );
    engine.snapshot(0).unwrap();
    engine.delete(&ns(), "ships", "b", 0).unwrap();
    drop(engine);
    let engine = open(&dir);
    assert_eq!(seen(&engine, &["zone", "secret"]), ["a", "b", "b"]);
    let exit = engine.events(&ns(), None, &[], None, 10).unwrap().events[2].clone();
    assert_eq!(
        (exit.kind, exit.entity_labels, exit.fence_labels),
        (EventKind::Exit, labels(&["secret"]), labels(&["zone"]))
    );
    let fence = engine.get_fence(&ns(), "harbour", None).unwrap().unwrap();
    assert_eq!(fence.labels, labels(&["zone"]));
    drop(engine);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn malformed_labels_are_refused() {
    let dir = scratch();
    let engine = open(&dir);
    let many: Vec<String> = (0..17).map(|i| format!("l{i}")).collect();
    let many: Vec<&str> = many.iter().map(String::as_str).collect();
    for bad in [
        ship("x", 1.0, &many),
        ship("y", 1.0, &["a,b"]),
        ship("z", 1.0, &[""]),
        ship("w", 1.0, &[" secret"]),
    ] {
        assert!(matches!(
            engine.upsert(&ns(), bad, 0),
            Err(DurableError::Index(IndexError::Invalid(
                EntityError::Labels
            )))
        ));
    }
    drop(engine);
    let _ = std::fs::remove_dir_all(&dir);
}

fn authed<T>(message: T) -> tonic::Request<T> {
    let mut request = tonic::Request::new(message);
    request
        .metadata_mut()
        .insert("authorization", "Bearer t-a".parse().unwrap());
    request
}

/// One HTTP/1.1 request; the status code and body.
async fn http(address: &str, path: &str, headers: &str) -> (u16, String) {
    let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
    let request = format!(
        "GET {path} HTTP/1.1\r\nhost: ezu\r\nauthorization: Bearer t-a\r\n{headers}connection: close\r\n\r\n"
    );
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    let status = response[9..12].parse().unwrap();
    let body = response.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
    (status, body)
}

#[tokio::test]
async fn governed_mode_refuses_reads_without_a_label_filter() {
    let dir = scratch();
    let engine = open(&dir);
    engine.upsert(&ns(), ship("a", 10.0, &[]), 0).unwrap();
    engine
        .upsert(&ns(), ship("b", 10.0, &["secret"]), 0)
        .unwrap();
    engine.upsert(&ns(), ship("u", 10.0, &["秘密"]), 0).unwrap();
    let auth: Arc<dyn Authenticator> = Arc::new(StaticToken::new("t-a", "t"));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let (_stop, stopped) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(serve(
        Api::new(Arc::new(engine), auth).governed(true),
        listener,
        None,
        async {
            let _ = stopped.await;
        },
    ));
    let mut client = EzuClient::connect(format!("http://{address}"))
        .await
        .unwrap();
    let query = |label_filter| proto::QueryRequest {
        collection: "ships".into(),
        query: Some(proto::query_request::Query::Bbox(proto::Bbox {
            west: -180.0,
            south: -90.0,
            east: 180.0,
            north: 90.0,
        })),
        limit: 10,
        label_filter,
        ..Default::default()
    };
    let refused = client.query(authed(query(None))).await.unwrap_err();
    assert_eq!(refused.code(), tonic::Code::FailedPrecondition);
    let found = client
        .query(authed(query(Some(proto::LabelFilter::default()))))
        .await
        .unwrap()
        .into_inner();
    let found: Vec<_> = found.entities.into_iter().map(|e| e.id).collect();
    assert_eq!(found, ["a"]);
    let refused = client
        .subscribe(authed(proto::SubscribeRequest::default()))
        .await
        .unwrap_err();
    assert_eq!(refused.code(), tonic::Code::FailedPrecondition);

    // Over HTTP the filter is a header.
    let (code, _) = http(&address, "/v1/entities/ships/b", "").await;
    assert_eq!(code, 400);
    let (code, body) = http(
        &address,
        "/v1/entities/ships/b",
        "x-ezu-readable-labels: eu\r\n",
    )
    .await;
    assert_eq!(code, 200);
    assert!(!body.contains("\"b\""), "{body}");
    let (_, body) = http(
        &address,
        "/v1/entities/ships/b",
        "x-ezu-readable-labels: eu, secret\r\n",
    )
    .await;
    assert!(body.contains("\"b\"") && body.contains("secret"), "{body}");
    // Labels are UTF-8, and so is the header.
    let (code, body) = http(
        &address,
        "/v1/entities/ships/u",
        "x-ezu-readable-labels: 秘密\r\n",
    )
    .await;
    assert_eq!(code, 200);
    assert!(body.contains("\"u\""), "{body}");

    // Flight exports are governed the same way.
    let channel = tonic::transport::Channel::from_shared(format!("http://{address}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut flight = arrow_flight::FlightClient::new(channel);
    flight.add_header("authorization", "Bearer t-a").unwrap();
    let ticket = serde_json::to_vec(&query(None)).unwrap();
    let error = flight
        .do_get(arrow_flight::Ticket::new(ticket))
        .await
        .err()
        .unwrap();
    assert!(error.to_string().contains("label_filter"), "{error}");
    drop(_stop);
    let _ = std::fs::remove_dir_all(&dir);
}
