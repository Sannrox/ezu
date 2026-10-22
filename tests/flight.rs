//! Arrow Flight (#7): a bulk load through DoPut, then an export through
//! DoGet that re-imports to identical entities, within the caller's
//! namespace.

use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::{FlightClient, Ticket};
use ezu::api::{Api, serve};
use ezu::auth::{Authenticator, StaticToken};
use ezu::flight::{PutCounts, from_batch, to_batch};
use ezu::{DurabilityConfig, DurableEngine, Entity, FieldValue, Geometry, Position, Quotas};
use futures::{StreamExt, TryStreamExt};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

static SEQ: AtomicU64 = AtomicU64::new(0);

/// A server with one static token, its address and its shutdown.
async fn server(token: &str, namespace: &str) -> (String, tokio::sync::oneshot::Sender<()>) {
    let dir = std::env::temp_dir().join(format!(
        "ezu-flight-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let engine = DurableEngine::open(
        DurabilityConfig {
            dir,
            fsync: false,
            segment_bytes: 64 << 20,
            snapshot_every_records: u64::MAX,
            snapshot_every_ms: i64::MAX,
            events_retained: 100,
            keys: None,
        },
        Quotas {
            max_entities: 1_000_000,
            max_collections: 4,
            max_vertices: 64,
            max_updates_per_second: u32::MAX,
            max_fences: 10,
        },
        None,
        0,
    )
    .unwrap();
    let auth: Arc<dyn Authenticator> = Arc::new(StaticToken::new(token, namespace));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let (stop, stopped) = tokio::sync::oneshot::channel();
    tokio::spawn(serve(
        Api::new(Arc::new(engine), auth),
        listener,
        None,
        async {
            let _ = stopped.await;
        },
    ));
    (address, stop)
}

async fn client(address: &str, token: &str) -> FlightClient {
    let channel = tonic::transport::Channel::from_shared(format!("http://{address}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = FlightClient::new(channel);
    client
        .add_header("authorization", &format!("Bearer {token}"))
        .unwrap();
    client
}

fn vessel(i: u64) -> Entity {
    Entity {
        collection: "vessels".into(),
        id: format!("v{i:07}"),
        geometry: Geometry::Point(Position::new(
            -179.0 + (i % 3_580) as f64 * 0.1,
            -80.0 + (i / 3_580) as f64 * 0.001,
        )),
        ts_ms: 1_000 + i as i64,
        speed: i.is_multiple_of(2).then_some(12.5),
        heading: None,
        ttl_ms: None,
        fields: BTreeMap::from([
            ("flag".into(), FieldValue::Text("xx".into())),
            ("tonnage".into(), FieldValue::Int(i as i64)),
            ("draft".into(), FieldValue::Float(1.0)),
        ]),
    }
}

fn world() -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "collection": "vessels",
        "query": {"bbox": {"west": -180, "south": -90, "east": 180, "north": 90}}
    }))
    .unwrap()
}

#[tokio::test]
async fn a_bulk_load_exports_and_reimports_to_identical_entities() {
    const COUNT: u64 = 100_000;
    let (address, _stop) = server("t-a", "tenant-a").await;
    let mut flight = client(&address, "t-a").await;
    let entities: Vec<Entity> = (0..COUNT).map(vessel).collect();
    let batches: Vec<_> = entities
        .chunks(10_000)
        .map(|chunk| Ok(to_batch(chunk).unwrap()))
        .collect();
    let started = Instant::now();
    let data = FlightDataEncoderBuilder::new().build(futures::stream::iter(batches));
    let results: Vec<_> = flight
        .do_put(data)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let loaded = started.elapsed();
    let inserted: u64 = results
        .iter()
        .map(|result| {
            serde_json::from_slice::<PutCounts>(&result.app_metadata)
                .unwrap()
                .inserted
        })
        .sum();
    assert_eq!(inserted, COUNT);
    eprintln!("loaded {COUNT} entities through DoPut in {loaded:?}");

    let exported: Vec<Entity> = flight
        .do_get(Ticket::new(world()))
        .await
        .unwrap()
        .map(|batch| from_batch(&batch.unwrap()).unwrap())
        .flat_map(futures::stream::iter)
        .map(Result::unwrap)
        .collect()
        .await;
    let mut sorted = exported.clone();
    sorted.sort_by(|a, b| a.id.cmp(&b.id));
    assert_eq!(sorted, entities);

    // The export imports again into another namespace unchanged.
    let (other, _stop_other) = server("t-b", "tenant-b").await;
    let mut second = client(&other, "t-b").await;
    let again = FlightDataEncoderBuilder::new().build(futures::stream::iter(
        exported
            .chunks(10_000)
            .map(|chunk| Ok(to_batch(chunk).unwrap()))
            .collect::<Vec<_>>(),
    ));
    second
        .do_put(again)
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let mut roundtrip: Vec<Entity> = second
        .do_get(Ticket::new(world()))
        .await
        .unwrap()
        .map(|batch| from_batch(&batch.unwrap()).unwrap())
        .flat_map(futures::stream::iter)
        .map(Result::unwrap)
        .collect()
        .await;
    roundtrip.sort_by(|a, b| a.id.cmp(&b.id));
    assert_eq!(roundtrip, entities);
}

#[tokio::test]
async fn refusals_are_counted_and_an_unknown_token_is_refused() {
    let (address, _stop) = server("t-a", "tenant-a").await;
    let mut flight = client(&address, "t-a").await;
    let mut bad = vessel(1);
    bad.geometry = Geometry::Point(Position::new(500.0, 0.0));
    let batch = to_batch(&[vessel(0), bad]).unwrap();
    let data = FlightDataEncoderBuilder::new().build(futures::stream::iter([Ok(batch)]));
    let results: Vec<_> = flight
        .do_put(data)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let counts: PutCounts = serde_json::from_slice(&results[0].app_metadata).unwrap();
    assert_eq!((counts.inserted, counts.refused), (1, 1));
    assert!(counts.first_refusal.is_some());

    let mut stranger = client(&address, "wrong").await;
    let error = stranger.do_get(Ticket::new(world())).await.err().unwrap();
    assert!(
        matches!(&error, FlightError::Tonic(status) if status.code() == tonic::Code::Unauthenticated),
        "{error}"
    );
}
