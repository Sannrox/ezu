//! The network API (#6): a tenant's token reaches only its own namespace
//! over gRPC and HTTP, and bad credentials are refused.

use ezu::api::proto::ezu_client::EzuClient;
use ezu::api::proto::{self, geometry::Shape, query_request};
use ezu::api::{Api, serve};
use ezu::auth::{Authenticator, Jwt};
use ezu::{DurabilityConfig, DurableEngine, Quotas};
use jsonwebtoken::{EncodingKey, Header, encode};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

static SEQ: AtomicU64 = AtomicU64::new(0);
const SECRET: &[u8] = b"test-only-hmac-secret-0123456789";

fn jwks() -> String {
    use base64::Engine as _;
    let k = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(SECRET);
    format!(r#"{{"keys":[{{"kty":"oct","kid":"k1","alg":"HS256","k":"{k}"}}]}}"#)
}

fn token(tenant: &str) -> String {
    let mut header = Header::new(jsonwebtoken::Algorithm::HS256);
    header.kid = Some("k1".into());
    let claims = serde_json::json!({
        "sub": format!("svc-{tenant}"),
        "aud": "ezu",
        "tenant": tenant,
        "exp": 4_102_444_800u64,
    });
    encode(&header, &claims, &EncodingKey::from_secret(SECRET)).unwrap()
}

async fn server() -> (String, tokio::sync::oneshot::Sender<()>) {
    server_with(None).await
}

async fn server_with(
    tls: Option<Arc<rustls::ServerConfig>>,
) -> (String, tokio::sync::oneshot::Sender<()>) {
    let dir = std::env::temp_dir().join(format!(
        "ezu-api-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let engine = DurableEngine::open(
        DurabilityConfig {
            dir,
            fsync: false,
            segment_bytes: 1 << 20,
            snapshot_every_records: 10_000,
            snapshot_every_ms: i64::MAX,
            events_retained: 1_000,
            keys: None,
        },
        Quotas {
            max_entities: 1_000,
            max_collections: 4,
            max_vertices: 64,
            max_updates_per_second: 10_000,
            max_fences: 1_000,
        },
        None,
        0,
    )
    .unwrap();
    let auth: Arc<dyn Authenticator> = Arc::new(Jwt::new(&jwks(), "ezu", None, "tenant").unwrap());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let (stop, stopped) = tokio::sync::oneshot::channel();
    tokio::spawn(serve(
        Api::new(Arc::new(engine), auth),
        listener,
        tls,
        async {
            let _ = stopped.await;
        },
    ));
    (address, stop)
}

fn authed<T>(message: T, token: &str) -> tonic::Request<T> {
    let mut request = tonic::Request::new(message);
    request
        .metadata_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
    request
}

fn ship(id: &str) -> proto::Entity {
    proto::Entity {
        collection: "ships".into(),
        id: id.into(),
        geometry: Some(proto::Geometry {
            shape: Some(Shape::Point(proto::Position {
                lon: 10.0,
                lat: 50.0,
            })),
        }),
        ts_ms: 1,
        speed: None,
        heading: None,
        ttl_ms: None,
        fields: Default::default(),
    }
}

fn world() -> proto::QueryRequest {
    proto::QueryRequest {
        collection: "ships".into(),
        query: Some(query_request::Query::Bbox(proto::Bbox {
            west: -180.0,
            south: -90.0,
            east: 180.0,
            north: 90.0,
        })),
        filters: Vec::new(),
        limit: 100,
        cursor: String::new(),
    }
}

#[tokio::test]
async fn a_tenant_token_reaches_only_its_own_namespace_over_grpc() {
    let (address, _stop) = server().await;
    let mut client = EzuClient::connect(format!("http://{address}"))
        .await
        .unwrap();
    let (a, b) = (token("tenant-a"), token("tenant-b"));
    let written = client
        .upsert(authed(
            proto::UpsertRequest {
                entities: vec![ship("s1")],
            },
            &a,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(written.results[0].outcome, proto::Outcome::Inserted as i32);

    // Tenant B sees, reads and deletes nothing of A's.
    let get = |token: &str| {
        authed(
            proto::GetRequest {
                collection: "ships".into(),
                id: "s1".into(),
            },
            token,
        )
    };
    assert!(
        client
            .get(get(&b))
            .await
            .unwrap()
            .into_inner()
            .entity
            .is_none()
    );
    assert!(
        client
            .query(authed(world(), &b))
            .await
            .unwrap()
            .into_inner()
            .entities
            .is_empty()
    );
    let removed = client
        .delete(authed(
            proto::DeleteRequest {
                collection: "ships".into(),
                id: "s1".into(),
            },
            &b,
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(!removed.removed);
    // B's own write lands in B's namespace only.
    client
        .upsert(authed(
            proto::UpsertRequest {
                entities: vec![ship("s1")],
            },
            &b,
        ))
        .await
        .unwrap();
    assert_eq!(
        client
            .query(authed(world(), &a))
            .await
            .unwrap()
            .into_inner()
            .entities
            .len(),
        1
    );
    let status = client
        .status(authed(proto::StatusRequest {}, &b))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        (status.namespace.as_str(), status.entity_count),
        ("tenant-b", 1)
    );
    assert!(
        client
            .get(get(&a))
            .await
            .unwrap()
            .into_inner()
            .entity
            .is_some()
    );

    // A malformed entity is refused alone; the rest of its batch is written.
    let mut broken = ship("s2");
    broken.geometry = None;
    let mixed = client
        .upsert(authed(
            proto::UpsertRequest {
                entities: vec![broken, ship("s3")],
            },
            &a,
        ))
        .await
        .unwrap()
        .into_inner();
    let outcomes: Vec<i32> = mixed.results.iter().map(|r| r.outcome).collect();
    assert_eq!(
        outcomes,
        [
            proto::Outcome::Refused as i32,
            proto::Outcome::Inserted as i32
        ]
    );
    assert!(!mixed.results[0].error.is_empty());

    // Missing, forged and wrong-audience credentials are refused.
    let missing = client
        .status(tonic::Request::new(proto::StatusRequest {}))
        .await
        .unwrap_err();
    assert_eq!(missing.code(), tonic::Code::Unauthenticated);
    let forged = encode(
        &Header::new(jsonwebtoken::Algorithm::HS256),
        &serde_json::json!({"aud": "ezu", "tenant": "tenant-a", "exp": 4_102_444_800u64}),
        &EncodingKey::from_secret(b"some-other-secret-0123456789abcd"),
    )
    .unwrap();
    let error = client
        .status(authed(proto::StatusRequest {}, &forged))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::Unauthenticated);
    let mut header = Header::new(jsonwebtoken::Algorithm::HS256);
    header.kid = Some("k1".into());
    let elsewhere = encode(
        &header,
        &serde_json::json!({"aud": "other", "tenant": "tenant-a", "exp": 4_102_444_800u64}),
        &EncodingKey::from_secret(SECRET),
    )
    .unwrap();
    let error = client
        .status(authed(proto::StatusRequest {}, &elsewhere))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::Unauthenticated);
}

/// One HTTP/1.1 request; the status code and body.
async fn http(
    address: &str,
    method: &str,
    path: &str,
    token: Option<&str>,
    body: &str,
) -> (u16, String) {
    let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
    let auth = token.map_or(String::new(), |t| format!("authorization: Bearer {t}\r\n"));
    let request = format!(
        "{method} {path} HTTP/1.1\r\nhost: ezu\r\n{auth}content-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    let status = response[9..12].parse().unwrap();
    let body = response.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
    (status, body)
}

#[tokio::test]
async fn the_http_api_serves_the_same_namespaces() {
    let (address, _stop) = server().await;
    let (a, b) = (token("tenant-a"), token("tenant-b"));
    let entity = r#"{"entities":[{"collection":"ships","id":"h1","geometry":{"shape":{"point":{"lon":1.0,"lat":2.0}}},"ts_ms":1,"fields":{"kind":{"value":{"text_value":"tug"}}}}]}"#;
    let (code, body) = http(&address, "POST", "/v1/entities", Some(&a), entity).await;
    assert_eq!(code, 200, "{body}");
    let (code, body) = http(&address, "GET", "/v1/entities/ships/h1", Some(&a), "").await;
    assert_eq!(code, 200);
    assert!(body.contains("\"h1\"") && body.contains("tug"), "{body}");
    let (_, body) = http(&address, "GET", "/v1/entities/ships/h1", Some(&b), "").await;
    assert!(!body.contains("\"h1\""), "{body}");
    let query = r#"{"collection":"ships","query":{"bbox":{"west":-180,"south":-90,"east":180,"north":90}},"limit":10}"#;
    let (_, body) = http(&address, "POST", "/v1/query", Some(&a), query).await;
    assert!(body.contains("\"h1\""), "{body}");
    let (code, _) = http(&address, "GET", "/v1/status", None, "").await;
    assert_eq!(code, 401);
    let (code, body) = http(&address, "DELETE", "/v1/entities/ships/h1", Some(&a), "").await;
    assert_eq!(code, 200);
    assert!(body.contains("true"), "{body}");
}

#[tokio::test]
async fn an_http_1_1_client_reaches_the_json_api_over_tls() {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let config =
        ezu::api::tls_config(&cert.cert.pem(), &cert.signing_key.serialize_pem(), None).unwrap();
    let (address, _stop) = server_with(Some(config)).await;
    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut client = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
    // Only HTTP/1.1, as a plain JSON client would offer.
    client.alpn_protocols = vec![b"http/1.1".to_vec()];
    let connector = tokio_rustls::TlsConnector::from(Arc::new(client));
    let tcp = tokio::net::TcpStream::connect(&address).await.unwrap();
    let name = rustls::pki_types::ServerName::try_from("localhost").unwrap();
    let mut stream = connector.connect(name, tcp).await.unwrap();
    let request = format!(
        "GET /v1/status HTTP/1.1\r\nhost: localhost\r\nauthorization: Bearer {}\r\nconnection: close\r\n\r\n",
        token("tenant-a")
    );
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut response = Vec::new();
    let _ = stream.read_to_end(&mut response).await;
    let response = String::from_utf8_lossy(&response);
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    assert!(response.contains("tenant-a"), "{response}");
}

#[tokio::test]
async fn fences_and_their_events_are_served_per_namespace_over_http() {
    let (address, _stop) = server().await;
    let (a, b) = (token("tenant-a"), token("tenant-b"));
    let fence = r#"{"shape":{"circle":{"center":{"lon":10.0,"lat":50.0},"meters":500}},"collections":["ships"]}"#;
    let (code, body) = http(&address, "PUT", "/v1/fences/harbour", Some(&a), fence).await;
    assert_eq!(code, 200, "{body}");
    let entity = r#"{"entities":[{"collection":"ships","id":"h1","geometry":{"shape":{"point":{"lon":10.0,"lat":50.0}}},"ts_ms":7}]}"#;
    http(&address, "POST", "/v1/entities", Some(&a), entity).await;
    let (code, body) = http(&address, "GET", "/v1/events?limit=10", Some(&a), "").await;
    assert_eq!(code, 200, "{body}");
    assert!(
        body.contains("\"harbour\"") && body.contains("\"kind\":1") && body.contains("\"ts_ms\":7"),
        "{body}"
    );
    // Resuming after the last event returns nothing new.
    let lsn = body
        .split("\"lsn\":")
        .nth(1)
        .unwrap()
        .split(',')
        .next()
        .unwrap();
    let path = format!("/v1/events?after_lsn={lsn}&after_index=0");
    let (_, body) = http(&address, "GET", &path, Some(&a), "").await;
    assert!(!body.contains("harbour"), "{body}");
    // Filtered by fence, one `fence` parameter per fence.
    let (_, body) = http(
        &address,
        "GET",
        "/v1/events?fence=harbour&fence=x",
        Some(&a),
        "",
    )
    .await;
    assert!(body.contains("\"harbour\""), "{body}");
    let (_, body) = http(&address, "GET", "/v1/events?fence=other", Some(&a), "").await;
    assert!(!body.contains("\"harbour\""), "{body}");
    let (_, body) = http(&address, "GET", "/v1/fences", Some(&a), "").await;
    assert!(body.contains("\"harbour\""), "{body}");
    let (_, body) = http(&address, "GET", "/v1/fences/harbour", Some(&b), "").await;
    assert!(!body.contains("harbour"), "{body}");
    let (_, body) = http(&address, "GET", "/v1/events", Some(&b), "").await;
    assert!(!body.contains("harbour"), "{body}");
    let (code, body) = http(&address, "DELETE", "/v1/fences/harbour", Some(&a), "").await;
    assert_eq!(code, 200);
    assert!(body.contains("true"), "{body}");
}

#[tokio::test]
async fn a_subscriber_gets_events_live_and_resumes_after_its_last_id() {
    let (address, _stop) = server().await;
    let a = token("tenant-a");
    let mut client = EzuClient::connect(format!("http://{address}"))
        .await
        .unwrap();
    let fence = proto::Fence {
        id: "harbour".into(),
        shape: Some(proto::fence::Shape::Circle(proto::Radius {
            center: Some(proto::Position {
                lon: 10.0,
                lat: 50.0,
            }),
            meters: 500.0,
        })),
        ..Default::default()
    };
    client
        .put_fence(authed(proto::PutFenceRequest { fence: Some(fence) }, &a))
        .await
        .unwrap();
    let mut stream = client
        .subscribe(authed(proto::SubscribeRequest::default(), &a))
        .await
        .unwrap()
        .into_inner();
    // Raised after the subscription started: delivered live.
    client
        .upsert(authed(
            proto::UpsertRequest {
                entities: vec![ship("s1")],
            },
            &a,
        ))
        .await
        .unwrap();
    let entered = stream.message().await.unwrap().unwrap();
    assert_eq!(
        (entered.fence.as_str(), entered.kind),
        ("harbour", proto::EventKind::Enter as i32)
    );
    client
        .upsert(authed(
            proto::UpsertRequest {
                entities: vec![ship("s2")],
            },
            &a,
        ))
        .await
        .unwrap();
    drop(stream);
    // Resuming after the first id yields exactly the second event.
    let mut resumed = client
        .subscribe(authed(
            proto::SubscribeRequest {
                after_lsn: Some(entered.lsn),
                after_index: entered.index,
                fences: vec!["harbour".into()],
            },
            &a,
        ))
        .await
        .unwrap()
        .into_inner();
    let next = resumed.message().await.unwrap().unwrap();
    assert_eq!(next.entity, "s2");
}
