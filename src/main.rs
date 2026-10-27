//! Ezu server. `ezu diagnostics` instead prints a support bundle for
//! `EZU_DATA_DIR` (no secrets, no tenant data) and exits. Configuration
//! comes from the environment:
//!
//! - `EZU_DATA_DIR` (required): WAL and snapshots.
//! - `EZU_LISTEN`: address for gRPC and HTTP/JSON, default `127.0.0.1:7600`.
//! - Auth, one of (the server refuses to start without one):
//!   - `EZU_STATIC_TOKEN_FILE` and `EZU_STATIC_NAMESPACE`: one shared token
//!     for standalone or development use;
//!   - `EZU_JWKS_FILE` and `EZU_JWT_AUDIENCE`, optionally `EZU_JWT_ISSUER`
//!     and `EZU_TENANT_CLAIM` (default `tenant`): JWTs whose tenant claim is
//!     the namespace.
//! - `EZU_GOVERNED=1`: every read must carry a label filter.
//! - `EZU_TLS_CERT` and `EZU_TLS_KEY` (PEM) serve over TLS; adding
//!   `EZU_TLS_CLIENT_CA` requires client certificates (mTLS).
//! - `EZU_REPLICATION_TOKEN_FILE`: the token followers present; without
//!   it no follower is served. A follower presents it to its leader.
//! - `EZU_FOLLOW`: the leader's URL (`http://` or `https://`); the server
//!   then runs as a read-only follower of it. Over `https`, `EZU_FOLLOW_CA`
//!   (PEM) verifies the leader, and `EZU_TLS_CERT` and `EZU_TLS_KEY`, when
//!   set, are the follower's client certificate.
//! - `EZU_NODE_ID`: a follower's node id. With it, the follower acknowledges
//!   what it applied (`Replication.Sync`), so it can be a sync standby.
//! - `EZU_SYNC_STANDBY`: on a leader, the node id of its sync standby. A
//!   write is then acknowledged only once that standby acknowledged it;
//!   `EZU_SYNC_TIMEOUT` (milliseconds, default 5000) bounds the wait, past
//!   which the write fails `UNAVAILABLE` with its outcome unknown. Without
//!   an acknowledgement for 10 seconds, the leader refuses reads and writes.

use ezu::api::{Api, serve};
use ezu::auth::{Authenticator, Jwt, StaticToken};
use ezu::{DurabilityConfig, DurableEngine, Quotas, SyncStandby};
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

/// Without an acknowledgement from the sync standby this long, a leader
/// refuses reads and writes: as long as a follower waits before it takes a
/// silent stream as lost.
const LEASE: Duration = Duration::from_secs(10);

fn var(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.is_empty())
}

fn read(name: &str) -> Result<Option<String>, String> {
    var(name)
        .map(|path| std::fs::read_to_string(&path).map_err(|error| format!("{name}: {error}")))
        .transpose()
}

fn authenticator() -> Result<Arc<dyn Authenticator>, String> {
    if let Some(token) = read("EZU_STATIC_TOKEN_FILE")? {
        let namespace = var("EZU_STATIC_NAMESPACE").ok_or("EZU_STATIC_NAMESPACE is required")?;
        return Ok(Arc::new(StaticToken::new(token.trim(), namespace)));
    }
    if let Some(jwks) = read("EZU_JWKS_FILE")? {
        let audience = var("EZU_JWT_AUDIENCE").ok_or("EZU_JWT_AUDIENCE is required")?;
        let claim = var("EZU_TENANT_CLAIM").unwrap_or_else(|| "tenant".into());
        return Ok(Arc::new(Jwt::new(
            &jwks,
            audience,
            var("EZU_JWT_ISSUER"),
            claim,
        )?));
    }
    Err("no authentication configured: set EZU_STATIC_TOKEN_FILE or EZU_JWKS_FILE".into())
}

fn tls() -> Result<Option<Arc<rustls::ServerConfig>>, String> {
    let (cert, key, ca) = (
        read("EZU_TLS_CERT")?,
        read("EZU_TLS_KEY")?,
        read("EZU_TLS_CLIENT_CA")?,
    );
    // Any TLS setting without a full server identity is refused rather
    // than quietly serving plaintext.
    match (cert, key) {
        (Some(cert), Some(key)) => ezu::api::tls_config(&cert, &key, ca.as_deref()).map(Some),
        (None, None) if ca.is_none() => Ok(None),
        _ => Err("TLS needs both EZU_TLS_CERT and EZU_TLS_KEY".into()),
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| {
            i64::try_from(since.as_millis()).unwrap_or(i64::MAX)
        })
}

/// The leader to follow, from `EZU_FOLLOW`, with TLS over `https`.
fn leader() -> Result<Option<tonic::transport::Endpoint>, String> {
    let Some(url) = var("EZU_FOLLOW") else {
        return Ok(None);
    };
    let identity = read("EZU_TLS_CERT")?.zip(read("EZU_TLS_KEY")?);
    ezu::follow::endpoint(&url, read("EZU_FOLLOW_CA")?, identity)
        .map(Some)
        .map_err(|error| format!("EZU_FOLLOW: {error}"))
}

/// The leader's sync standby, from `EZU_SYNC_STANDBY` and `EZU_SYNC_TIMEOUT`.
fn sync_standby() -> Result<Option<SyncStandby>, String> {
    let timeout = match var("EZU_SYNC_TIMEOUT") {
        Some(ms) => Duration::from_millis(
            ms.parse()
                .map_err(|_| format!("EZU_SYNC_TIMEOUT: not milliseconds: {ms}"))?,
        ),
        None => Duration::from_secs(5),
    };
    Ok(var("EZU_SYNC_STANDBY").map(|node_id| SyncStandby {
        node_id,
        timeout,
        lease: LEASE,
    }))
}

async fn run() -> Result<(), String> {
    let dir = var("EZU_DATA_DIR").ok_or("EZU_DATA_DIR is required")?;
    let auth = authenticator()?;
    let tls = tls()?;
    let leader = leader()?;
    let replication_token =
        read("EZU_REPLICATION_TOKEN_FILE")?.map(|token| token.trim().to_string());
    let standby = sync_standby()?;
    if standby.is_some() {
        if leader.is_some() {
            return Err("EZU_SYNC_STANDBY is a leader's setting; a follower has none".into());
        }
        if replication_token.is_none() {
            return Err("EZU_SYNC_STANDBY needs EZU_REPLICATION_TOKEN_FILE".into());
        }
    }
    let node_id = var("EZU_NODE_ID");
    let now = now_ms();
    let config = DurabilityConfig {
        dir: dir.into(),
        fsync: true,
        segment_bytes: 64 << 20,
        snapshot_every_records: 1_000_000,
        snapshot_every_ms: 10 * 60 * 1000,
        events_retained: 100_000,
        keys: None,
    };
    let quotas = Quotas {
        max_entities: 10_000_000,
        max_collections: 1_000,
        max_vertices: 10_000,
        max_updates_per_second: 100_000,
        max_fences: 100_000,
    };
    let engine = match (&leader, standby) {
        (Some(_), _) => DurableEngine::open_follower(config, quotas, now),
        (None, None) => DurableEngine::open(config, quotas, None, now),
        (None, Some(standby)) => DurableEngine::open_sync(config, quotas, None, now, standby),
    }
    .map_err(|error| error.to_string())?;
    let listen = var("EZU_LISTEN").unwrap_or_else(|| "127.0.0.1:7600".into());
    let listener = tokio::net::TcpListener::bind(&listen)
        .await
        .map_err(|error| format!("{listen}: {error}"))?;
    eprintln!("ezu {} listening on {listen}", env!("CARGO_PKG_VERSION"));
    let engine = Arc::new(engine);
    if let Some(leader) = leader {
        let token = replication_token
            .clone()
            .ok_or("EZU_FOLLOW needs EZU_REPLICATION_TOKEN_FILE")?;
        let follower = engine.clone();
        tokio::spawn(async move {
            let retry = |why: &str| eprintln!("ezu: following the leader: {why}; reconnecting");
            let stopped = match node_id {
                Some(node_id) => {
                    ezu::follow::standby(follower, leader, token, node_id, retry).await
                }
                None => ezu::follow::follow(follower, leader, token, retry).await,
            };
            eprintln!("ezu: following stopped: {stopped}");
        });
    }
    // Expired entities are hidden at once but hold memory and quota until
    // swept; sweep them in the background, off the async workers. The same
    // pass rechecks namespace keys, so a revoked key seals its namespace.
    let sweeper = engine.clone();
    tokio::spawn(async move {
        let mut every = tokio::time::interval(std::time::Duration::from_secs(10));
        loop {
            every.tick().await;
            // A follower's removals come from its leader's log.
            if sweeper.replication().is_some() {
                return;
            }
            let engine = sweeper.clone();
            // The wait for a sync standby happens off the blocking thread.
            let swept = move || {
                engine.deferring(|engine| {
                    engine.refresh_keys()?;
                    engine.sweep_expired(now_ms())
                })
            };
            let swept = match tokio::task::spawn_blocking(swept).await {
                Ok(pending) => Ok(sweeper.acknowledged(pending).await.and_then(|swept| swept)),
                Err(error) => Err(error),
            };
            match swept {
                Ok(Ok(_)) => {}
                // Waiting for the sync standby is reported by /healthz.
                Ok(Err(ezu::durable::DurableError::NoStandby)) => {}
                Ok(Err(error)) => eprintln!("ezu: expiry sweep failed: {error}"),
                Err(error) => eprintln!("ezu: expiry sweep task failed: {error}"),
            }
        }
    });
    let shutdown = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    let governed = matches!(var("EZU_GOVERNED").as_deref(), Some("1" | "true"));
    serve(
        Api::new(engine, auth)
            .governed(governed)
            .replication_token(replication_token.as_deref()),
        listener,
        tls,
        shutdown,
    )
    .await
    .map_err(|error| error.to_string())
}

#[tokio::main]
async fn main() -> ExitCode {
    if std::env::args().nth(1).as_deref() == Some("diagnostics") {
        let dir = var("EZU_DATA_DIR").unwrap_or_default();
        println!("{:#}", ezu::diagnostics::bundle(std::path::Path::new(&dir)));
        return ExitCode::SUCCESS;
    }
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("ezu: {error}");
            ExitCode::FAILURE
        }
    }
}
