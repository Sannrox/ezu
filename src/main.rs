//! Ezu server. Configuration comes from the environment:
//!
//! - `EZU_DATA_DIR` (required): WAL and snapshots.
//! - `EZU_LISTEN`: address for gRPC and HTTP/JSON, default `127.0.0.1:7600`.
//! - Auth, one of (the server refuses to start without one):
//!   - `EZU_STATIC_TOKEN_FILE` and `EZU_STATIC_NAMESPACE`: one shared token
//!     for standalone or development use;
//!   - `EZU_JWKS_FILE` and `EZU_JWT_AUDIENCE`, optionally `EZU_JWT_ISSUER`
//!     and `EZU_TENANT_CLAIM` (default `tenant`): JWTs whose tenant claim is
//!     the namespace.
//! - `EZU_TLS_CERT` and `EZU_TLS_KEY` (PEM) serve over TLS; adding
//!   `EZU_TLS_CLIENT_CA` requires client certificates (mTLS).

use ezu::api::{Api, serve};
use ezu::auth::{Authenticator, Jwt, StaticToken};
use ezu::{DurabilityConfig, DurableEngine, Quotas};
use std::process::ExitCode;
use std::sync::Arc;

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

async fn run() -> Result<(), String> {
    let dir = var("EZU_DATA_DIR").ok_or("EZU_DATA_DIR is required")?;
    let auth = authenticator()?;
    let tls = tls()?;
    let now = now_ms();
    let engine = DurableEngine::open(
        DurabilityConfig {
            dir: dir.into(),
            fsync: true,
            segment_bytes: 64 << 20,
            snapshot_every_records: 1_000_000,
            snapshot_every_ms: 10 * 60 * 1000,
            keys: None,
        },
        Quotas {
            max_entities: 10_000_000,
            max_collections: 1_000,
            max_vertices: 10_000,
            max_updates_per_second: 100_000,
        },
        None,
        now,
    )
    .map_err(|error| error.to_string())?;
    let listen = var("EZU_LISTEN").unwrap_or_else(|| "127.0.0.1:7600".into());
    let listener = tokio::net::TcpListener::bind(&listen)
        .await
        .map_err(|error| format!("{listen}: {error}"))?;
    eprintln!("ezu {} listening on {listen}", env!("CARGO_PKG_VERSION"));
    let engine = Arc::new(engine);
    // Expired entities are hidden at once but hold memory and quota until
    // swept; sweep them in the background, off the async workers. The same
    // pass rechecks namespace keys, so a revoked key seals its namespace.
    let sweeper = engine.clone();
    tokio::spawn(async move {
        let mut every = tokio::time::interval(std::time::Duration::from_secs(10));
        loop {
            every.tick().await;
            let engine = sweeper.clone();
            let swept = move || {
                engine.refresh_keys()?;
                engine.sweep_expired(now_ms())
            };
            match tokio::task::spawn_blocking(swept).await {
                Ok(Ok(_)) => {}
                Ok(Err(error)) => eprintln!("ezu: expiry sweep failed: {error}"),
                Err(error) => eprintln!("ezu: expiry sweep task failed: {error}"),
            }
        }
    });
    let shutdown = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    serve(Api::new(engine, auth), listener, tls, shutdown)
        .await
        .map_err(|error| error.to_string())
}

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("ezu: {error}");
            ExitCode::FAILURE
        }
    }
}
