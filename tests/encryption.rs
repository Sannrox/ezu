//! Encryption at rest (#11): an encrypted namespace's data never reaches
//! disk in plaintext, and losing its key seals that namespace alone.

use ezu::keys::KeyConfig;
use ezu::keys::sekai_keys::{
    Dek, KeyError, KeyId, KeyProvider, KeyPurpose, MemoryKeyProvider, ProviderDescription,
    RuntimeProfile, TenantId, UnwrapGrant, UnwrapOutcome, WrappedDek,
};
use ezu::{
    DurabilityConfig, DurableEngine, DurableError, Entity, FieldValue, Geometry, Identity,
    Namespace, Position, Query, QueryRequest, Quotas,
};
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

static SEQ: AtomicU64 = AtomicU64::new(0);

fn scratch(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "ezu-encryption-{name}-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = fs::remove_dir_all(&dir);
    dir
}

/// A provider whose unwraps can be denied, as when a customer withdraws
/// access, and allowed again.
struct Switchable {
    inner: MemoryKeyProvider,
    denied: AtomicBool,
}

impl KeyProvider for Switchable {
    fn describe(&self) -> ProviderDescription {
        self.inner.describe()
    }
    fn wrap(
        &self,
        tenant: &TenantId,
        purpose: KeyPurpose,
        dek: &Dek,
    ) -> Result<WrappedDek, KeyError> {
        self.inner.wrap(tenant, purpose, dek)
    }
    fn rewrap(&self, wrapped: &WrappedDek, dek: &Dek) -> Result<WrappedDek, KeyError> {
        self.inner.rewrap(wrapped, dek)
    }
    fn unwrap(
        &self,
        wrapped: &WrappedDek,
        grant: &UnwrapGrant,
        engine: &str,
        now: i64,
    ) -> Result<UnwrapOutcome, KeyError> {
        if self.denied.load(Ordering::Relaxed) && grant.tenant_id.as_str() == "alpha" {
            return Err(KeyError::GrantDenied);
        }
        self.inner.unwrap(wrapped, grant, engine, now)
    }
    fn rotate_kek(&self) -> Result<KeyId, KeyError> {
        self.inner.rotate_kek()
    }
    fn complete_kek_rotation(&self) -> Result<(), KeyError> {
        self.inner.complete_kek_rotation()
    }
    fn revoke_kek(&self) -> Result<(), KeyError> {
        self.inner.revoke_kek()
    }
    fn revoke_dek(&self, dek_id: &KeyId) -> Result<(), KeyError> {
        self.inner.revoke_dek(dek_id)
    }
}

fn provider() -> Arc<Switchable> {
    Arc::new(Switchable {
        inner: MemoryKeyProvider::new(RuntimeProfile::Development).unwrap(),
        denied: AtomicBool::new(false),
    })
}

fn grant(namespace: &str) -> (String, UnwrapGrant) {
    (
        namespace.to_string(),
        UnwrapGrant {
            tenant_id: TenantId::new(namespace).unwrap(),
            purpose: KeyPurpose::Store,
            expires_unix_secs: i64::MAX,
        },
    )
}

fn config(dir: &Path, provider: &Arc<Switchable>, encrypted: &[&str]) -> DurabilityConfig {
    DurabilityConfig {
        dir: dir.to_path_buf(),
        fsync: true,
        segment_bytes: 4_096,
        snapshot_every_records: u64::MAX,
        snapshot_every_ms: i64::MAX,
        events_retained: 1_000,
        keys: Some(KeyConfig {
            provider: provider.clone(),
            grants: encrypted
                .iter()
                .map(|ns| grant(ns))
                .collect::<HashMap<_, _>>(),
        }),
    }
}

fn open(config: DurabilityConfig) -> DurableEngine {
    DurableEngine::open(
        config,
        Quotas {
            max_entities: 10_000,
            max_collections: 8,
            max_vertices: 64,
            max_updates_per_second: u32::MAX,
            max_fences: 1_000,
        },
        None,
        0,
    )
    .unwrap()
}

fn ns(name: &str) -> Namespace {
    Namespace::of(&Identity::authenticated("svc", name))
}

fn entity(namespace: &str, i: u64) -> Entity {
    Entity {
        collection: "c".into(),
        id: format!("{namespace}-id-{i}"),
        geometry: Geometry::Point(Position::new(i as f64, 1.0)),
        ts_ms: i as i64 + 1,
        speed: None,
        heading: None,
        ttl_ms: None,
        fields: BTreeMap::from([(
            "secret".into(),
            FieldValue::Text(format!("{namespace}-marker-{i}")),
        )]),
    }
}

fn count(engine: &DurableEngine, namespace: &str) -> Result<usize, DurableError> {
    engine
        .query(
            &ns(namespace),
            &QueryRequest {
                collection: "c".into(),
                query: Query::Bbox([-180.0, -90.0, 180.0, 90.0]),
                filters: Vec::new(),
                limit: 10_000,
                cursor: None,
            },
            0,
        )
        .map(|page| page.entities.len())
}

fn fence(id: &str) -> ezu::fence::Fence {
    ezu::fence::Fence {
        id: id.into(),
        shape: ezu::fence::Shape::Circle {
            center: Position::new(1.0, 1.0),
            meters: 1_000.0,
        },
        collections: Vec::new(),
        filters: Vec::new(),
        dwell_ms: None,
        inside_per_second: None,
    }
}

/// Whether any file under `dir` contains `needle`.
fn on_disk(dir: &Path, needle: &str) -> bool {
    fs::read_dir(dir).unwrap().any(|entry| {
        let path = entry.unwrap().path();
        if path.is_dir() {
            return on_disk(&path, needle);
        }
        let bytes = fs::read(&path).unwrap();
        bytes
            .windows(needle.len())
            .any(|window| window == needle.as_bytes())
    })
}

#[test]
fn an_encrypted_namespace_never_reaches_disk_in_plaintext() {
    let dir = scratch("scan");
    let keys = provider();
    let engine = open(config(&dir, &keys, &["alpha"]));
    // Fences and their memberships are the namespace's data too.
    engine
        .put_fence(&ns("alpha"), fence("alpha-fence-marker"), 0)
        .unwrap();
    for i in 0..6 {
        engine.upsert(&ns("alpha"), entity("alpha", i), 0).unwrap();
        engine.upsert(&ns("beta"), entity("beta", i), 0).unwrap();
    }
    // Half in a snapshot, half only in the log.
    engine.snapshot(0).unwrap();
    for i in 6..12 {
        engine.upsert(&ns("alpha"), entity("alpha", i), 0).unwrap();
        engine.upsert(&ns("beta"), entity("beta", i), 0).unwrap();
    }
    engine.delete(&ns("alpha"), "c", "alpha-id-0", 0).unwrap();
    drop(engine);
    // The scan finds plaintext where it exists: the unencrypted namespace.
    assert!(on_disk(&dir, "beta-marker-1"));
    assert!(on_disk(&dir, "beta-marker-9"));
    assert!(!on_disk(&dir, "alpha-fence-marker"));
    for i in 0..12 {
        assert!(!on_disk(&dir, &format!("alpha-marker-{i}")), "marker {i}");
        assert!(!on_disk(&dir, &format!("alpha-id-{i}")), "id {i}");
    }
    let engine = open(config(&dir, &keys, &["alpha"]));
    assert_eq!(count(&engine, "alpha").unwrap(), 11);
    assert!(
        engine
            .get_fence(&ns("alpha"), "alpha-fence-marker")
            .unwrap()
            .is_some()
    );
    assert_eq!(count(&engine, "beta").unwrap(), 12);
    drop(engine);
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn a_withdrawn_key_seals_one_namespace_until_it_returns() {
    let dir = scratch("seal");
    let keys = provider();
    let engine = open(config(&dir, &keys, &["alpha", "gamma"]));
    engine.put_fence(&ns("alpha"), fence("a"), 0).unwrap();
    for i in 0..5 {
        engine.upsert(&ns("alpha"), entity("alpha", i), 0).unwrap();
        engine.upsert(&ns("gamma"), entity("gamma", i), 0).unwrap();
    }
    keys.denied.store(true, Ordering::Relaxed);
    assert_eq!(engine.refresh_keys().unwrap(), vec!["alpha".to_string()]);
    // Alpha refuses reads and writes; gamma carries on.
    assert!(matches!(
        count(&engine, "alpha"),
        Err(DurableError::Sealed(_))
    ));
    assert!(matches!(
        engine.upsert(&ns("alpha"), entity("alpha", 9), 0),
        Err(DurableError::Sealed(_))
    ));
    assert!(matches!(
        engine.get(&ns("alpha"), "c", "alpha-id-1", 0),
        Err(DurableError::Sealed(_))
    ));
    engine.upsert(&ns("gamma"), entity("gamma", 5), 0).unwrap();
    assert_eq!(count(&engine, "gamma").unwrap(), 6);
    // Snapshots carry alpha's sealed data; the log it was in is dropped.
    engine.snapshot(0).unwrap();
    engine.upsert(&ns("gamma"), entity("gamma", 6), 0).unwrap();
    drop(engine);

    // Still withdrawn after a restart: alpha opens sealed, from the log too.
    let engine = open(config(&dir, &keys, &["alpha", "gamma"]));
    assert!(matches!(
        count(&engine, "alpha"),
        Err(DurableError::Sealed(_))
    ));
    assert_eq!(count(&engine, "gamma").unwrap(), 7);
    engine.snapshot(0).unwrap();
    drop(engine);

    // The key is back: alpha's data was kept whole.
    keys.denied.store(false, Ordering::Relaxed);
    let engine = open(config(&dir, &keys, &["alpha", "gamma"]));
    assert_eq!(count(&engine, "alpha").unwrap(), 5);
    assert!(engine.get_fence(&ns("alpha"), "a").unwrap().is_some());
    assert_eq!(count(&engine, "gamma").unwrap(), 7);
    drop(engine);
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn a_missing_grant_carries_logged_writes_through_snapshots() {
    let dir = scratch("grant");
    let keys = provider();
    // A grant the provider refuses never gets a key, and the namespace
    // stays sealed rather than written unreadably.
    keys.denied.store(true, Ordering::Relaxed);
    let engine = open(config(&dir, &keys, &["alpha"]));
    assert!(matches!(
        engine.upsert(&ns("alpha"), entity("alpha", 0), 0),
        Err(DurableError::Sealed(_))
    ));
    drop(engine);
    keys.denied.store(false, Ordering::Relaxed);
    let engine = open(config(&dir, &keys, &["alpha"]));
    for i in 0..4 {
        engine.upsert(&ns("alpha"), entity("alpha", i), 0).unwrap();
    }
    drop(engine);
    // Opened without alpha's grant, its logged records are carried, unread,
    // into the snapshot that drops the log.
    let engine = open(config(&dir, &keys, &[]));
    assert!(matches!(
        count(&engine, "alpha"),
        Err(DurableError::Sealed(_))
    ));
    engine.upsert(&ns("beta"), entity("beta", 0), 0).unwrap();
    engine.snapshot(0).unwrap();
    drop(engine);
    let engine = open(config(&dir, &keys, &["alpha"]));
    assert_eq!(count(&engine, "alpha").unwrap(), 4);
    assert_eq!(count(&engine, "beta").unwrap(), 1);
    drop(engine);
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn a_rotated_key_takes_over_at_the_next_snapshot() {
    let dir = scratch("rotate");
    let keys = provider();
    let engine = open(config(&dir, &keys, &["alpha"]));
    engine.upsert(&ns("alpha"), entity("alpha", 0), 0).unwrap();
    let stored = || -> serde_json::Value {
        let file = fs::read_dir(dir.join("keys"))
            .unwrap()
            .map(|e| e.unwrap().path())
            .find(|p| p.extension().is_some_and(|x| x == "json"))
            .unwrap();
        serde_json::from_slice(&fs::read(file).unwrap()).unwrap()
    };
    let first = stored()["current"].clone();
    // A rotation with no write since the last snapshot keeps that snapshot,
    // and with it the old key.
    engine.snapshot(0).unwrap();
    engine.rotate_key(&ns("alpha")).unwrap();
    engine.snapshot(0).unwrap();
    assert_eq!(stored()["wrapped"].as_array().unwrap().len(), 2);
    engine.upsert(&ns("alpha"), entity("alpha", 1), 0).unwrap();
    // Both keys are kept until the data under the old one is re-sealed.
    assert_eq!(stored()["wrapped"].as_array().unwrap().len(), 2);
    assert_ne!(stored()["current"], first);
    drop(engine);
    // Recovery reads records under both keys.
    let engine = open(config(&dir, &keys, &["alpha"]));
    assert_eq!(count(&engine, "alpha").unwrap(), 2);
    engine.snapshot(0).unwrap();
    assert_eq!(stored()["wrapped"].as_array().unwrap().len(), 1);
    drop(engine);
    let engine = open(config(&dir, &keys, &["alpha"]));
    assert_eq!(count(&engine, "alpha").unwrap(), 2);
    drop(engine);
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn a_lost_key_file_seals_its_namespace_alone() {
    let dir = scratch("lost");
    let keys = provider();
    // Long enough that a name derived from it would not fit a file name.
    let long = "l".repeat(200);
    let engine = open(config(&dir, &keys, &["alpha", "gamma"]));
    for i in 0..3 {
        engine.upsert(&ns("alpha"), entity("alpha", i), 0).unwrap();
        engine.upsert(&ns("gamma"), entity("gamma", i), 0).unwrap();
        engine.upsert(&ns(&long), entity("long", i), 0).unwrap();
    }
    engine.snapshot(0).unwrap();
    engine.upsert(&ns("alpha"), entity("alpha", 3), 0).unwrap();
    drop(engine);
    for file in fs::read_dir(dir.join("keys")).unwrap() {
        let path = file.unwrap().path();
        let stored: serde_json::Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        if stored["namespace"] == "alpha" {
            fs::remove_file(path).unwrap();
        }
    }
    let engine = open(config(&dir, &keys, &["alpha", "gamma"]));
    assert!(matches!(
        count(&engine, "alpha"),
        Err(DurableError::Sealed(_))
    ));
    assert_eq!(count(&engine, "gamma").unwrap(), 3);
    assert_eq!(count(&engine, &long).unwrap(), 3);
    engine.snapshot(0).unwrap();
    drop(engine);
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn plaintext_data_of_a_namespace_whose_first_key_fails_is_carried() {
    let dir = scratch("first");
    let keys = provider();
    let engine = open(config(&dir, &keys, &[]));
    engine.upsert(&ns("alpha"), entity("alpha", 0), 0).unwrap();
    engine.snapshot(0).unwrap();
    engine.upsert(&ns("alpha"), entity("alpha", 1), 0).unwrap();
    drop(engine);
    // Encryption is switched on, but the provider refuses the grant.
    keys.denied.store(true, Ordering::Relaxed);
    let engine = open(config(&dir, &keys, &["alpha"]));
    assert!(matches!(
        count(&engine, "alpha"),
        Err(DurableError::Sealed(_))
    ));
    engine.upsert(&ns("beta"), entity("beta", 0), 0).unwrap();
    engine.snapshot(0).unwrap();
    drop(engine);
    keys.denied.store(false, Ordering::Relaxed);
    let engine = open(config(&dir, &keys, &["alpha"]));
    assert_eq!(count(&engine, "alpha").unwrap(), 2);
    assert_eq!(count(&engine, "beta").unwrap(), 1);
    // Now encrypted: the next snapshot after a write seals it (one with no
    // write since keeps the set already published at that point).
    engine.upsert(&ns("beta"), entity("beta", 1), 0).unwrap();
    engine.snapshot(0).unwrap();
    drop(engine);
    assert!(!on_disk(&dir, "alpha-marker-0"));
    let _ = fs::remove_dir_all(&dir);
}
