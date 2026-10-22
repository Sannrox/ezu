//! Per-namespace encryption keys from an external key provider.
//!
//! A namespace configured with a key gets a data key (DEK), minted here,
//! wrapped by the provider and stored only in wrapped form under
//! `keys/<sha-256 of the namespace>.json`. Plaintext DEKs live in memory only. Each
//! unwrap goes through the provider with the namespace's grant, so a revoked
//! key or an expired grant seals that namespace and no other.
//!
//! A sealed namespace refuses reads and writes. Its data stays on disk,
//! encrypted, and is carried forward unread until its key returns.

use aes_gcm::aead::{Aead, KeyInit, Payload};
use aes_gcm::{Aes256Gcm, Nonce};
use rand::RngCore;
use sekai_keys::{Dek, KeyId, KeyProvider, KeyPurpose, TenantId, UnwrapGrant, WrappedDek};
use sha2::Digest;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

pub use sekai_keys;

const ENGINE: &str = "ezu";

/// Which namespaces are encrypted, and how their keys are reached.
#[derive(Clone)]
pub struct KeyConfig {
    pub provider: Arc<dyn KeyProvider + Send + Sync>,
    /// The unwrap grant for each encrypted namespace; the grant's tenant is
    /// the namespace.
    pub grants: HashMap<String, UnwrapGrant>,
}

impl std::fmt::Debug for KeyConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeyConfig")
            .field("namespaces", &self.grants.keys().collect::<Vec<_>>())
            .finish_non_exhaustive()
    }
}

/// The stored, wrapped keys of one namespace: the current DEK and any
/// older ones still needed to read data written before a rotation.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct Stored {
    version: u32,
    namespace: String,
    current: String,
    wrapped: Vec<WrappedDek>,
}

/// A namespace's key state in memory.
enum State {
    Open {
        current: KeyId,
        deks: HashMap<String, Dek>,
    },
    /// Why it is sealed; the namespace refuses reads and writes.
    Sealed(String),
}

pub struct Keyring {
    config: Option<KeyConfig>,
    dir: PathBuf,
    states: RwLock<HashMap<String, State>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sealed(pub String);

impl std::fmt::Display for Sealed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "namespace unavailable: {}", self.0)
    }
}

/// A namespace's key file; hashed, so any namespace fits a file name.
fn file(dir: &Path, namespace: &str) -> PathBuf {
    let digest = sha2::Sha256::digest(namespace.as_bytes());
    let name: String = digest.iter().map(|byte| format!("{byte:02x}")).collect();
    dir.join(format!("{name}.json"))
}

/// Namespaces that have stored keys, as each file names them. A file that
/// does not parse fails the open: it may hold the only copy of a key.
fn stored_namespaces(dir: &Path) -> Result<Vec<String>, String> {
    let mut found = Vec::new();
    for entry in fs::read_dir(dir).map_err(|error| error.to_string())? {
        let path = entry.map_err(|error| error.to_string())?.path();
        if path.extension().is_some_and(|ext| ext == "json") {
            let bytes = fs::read(&path).map_err(|error| error.to_string())?;
            let stored: Stored = serde_json::from_slice(&bytes)
                .map_err(|error| format!("{}: {error}", path.display()))?;
            if file(dir, &stored.namespace) != path {
                return Err(format!("{}: names another namespace", path.display()));
            }
            found.push(stored.namespace);
        }
    }
    Ok(found)
}

fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| {
            i64::try_from(since.as_secs()).unwrap_or(i64::MAX)
        })
}

impl Keyring {
    /// Load every namespace's keys. A namespace with stored keys but no
    /// grant, or whose keys do not unwrap, opens sealed.
    pub fn open(dir: &Path, config: Option<KeyConfig>) -> Result<Self, String> {
        fs::create_dir_all(dir).map_err(|error| error.to_string())?;
        let keyring = Self {
            config,
            dir: dir.to_path_buf(),
            states: RwLock::new(HashMap::new()),
        };
        for namespace in stored_namespaces(dir)? {
            let state = keyring.unwrap_stored(&namespace);
            keyring.set(&namespace, state)?;
        }
        Ok(keyring)
    }

    fn set(&self, namespace: &str, state: State) -> Result<(), String> {
        self.states
            .write()
            .map_err(|_| "keyring lock".to_string())?
            .insert(namespace.to_string(), state);
        Ok(())
    }

    fn read_stored(&self, namespace: &str) -> Result<Option<Stored>, String> {
        match fs::read(file(&self.dir, namespace)) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map(Some)
                .map_err(|error| format!("stored keys: {error}")),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.to_string()),
        }
    }

    fn write_stored(&self, namespace: &str, stored: &Stored) -> Result<(), String> {
        let path = file(&self.dir, namespace);
        let temp = path.with_extension("json.tmp");
        let bytes = serde_json::to_vec(stored).map_err(|error| error.to_string())?;
        fs::write(&temp, bytes).map_err(|error| error.to_string())?;
        fs::File::open(&temp)
            .and_then(|f| f.sync_all())
            .map_err(|error| error.to_string())?;
        fs::rename(&temp, &path).map_err(|error| error.to_string())?;
        crate::wal::sync_dir(&self.dir).map_err(|error| error.to_string())
    }

    fn unwrap_stored(&self, namespace: &str) -> State {
        let Some(config) = &self.config else {
            return State::Sealed("no key provider is configured".into());
        };
        let Some(grant) = config.grants.get(namespace) else {
            return State::Sealed("no key grant for this namespace".into());
        };
        let stored = match self.read_stored(namespace) {
            Ok(Some(stored)) => stored,
            Ok(None) => return State::Sealed("its keys are missing".into()),
            Err(error) => return State::Sealed(error),
        };
        let mut deks = HashMap::new();
        for wrapped in &stored.wrapped {
            match config.provider.unwrap(wrapped, grant, ENGINE, now_secs()) {
                Ok(outcome) => {
                    deks.insert(wrapped.dek_id.as_str().to_string(), outcome.dek);
                }
                Err(error) => return State::Sealed(format!("key unwrap failed: {error}")),
            }
        }
        match KeyId::new(stored.current.clone()) {
            Ok(current) if deks.contains_key(current.as_str()) => State::Open { current, deks },
            _ => State::Sealed("its current key is missing".into()),
        }
    }

    /// Whether `namespace` is encrypted: it has a grant or stored keys.
    pub fn encrypted(&self, namespace: &str) -> bool {
        self.config
            .as_ref()
            .is_some_and(|config| config.grants.contains_key(namespace))
            || self
                .states
                .read()
                .map(|states| states.contains_key(namespace))
                .unwrap_or(true)
    }

    /// Refuse when `namespace` is sealed.
    pub fn check(&self, namespace: &str) -> Result<(), Sealed> {
        match self
            .states
            .read()
            .map_err(|_| Sealed("keyring lock".into()))?
            .get(namespace)
        {
            Some(State::Sealed(why)) => Err(Sealed(why.clone())),
            _ => Ok(()),
        }
    }

    pub fn sealed(&self) -> Vec<String> {
        self.states
            .read()
            .map(|states| {
                states
                    .iter()
                    .filter(|(_, state)| matches!(state, State::Sealed(_)))
                    .map(|(name, _)| name.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// The current key of an encrypted namespace, minting and storing one
    /// on first use. `None` for a namespace without a grant.
    pub fn current(&self, namespace: &str) -> Result<Option<(KeyId, Dek)>, Sealed> {
        self.check(namespace)?;
        if let Some(State::Open { current, deks }) = self
            .states
            .read()
            .map_err(|_| Sealed("keyring lock".into()))?
            .get(namespace)
        {
            let dek = deks
                .get(current.as_str())
                .ok_or(Sealed("current key".into()))?;
            return Ok(Some((current.clone(), Dek::from_bytes(*dek.as_bytes()))));
        }
        let Some(config) = &self.config else {
            return Ok(None);
        };
        if !config.grants.contains_key(namespace) {
            return Ok(None);
        }
        self.mint(namespace, Vec::new()).map(Some)
    }

    /// A new current key for `namespace`, kept beside the older ones until a
    /// snapshot re-encrypts everything under it.
    pub fn rotate(&self, namespace: &str) -> Result<KeyId, Sealed> {
        self.check(namespace)?;
        let older = self
            .read_stored(namespace)
            .map_err(Sealed)?
            .map(|stored| stored.wrapped)
            .unwrap_or_default();
        self.mint(namespace, older).map(|(id, _)| id)
    }

    fn mint(&self, namespace: &str, older: Vec<WrappedDek>) -> Result<(KeyId, Dek), Sealed> {
        let config = self
            .config
            .as_ref()
            .ok_or(Sealed("no key provider".into()))?;
        let tenant = TenantId::new(namespace).map_err(|error| Sealed(error.to_string()))?;
        let dek = Dek::generate();
        let wrapped = config
            .provider
            .wrap(&tenant, KeyPurpose::Store, &dek)
            .map_err(|error| Sealed(format!("key wrap failed: {error}")))?;
        // Wrapping does not check the grant; an unwrap does. A key this
        // namespace could not read back after a restart is never used.
        let grant = config
            .grants
            .get(namespace)
            .ok_or(Sealed("no key grant for this namespace".into()))?;
        config
            .provider
            .unwrap(&wrapped, grant, ENGINE, now_secs())
            .map_err(|error| Sealed(format!("key unwrap failed: {error}")))?;
        let id = wrapped.dek_id.clone();
        let mut all = older;
        all.push(wrapped);
        // Stored wrapped before first use, so no data is ever written under
        // a key that a restart could not find.
        self.write_stored(
            namespace,
            &Stored {
                version: 1,
                namespace: namespace.to_string(),
                current: id.as_str().to_string(),
                wrapped: all,
            },
        )
        .map_err(Sealed)?;
        let mut states = self
            .states
            .write()
            .map_err(|_| Sealed("keyring lock".into()))?;
        let entry = states.entry(namespace.to_string()).or_insert(State::Open {
            current: id.clone(),
            deks: HashMap::new(),
        });
        if let State::Open { current, deks } = entry {
            *current = id.clone();
            deks.insert(id.as_str().to_string(), Dek::from_bytes(*dek.as_bytes()));
        }
        Ok((id, dek))
    }

    /// The key `dek_id` of `namespace`, for reading.
    pub fn key(&self, namespace: &str, dek_id: &str) -> Result<Dek, Sealed> {
        match self
            .states
            .read()
            .map_err(|_| Sealed("keyring lock".into()))?
            .get(namespace)
        {
            Some(State::Open { deks, .. }) => deks
                .get(dek_id)
                .map(|dek| Dek::from_bytes(*dek.as_bytes()))
                .ok_or_else(|| Sealed(format!("unknown key {dek_id}"))),
            Some(State::Sealed(why)) => Err(Sealed(why.clone())),
            None => Err(Sealed("no keys for this namespace".into())),
        }
    }

    /// Namespaces with a grant, encrypted from their first write.
    pub fn granted(&self) -> Vec<String> {
        self.config
            .as_ref()
            .map(|config| config.grants.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// Every open namespace with its current key id.
    pub fn current_ids(&self) -> Vec<(String, String)> {
        self.states
            .read()
            .map(|states| {
                states
                    .iter()
                    .filter_map(|(name, state)| match state {
                        State::Open { current, .. } => {
                            Some((name.clone(), current.as_str().to_string()))
                        }
                        State::Sealed(_) => None,
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Seal `namespace` from now on.
    pub fn seal_namespace(&self, namespace: &str, why: &str) -> Result<(), String> {
        self.set(namespace, State::Sealed(why.to_string()))
    }

    /// Unwrap every open namespace's keys again; one whose unwrap fails is
    /// sealed. Returns the namespaces sealed by this check, each with the
    /// current key it had, which seals its in-memory data one last time
    /// before the key is forgotten.
    pub fn refresh(&self) -> Vec<(String, Option<(KeyId, Dek)>)> {
        let names: Vec<String> = self
            .states
            .read()
            .map(|states| {
                states
                    .iter()
                    .filter(|(_, state)| matches!(state, State::Open { .. }))
                    .map(|(name, _)| name.clone())
                    .collect()
            })
            .unwrap_or_default();
        let mut sealed = Vec::new();
        for name in names {
            if let State::Sealed(why) = self.unwrap_stored(&name) {
                let last = self.current(&name).ok().flatten();
                let _ = self.set(&name, State::Sealed(why));
                sealed.push((name, last));
            }
        }
        sealed
    }

    /// Keep only `keep`, the key a published snapshot sealed all of
    /// `namespace` under, as its current key. Named by the caller, not read
    /// from disk: a rotation whose publish failed halfway may have stored a
    /// newer key that was never used.
    pub fn retire_older(&self, namespace: &str, keep: &str) -> Result<(), String> {
        let Some(stored) = self.read_stored(namespace)? else {
            return Ok(());
        };
        if stored.wrapped.len() <= 1 && stored.current == keep {
            return Ok(());
        }
        let wrapped: Vec<WrappedDek> = stored
            .wrapped
            .into_iter()
            .filter(|wrapped| wrapped.dek_id.as_str() == keep)
            .collect();
        if wrapped.is_empty() {
            return Err(format!("key {keep} is not stored"));
        }
        self.write_stored(
            namespace,
            &Stored {
                version: 1,
                namespace: namespace.to_string(),
                current: keep.to_string(),
                wrapped,
            },
        )?;
        if let Some(State::Open { deks, .. }) = self
            .states
            .write()
            .map_err(|_| "keyring lock".to_string())?
            .get_mut(namespace)
        {
            deks.retain(|id, _| id == keep);
        }
        Ok(())
    }
}

/// Seal `plaintext` under `dek`, binding `aad`: nonce then ciphertext.
pub fn seal(dek: &Dek, aad: &[u8], plaintext: &[u8]) -> Result<Vec<u8>, String> {
    let cipher = Aes256Gcm::new_from_slice(dek.as_bytes()).map_err(|_| "cipher".to_string())?;
    let mut nonce = [0u8; 12];
    rand::thread_rng().fill_bytes(&mut nonce);
    let mut out = nonce.to_vec();
    out.extend(
        cipher
            .encrypt(
                &Nonce::from(nonce),
                Payload {
                    msg: plaintext,
                    aad,
                },
            )
            .map_err(|_| "encryption failed".to_string())?,
    );
    Ok(out)
}

/// Open what [`seal`] produced; fails on any tampering or a wrong key.
pub fn open(dek: &Dek, aad: &[u8], sealed: &[u8]) -> Result<Vec<u8>, String> {
    if sealed.len() < 12 + 16 {
        return Err("sealed data is too short".into());
    }
    let cipher = Aes256Gcm::new_from_slice(dek.as_bytes()).map_err(|_| "cipher".to_string())?;
    let (nonce, body) = sealed.split_at(12);
    let nonce: [u8; 12] = nonce.try_into().map_err(|_| "nonce".to_string())?;
    cipher
        .decrypt(&Nonce::from(nonce), Payload { msg: body, aad })
        .map_err(|_| "decryption failed".to_string())
}
