//! A diagnostics bundle for support: what the data directory holds and how
//! the process is configured, as JSON. It contains no secrets and no tenant
//! data: counts, sizes, LSNs and format versions, and the names (never the
//! values) of the `EZU_*` settings present. It only reads files, so it runs
//! next to a live server without taking its lock.

use serde_json::{Value, json};
use std::fs;
use std::path::Path;

fn files(dir: &Path, extension: &str) -> Vec<(String, u64)> {
    let mut found: Vec<(String, u64)> = fs::read_dir(dir)
        .map(|entries| {
            entries
                .filter_map(Result::ok)
                .filter(|entry| entry.path().extension().is_some_and(|ext| ext == extension))
                .map(|entry| {
                    let size = entry.metadata().map_or(0, |meta| meta.len());
                    (entry.file_name().to_string_lossy().into_owned(), size)
                })
                .collect()
        })
        .unwrap_or_default();
    found.sort();
    found
}

/// One snapshot set: its LSN, format version and part count, from the
/// manifest; namespaces are not listed.
fn set(dir: &Path) -> Value {
    let manifest: Option<Value> = fs::read(dir.join("MANIFEST.json"))
        .ok()
        .and_then(|bytes| serde_json::from_slice(&bytes).ok());
    let bytes: u64 = fs::read_dir(dir)
        .map(|entries| {
            entries
                .filter_map(Result::ok)
                .filter_map(|entry| entry.metadata().ok())
                .map(|meta| meta.len())
                .sum()
        })
        .unwrap_or(0);
    match manifest {
        Some(manifest) => json!({
            "lsn": manifest["lsn"],
            "version": manifest["version"],
            "parts": manifest["parts"].as_array().map_or(0, Vec::len),
            "bytes": bytes,
        }),
        None => {
            json!({ "name": dir.file_name().map(|n| n.to_string_lossy().into_owned()), "unreadable": true })
        }
    }
}

/// The bundle for the data directory `dir` and the current environment.
pub fn bundle(dir: &Path) -> Value {
    let segments = files(&dir.join("wal"), "seg");
    let snapshots = dir.join("snapshots");
    let sets: Vec<Value> = fs::read_dir(&snapshots)
        .map(|entries| {
            let mut dirs: Vec<_> = entries
                .filter_map(Result::ok)
                .map(|entry| entry.path())
                .filter(|path| path.is_dir())
                .collect();
            dirs.sort();
            dirs.iter().map(|path| set(path)).collect()
        })
        .unwrap_or_default();
    let mut settings: Vec<String> = std::env::vars_os()
        .filter_map(|(name, _)| name.into_string().ok())
        .filter(|name| name.starts_with("EZU_"))
        .collect();
    settings.sort();
    json!({
        "version": env!("CARGO_PKG_VERSION"),
        "os": std::env::consts::OS,
        "arch": std::env::consts::ARCH,
        "settings_present": settings,
        "data_dir_exists": dir.is_dir(),
        "wal": {
            "segments": segments.len(),
            "bytes": segments.iter().map(|(_, size)| size).sum::<u64>(),
            "first_segment": segments.first().map(|(name, _)| name),
            "last_segment": segments.last().map(|(name, _)| name),
        },
        "snapshot_sets": sets,
        "legacy_snapshots": files(&snapshots, "arrow").len(),
        "encrypted_namespaces": files(&dir.join("keys"), "json").len(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_bundle_holds_counts_and_setting_names_but_no_values() {
        let dir = std::env::temp_dir().join(format!("ezu-diag-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(dir.join("wal")).unwrap();
        fs::write(dir.join("wal").join("00000000000000000001.seg"), [0u8; 64]).unwrap();
        let set = dir.join("snapshots").join("00000000000000000009");
        fs::create_dir_all(&set).unwrap();
        fs::write(
            set.join("MANIFEST.json"),
            r#"{"version":4,"lsn":9,"parts":[{"namespace":"secret-tenant"}]}"#,
        )
        .unwrap();
        let bundle = bundle(&dir).to_string();
        assert!(bundle.contains("\"segments\":1"), "{bundle}");
        assert!(
            bundle.contains("\"lsn\":9") && bundle.contains("\"parts\":1"),
            "{bundle}"
        );
        assert!(!bundle.contains("secret-tenant"), "{bundle}");
        let _ = fs::remove_dir_all(&dir);
    }
}
