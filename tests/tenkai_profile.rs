//! Tenkai product: committed profile matches the shipped query and event surfaces.

use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT: AtomicU64 = AtomicU64::new(0);

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn temp_dir() -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "ezu-tenkai-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    fs::create_dir_all(&path).unwrap();
    path
}

fn crate_version() -> String {
    let text = fs::read_to_string(workspace_root().join("Cargo.toml")).unwrap();
    for line in text.lines() {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix("version = \"")
            && let Some(end) = rest.find('"')
        {
            return rest[..end].to_string();
        }
    }
    panic!("crate version missing from Cargo.toml");
}

fn tenkai_profile_cmd() -> Command {
    let mut cmd = Command::new("python3");
    cmd.arg(workspace_root().join("scripts/tenkai-profile.py"));
    cmd
}

fn tenkai_profile_stdout(args: &[&str]) -> String {
    let output = tenkai_profile_cmd().args(args).output().unwrap();
    assert!(
        output.status.success(),
        "tenkai-profile.py {args:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap()
}

#[test]
fn committed_tenkai_product_matches_generated_v2_profile() {
    let version = crate_version();
    let tenkai = fs::read_to_string(workspace_root().join("deploy/tenkai.toml")).unwrap();
    assert!(
        tenkai.contains(&format!("version = \"{version}\"")),
        "deploy/tenkai.toml product.version must match crate {version}"
    );
    assert!(
        tenkai.contains(&format!("ghcr.io/sannrox/ezu:{version}")),
        "deploy/tenkai.toml must install ghcr.io/sannrox/ezu:{version}"
    );
    assert!(
        !tenkai.contains(":latest") && !tenkai.contains("/latest"),
        "Tenkai product must not use a latest tag"
    );
    assert!(
        tenkai.contains("[compatibility]"),
        "deploy/tenkai.toml must declare a compatibility profile"
    );
    assert!(
        tenkai.contains("version = 2"),
        "deploy/tenkai.toml must declare profile version 2"
    );
    assert!(
        tenkai.contains("\"ezu.query\" = \"0.1.0\""),
        "deploy/tenkai.toml must provide ezu.query: {tenkai}"
    );
    assert!(
        tenkai.contains("\"ezu.events\" = \"0.1.0\""),
        "deploy/tenkai.toml must provide ezu.events: {tenkai}"
    );

    let printed = tenkai_profile_stdout(&["--print-profile"]);
    assert!(
        tenkai.ends_with(&printed),
        "deploy/tenkai.toml must end with the generated profile:\n{printed}"
    );
    let check = tenkai_profile_cmd().arg("--check").output().unwrap();
    assert!(
        check.status.success(),
        "tenkai-profile.py --check failed: {}",
        String::from_utf8_lossy(&check.stderr)
    );
    let self_test = tenkai_profile_cmd().arg("--self-test").output().unwrap();
    assert!(
        self_test.status.success(),
        "tenkai-profile.py --self-test failed: {}",
        String::from_utf8_lossy(&self_test.stderr)
    );
}

#[test]
fn tenkai_profile_check_refuses_a_stale_product() {
    let original = fs::read_to_string(workspace_root().join("deploy/tenkai.toml")).unwrap();
    let dir = temp_dir();
    let stale_query = original.replace("\"ezu.query\" = \"0.1.0\"", "\"ezu.query\" = \"0.0.1\"");
    let stale_events = original.replace("\"ezu.events\" = \"0.1.0\"", "\"ezu.events\" = \"0.0.1\"");
    let stale_id = original.replace("\"ezu.query\"", "\"spatial.query\"");
    let stale_profile = original.replacen("version = 2", "version = 1", 1);
    let stale_env = original.replace("--env-file ezu.env", "--env-file /work/ezu.env");
    for (name, body) in [
        ("provides-query", stale_query),
        ("provides-events", stale_events),
        ("contract-id", stale_id),
        ("profile-version", stale_profile),
        ("env-file-host-path", stale_env),
    ] {
        assert_ne!(body, original, "{name} mutation must change the product");
        let path = dir.join(name);
        fs::write(&path, &body).unwrap();
        let output = tenkai_profile_cmd()
            .arg("--check")
            .arg("--file")
            .arg(&path)
            .output()
            .unwrap();
        assert!(
            !output.status.success(),
            "{name} must be refused by tenkai-profile.py --check"
        );
    }
    let _ = fs::remove_dir_all(&dir);
}
