//! The efficient sync path: resolve the desired resource set from the control
//! plane, then reconcile the workspace directly (download from object store →
//! verify → extract). One transfer instead of the generic upload/execute
//! round-trips.

use std::fmt;
use std::path::PathBuf;
use std::time::Duration;

use serde::Deserialize;

use crate::reconcile::{apply_diff, read_manifest, ResolvedResource, Summary};

/// Resolved sync configuration — see `run()` for the env-var mapping.
pub struct SyncConfig {
    pub resolve_url: String,
    pub token: String,
    pub workspace_root: PathBuf,
}

#[derive(Debug)]
pub enum SyncError {
    /// Sync is not configured (`RESOLVE_URL` unset) — a valid way to run the
    /// controller as a pure generic sandbox runtime, not a failure.
    Disabled,
    /// Missing/invalid local configuration — not retryable.
    Config(String),
    /// The control plane / object store failed — retryable.
    Upstream(String),
}

impl fmt::Display for SyncError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Disabled => f.write_str("sync disabled — RESOLVE_URL not set"),
            Self::Config(m) | Self::Upstream(m) => f.write_str(m),
        }
    }
}

/// Retry boot sync with exponential backoff on upstream errors (network, control plane).
/// Config errors and disabled sync don't retry. Returns the first non-upstream error or
/// the last error after exhausting retries.
pub fn run_with_retries(max_attempts: usize) -> Result<Summary, SyncError> {
    for attempt in 1..=max_attempts {
        match run() {
            Err(SyncError::Upstream(msg)) if attempt < max_attempts => {
                #[allow(clippy::cast_possible_truncation)]
                let backoff_ms = 100 * 2_u64.pow((attempt - 1) as u32);
                tracing::warn!(
                    attempt,
                    max_attempts,
                    backoff_ms,
                    "sync attempt failed ({msg}), retrying…"
                );
                std::thread::sleep(Duration::from_millis(backoff_ms));
            }
            result => return result,
        }
    }
    unreachable!()
}

#[derive(Deserialize)]
struct ResolveResponse {
    items: Vec<ResolvedResource>,
    #[serde(default)]
    digest: String,
}

/// Read the sync configuration from the environment and reconcile.
pub fn run() -> Result<Summary, SyncError> {
    // RESOLVE_URL is the FULL resolve endpoint URL — the controller assumes no
    // path layout on the control plane.
    let resolve_url = std::env::var("RESOLVE_URL").unwrap_or_default();
    let resolve_url = resolve_url.trim().to_string();
    let token = std::env::var("RESOLVE_TOKEN").unwrap_or_default();
    let token = token.trim().to_string();
    let workspace = std::env::var("WORKSPACE_ROOT").unwrap_or_default();
    let workspace = workspace.trim().to_string();

    if resolve_url.is_empty() {
        return Err(SyncError::Disabled);
    }
    if token.is_empty() || workspace.is_empty() {
        return Err(SyncError::Config(
            "RESOLVE_TOKEN / WORKSPACE_ROOT required when RESOLVE_URL is set".into(),
        ));
    }
    run_with(&SyncConfig {
        resolve_url,
        token,
        workspace_root: PathBuf::from(workspace),
    })
}

/// Resolve the desired set and reconcile the workspace. Never deletes on a
/// failed resolve — the last-good workspace state is kept.
pub fn run_with(cfg: &SyncConfig) -> Result<Summary, SyncError> {
    let workspace_root = cfg.workspace_root.as_path();

    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(60))
        .build()
        .map_err(|e| SyncError::Config(format!("http client init failed: {e}")))?;

    let resp = client
        .get(&cfg.resolve_url)
        .bearer_auth(&cfg.token)
        .send()
        .map_err(|e| SyncError::Upstream(format!("resolve failed: {e}")))?;
    if !resp.status().is_success() {
        return Err(SyncError::Upstream(format!(
            "resolve failed HTTP {}",
            resp.status()
        )));
    }
    let body: ResolveResponse = resp
        .json()
        .map_err(|e| SyncError::Upstream(format!("resolve response parse failed: {e}")))?;
    tracing::info!(items = body.items.len(), digest = %body.digest, "resolved desired set");

    // First boot may run before the agent has created its workspace — the
    // manifest write needs the directory to exist.
    if let Err(e) = std::fs::create_dir_all(workspace_root) {
        return Err(SyncError::Config(format!(
            "workspace root unavailable: {e}"
        )));
    }
    let manifest = read_manifest(workspace_root);
    let summary = apply_diff(
        &client,
        workspace_root,
        &body.items,
        &manifest,
        &body.digest,
    );

    tracing::info!(
        added = summary.added.len(),
        updated = summary.updated.len(),
        removed = summary.removed.len(),
        errors = summary.errors.len(),
        "sync done"
    );
    for e in &summary.errors {
        tracing::error!("apply failed: {e}");
    }
    Ok(summary)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bundle::sha256_hex;
    use axum::routing::get;
    use axum::{Json, Router};
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use serde_json::{json, Value};

    fn tgz(entries: &[(&str, &[u8])]) -> Vec<u8> {
        let mut builder = tar::Builder::new(GzEncoder::new(Vec::new(), Compression::default()));
        for (path, data) in entries {
            let mut header = tar::Header::new_gnu();
            header.set_mode(0o644);
            header.set_size(data.len() as u64);
            header.set_cksum();
            builder.append_data(&mut header, path, *data).unwrap();
        }
        builder.into_inner().unwrap().finish().unwrap()
    }

    /// Serve `app` on an ephemeral local port from a background thread (the
    /// tests are sync because `run_with` uses blocking reqwest). The port is
    /// known before the router is built so bundle URLs can reference it.
    fn spawn_stub(app: Router) -> u16 {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        listener.set_nonblocking(true).unwrap();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async {
                let listener = tokio::net::TcpListener::from_std(listener).unwrap();
                axum::serve(listener, app).await.unwrap();
            });
        });
        port
    }

    fn spawn_bundle_host(bundle: Vec<u8>) -> u16 {
        spawn_stub(Router::new().route(
            "/bundle",
            get(move || {
                let bundle = bundle.clone();
                async move { bundle }
            }),
        ))
    }

    fn spawn_resolve_host(resolve: Value) -> u16 {
        spawn_stub(Router::new().route(
            "/resolve",
            get(move || {
                let resolve = resolve.clone();
                async move { Json(resolve) }
            }),
        ))
    }

    fn test_workspace(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("asc-sync-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    fn cfg(resolve_port: u16, workspace_root: PathBuf) -> SyncConfig {
        SyncConfig {
            resolve_url: format!("http://127.0.0.1:{resolve_port}/resolve"),
            token: "test-token".into(),
            workspace_root,
        }
    }

    fn resolve_item(sha256: &str, bundle_port: u16) -> Value {
        json!({
            "items": [{
                "type": "skill",
                "name": "weather",
                "version": "1",
                "sha256": sha256,
                "targetPath": "skills/weather",
                "bundleUrl": format!("http://127.0.0.1:{bundle_port}/bundle"),
            }],
            "digest": "d1",
        })
    }

    #[test]
    fn end_to_end_apply_noop_and_remove() {
        let workspace = test_workspace("e2e");
        let bundle = tgz(&[("SKILL.md", b"# weather")]);
        let sha = sha256_hex(&bundle);
        let bundle_port = spawn_bundle_host(bundle);

        // First sync: the item is applied and recorded in the manifest.
        let port = spawn_resolve_host(resolve_item(&sha, bundle_port));
        let summary = run_with(&cfg(port, workspace.clone())).unwrap();
        assert_eq!(summary.added, ["skill/weather@1"]);
        assert!(summary.errors.is_empty());
        assert_eq!(
            std::fs::read(workspace.join("skills/weather/SKILL.md")).unwrap(),
            b"# weather"
        );

        // Second sync against the same desired set: a no-op.
        let summary = run_with(&cfg(port, workspace.clone())).unwrap();
        assert!(
            summary.added.is_empty() && summary.updated.is_empty() && summary.removed.is_empty()
        );

        // The item drops out of the desired set: the owned path is pruned.
        let port = spawn_resolve_host(json!({ "items": [], "digest": "d2" }));
        let summary = run_with(&cfg(port, workspace.clone())).unwrap();
        assert_eq!(summary.removed, ["skills/weather"]);
        assert!(!workspace.join("skills/weather").exists());
    }

    #[test]
    fn sha256_mismatch_is_isolated_and_places_nothing() {
        let workspace = test_workspace("badsha");
        let bundle_port = spawn_bundle_host(tgz(&[("SKILL.md", b"# weather")]));
        // The resolve response advertises a hash the real bundle can't match.
        let port = spawn_resolve_host(resolve_item(&"0".repeat(64), bundle_port));

        let summary = run_with(&cfg(port, workspace.clone())).unwrap();
        assert!(summary.added.is_empty());
        assert_eq!(summary.errors.len(), 1);
        assert!(
            summary.errors[0].contains("sha256 mismatch"),
            "{:?}",
            summary.errors
        );
        assert!(!workspace.join("skills/weather").exists());
    }

    #[test]
    fn failed_resolve_is_upstream_error() {
        let workspace = test_workspace("down");
        // Nothing listens on this port (bound then dropped) — connection refused.
        let port = {
            let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            l.local_addr().unwrap().port()
        };
        let err = run_with(&cfg(port, workspace)).unwrap_err();
        assert!(matches!(err, SyncError::Upstream(_)));
    }

    #[test]
    fn malformed_response_missing_items_is_upstream_error() {
        let workspace = test_workspace("malformed");
        // Response without "items" field — e.g. error envelope or payload refactor
        let port = spawn_resolve_host(json!({ "error": "maintenance" }));
        let err = run_with(&cfg(port, workspace)).unwrap_err();
        assert!(matches!(err, SyncError::Upstream(_)));
        assert!(
            err.to_string().contains("parse failed"),
            "{}",
            err.to_string()
        );
    }

    #[test]
    fn empty_items_array_removes_all_managed() {
        let workspace = test_workspace("empty-intentional");
        let bundle = tgz(&[("SKILL.md", b"# weather")]);
        let sha = sha256_hex(&bundle);
        let bundle_port = spawn_bundle_host(bundle);

        // First sync: apply the item and record it in the manifest.
        let port = spawn_resolve_host(resolve_item(&sha, bundle_port));
        let summary = run_with(&cfg(port, workspace.clone())).unwrap();
        assert_eq!(summary.added, ["skill/weather@1"]);
        assert!(workspace.join("skills/weather").exists());

        // Explicit "items": [] should remove it (intentional wipe).
        let port = spawn_resolve_host(json!({ "items": [], "digest": "d2" }));
        let summary = run_with(&cfg(port, workspace.clone())).unwrap();
        assert_eq!(summary.removed, ["skills/weather"]);
        assert!(!workspace.join("skills/weather").exists());
    }

    #[test]
    fn extraction_failure_preserves_last_good_version() {
        let workspace = test_workspace("atomic-extract");
        let bundle_v1 = tgz(&[("SKILL.md", b"# weather v1")]);
        let sha_v1 = sha256_hex(&bundle_v1);
        let bundle_port = spawn_bundle_host(bundle_v1);

        // First sync: apply v1
        let port = spawn_resolve_host(resolve_item(&sha_v1, bundle_port));
        let summary = run_with(&cfg(port, workspace.clone())).unwrap();
        assert_eq!(summary.added, ["skill/weather@1"]);
        assert_eq!(
            std::fs::read(workspace.join("skills/weather/SKILL.md")).unwrap(),
            b"# weather v1"
        );

        // Second sync: resolve claims a new version with mismatched hash.
        // The bad bundle download succeeds, but hash verification fails.
        // This should NOT corrupt the existing v1 content.
        let bad_bundle_port = spawn_bundle_host(tgz(&[("SKILL.md", b"# weather v2")]));
        let port = spawn_resolve_host(json!({
            "items": [{
                "type": "skill",
                "name": "weather",
                "version": "2",
                "sha256": "0".repeat(64),  // Wrong hash
                "targetPath": "skills/weather",
                "bundleUrl": format!("http://127.0.0.1:{bad_bundle_port}/bundle"),
            }],
            "digest": "d2",
        }));
        let summary = run_with(&cfg(port, workspace.clone())).unwrap();

        // Apply should fail (hash mismatch)
        assert!(!summary.errors.is_empty());
        assert!(summary.errors[0].contains("sha256 mismatch"));

        // But the last-good v1 content must be preserved — extraction was atomic.
        assert_eq!(
            std::fs::read(workspace.join("skills/weather/SKILL.md")).unwrap(),
            b"# weather v1",
            "last-good content should be preserved after extraction failure"
        );
    }

    // ── SyncError classification tests ──────────────────────────────────────

    #[test]
    fn sync_disabled_when_resolve_url_not_set() {
        let _env = crate::test_env::lock();
        std::env::remove_var("RESOLVE_URL");
        std::env::set_var("RESOLVE_TOKEN", "token");
        std::env::set_var("WORKSPACE_ROOT", "/tmp/test");

        let result = run();
        assert!(matches!(result, Err(SyncError::Disabled)));

        std::env::remove_var("RESOLVE_TOKEN");
        std::env::remove_var("WORKSPACE_ROOT");
    }

    #[test]
    fn sync_disabled_when_resolve_url_empty() {
        let _env = crate::test_env::lock();
        std::env::set_var("RESOLVE_URL", "   ");
        std::env::set_var("RESOLVE_TOKEN", "token");
        std::env::set_var("WORKSPACE_ROOT", "/tmp/test");

        let result = run();
        assert!(matches!(result, Err(SyncError::Disabled)));

        std::env::remove_var("RESOLVE_URL");
        std::env::remove_var("RESOLVE_TOKEN");
        std::env::remove_var("WORKSPACE_ROOT");
    }

    #[test]
    fn config_error_missing_token() {
        let _env = crate::test_env::lock();
        std::env::set_var("RESOLVE_URL", "http://example.com/resolve");
        std::env::remove_var("RESOLVE_TOKEN");
        std::env::set_var("WORKSPACE_ROOT", "/tmp/test");

        let result = run();
        assert!(
            matches!(
                result,
                Err(SyncError::Config(ref m)) if m.contains("RESOLVE_TOKEN")
            ),
            "Got: {result:?}"
        );

        std::env::remove_var("RESOLVE_URL");
        std::env::remove_var("WORKSPACE_ROOT");
    }

    #[test]
    fn config_error_missing_workspace() {
        let _env = crate::test_env::lock();
        std::env::set_var("RESOLVE_URL", "http://example.com/resolve");
        std::env::set_var("RESOLVE_TOKEN", "token");
        std::env::remove_var("WORKSPACE_ROOT");

        let result = run();
        assert!(
            matches!(
                result,
                Err(SyncError::Config(ref m)) if m.contains("WORKSPACE_ROOT")
            ),
            "Got: {result:?}"
        );

        std::env::remove_var("RESOLVE_URL");
        std::env::remove_var("RESOLVE_TOKEN");
    }

    #[test]
    fn upstream_error_on_network_failure() {
        let _env = crate::test_env::lock();
        // Use a port that's not listening
        let port = {
            let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            l.local_addr().unwrap().port()
        };

        std::env::set_var("RESOLVE_URL", format!("http://127.0.0.1:{port}/resolve"));
        std::env::set_var("RESOLVE_TOKEN", "token");
        std::env::set_var("WORKSPACE_ROOT", "/tmp/test");

        let result = run();
        assert!(
            matches!(result, Err(SyncError::Upstream(_))),
            "Network failure should be Upstream error, got: {result:?}"
        );

        std::env::remove_var("RESOLVE_URL");
        std::env::remove_var("RESOLVE_TOKEN");
        std::env::remove_var("WORKSPACE_ROOT");
    }

    #[test]
    fn config_errors_do_not_retry() {
        let _env = crate::test_env::lock();
        // Clean up any existing state
        std::env::remove_var("RESOLVE_URL");
        std::env::remove_var("RESOLVE_TOKEN");
        std::env::remove_var("WORKSPACE_ROOT");

        // Missing token is a config error
        std::env::set_var("RESOLVE_URL", "http://example.com/resolve");
        std::env::remove_var("RESOLVE_TOKEN");
        std::env::set_var("WORKSPACE_ROOT", "/tmp/test");

        let result = run_with_retries(3);
        assert!(
            matches!(
                result,
                Err(SyncError::Config(ref m)) if m.contains("RESOLVE_TOKEN")
            ),
            "Config errors should not be retried, got: {result:?}"
        );

        std::env::remove_var("RESOLVE_URL");
        std::env::remove_var("WORKSPACE_ROOT");
    }
}
