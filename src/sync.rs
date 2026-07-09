//! The efficient sync path: resolve the desired resource set from the control
//! plane, then reconcile the workspace directly (download from object store →
//! verify → extract). One transfer instead of the generic upload/execute
//! round-trips.

use std::fmt;
use std::path::Path;
use std::time::Duration;

use serde::Deserialize;

use crate::reconcile::{apply_diff, read_manifest, ResolvedResource, Summary};

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

#[derive(Deserialize)]
struct ResolveResponse {
    #[serde(default)]
    items: Vec<ResolvedResource>,
    #[serde(default)]
    digest: String,
}

/// Resolve the desired set and reconcile the workspace. Never deletes on a
/// failed resolve — the last-good workspace state is kept.
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
    let workspace_root = Path::new(&workspace);

    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(60))
        .build()
        .map_err(|e| SyncError::Config(format!("http client init failed: {e}")))?;

    let resp = client
        .get(&resolve_url)
        .bearer_auth(&token)
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
    let summary = apply_diff(&client, workspace_root, &body.items, &manifest);

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
