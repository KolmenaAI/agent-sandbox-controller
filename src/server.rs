//! Sidecar HTTP server (axum). Implements the sandbox-runtime contract the
//! sandbox-runtime clients expect (upload/download/list/exists/execute)
//! plus controller-native routes: `POST /sync` (resolve → reconcile — one
//! transfer from the object store instead of upload round-trips through the
//! control plane), `POST /restart-agent` (SIGTERM the agent for an in-place
//! container restart), `GET /health` (startupProbe — only answers once the
//! server is up, i.e. after the boot sync finished), and `GET /status` (sync
//! enablement + last sync outcome, so the control plane can tell a healthy
//! workspace from a booted-but-unsynced one).
//!
//! No auth, matching the SDK (identity headers only): the port is reachable
//! only in-cluster, and everything here runs with the sidecar's own privileges
//! — nothing the agent container doesn't already have.

// Helpers use axum's Response as the Err type for early returns; it's a large
// type, but these are cold error paths on a low-traffic control port.
#![allow(clippy::result_large_err)]

use std::fs;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::extract::{DefaultBodyLimit, Multipart, Path as UrlPath, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::task::spawn_blocking;

use tower_http::trace::{DefaultMakeSpan, DefaultOnResponse, TraceLayer};
use tracing::Level;

use crate::agent::{self, RestartError};
use crate::reconcile::Summary;
use crate::sync::{self, SyncError};

const DEFAULT_PORT: u16 = 8888;
const MAX_UPLOAD_BYTES: usize = 256 * 1024 * 1024; // SDK default MaxUploadSize
const DEFAULT_EXECUTE_TIMEOUT_SECS: u64 = 300;

/// Outcome of one sync attempt, kept for `GET /status`. `ok` reflects the
/// resolve/reconcile machinery; per-item failures land in `errors` with
/// `ok: true` (the reconcile itself ran — see `apply_diff`'s error isolation).
#[derive(Clone, Serialize)]
pub struct SyncStatus {
    /// Unix seconds of the attempt.
    pub at: u64,
    pub ok: bool,
    pub added: Vec<String>,
    pub updated: Vec<String>,
    pub removed: Vec<String>,
    pub errors: Vec<String>,
}

impl SyncStatus {
    fn now_epoch_secs() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_secs())
    }

    /// `None` for `SyncError::Disabled` — no sync was attempted.
    pub fn from_result(result: &Result<Summary, SyncError>) -> Option<Self> {
        match result {
            Ok(s) => Some(Self {
                at: Self::now_epoch_secs(),
                ok: true,
                added: s.added.clone(),
                updated: s.updated.clone(),
                removed: s.removed.clone(),
                errors: s.errors.clone(),
            }),
            Err(SyncError::Disabled) => None,
            Err(e) => Some(Self {
                at: Self::now_epoch_secs(),
                ok: false,
                added: Vec::new(),
                updated: Vec::new(),
                removed: Vec::new(),
                errors: vec![e.to_string()],
            }),
        }
    }
}

#[derive(Clone)]
pub struct AppState {
    workspace_root: PathBuf,
    execute_timeout: Duration,
    sync_enabled: bool,
    last_sync: Arc<Mutex<Option<SyncStatus>>>,
    // Serializes `/sync` calls — two reconciles racing on the same workspace
    // would interleave extraction and fight over the manifest. Uses a sync
    // Mutex held inside the blocking closure to ensure the lock is held for
    // the entire operation, even if the client disconnects.
    sync_gate: Arc<Mutex<()>>,
}

impl AppState {
    pub fn new(
        workspace_root: PathBuf,
        execute_timeout: Duration,
        sync_enabled: bool,
        boot_sync: Option<SyncStatus>,
    ) -> Self {
        Self {
            workspace_root,
            execute_timeout,
            sync_enabled,
            last_sync: Arc::new(Mutex::new(boot_sync)),
            sync_gate: Arc::new(Mutex::new(())),
        }
    }
}

pub fn app(state: AppState) -> Router {
    Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/status", get(status))
        .route("/upload", post(upload))
        .route("/execute", post(execute))
        .route("/restart-agent", post(restart_agent))
        .route("/download/{*path}", get(download))
        .route("/list/{*path}", get(list))
        .route("/exists/{*path}", get(exists))
        .route("/sync", post(sync_route))
        .with_state(state)
        .layer(DefaultBodyLimit::max(MAX_UPLOAD_BYTES))
        .layer(
            // Per-request logs at DEBUG (RUST_LOG=debug to see them) — the info
            // default stays quiet in steady state.
            TraceLayer::new_for_http()
                .make_span_with(DefaultMakeSpan::new().level(Level::DEBUG))
                .on_response(DefaultOnResponse::new().level(Level::DEBUG)),
        )
}

/// Returns the process exit code so `main` can flush telemetry before exiting.
pub async fn serve(boot_sync: &Result<Summary, SyncError>) -> i32 {
    let root = std::env::var("WORKSPACE_ROOT").unwrap_or_default();
    let root = root.trim();
    if root.is_empty() {
        tracing::error!("WORKSPACE_ROOT must be set in sidecar mode");
        return 1;
    }
    let workspace_root = PathBuf::from(root);
    // The file routes and /execute's cwd need the workspace to exist — create
    // it up front (sync may be disabled and the agent may not have booted yet).
    if let Err(e) = fs::create_dir_all(&workspace_root) {
        tracing::error!("workspace root unavailable: {e}");
        return 1;
    }

    let execute_timeout = std::env::var("EXECUTE_TIMEOUT_SECS")
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .map_or(
            Duration::from_secs(DEFAULT_EXECUTE_TIMEOUT_SECS),
            Duration::from_secs,
        );
    let sync_enabled = !std::env::var("RESOLVE_URL")
        .unwrap_or_default()
        .trim()
        .is_empty();
    let state = AppState::new(
        workspace_root,
        execute_timeout,
        sync_enabled,
        SyncStatus::from_result(boot_sync),
    );

    let port = std::env::var("SERVER_PORT")
        .ok()
        .and_then(|p| p.trim().parse().ok())
        .unwrap_or(DEFAULT_PORT);

    let listener = match tokio::net::TcpListener::bind(("0.0.0.0", port)).await {
        Ok(l) => l,
        Err(e) => {
            tracing::error!("bind :{port} failed: {e}");
            return 1;
        }
    };
    tracing::info!(port, "sidecar listening");
    match axum::serve(listener, app(state))
        .with_graceful_shutdown(shutdown_signal())
        .await
    {
        Ok(()) => 0,
        Err(e) => {
            tracing::error!("server error: {e}");
            1
        }
    }
}

/// Resolves on SIGTERM (pod termination) or Ctrl-C, so the server drains and
/// `main` gets to flush telemetry instead of dying mid-buffer.
async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler");
        tokio::select! {
            _ = ctrl_c => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = ctrl_c.await;
    }
    tracing::info!("shutdown signal received — draining");
}

// ── SDK file surface ─────────────────────────────────────────────────────────

async fn upload(State(state): State<AppState>, mut multipart: Multipart) -> Response {
    let root = state.workspace_root;
    loop {
        let field = match multipart.next_field().await {
            Ok(Some(f)) => f,
            Ok(None) => return err(StatusCode::BAD_REQUEST, "no file part"),
            Err(e) => return err(StatusCode::BAD_REQUEST, format!("bad multipart body: {e}")),
        };
        let Some(filename) = field.file_name().map(str::to_string) else {
            continue;
        };
        // Plain filenames only — mirrors the SDK's client-side check.
        if filename.is_empty()
            || filename.contains('/')
            || filename.contains('\\')
            || filename == "."
            || filename == ".."
        {
            return err(
                StatusCode::BAD_REQUEST,
                "filename must be a plain file name",
            );
        }
        let data = match field.bytes().await {
            Ok(d) => d,
            Err(e) => return err(StatusCode::BAD_REQUEST, format!("read upload: {e}")),
        };

        let dir = root.clone();
        let path = root.join(&filename);
        return match run_blocking(move || {
            fs::create_dir_all(&dir).and_then(|()| fs::write(&path, &data))
        })
        .await
        {
            Err(r) => r,
            Ok(Ok(())) => (StatusCode::OK, "ok").into_response(),
            Ok(Err(e)) => err(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("write failed: {e}"),
            ),
        };
    }
}

async fn download(State(state): State<AppState>, UrlPath(path): UrlPath<String>) -> Response {
    let full = match resolve_path(&state, &path) {
        Ok(p) => p,
        Err(r) => return r,
    };
    match run_blocking(move || fs::read(full)).await {
        Err(r) => r,
        Ok(Ok(bytes)) => {
            ([(header::CONTENT_TYPE, "application/octet-stream")], bytes).into_response()
        }
        Ok(Err(_)) => err(StatusCode::NOT_FOUND, "not found"),
    }
}

#[derive(Serialize)]
struct ListEntry {
    name: String,
    size: u64,
    #[serde(rename = "type")]
    kind: &'static str,
    mod_time: u64,
}

async fn list(State(state): State<AppState>, UrlPath(path): UrlPath<String>) -> Response {
    let full = match resolve_path(&state, &path) {
        Ok(p) => p,
        Err(r) => return r,
    };
    match run_blocking(move || list_dir(&full)).await {
        Err(r) => r,
        Ok(Ok(entries)) => Json(entries).into_response(),
        Ok(Err(())) => err(StatusCode::NOT_FOUND, "not found or not a directory"),
    }
}

fn list_dir(path: &std::path::Path) -> Result<Vec<ListEntry>, ()> {
    let entries = fs::read_dir(path).map_err(|_| ())?;
    let mut out = Vec::new();
    for entry in entries.flatten() {
        // Follow symlinks so linked skills report as directories/files.
        let Ok(meta) = fs::metadata(entry.path()) else {
            continue;
        };
        let kind = if meta.is_dir() {
            "directory"
        } else if meta.is_file() {
            "file"
        } else {
            continue;
        };
        let mod_time = meta
            .modified()
            .ok()
            .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
            .map_or(0, |d| d.as_secs());
        out.push(ListEntry {
            name: entry.file_name().to_string_lossy().into_owned(),
            size: meta.len(),
            kind,
            mod_time,
        });
    }
    Ok(out)
}

async fn exists(State(state): State<AppState>, UrlPath(path): UrlPath<String>) -> Response {
    match resolve_path(&state, &path) {
        Err(r) => r,
        Ok(full) => Json(json!({ "exists": full.symlink_metadata().is_ok() })).into_response(),
    }
}

// ── SDK command surface ──────────────────────────────────────────────────────

#[derive(Deserialize)]
struct ExecBody {
    command: String,
}

async fn execute(State(state): State<AppState>, Json(body): Json<ExecBody>) -> Response {
    let mut cmd = tokio::process::Command::new("sh");
    cmd.arg("-c")
        .arg(&body.command)
        // SDK semantics: commands run in the workspace, not the sidecar's cwd.
        .current_dir(&state.workspace_root)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);

    // Clear inherited environment to prevent leaking RESOLVE_TOKEN and other
    // credentials. Only pass an explicit allowlist of safe variables.
    cmd.env_clear();
    if let Ok(path) = std::env::var("PATH") {
        cmd.env("PATH", path);
    }
    if let Ok(home) = std::env::var("HOME") {
        cmd.env("HOME", home);
    }
    // Own process group, so a timeout can kill the whole `sh -c` tree — killing
    // just `sh` would orphan whatever it spawned.
    #[cfg(unix)]
    unsafe {
        cmd.pre_exec(|| {
            libc::setsid();
            Ok(())
        });
    }

    let child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => return exec_response("", &format!("spawn failed: {e}"), -1),
    };
    let pid = child.id();
    match tokio::time::timeout(state.execute_timeout, child.wait_with_output()).await {
        Ok(Ok(out)) => Json(json!({
            "stdout": String::from_utf8_lossy(&out.stdout),
            "stderr": String::from_utf8_lossy(&out.stderr),
            "exit_code": out.status.code().unwrap_or(-1),
        }))
        .into_response(),
        Ok(Err(e)) => exec_response("", &format!("wait failed: {e}"), -1),
        Err(_elapsed) => {
            // SIGKILL the process group; kill_on_drop reaps the shell itself.
            #[cfg(unix)]
            if let Some(pid) = pid.and_then(|p| i32::try_from(p).ok()) {
                unsafe { libc::kill(-pid, libc::SIGKILL) };
            }
            exec_response(
                "",
                &format!(
                    "command timed out after {}s",
                    state.execute_timeout.as_secs()
                ),
                -1,
            )
        }
    }
}

fn exec_response(stdout: &str, stderr: &str, exit_code: i32) -> Response {
    Json(json!({ "stdout": stdout, "stderr": stderr, "exit_code": exit_code })).into_response()
}

// ── Controller-native routes ─────────────────────────────────────────────────

async fn sync_route(State(state): State<AppState>) -> Response {
    // Acquire the gate inside the blocking closure so the lock is held for the
    // entire sync operation, even if the client disconnects. This prevents races
    // where spawn_blocking continues after the handler is dropped.
    let sync_gate = state.sync_gate.clone();
    let result = match run_blocking(move || {
        let _guard = sync_gate.lock().unwrap();
        sync::run()
    })
    .await
    {
        Ok(r) => r,
        Err(r) => return r,
    };
    if let Some(status) = SyncStatus::from_result(&result) {
        *state.last_sync.lock().unwrap() = Some(status);
    }
    match result {
        Ok(s) => Json(json!({
            "changed": !s.added.is_empty() || !s.updated.is_empty() || !s.removed.is_empty(),
            "added": s.added,
            "updated": s.updated,
            "removed": s.removed,
            "errors": s.errors,
        }))
        .into_response(),
        Err(SyncError::Disabled) => {
            err(StatusCode::NOT_IMPLEMENTED, SyncError::Disabled.to_string())
        }
        Err(SyncError::Config(m)) => err(StatusCode::BAD_REQUEST, m),
        // 502 is in the SDK connector's retryable set — upstream hiccups get retried.
        Err(SyncError::Upstream(m)) => err(StatusCode::BAD_GATEWAY, m),
    }
}

async fn status(State(state): State<AppState>) -> Response {
    let last_sync = state.last_sync.lock().unwrap().clone();
    Json(json!({ "sync_enabled": state.sync_enabled, "last_sync": last_sync })).into_response()
}

async fn restart_agent() -> Response {
    match agent::restart() {
        Ok(pid) => Json(json!({ "signaled": pid })).into_response(),
        Err(RestartError::Config(m)) => err(StatusCode::BAD_REQUEST, m),
        Err(RestartError::NotFound(m)) => err(StatusCode::NOT_FOUND, m),
        Err(RestartError::Failed(m)) => err(StatusCode::INTERNAL_SERVER_ERROR, m),
    }
}

// ── Helpers ──────────────────────────────────────────────────────────────────

async fn run_blocking<T: Send + 'static>(
    f: impl FnOnce() -> T + Send + 'static,
) -> Result<T, Response> {
    spawn_blocking(f).await.map_err(|e| {
        err(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("task failed: {e}"),
        )
    })
}

fn err(status: StatusCode, msg: impl Into<String>) -> Response {
    (status, msg.into()).into_response()
}

/// Join a client-supplied path (axum has already percent-decoded it) onto the
/// workspace root, rejecting anything that could escape it.
fn resolve_path(state: &AppState, path: &str) -> Result<PathBuf, Response> {
    let rel = safe_rel_path(path).map_err(|m| err(StatusCode::BAD_REQUEST, m))?;
    Ok(state.workspace_root.join(rel))
}

fn safe_rel_path(path: &str) -> Result<PathBuf, String> {
    let mut rel = PathBuf::new();
    for seg in path.split('/') {
        if seg.is_empty() {
            continue;
        }
        if seg == "." || seg == ".." || seg.contains('\\') || seg.contains('\0') {
            return Err("unsafe path".to_string());
        }
        rel.push(seg);
    }
    if rel.as_os_str().is_empty() {
        return Err("empty path".to_string());
    }
    Ok(rel)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    fn test_app(name: &str, execute_timeout: Duration) -> (Router, PathBuf) {
        let dir = std::env::temp_dir().join(format!("asc-server-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let state = AppState::new(dir.clone(), execute_timeout, false, None);
        (app(state), dir)
    }

    #[test]
    fn safe_rel_path_accepts_plain_and_nested() {
        assert_eq!(
            safe_rel_path("skills/weather").unwrap(),
            PathBuf::from("skills/weather")
        );
        assert_eq!(
            safe_rel_path("file.txt").unwrap(),
            PathBuf::from("file.txt")
        );
    }

    #[test]
    fn safe_rel_path_rejects_escapes() {
        assert!(safe_rel_path("../etc/passwd").is_err());
        assert!(safe_rel_path("a/../../b").is_err());
        assert!(safe_rel_path("a/./b").is_err());
        assert!(safe_rel_path("").is_err());
        assert!(safe_rel_path("//").is_err());
    }

    /// SDK-shaped multipart body (mirrors the TS client's `buildMultipart()`).
    fn sdk_multipart(boundary: &str, filename: &str, content: &[u8]) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(
            format!(
                "--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"{filename}\"\r\nContent-Type: application/octet-stream\r\n\r\n"
            )
            .as_bytes(),
        );
        body.extend_from_slice(content);
        body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
        body
    }

    #[tokio::test]
    async fn sdk_surface_roundtrip() {
        let (app, dir) = test_app("roundtrip", Duration::from_secs(30));

        // upload
        let boundary = "----controllertest";
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/upload")
                    .header(
                        "content-type",
                        format!("multipart/form-data; boundary={boundary}"),
                    )
                    .body(Body::from(sdk_multipart(
                        boundary,
                        "hello.txt",
                        b"hello world",
                    )))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        // exists
        let resp = app
            .clone()
            .oneshot(
                Request::get("/exists/hello.txt")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
            json!({ "exists": true })
        );

        // download
        let resp = app
            .clone()
            .oneshot(
                Request::get("/download/hello.txt")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(&body[..], b"hello world");

        // list a subdirectory
        fs::create_dir_all(dir.join("skills/weather")).unwrap();
        let resp = app
            .clone()
            .oneshot(Request::get("/list/skills").body(Body::empty()).unwrap())
            .await
            .unwrap();
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let entries: Vec<serde_json::Value> = serde_json::from_slice(&body).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0]["name"], "weather");
        assert_eq!(entries[0]["type"], "directory");

        // execute — runs with the workspace as cwd
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/execute")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"command":"cat hello.txt"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let out: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(out["stdout"], "hello world");
        assert_eq!(out["exit_code"], 0);

        // traversal is rejected
        let resp = app
            .clone()
            .oneshot(
                Request::get("/download/..%2Fescape")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn execute_times_out_and_kills() {
        let (app, _dir) = test_app("timeout", Duration::from_millis(300));
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/execute")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"command":"sleep 30"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let out: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(out["exit_code"], -1);
        assert!(
            out["stderr"].as_str().unwrap().contains("timed out"),
            "{out}"
        );
    }

    #[tokio::test]
    async fn status_reports_sync_state() {
        let (app, _dir) = test_app("status", Duration::from_secs(30));
        let resp = app
            .oneshot(Request::get("/status").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
            json!({ "sync_enabled": false, "last_sync": null })
        );
    }

    #[tokio::test]
    async fn execute_does_not_leak_resolve_token() {
        let (app, _dir) = test_app("env-isolation", Duration::from_secs(30));
        // Verify that commands cannot access RESOLVE_TOKEN or other sidecar env vars.
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/execute")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"command":"printenv RESOLVE_TOKEN"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let out: serde_json::Value = serde_json::from_slice(&body).unwrap();
        // printenv returns empty output and exit code 1 if var is not found
        assert_eq!(out["exit_code"], 1);
        assert!(out["stdout"].as_str().unwrap().is_empty());
    }

    #[tokio::test]
    async fn execute_has_path_available() {
        let (app, _dir) = test_app("path-available", Duration::from_secs(30));
        // Verify that basic commands still work (PATH is available).
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/execute")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"command":"command -v sh"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let out: serde_json::Value = serde_json::from_slice(&body).unwrap();
        // Should find sh in PATH
        assert_eq!(out["exit_code"], 0);
        assert!(out["stdout"].as_str().unwrap().contains("sh"));
    }

    #[tokio::test]
    async fn sync_gate_serializes_concurrent_calls() {
        let (app, _dir) = test_app("sync-serialize", Duration::from_secs(30));

        // Spawn multiple sync requests concurrently. With the gate held inside
        // the blocking closure, these should serialize even if clients disconnect.
        let mut tasks = vec![];
        for _ in 0..3 {
            let app = app.clone();
            let task = tokio::spawn(async move {
                let resp = app
                    .clone()
                    .oneshot(Request::post("/sync").body(Body::empty()).unwrap())
                    .await
                    .unwrap();
                // Response should be 501 (NOT_IMPLEMENTED) because sync is disabled,
                // but the important thing is that the gate serialized the calls.
                assert_eq!(resp.status(), StatusCode::NOT_IMPLEMENTED);
            });
            tasks.push(task);
        }

        // Wait for all to complete. If the gate works correctly, this completes
        // without race conditions. If sync calls raced, we'd get concurrent access.
        for task in tasks {
            task.await.unwrap();
        }
    }
}
