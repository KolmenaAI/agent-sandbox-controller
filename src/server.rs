//! Sidecar HTTP server (axum). Implements the sandbox-runtime contract the
//! sandbox-runtime clients expect (upload/download/list/exists/execute)
//! plus controller-native routes: `POST /sync` (resolve → reconcile — one
//! transfer from the object store instead of upload round-trips through the
//! control plane), `POST /restart-agent` (SIGTERM the agent for an in-place
//! container restart), and `GET /health` (startupProbe — only answers once the
//! server is up, i.e. after the boot sync finished).
//!
//! No auth, matching the SDK (identity headers only): the port is reachable
//! only in-cluster, and everything here runs with the sidecar's own privileges
//! — nothing the agent container doesn't already have.

// Helpers use axum's Response as the Err type for early returns; it's a large
// type, but these are cold error paths on a low-traffic control port.
#![allow(clippy::result_large_err)]

use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::time::UNIX_EPOCH;

use axum::extract::{DefaultBodyLimit, Multipart, Path as UrlPath};
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
use crate::sync::{self, SyncError};

const DEFAULT_PORT: u16 = 8888;
const MAX_UPLOAD_BYTES: usize = 256 * 1024 * 1024; // SDK default MaxUploadSize

pub fn app() -> Router {
    let router = Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/upload", post(upload))
        .route("/execute", post(execute))
        .route("/restart-agent", post(restart_agent))
        .route("/download/{*path}", get(download))
        .route("/list/{*path}", get(list))
        .route("/exists/{*path}", get(exists));
    let router = router.route("/sync", post(sync_route));
    router.layer(DefaultBodyLimit::max(MAX_UPLOAD_BYTES)).layer(
        // Per-request logs at DEBUG (RUST_LOG=debug to see them) — the info
        // default stays quiet in steady state.
        TraceLayer::new_for_http()
            .make_span_with(DefaultMakeSpan::new().level(Level::DEBUG))
            .on_response(DefaultOnResponse::new().level(Level::DEBUG)),
    )
}

pub async fn serve() {
    let port = std::env::var("SERVER_PORT")
        .ok()
        .and_then(|p| p.trim().parse().ok())
        .unwrap_or(DEFAULT_PORT);

    let listener = match tokio::net::TcpListener::bind(("0.0.0.0", port)).await {
        Ok(l) => l,
        Err(e) => {
            tracing::error!("bind :{port} failed: {e}");
            std::process::exit(1);
        }
    };
    tracing::info!(port, "sidecar listening");
    if let Err(e) = axum::serve(listener, app()).await {
        tracing::error!("server error: {e}");
        std::process::exit(1);
    }
}

// ── SDK file surface ─────────────────────────────────────────────────────────

async fn upload(mut multipart: Multipart) -> Response {
    let root = match files_root() {
        Ok(r) => r,
        Err(r) => return r,
    };
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

async fn download(UrlPath(path): UrlPath<String>) -> Response {
    let full = match resolve_path(&path) {
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

async fn list(UrlPath(path): UrlPath<String>) -> Response {
    let full = match resolve_path(&path) {
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
            .map(|d| d.as_secs())
            .unwrap_or(0);
        out.push(ListEntry {
            name: entry.file_name().to_string_lossy().into_owned(),
            size: meta.len(),
            kind,
            mod_time,
        });
    }
    Ok(out)
}

async fn exists(UrlPath(path): UrlPath<String>) -> Response {
    match resolve_path(&path) {
        Err(r) => r,
        Ok(full) => Json(json!({ "exists": full.symlink_metadata().is_ok() })).into_response(),
    }
}

// ── SDK command surface ──────────────────────────────────────────────────────

#[derive(Deserialize)]
struct ExecBody {
    command: String,
}

async fn execute(Json(body): Json<ExecBody>) -> Response {
    let result =
        run_blocking(move || Command::new("sh").arg("-c").arg(&body.command).output()).await;
    match result {
        Err(r) => r,
        Ok(Ok(out)) => Json(json!({
            "stdout": String::from_utf8_lossy(&out.stdout),
            "stderr": String::from_utf8_lossy(&out.stderr),
            "exit_code": out.status.code().unwrap_or(-1),
        }))
        .into_response(),
        Ok(Err(e)) => {
            Json(json!({ "stdout": "", "stderr": format!("spawn failed: {e}"), "exit_code": -1 }))
                .into_response()
        }
    }
}

// ── Controller-native routes ─────────────────────────────────────────────────

async fn sync_route() -> Response {
    match run_blocking(sync::run).await {
        Err(r) => r,
        Ok(Ok(s)) => Json(json!({
            "changed": !s.added.is_empty() || !s.updated.is_empty() || !s.removed.is_empty(),
            "added": s.added,
            "updated": s.updated,
            "removed": s.removed,
            "errors": s.errors,
        }))
        .into_response(),
        Ok(Err(SyncError::Disabled)) => {
            err(StatusCode::NOT_IMPLEMENTED, SyncError::Disabled.to_string())
        }
        Ok(Err(SyncError::Config(m))) => err(StatusCode::BAD_REQUEST, m),
        // 502 is in the SDK connector's retryable set — upstream hiccups get retried.
        Ok(Err(SyncError::Upstream(m))) => err(StatusCode::BAD_GATEWAY, m),
    }
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

fn files_root() -> Result<PathBuf, Response> {
    let root = std::env::var("WORKSPACE_ROOT").unwrap_or_default();
    let root = root.trim().to_string();
    if root.is_empty() {
        return Err(err(
            StatusCode::INTERNAL_SERVER_ERROR,
            "WORKSPACE_ROOT not set",
        ));
    }
    Ok(PathBuf::from(root))
}

/// Join a client-supplied path (axum has already percent-decoded it) onto the
/// workspace root, rejecting anything that could escape it.
fn resolve_path(path: &str) -> Result<PathBuf, Response> {
    let root = files_root()?;
    let rel = safe_rel_path(path).map_err(|m| err(StatusCode::BAD_REQUEST, m))?;
    Ok(root.join(rel))
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

    /// SDK-shaped multipart body (mirrors the TS client's buildMultipart()).
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
        let dir = std::env::temp_dir().join(format!("asc-test-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        std::env::set_var("WORKSPACE_ROOT", &dir);
        let app = app();

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

        // execute
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/execute")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"command":"printf hi"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let out: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(out["stdout"], "hi");
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
}
