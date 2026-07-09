//! agent-sandbox-controller — a generic, agent-agnostic sandbox-side control
//! app. It exposes the sandbox-runtime HTTP API (upload/download/list/exists/
//! execute) and — when `RESOLVE_URL` is set — materializes an agent's declared
//! resource set (skills, later knowledge/…) onto the workspace volume, so the
//! agent — any runtime — just reads it on boot: no resource-sync logic and no
//! object-store credentials in any agent image.
//!
//! Modes (env `MODE`):
//! - default (oneshot): sync once, exit 0 — for initContainers and ephemeral
//!   Jobs against a stopped agent's volume. Never blocks the pod from booting.
//! - `sidecar`: sync once, then serve the sandbox-runtime HTTP API (see
//!   `server.rs`) so the control plane can push files, run commands, trigger
//!   re-syncs, and restart the agent in place.

mod agent;
mod bundle;
mod reconcile;
mod server;
mod sync;
mod telemetry;

fn main() {
    let telemetry = telemetry::init();

    // Boot sync with retries on upstream (network/control-plane) errors.
    // Disabled and config errors don't retry. For oneshot (Jobs), failures
    // should propagate as exit code 1; for sidecar, best-effort (log and continue).
    let boot_sync = sync::run_with_retries(3);
    match &boot_sync {
        Ok(_) => {}
        Err(sync::SyncError::Disabled) => tracing::info!("{}", sync::SyncError::Disabled),
        Err(e) => tracing::warn!("boot sync failed ({e})"),
    }

    let exit_code = if std::env::var("MODE").unwrap_or_default().trim() == "sidecar" {
        // Two worker threads, not one-per-core: this resident per-pod sidecar
        // serves a low-traffic control port, and async I/O multiplexing gives
        // request concurrency regardless of thread count. Blocking work (file
        // I/O, execute, sync) runs on tokio's separate on-demand blocking pool.
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("tokio runtime");
        // Returns on SIGTERM/Ctrl-C (graceful drain) or fatal error — either
        // way telemetry gets flushed below before the process exits. Sidecar
        // is best-effort: serve even if sync failed, the control plane can
        // check GET /status to see the outcome.
        rt.block_on(server::serve(&boot_sync))
    } else {
        // Oneshot (Job): exit 1 if sync failed (excluding Disabled which is OK).
        match boot_sync {
            Ok(_) | Err(sync::SyncError::Disabled) => 0,
            Err(_) => 1,
        }
    };

    telemetry.shutdown();
    std::process::exit(exit_code);
}
