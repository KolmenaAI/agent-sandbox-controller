# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```sh
cargo test                                  # run all tests
cargo test reconcile::                      # run tests in one module
cargo test test_name                        # run a single test by name substring
cargo fmt --check                           # CI enforces formatting
cargo clippy --all-targets -- -D warnings   # CI treats clippy warnings as errors
cargo build --release                       # size-optimized static binary (opt-level=z, LTO, panic=abort)
```

CI (`.github/workflows/ci.yml`) runs `pre-commit run --all-files` plus `cargo test` on every PR/push. A release is a `vX.Y.Z` git tag (and only that): the `release` job verifies `Cargo.toml` matches the tag, then publishes the image to GHCR at the exact version (`ghcr.io/<owner>/agent-sandbox-controller:X.Y.Z` — no floating edge/latest tags) and creates a GitHub Release. Consumers pin the version.

Pre-commit hooks (`.pre-commit-config.yaml`) run rustfmt, cargo check, and clippy plus generic hygiene checks; set up with `pre-commit install`. Clippy runs at `pedantic` + `nursery` strictness via `[lints.clippy]` in Cargo.toml (warn level — the `-D warnings` flag promotes to errors).

## What this is

A small, agent-agnostic control binary that runs **resident in every agent pod** (as a native sidecar) or as an initContainer/Job (oneshot). It has two responsibilities:

1. **Declarative resource sync** — resolve the agent's desired resource set from the control plane (`GET RESOLVE_URL` with bearer `RESOLVE_TOKEN`), then reconcile the workspace volume: download each bundle (presigned URL), verify sha256, extract the tar.gz into `WORKSPACE_ROOT/{targetPath}`.
2. **Sandbox-runtime HTTP API** (sidecar mode, port 8888) — the file/command contract that agent-sandbox client SDKs expect (`/upload`, `/download`, `/list`, `/exists`, `/execute`), plus `/sync`, `/restart-agent`, and `/health`.

Binary size and RSS matter (it runs in every pod) — hence the aggressive release profile, `worker_threads(2)` tokio runtime, and blocking `reqwest` for the sync path.

## Architecture

Flow in `main.rs`: init telemetry → boot sync (best-effort; a failure must never block the pod) → if `MODE=sidecar`, serve HTTP; otherwise exit 0.

- `sync.rs` — resolve-and-reconcile orchestration. Distinguishes `SyncError::Disabled` (no `RESOLVE_URL` — valid, sync off), `Config` (not retryable), `Upstream` (retryable). A failed resolve keeps the last-good workspace (never deletes).
- `reconcile.rs` — the diff/apply engine. Ownership is tracked in `WORKSPACE_ROOT/.managed.json` keyed by `targetPath`: only paths the controller placed are ever pruned; user/agent content is never touched. `diff()` is pure (desired set vs manifest, compared by sha256); `apply_diff()` isolates per-item errors so one bad bundle can't abort the batch; manifest writes are atomic (tmp + rename).
- `bundle.rs` — download/verify/extract, hardened: size cap, file-count cap, rejects absolute paths and `..` traversal, skips symlinks/hardlinks/devices.
- `server.rs` — axum router. `app()` builds the `Router` (tests exercise it directly via `tower::ServiceExt`); blocking work (file I/O, execute, sync) goes through `spawn_blocking`. All file paths are relative to `WORKSPACE_ROOT` with traversal rejected. No auth by design — in-cluster-only port.
- `agent.rs` — `/restart-agent` implementation: scans `/proc/*/cmdline` for `AGENT_PROCESS_PATTERN` in the shared PID namespace and SIGTERMs the match (Linux-only; stub for other targets). Relies on `shareProcessNamespace: true` and matching uid (1001 in the image) — no capabilities needed.
- `telemetry.rs` — tracing to stdout always; OTLP export of WARN+ when `OTEL_EXPORTER_OTLP_ENDPOINT` is set, flushed on oneshot exit.

## Conventions

- **Runtime toggles, not cargo features**: everything is compiled in; capabilities arm by env-var presence (`RESOLVE_URL` → sync, `OTEL_EXPORTER_OTLP_ENDPOINT` → OTLP). One image serves every deployment shape — keep new capabilities env-armed the same way.
- **The sync contract is the stable control-plane integration surface** (documented in README.md): env vars, resolve response shape (`{items: [{type, name, version, sha256, targetPath, bundleUrl}], digest}`, camelCase), `.managed.json` ownership semantics, and never-delete-on-failed-resolve. Changes here affect the control plane — treat as breaking.
- The HTTP file/command routes mirror the sandbox-runtime SDK contract (including its plain-filename upload check and 256 MB body limit) — don't change their shapes unilaterally.
- Error enums split retryable vs non-retryable variants (`SyncError`, `RestartError`) — keep that distinction for new error paths.
- Logging: `RUST_LOG=agent_sandbox_controller=debug` for app-level debug (per-request HTTP logs are at DEBUG); default is `info` and quiet in steady state.
- Tests are inline `#[cfg(test)]` modules (see `reconcile.rs` and `server.rs`).

## Kubernetes context

The Dockerfile builds a static musl binary into Alpine (busybox `sh` is required by `/execute`; ~10 MB total), running as uid 1001 to match the workspace volume's `fsGroup`. In-place agent restart needs `shareProcessNamespace: true` and Kubernetes ≥ 1.29 for native sidecars; `examples/verify-pidshare.yaml` is a smoke test of the restart path on a given runtime class (runc/Kata/gVisor all verified).
