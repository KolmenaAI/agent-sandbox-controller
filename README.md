# agent-sandbox-controller

A tiny, **agent-agnostic** control binary for the sandbox side of an agent
platform. It reconciles an agent's **declared resource set** (skills, knowledge
files, any bundled content) onto the agent's workspace volume, and exposes a
**sandbox-runtime HTTP API** so a control plane can manage the sandbox
generically — push files, run commands, trigger re-syncs, restart the agent
process in place. The agent itself — whatever runtime it is — holds no
resource-sync logic and no object-store credentials: it just reads its workspace.

Built for the open-source **[kubernetes-sigs/agent-sandbox](https://github.com/kubernetes-sigs/agent-sandbox)**
project: its HTTP API speaks the same runtime protocol as the upstream sandbox
SDKs, so any agent-sandbox client (Go, or the TypeScript port) can drive it. It
runs as a sidecar/initContainer in an agent-sandbox pod and needs nothing from
the platform beyond the resolve contract below — so it's reusable by anyone
building on agent-sandbox, not tied to a particular product.

Rust: a ~2 MB static binary with negligible RSS, because it runs resident in
**every** agent pod.

## Why

Putting resource delivery inside the agent image couples the feature to every
agent release and forces a reimplementation per agent type. This moves it into
one small control-plane-owned component driven by a stable HTTP contract — new
resource types are a server-side change, not an agent release.

## Modes (env `MODE`)

| Mode | Runs as | Does |
|---|---|---|
| *(default)* oneshot | an **initContainer**, or an **ephemeral Job** against a stopped agent's volume | sync once → exit `0` (never blocks the pod from booting) |
| `sidecar` | a **native sidecar** (initContainer with `restartPolicy: Always`) | sync once, then serve the HTTP API below |

## HTTP API (sidecar mode, `:8888`)

Implements the sandbox-runtime contract that agent-sandbox client SDKs expect
(`files.write/read/list/exists`, `commands.run`), plus controller-native routes.
File paths are relative to `WORKSPACE_ROOT`; traversal is rejected.

| Route | SDK call | Behaviour |
|---|---|---|
| `POST /upload` | `Files.write` | multipart single file (plain filename) → written into `WORKSPACE_ROOT` |
| `GET /download/{path}` | `Files.read` | file bytes |
| `GET /list/{path}` | `Files.list` | `[{name, size, type: "file"\|"directory", mod_time}]` |
| `GET /exists/{path}` | `Files.exists` | `{exists: bool}` |
| `POST /execute` | `Commands.run` | `{command}` → `sh -c` with the workspace as cwd → `{stdout, stderr, exit_code}`; killed (whole process group) after `EXECUTE_TIMEOUT_SECS` (default 300) |
| `POST /sync` | — | resolve → reconcile the workspace (one transfer from the object store — skips the download-to-control-plane + upload double hop); returns `{changed, added, updated, removed, errors}`; concurrent calls are serialized |
| `POST /restart-agent` | — | SIGTERM the agent process (shared PID namespace) → the kubelet restarts just that container in place — no pod reschedule, no image pull; returns `{signaled: pid}` |
| `GET /health` | — | `ok` — startupProbe target; answers only after the boot sync attempt (succeeded or failed), so the sidecar is ready to serve requests; the boot sync is best-effort and doesn't block the pod |
| `GET /status` | — | `{sync_enabled, last_sync}` — outcome of the last sync attempt (`{at, ok, added, updated, removed, errors}` or `null`); use this to check if the workspace is actually synced, not just booted |

No auth, matching the SDK (identity headers only): expose the port in-cluster
only. Everything here runs with the sidecar's own privileges — nothing the agent
container doesn't already have. `/execute` is remote command execution by
design, so restrict who can reach the port with a NetworkPolicy:

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: sandbox-controller-ingress
spec:
  podSelector:
    matchLabels: { app: my-agent } # your agent pods
  policyTypes: [Ingress]
  ingress:
    - from:
        - podSelector:
            matchLabels: { app: control-plane } # only the control plane
      ports:
        - port: 8888
```

## Sync contract (stable — the control-plane integration surface)

- **Env**
  - `RESOLVE_URL` — full URL of the control plane's resolve endpoint (the
    controller assumes nothing about its path layout). Optional: leave unset to
    disable sync and run as a pure generic sandbox runtime.
  - `RESOLVE_TOKEN` — bearer token sent to `RESOLVE_URL` (the agent's identity).
  - `WORKSPACE_ROOT` — absolute path of the agent workspace on the mounted volume.
  - `MODE` — `sidecar` to serve the HTTP API; unset/anything else = oneshot.
  - `SERVER_PORT` — sidecar listen port (default `8888`).
  - `EXECUTE_TIMEOUT_SECS` — wall-clock limit for `/execute` commands (default
    `300`); on expiry the whole process group is SIGKILLed.
  - `AGENT_PROCESS_PATTERN` — substring matched against `/proc/*/cmdline` to find
    the agent process for `/restart-agent` (e.g. the agent's entrypoint script).
    Where several processes match, the lowest PID (the container's root process)
    is signaled.
- **Resolve** — `GET {RESOLVE_URL}` with `Authorization: Bearer {RESOLVE_TOKEN}`
  → `{ items: [{ type, name, version, sha256, targetPath, bundleUrl }], digest }`.
- **Placement** — for each item, download `bundleUrl` (presigned; no auth),
  verify `sha256`, extract the `.tar.gz` into `{WORKSPACE_ROOT}/{targetPath}`.
- **Ownership** — `{WORKSPACE_ROOT}/.managed.json` = `{ [targetPath]: { version,
  sha256 } }`. Prune only listed paths no longer desired; never touch anything
  else in the workspace.
- **Failure** — a failed resolve keeps the last-good workspace (no deletes).

## Kubernetes

Requirements for the in-place agent restart (`POST /restart-agent`):

- **`shareProcessNamespace: true`** on the pod — the sidecar finds the agent's
  PID in the shared namespace. The binary itself needs no privileges or
  capabilities.
- **Matching uid** — the kernel permits the signal when the sidecar's uid equals
  the agent's (the image runs as uid 1001; set `runAsUser` to your agent's uid
  if it differs), so no `CAP_KILL` is required.
- **Native sidecars** (`initContainers` entry with `restartPolicy: Always`)
  need Kubernetes ≥ 1.29.
- Works under **runc, Kata Containers, and gVisor** — the shared PID namespace,
  cross-container same-uid SIGTERM, and container-only in-place restart are
  verified on all three (note: a SIGTERM'd agent reports exit code 143 under
  runc/gVisor but raw 15 under Kata, if you alert on exit codes).

Native sidecar in the agent pod (PodSecurity `restricted`-compliant — the
controller needs none of the restricted privileges):

```yaml
spec:
  shareProcessNamespace: true
  initContainers:
    - name: sandbox-controller
      image: agent-sandbox-controller:latest
      restartPolicy: Always # native sidecar
      securityContext:
        runAsNonRoot: true
        runAsUser: 1001 # match your agent's uid (signal permission) + volume fsGroup
        allowPrivilegeEscalation: false
        capabilities: { drop: [ALL] }
        seccompProfile: { type: RuntimeDefault }
      env:
        - { name: MODE, value: sidecar }
        - { name: RESOLVE_URL, value: http://control-plane:8080/resolve }
        - { name: RESOLVE_TOKEN, value: … } # stamped by the control plane
        - { name: WORKSPACE_ROOT, value: /data/workspace }
        - { name: AGENT_PROCESS_PATTERN, value: my-agent-entrypoint.js }
      ports:
        - containerPort: 8888
      startupProbe:
        # Answers only after the boot sync — gates the agent container's start
        # so the workspace is reconciled before the agent reads it.
        httpGet: { path: /health, port: 8888 }
      volumeMounts:
        - { name: workspace, mountPath: /data }
```

For a **stopped** agent, run the same image as a plain Job (default oneshot
mode) mounting the agent's volume — reconcile without booting the agent.

### Verify your cluster/runtime

[`examples/verify-pidshare.yaml`](examples/verify-pidshare.yaml) is a ~30-second
smoke test of the restart path: a native-sidecar "controller" finds a
distinctive process from the "agent" container, SIGTERMs it, and confirms it
reappears with a new PID while the pod's `RESTARTS` ticks to 1 and the pod
itself is never rescheduled. Set `runtimeClassName` to whatever your agent pods
use, apply, and follow the `controller` container's logs to a `VERIFY PASS`.

## Runtime toggles (one image, env-armed)

Everything is compiled into the single binary/image; capabilities switch on by
the presence of their env vars:

| Env var | When set | When unset |
|---|---|---|
| `RESOLVE_URL` | declarative sync enabled (boot sync + `POST /sync`) | sync disabled — pure generic sandbox runtime (`/sync` → 501) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | WARN+ events exported via OTLP to your observability backend; `OTEL_SERVICE_NAME` overrides the service name | stdout logging only |

## Logging

`tracing` with `RUST_LOG` filtering (default `info`). For app-level debug
(per-request HTTP logs, sync detail) use the scoped form
`RUST_LOG=agent_sandbox_controller=debug` — a bare `RUST_LOG=debug` also
enables dependency chatter (e.g. hyper's two connection-pool lines per bundle
download). OTLP export batches on a dedicated thread and is flushed on oneshot
exit, so Job/initContainer errors aren't lost. In sidecar mode the server
drains on SIGTERM and flushes the OTLP buffer before exiting, so pod-shutdown
errors aren't lost either.

Bundle downloads are deliberately sequential over one pooled connection —
dozens of small bundles sync in well under a second; bounded concurrency is an
easy future upgrade if bundle counts/sizes grow.

## Develop

```sh
cargo test
cargo clippy --all-targets -- -D warnings
cargo build --release
```

The toolchain is pinned in `rust-toolchain.toml` (kept in lockstep with the
Dockerfile builder image); rustup picks it up automatically. Install the
pre-commit hooks with `pre-commit install` — CI runs the same hooks.

## License

Apache-2.0.
