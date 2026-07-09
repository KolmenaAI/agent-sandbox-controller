//! Find and signal the agent process. The pod runs with
//! `shareProcessNamespace: true`, so the sidecar sees the agent container's
//! processes in /proc and can SIGTERM the agent — the kubelet then restarts
//! just that container in place (no pod reschedule, no image pull).

pub enum RestartError {
    /// `AGENT_PROCESS_PATTERN` not configured — not retryable.
    Config(String),
    /// No matching process right now (agent may be mid-restart).
    NotFound(String),
    /// kill(2) failed.
    Failed(String),
}

/// SIGTERM the process whose /proc cmdline matches `AGENT_PROCESS_PATTERN`.
pub fn restart() -> Result<i32, RestartError> {
    let pattern = std::env::var("AGENT_PROCESS_PATTERN").unwrap_or_default();
    let pattern = pattern.trim().to_string();
    if pattern.is_empty() {
        return Err(RestartError::Config("AGENT_PROCESS_PATTERN not set".into()));
    }

    let pid = find_pid(&pattern)
        .ok_or_else(|| RestartError::NotFound(format!("no process matching {pattern:?}")))?;

    let rc = unsafe { libc::kill(pid, libc::SIGTERM) };
    if rc != 0 {
        return Err(RestartError::Failed(format!(
            "kill({pid}) failed: {}",
            std::io::Error::last_os_error()
        )));
    }
    tracing::info!(pid, pattern, "sent SIGTERM to agent");
    Ok(pid)
}

#[cfg(target_os = "linux")]
fn find_pid(pattern: &str) -> Option<i32> {
    let self_pid = std::process::id() as i32;
    // Lowest matching PID: if the agent forked children with the same cmdline,
    // the lowest is most likely the container's root process — SIGTERMing a
    // child would not trigger the container restart.
    let mut best: Option<i32> = None;
    for entry in std::fs::read_dir("/proc").ok()?.flatten() {
        let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|s| s.parse::<i32>().ok())
        else {
            continue;
        };
        if pid == self_pid {
            continue;
        }
        let Ok(raw) = std::fs::read(format!("/proc/{pid}/cmdline")) else {
            continue;
        };
        let cmdline = raw
            .split(|b| *b == 0)
            .map(String::from_utf8_lossy)
            .collect::<Vec<_>>()
            .join(" ");
        if cmdline.contains(pattern) && best.is_none_or(|b| pid < b) {
            best = Some(pid);
        }
    }
    best
}

#[cfg(not(target_os = "linux"))]
const fn find_pid(_pattern: &str) -> Option<i32> {
    None // /proc scanning is Linux-only; dev machines just report not-found.
}
