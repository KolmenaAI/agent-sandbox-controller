//! Find and signal the agent process. The pod runs with
//! `shareProcessNamespace: true`, so the sidecar sees the agent container's
//! processes in /proc and can SIGTERM the agent — the kubelet then restarts
//! just that container in place (no pod reschedule, no image pull).

#[derive(Debug)]
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
    let self_pid = std::process::id().cast_signed();
    // Lowest matching PID: if the agent forked children with the same argv[0],
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
        // Extract argv[0] (first null-terminated string in cmdline).
        let argv0 = raw
            .split(|b| *b == 0)
            .next()
            .and_then(|s| std::str::from_utf8(s).ok())
            .unwrap_or("");
        // Match pattern against argv[0], not the full command line.
        // This prevents /execute commands with matching filenames from being killed.
        if argv0.contains(pattern) && best.is_none_or(|b| pid < b) {
            best = Some(pid);
        }
    }
    best
}

#[cfg(not(target_os = "linux"))]
const fn find_pid(_pattern: &str) -> Option<i32> {
    None // /proc scanning is Linux-only; dev machines just report not-found.
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn restart_config_error_pattern_not_set() {
        std::env::remove_var("AGENT_PROCESS_PATTERN");
        let result = restart();
        assert!(matches!(result, Err(RestartError::Config(m)) if m.contains("not set")));
    }

    #[test]
    fn restart_config_error_pattern_empty() {
        std::env::set_var("AGENT_PROCESS_PATTERN", "   ");
        let result = restart();
        let ok = matches!(result, Err(RestartError::Config(_)));
        std::env::remove_var("AGENT_PROCESS_PATTERN");
        assert!(ok);
    }

    #[cfg(target_os = "linux")]
    mod linux_tests {
        use super::*;

        #[test]
        fn find_pid_returns_none_when_no_match() {
            let pattern = "definitely_not_running_process_xyz_123";
            let pid = find_pid(pattern);
            assert_eq!(pid, None);
        }

        #[test]
        fn find_pid_excludes_self() {
            let self_pid = std::process::id() as i32;
            let pattern = "cargo";
            let pid = find_pid(pattern);

            if let Some(found) = pid {
                assert_ne!(found, self_pid, "find_pid must exclude self process");
            }
        }

        #[test]
        fn restart_signal_success() {
            use std::process::Command;

            let child = Command::new("sleep")
                .arg("1000")
                .spawn()
                .expect("spawn sleep");

            let pid = child.id() as i32;
            let pattern = "sleep";
            std::env::set_var("AGENT_PROCESS_PATTERN", pattern);

            std::thread::sleep(std::time::Duration::from_millis(100));

            let result = restart();
            assert!(
                matches!(result, Ok(found_pid) if found_pid == pid),
                "restart should signal the sleep process. Got {:?}",
                result
            );

            std::env::remove_var("AGENT_PROCESS_PATTERN");
        }
    }
}
