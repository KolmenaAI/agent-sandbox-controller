//! Single source of truth for the "safe relative path" rule used across the
//! file surface (`server.rs`) and the reconciler (`reconcile.rs`): reject
//! absolute paths, `.`/`..` segments, backslashes, and null bytes, and return
//! the cleaned relative path safe to `join` onto a trusted root.

use std::path::PathBuf;

/// Validate a client- or control-plane-supplied path. On success returns it as
/// a clean relative `PathBuf`; on failure returns a short reason suitable for a
/// 400 response body.
pub fn safe_rel_path(path: &str) -> Result<PathBuf, String> {
    if path.starts_with('/') {
        return Err("unsafe path".to_string());
    }
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

    #[test]
    fn accepts_plain_and_nested() {
        assert_eq!(
            safe_rel_path("skills/weather").unwrap(),
            PathBuf::from("skills/weather")
        );
        assert_eq!(
            safe_rel_path("file.txt").unwrap(),
            PathBuf::from("file.txt")
        );
        assert_eq!(safe_rel_path("a/b/c/d").unwrap(), PathBuf::from("a/b/c/d"));
    }

    #[test]
    fn rejects_escapes() {
        assert!(safe_rel_path("/etc/passwd").is_err());
        assert!(safe_rel_path("../etc/passwd").is_err());
        assert!(safe_rel_path("a/../../b").is_err());
        assert!(safe_rel_path("a/./b").is_err());
        assert!(safe_rel_path("file\\path").is_err());
        assert!(safe_rel_path("file\0name").is_err());
        assert!(safe_rel_path("").is_err());
        assert!(safe_rel_path("//").is_err());
    }
}
