//! Reconcile the desired resource set against the workspace PVC.
//!
//! Ownership is tracked in `<workspace>/.managed.json` (keyed by `targetPath`), so
//! we only ever prune paths WE placed — never bundled or user-imported content.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::bundle::{download_bundle, extract_tgz, sha256_hex};

const MANIFEST_FILE: &str = ".managed.json";
const BUNDLE_MAX_BYTES: usize = 25 * 1024 * 1024;
// Unpacked cap is deliberately larger than the download cap — a legitimately
// well-compressed 25 MB bundle must not fail on extraction.
const BUNDLE_MAX_UNPACKED_BYTES: u64 = 100 * 1024 * 1024;
const BUNDLE_MAX_FILES: usize = 2000;

/// One resolved item from the control plane's resolve endpoint (camelCase JSON).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResolvedResource {
    #[serde(rename = "type")]
    pub kind: String,
    pub name: String,
    pub version: String,
    pub sha256: String,
    pub target_path: String,
    pub bundle_url: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestEntry {
    pub version: String,
    pub sha256: String,
}

/// What this controller has placed, keyed by `targetPath`.
pub type Manifest = HashMap<String, ManifestEntry>;

#[derive(Debug, Default)]
pub struct Summary {
    pub added: Vec<String>,
    pub updated: Vec<String>,
    pub removed: Vec<String>,
    pub errors: Vec<String>,
}

pub struct Diff<'a> {
    pub apply: Vec<&'a ResolvedResource>,
    pub remove: Vec<String>,
}

/// Pure reconcile diff: desired set vs the manifest we own. Add/update when the
/// content hash differs; remove owned `targetPath`s that dropped out of the set.
/// Unsafe manifest keys are ignored.
pub fn diff<'a>(desired: &'a [ResolvedResource], manifest: &Manifest) -> Diff<'a> {
    let wanted: HashSet<&str> = desired.iter().map(|d| d.target_path.as_str()).collect();

    let apply = desired
        .iter()
        .filter(|d| {
            manifest.get(&d.target_path).map(|m| m.sha256.as_str())
                != Some(&d.sha256.to_lowercase())
        })
        .collect();

    let remove = manifest
        .keys()
        .filter(|tp| !wanted.contains(tp.as_str()) && validate_target_path(tp).is_ok())
        .cloned()
        .collect();

    Diff { apply, remove }
}

fn validate_target_path(path: &str) -> Result<()> {
    // Reject absolute paths (leading /)
    if path.starts_with('/') {
        bail!("unsafe path: {path}");
    }

    let mut rel = PathBuf::new();
    for seg in path.split('/') {
        if seg.is_empty() {
            continue;
        }
        // Reject . and .. segments, backslashes, and null bytes
        if seg == "." || seg == ".." || seg.contains('\\') || seg.contains('\0') {
            bail!("unsafe path: {path}");
        }
        rel.push(seg);
    }
    if rel.as_os_str().is_empty() {
        bail!("empty path");
    }
    Ok(())
}

fn manifest_path(workspace_root: &Path) -> PathBuf {
    workspace_root.join(MANIFEST_FILE)
}

/// Read the ownership manifest. A missing or corrupt file is treated as empty —
/// a full re-apply is safe and idempotent.
pub fn read_manifest(workspace_root: &Path) -> Manifest {
    fs::read(manifest_path(workspace_root))
        .ok()
        .and_then(|bytes| serde_json::from_slice(&bytes).ok())
        .unwrap_or_default()
}

/// Write the manifest atomically (tmp + rename).
pub fn write_manifest(workspace_root: &Path, manifest: &Manifest) -> Result<()> {
    let path = manifest_path(workspace_root);
    let tmp = path.with_extension("json.tmp");
    let json = serde_json::to_vec_pretty(manifest)?;
    fs::write(&tmp, json).with_context(|| format!("write {}", tmp.display()))?;
    fs::rename(&tmp, &path).with_context(|| format!("rename {}", path.display()))?;
    Ok(())
}

/// Apply a diff to the PVC. Per-item errors are isolated so one bad bundle can't
/// abort the batch. Returns the summary and the updated manifest.
pub fn apply_diff(
    client: &reqwest::blocking::Client,
    workspace_root: &Path,
    desired: &[ResolvedResource],
    manifest: &Manifest,
) -> Summary {
    let mut summary = Summary::default();
    let mut next = manifest.clone();
    let Diff { apply, remove } = diff(desired, manifest);

    for item in apply {
        if let Err(err) = apply_one(client, workspace_root, item) {
            summary.errors.push(format!("{}: {err}", item.target_path));
            continue;
        }
        let label = format!("{}/{}@{}", item.kind, item.name, item.version);
        if manifest.contains_key(&item.target_path) {
            summary.updated.push(label);
        } else {
            summary.added.push(label);
        }
        next.insert(
            item.target_path.clone(),
            ManifestEntry {
                version: item.version.clone(),
                sha256: item.sha256.to_lowercase(),
            },
        );
    }

    for target_path in remove {
        if validate_target_path(&target_path).is_ok() {
            match fs::remove_dir_all(workspace_root.join(&target_path)) {
                Ok(()) | Err(_) => {} // tolerate already-gone
            }
        }
        next.remove(&target_path);
        summary.removed.push(target_path);
    }

    // Persist the reconciled ownership set.
    if let Err(err) = write_manifest(workspace_root, &next) {
        summary.errors.push(format!("manifest: {err}"));
    }
    summary
}

fn apply_one(
    client: &reqwest::blocking::Client,
    workspace_root: &Path,
    item: &ResolvedResource,
) -> Result<()> {
    validate_target_path(&item.target_path)?;
    let bytes = download_bundle(client, &item.bundle_url, BUNDLE_MAX_BYTES)?;
    let got = sha256_hex(&bytes);
    let want = item.sha256.to_lowercase();
    if got != want {
        anyhow::bail!("sha256 mismatch (expected {want}, got {got})");
    }

    let target = workspace_root.join(&item.target_path);
    // Extract to a temp dir first; only atomically swap on success. This ensures
    // extraction failure (disk full, corrupt tar) doesn't leave partial content
    // or delete the last-good version.
    let tmp_name = format!(".extracted-{}", Uuid::new_v4());
    let tmp_dir = workspace_root.join(&tmp_name);
    fs::create_dir_all(&tmp_dir).with_context(|| format!("mkdir {}", tmp_dir.display()))?;

    // Extract into temp directory. If this fails, the temp dir will be cleaned
    // up below and the target remains untouched.
    if let Err(e) = extract_tgz(
        &bytes,
        &tmp_dir,
        BUNDLE_MAX_FILES,
        BUNDLE_MAX_UNPACKED_BYTES,
    ) {
        let _ = fs::remove_dir_all(&tmp_dir);
        return Err(e);
    }

    // Extraction succeeded. Atomically swap: remove old target (if any), then
    // rename temp to target. The rename is atomic on POSIX.
    if target.symlink_metadata().is_ok() {
        fs::remove_dir_all(&target)
            .or_else(|_| fs::remove_file(&target))
            .with_context(|| format!("remove old {}", target.display()))?;
    }
    // Ensure parent directory of target exists before renaming.
    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent).with_context(|| format!("mkdir parent {}", parent.display()))?;
    }
    fs::rename(&tmp_dir, &target)
        .with_context(|| format!("rename {} to {}", tmp_dir.display(), target.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(target_path: &str, sha256: &str) -> ResolvedResource {
        ResolvedResource {
            kind: "skill".into(),
            name: target_path.rsplit('/').next().unwrap_or(target_path).into(),
            version: "1".into(),
            sha256: sha256.into(),
            target_path: target_path.into(),
            bundle_url: format!("https://s3/{target_path}"),
        }
    }

    fn manifest(entries: &[(&str, &str)]) -> Manifest {
        entries
            .iter()
            .map(|(tp, sha)| {
                (
                    tp.to_string(),
                    ManifestEntry {
                        version: "1".into(),
                        sha256: sha.to_string(),
                    },
                )
            })
            .collect()
    }

    #[test]
    fn adds_items_absent_from_manifest() {
        let desired = vec![item("skills/weather", "aaa")];
        let d = diff(&desired, &Manifest::new());
        assert_eq!(
            d.apply
                .iter()
                .map(|a| a.target_path.as_str())
                .collect::<Vec<_>>(),
            ["skills/weather"]
        );
        assert!(d.remove.is_empty());
    }

    #[test]
    fn applies_only_when_hash_changed() {
        let m = manifest(&[("skills/weather", "aaa"), ("skills/xlsx", "bbb")]);
        let desired = vec![item("skills/weather", "aaa"), item("skills/xlsx", "ccc")];
        let d = diff(&desired, &m);
        assert_eq!(
            d.apply
                .iter()
                .map(|a| a.target_path.as_str())
                .collect::<Vec<_>>(),
            ["skills/xlsx"]
        );
        assert!(d.remove.is_empty());
    }

    #[test]
    fn hash_compare_is_case_insensitive() {
        let m = manifest(&[("skills/weather", "abcdef")]);
        let desired = vec![item("skills/weather", "ABCDEF")];
        assert!(diff(&desired, &m).apply.is_empty());
    }

    #[test]
    fn removes_owned_paths_no_longer_desired() {
        let m = manifest(&[("skills/weather", "aaa"), ("skills/stale", "bbb")]);
        let desired = vec![item("skills/weather", "aaa")];
        let d = diff(&desired, &m);
        assert!(d.apply.is_empty());
        assert_eq!(d.remove, ["skills/stale"]);
    }

    #[test]
    fn never_removes_unowned_paths() {
        let desired = vec![item("skills/weather", "aaa")];
        assert!(diff(&desired, &Manifest::new()).remove.is_empty());
    }

    #[test]
    fn validates_target_path_rejects_absolute() {
        // Absolute paths should be rejected even if they appear relative after join
        assert!(validate_target_path("/etc/passwd").is_err());
    }

    #[test]
    fn validates_target_path_rejects_parent_traversal() {
        assert!(validate_target_path("../etc/passwd").is_err());
        assert!(validate_target_path("a/../../b").is_err());
        assert!(validate_target_path("skills/..").is_err());
    }

    #[test]
    fn validates_target_path_rejects_dot_segments() {
        assert!(validate_target_path("a/./b").is_err());
        assert!(validate_target_path(".").is_err());
        assert!(validate_target_path("./file").is_err());
    }

    #[test]
    fn validates_target_path_rejects_empty() {
        assert!(validate_target_path("").is_err());
        assert!(validate_target_path("//").is_err());
    }

    #[test]
    fn validates_target_path_rejects_null_bytes() {
        assert!(validate_target_path("file\0name").is_err());
    }

    #[test]
    fn validates_target_path_rejects_backslash() {
        assert!(validate_target_path("file\\path").is_err());
    }

    #[test]
    fn validates_target_path_accepts_safe_paths() {
        assert!(validate_target_path("skills/weather").is_ok());
        assert!(validate_target_path("file.txt").is_ok());
        assert!(validate_target_path("a/b/c/d").is_ok());
        assert!(validate_target_path("skills-v1_2.3").is_ok());
    }

    #[test]
    fn ignores_unsafe_manifest_keys_in_diff() {
        let m = manifest(&[
            ("skills/weather", "aaa"),
            ("../dangerous", "bbb"),
            ("/etc/passwd", "ccc"),
        ]);
        let desired = vec![item("skills/weather", "aaa")];
        let d = diff(&desired, &m);
        // Only the unowned safe key should be in remove; unsafe ones are ignored
        assert!(d.apply.is_empty());
        assert!(d.remove.is_empty());
    }

    #[test]
    fn removes_unowned_safe_manifest_keys() {
        let m = manifest(&[("skills/weather", "aaa"), ("skills/stale", "bbb")]);
        let desired = vec![item("skills/weather", "aaa")];
        let d = diff(&desired, &m);
        assert!(d.apply.is_empty());
        assert_eq!(d.remove, ["skills/stale"]);
    }
}
