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
    /// Digest from the resolve response, enables convergence verification.
    pub digest: String,
}

pub struct Diff<'a> {
    pub apply: Vec<&'a ResolvedResource>,
    pub remove: Vec<String>,
}

/// Reconcile diff: desired set vs the manifest we own. Add/update when the
/// content hash differs, OR when content is missing from the volume (self-heal
/// damaged workspaces). Remove owned `targetPath`s that dropped out of the set.
/// Unsafe manifest keys are ignored.
pub fn diff<'a>(
    workspace_root: &Path,
    desired: &'a [ResolvedResource],
    manifest: &Manifest,
) -> Diff<'a> {
    let wanted: HashSet<&str> = desired.iter().map(|d| d.target_path.as_str()).collect();

    let apply = desired
        .iter()
        .filter(|d| {
            let sha_matches = manifest.get(&d.target_path).map(|m| m.sha256.as_str())
                == Some(&d.sha256.to_lowercase());
            if !sha_matches {
                return true; // Hash mismatch: re-place
            }
            // Hash matches, but check if content actually exists on volume
            let target = workspace_root.join(&d.target_path);
            !target.exists()
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
    match fs::read(manifest_path(workspace_root)) {
        Ok(bytes) => match serde_json::from_slice(&bytes) {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!(
                    path = ?manifest_path(workspace_root),
                    "manifest corrupted, resetting: {e}"
                );
                Manifest::new()
            }
        },
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Manifest::new(),
        Err(e) => {
            tracing::warn!(
                path = ?manifest_path(workspace_root),
                "manifest read failed, resetting: {e}"
            );
            Manifest::new()
        }
    }
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
/// abort the batch. Returns the summary with the updated manifest (only writes if changed).
pub fn apply_diff(
    client: &reqwest::blocking::Client,
    workspace_root: &Path,
    desired: &[ResolvedResource],
    manifest: &Manifest,
    digest: &str,
) -> Summary {
    let mut summary = Summary {
        digest: digest.to_string(),
        ..Default::default()
    };
    let mut next = manifest.clone();
    let Diff { apply, remove } = diff(workspace_root, desired, manifest);

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
            let target = workspace_root.join(&target_path);
            match fs::remove_dir_all(&target) {
                Ok(()) => {
                    // Successfully removed: forget it from the manifest
                    next.remove(&target_path);
                    summary.removed.push(target_path);
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    // Already gone (or never existed): safe to forget from manifest
                    next.remove(&target_path);
                    summary.removed.push(target_path);
                }
                Err(e) => {
                    // Real error (permissions, in use, I/O error, etc.): keep in manifest
                    // and report so operator can investigate. Path remains managed and we'll
                    // retry on next sync.
                    summary
                        .errors
                        .push(format!("{target_path}: remove failed: {e}"));
                }
            }
        }
    }

    // Persist the reconciled ownership set if manifest changed.
    if next != *manifest {
        if let Err(err) = write_manifest(workspace_root, &next) {
            summary.errors.push(format!("manifest: {err}"));
        }
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
        let workspace = std::env::temp_dir().join("test-diff-absent");
        let _ = fs::remove_dir_all(&workspace);
        fs::create_dir_all(&workspace).unwrap();

        let desired = vec![item("skills/weather", "aaa")];
        let d = diff(&workspace, &desired, &Manifest::new());
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
        let workspace = std::env::temp_dir().join("test-diff-hash");
        let _ = fs::remove_dir_all(&workspace);
        fs::create_dir_all(&workspace).unwrap();
        // Create the existing content so it "exists" and matches by hash
        fs::create_dir_all(workspace.join("skills/weather")).unwrap();
        fs::write(workspace.join("skills/weather/file"), "v1").unwrap();

        let m = manifest(&[("skills/weather", "aaa"), ("skills/xlsx", "bbb")]);
        let desired = vec![item("skills/weather", "aaa"), item("skills/xlsx", "ccc")];
        let d = diff(&workspace, &desired, &m);
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
        let workspace = std::env::temp_dir().join("test-diff-case");
        let _ = fs::remove_dir_all(&workspace);
        fs::create_dir_all(&workspace).unwrap();
        fs::create_dir_all(workspace.join("skills/weather")).unwrap();

        let m = manifest(&[("skills/weather", "abcdef")]);
        let desired = vec![item("skills/weather", "ABCDEF")];
        assert!(diff(&workspace, &desired, &m).apply.is_empty());
    }

    #[test]
    fn removes_owned_paths_no_longer_desired() {
        let workspace = std::env::temp_dir().join("test-diff-remove");
        let _ = fs::remove_dir_all(&workspace);
        fs::create_dir_all(&workspace).unwrap();
        fs::create_dir_all(workspace.join("skills/weather")).unwrap();

        let m = manifest(&[("skills/weather", "aaa"), ("skills/stale", "bbb")]);
        let desired = vec![item("skills/weather", "aaa")];
        let d = diff(&workspace, &desired, &m);
        assert!(d.apply.is_empty());
        assert_eq!(d.remove, ["skills/stale"]);
    }

    #[test]
    fn never_removes_unowned_paths() {
        let workspace = std::env::temp_dir().join("test-diff-unowned");
        let _ = fs::remove_dir_all(&workspace);
        fs::create_dir_all(&workspace).unwrap();

        let desired = vec![item("skills/weather", "aaa")];
        assert!(diff(&workspace, &desired, &Manifest::new())
            .remove
            .is_empty());
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
        let workspace = std::env::temp_dir().join("test-diff-unsafe-keys");
        let _ = fs::remove_dir_all(&workspace);
        fs::create_dir_all(&workspace).unwrap();
        fs::create_dir_all(workspace.join("skills/weather")).unwrap();

        let m = manifest(&[
            ("skills/weather", "aaa"),
            ("../dangerous", "bbb"),
            ("/etc/passwd", "ccc"),
        ]);
        let desired = vec![item("skills/weather", "aaa")];
        let d = diff(&workspace, &desired, &m);
        // Only the unowned safe key should be in remove; unsafe ones are ignored
        assert!(d.apply.is_empty());
        assert!(d.remove.is_empty());
    }

    #[test]
    fn removes_unowned_safe_manifest_keys() {
        let workspace = std::env::temp_dir().join("test-diff-unowned-safe");
        let _ = fs::remove_dir_all(&workspace);
        fs::create_dir_all(&workspace).unwrap();
        fs::create_dir_all(workspace.join("skills/weather")).unwrap();

        let m = manifest(&[("skills/weather", "aaa"), ("skills/stale", "bbb")]);
        let desired = vec![item("skills/weather", "aaa")];
        let d = diff(&workspace, &desired, &m);
        assert!(d.apply.is_empty());
        assert_eq!(d.remove, ["skills/stale"]);
    }

    #[test]
    fn detects_missing_content_and_reapplies() {
        let workspace = std::env::temp_dir().join("test-diff-missing");
        let _ = fs::remove_dir_all(&workspace);
        fs::create_dir_all(&workspace).unwrap();
        // Create skills/stale but NOT skills/weather (simulates out-of-band deletion)

        // Manifest says both exist with matching hashes
        let m = manifest(&[("skills/weather", "aaa"), ("skills/stale", "bbb")]);
        // But we only desire weather
        let desired = vec![item("skills/weather", "aaa")];
        let d = diff(&workspace, &desired, &m);

        // Since skills/weather doesn't exist on the volume, it should be re-applied
        // even though the manifest sha matches
        assert_eq!(
            d.apply
                .iter()
                .map(|a| a.target_path.as_str())
                .collect::<Vec<_>>(),
            ["skills/weather"]
        );
        // skills/stale should be removed (dropped from desired set)
        assert_eq!(d.remove, ["skills/stale"]);
    }
}

#[cfg(test)]
mod apply_diff_tests {
    use super::*;

    #[test]
    fn removal_errors_are_reported_and_paths_stay_managed() {
        let workspace = std::env::temp_dir().join("test-apply-removal-error");
        let _ = fs::remove_dir_all(&workspace);
        fs::create_dir_all(&workspace).unwrap();

        // Create a managed skill directory
        fs::create_dir_all(workspace.join("skills/weather")).unwrap();
        fs::write(workspace.join("skills/weather/data"), "content").unwrap();

        // Manifest says we own skills/weather
        let mut manifest = std::collections::HashMap::new();
        manifest.insert(
            "skills/weather".to_string(),
            ManifestEntry {
                version: "1".into(),
                sha256: "aaa".to_string(),
            },
        );
        // Write the manifest to disk so apply_diff can read it back
        write_manifest(&workspace, &manifest).unwrap();

        // Desired set is empty (we want to remove it)
        let desired: Vec<ResolvedResource> = vec![];

        // Make the directory read-only to cause removal to fail
        #[cfg(unix)]
        {
            use std::fs::Permissions;
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(workspace.join("skills"), Permissions::from_mode(0o555)).unwrap();
        }

        // Try to apply (which would remove the read-only directory)
        let client = reqwest::blocking::Client::new();
        let summary = apply_diff(&client, &workspace, &desired, &manifest, "");

        // Restore permissions for cleanup
        #[cfg(unix)]
        {
            use std::fs::Permissions;
            use std::os::unix::fs::PermissionsExt;
            let _ = fs::set_permissions(workspace.join("skills"), Permissions::from_mode(0o755));
        }

        // The removal should have failed and been reported
        assert!(
            !summary.errors.is_empty(),
            "Should have error for failed removal"
        );
        assert!(
            summary.errors[0].contains("remove failed"),
            "Error should mention removal: {}",
            summary.errors[0]
        );

        // The path should NOT be in the "removed" list (failed removal)
        assert!(
            summary.removed.is_empty(),
            "Failed removal should not be reported as removed"
        );

        // Verify the path is still in the manifest (not orphaned)
        let updated_manifest = read_manifest(&workspace);
        assert!(
            updated_manifest.contains_key("skills/weather"),
            "Failed removal should keep path in manifest"
        );
    }

    #[test]
    fn removal_of_missing_content_succeeds() {
        let workspace = std::env::temp_dir().join("test-apply-missing-removal");
        let _ = fs::remove_dir_all(&workspace);
        fs::create_dir_all(&workspace).unwrap();

        // Manifest says we own skills/weather, but it doesn't actually exist
        let mut manifest = std::collections::HashMap::new();
        manifest.insert(
            "skills/weather".to_string(),
            ManifestEntry {
                version: "1".into(),
                sha256: "aaa".to_string(),
            },
        );

        // Desired set is empty (we want to remove it)
        let desired: Vec<ResolvedResource> = vec![];

        // Try to apply (which would remove the missing directory)
        let client = reqwest::blocking::Client::new();
        let summary = apply_diff(&client, &workspace, &desired, &manifest, "");

        // Should succeed (NotFound is treated as already-removed)
        assert!(
            summary.errors.is_empty(),
            "Missing removal should succeed: {:?}",
            summary.errors
        );

        // The path should be in the "removed" list
        assert_eq!(summary.removed, ["skills/weather"]);

        // Verify the path is gone from the manifest
        let updated_manifest = read_manifest(&workspace);
        assert!(
            !updated_manifest.contains_key("skills/weather"),
            "Removed path should be gone from manifest"
        );
    }
}
