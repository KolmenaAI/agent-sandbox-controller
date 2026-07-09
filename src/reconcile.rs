//! Reconcile the desired resource set against the workspace PVC.
//!
//! Ownership is tracked in `<workspace>/.managed.json` (keyed by `targetPath`), so
//! we only ever prune paths WE placed — never bundled or user-imported content.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::bundle::{download_bundle, extract_tgz, sha256_hex};

const MANIFEST_FILE: &str = ".managed.json";
const BUNDLE_MAX_BYTES: usize = 25 * 1024 * 1024;
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
        .filter(|tp| !wanted.contains(tp.as_str()))
        .cloned()
        .collect();

    Diff { apply, remove }
}

fn manifest_path(workspace_root: &Path) -> PathBuf {
    workspace_root.join(MANIFEST_FILE)
}

/// Read the ownership manifest. A missing or corrupt file is treated as empty —
/// a full re-apply is safe and idempotent.
pub fn read_manifest(workspace_root: &Path) -> Manifest {
    match fs::read(manifest_path(workspace_root)) {
        Ok(bytes) => serde_json::from_slice(&bytes).unwrap_or_default(),
        Err(_) => Manifest::new(),
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
/// abort the batch. Returns the summary and the updated manifest.
pub fn apply_diff(
    client: &reqwest::blocking::Client,
    workspace_root: &Path,
    desired: &[ResolvedResource],
    manifest: Manifest,
) -> Summary {
    let mut summary = Summary::default();
    let mut next = manifest.clone();
    let Diff { apply, remove } = diff(desired, &manifest);

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
        match fs::remove_dir_all(workspace_root.join(&target_path)) {
            Ok(()) | Err(_) => {} // tolerate already-gone
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
    let bytes = download_bundle(client, &item.bundle_url, BUNDLE_MAX_BYTES)?;
    let got = sha256_hex(&bytes);
    let want = item.sha256.to_lowercase();
    if got != want {
        anyhow::bail!("sha256 mismatch (expected {want}, got {got})");
    }

    let target = workspace_root.join(&item.target_path);
    // Clear any existing entry — could be a stale managed dir or a bundled
    // symlink an agent-side installer created — before extracting the real folder.
    if target.symlink_metadata().is_ok() && fs::remove_dir_all(&target).is_err() {
        let _ = fs::remove_file(&target);
    }
    fs::create_dir_all(&target).with_context(|| format!("mkdir {}", target.display()))?;
    extract_tgz(&bytes, &target, BUNDLE_MAX_FILES, BUNDLE_MAX_BYTES as u64)?;
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
}
