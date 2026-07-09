//! Fetch, verify, and safely extract a resource bundle (a gzip-tar).

use std::io::Cursor;
use std::path::{Component, Path};

use anyhow::{bail, Result};
use flate2::read::GzDecoder;
use sha2::{Digest, Sha256};
use tar::Archive;

/// Download a bundle from a (presigned) URL — no auth header; the URL is the
/// capability. Hard size cap so a bad/huge object can't exhaust memory.
pub fn download_bundle(
    client: &reqwest::blocking::Client,
    url: &str,
    max_bytes: usize,
) -> Result<Vec<u8>> {
    let resp = client.get(url).send()?;
    if !resp.status().is_success() {
        bail!("download HTTP {}", resp.status());
    }
    let bytes = resp.bytes()?;
    if bytes.len() > max_bytes {
        bail!("bundle too large: {} > {}", bytes.len(), max_bytes);
    }
    Ok(bytes.to_vec())
}

pub fn sha256_hex(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hex::encode(hasher.finalize())
}

/// Extract a gzip-tar (contents at archive root) into `dest`. Hardened: reject
/// absolute paths / traversal, files and dirs only (no symlinks/hardlinks/
/// devices), and cap the file count + total unpacked bytes.
pub fn extract_tgz(data: &[u8], dest: &Path, max_files: usize, max_total_bytes: u64) -> Result<()> {
    let mut archive = Archive::new(GzDecoder::new(Cursor::new(data)));
    let mut files = 0usize;
    let mut total = 0u64;

    for entry in archive.entries()? {
        let mut entry = entry?;
        let path = entry.path()?.into_owned();

        if path.is_absolute() || path.components().any(|c| matches!(c, Component::ParentDir)) {
            bail!("unsafe path in bundle: {}", path.display());
        }
        let entry_type = entry.header().entry_type();
        if !entry_type.is_file() && !entry_type.is_dir() {
            continue; // skip symlinks/hardlinks/devices
        }

        files += 1;
        total += entry.header().size().unwrap_or(0);
        if files > max_files || total > max_total_bytes {
            bail!("bundle exceeds limits ({files} files, {total} bytes)");
        }

        // `unpack_in` also refuses to write outside `dest` (defense in depth).
        if !entry.unpack_in(dest)? {
            bail!("refused unsafe tar entry: {}", path.display());
        }
    }
    Ok(())
}
