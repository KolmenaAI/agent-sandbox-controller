//! Fetch, verify, and safely extract a resource bundle (a gzip-tar).

use std::io::{Cursor, Read};
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
    // Enforce the cap BEFORE buffering: reject on the declared length when the
    // server sends one, and stream with a hard limit either way, so a huge
    // object can never balloon this resident sidecar's memory.
    if let Some(len) = resp.content_length() {
        if len > max_bytes as u64 {
            bail!("bundle too large: {len} > {max_bytes}");
        }
    }
    let mut bytes = Vec::new();
    resp.take(max_bytes as u64 + 1).read_to_end(&mut bytes)?;
    if bytes.len() > max_bytes {
        bail!("bundle too large: > {max_bytes}");
    }
    Ok(bytes)
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

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::write::GzEncoder;
    use flate2::Compression;

    /// Build a gzip-tar of `(path, content)` regular-file entries in memory.
    fn tgz(entries: &[(&str, &[u8])]) -> Vec<u8> {
        let mut builder = tar::Builder::new(GzEncoder::new(Vec::new(), Compression::default()));
        for (path, data) in entries {
            let mut header = tar::Header::new_gnu();
            header.set_mode(0o644);
            header.set_size(data.len() as u64);
            header.set_cksum();
            builder.append_data(&mut header, path, *data).unwrap();
        }
        builder.into_inner().unwrap().finish().unwrap()
    }

    fn test_dir(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("asc-bundle-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn extracts_regular_files() {
        let dir = test_dir("ok");
        let data = tgz(&[("SKILL.md", b"hello"), ("sub/inner.txt", b"nested")]);
        extract_tgz(&data, &dir, 100, 1024).unwrap();
        assert_eq!(std::fs::read(dir.join("SKILL.md")).unwrap(), b"hello");
        assert_eq!(std::fs::read(dir.join("sub/inner.txt")).unwrap(), b"nested");
    }

    #[test]
    fn rejects_parent_traversal() {
        let dir = test_dir("traversal");
        // tar::Builder itself refuses `..` paths, so write the raw name bytes
        // directly — the shape a hostile bundle server would produce.
        let mut builder = tar::Builder::new(GzEncoder::new(Vec::new(), Compression::default()));
        let mut header = tar::Header::new_gnu();
        let name = b"../evil.txt";
        header.as_old_mut().name[..name.len()].copy_from_slice(name);
        header.set_mode(0o644);
        header.set_size(1);
        header.set_cksum();
        builder.append(&header, &b"x"[..]).unwrap();
        let data = builder.into_inner().unwrap().finish().unwrap();

        let err = extract_tgz(&data, &dir, 100, 1024).unwrap_err();
        assert!(err.to_string().contains("unsafe path"), "{err}");
        assert!(!dir.parent().unwrap().join("evil.txt").exists());
    }

    #[test]
    fn skips_symlink_entries() {
        let dir = test_dir("symlink");
        let mut builder = tar::Builder::new(GzEncoder::new(Vec::new(), Compression::default()));
        let mut header = tar::Header::new_gnu();
        header.set_entry_type(tar::EntryType::Symlink);
        header.set_size(0);
        builder
            .append_link(&mut header, "link", "/etc/passwd")
            .unwrap();
        let data = builder.into_inner().unwrap().finish().unwrap();

        extract_tgz(&data, &dir, 100, 1024).unwrap();
        assert!(dir.join("link").symlink_metadata().is_err());
    }

    #[test]
    fn enforces_file_count_cap() {
        let dir = test_dir("count");
        let data = tgz(&[("a", b"1"), ("b", b"2"), ("c", b"3")]);
        let err = extract_tgz(&data, &dir, 2, 1024).unwrap_err();
        assert!(err.to_string().contains("exceeds limits"), "{err}");
    }

    #[test]
    fn enforces_total_size_cap() {
        let dir = test_dir("size");
        let data = tgz(&[("big", b"0123456789")]);
        let err = extract_tgz(&data, &dir, 100, 5).unwrap_err();
        assert!(err.to_string().contains("exceeds limits"), "{err}");
    }

    #[test]
    fn sha256_hex_matches_known_vector() {
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }
}
