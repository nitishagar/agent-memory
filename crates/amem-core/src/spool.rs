//! Ingest spool: the crash-safe handoff between shell hooks and the store.
//!
//! Hook CLIs append JSON lines to per-process spool files and atomically
//! rename them into the spool directory — never shared appends (concurrent
//! large writes can interleave). Files are 0600, shredded after ingest, and
//! left in place on crash so the next pass replays them (idempotent ids make
//! redelivery safe).

use std::path::{Path, PathBuf};

use crate::error::Result;

/// Spool line protocol version.
pub const SPOOL_VERSION: u32 = 1;

/// Write one JSON line to a fresh per-process spool file, then atomically
/// rename it into `spool_dir`. Returns the final path.
///
/// # Errors
///
/// Returns [`crate::error::Error::Io`] on filesystem failures.
pub fn spool_line(spool_dir: &Path, line: &str) -> Result<PathBuf> {
    std::fs::create_dir_all(spool_dir)?;
    let pid = std::process::id();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    let staging = spool_dir.join(format!(".tmp-{pid}-{nanos}"));
    std::fs::write(&staging, format!("{line}\n"))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&staging, std::fs::Permissions::from_mode(0o600))?;
    }
    let counter = nanos % 1_000_000;
    let target = spool_dir.join(format!("spool-{pid}-{nanos}-{counter}.jsonl"));
    std::fs::rename(&staging, &target)?;
    Ok(target)
}

/// List pending spool files, oldest first. Staging dotfiles are skipped
/// (a sibling process may still be writing them).
///
/// # Errors
///
/// Returns [`crate::error::Error::Io`] when the directory cannot be read.
pub fn pending_spools(spool_dir: &Path) -> Result<Vec<PathBuf>> {
    if !spool_dir.exists() {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    for entry in std::fs::read_dir(spool_dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with("spool-") && name.ends_with(".jsonl") {
            out.push(entry.path());
        }
    }
    out.sort();
    Ok(out)
}

/// Shred a spool file: overwrite with zeros, then remove. Best-effort
/// overwrite — removal is the guarantee.
///
/// # Errors
///
/// Returns [`crate::error::Error::Io`] when the file cannot be removed.
pub fn shred(path: &Path) -> Result<()> {
    if let Ok(meta) = std::fs::metadata(path) {
        let len = meta.len().min(1024 * 1024) as usize;
        if len > 0 {
            let _ = std::fs::write(path, vec![0u8; len]);
        }
    }
    std::fs::remove_file(path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spool_round_trip_and_shred() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("spool");
        let p = spool_line(&dir, r#"{"v":1}"#).unwrap();
        assert!(p.exists());
        let pending = pending_spools(&dir).unwrap();
        assert_eq!(pending, vec![p.clone()]);
        shred(&p).unwrap();
        assert!(!p.exists());
        assert!(pending_spools(&dir).unwrap().is_empty());
    }

    #[test]
    fn missing_dir_yields_no_pending() {
        let tmp = tempfile::tempdir().unwrap();
        assert!(pending_spools(&tmp.path().join("nope")).unwrap().is_empty());
    }
}
