//! Ingest spool: the crash-safe handoff between shell hooks and the store.
//!
//! Hook CLIs append JSON lines to per-process spool files and atomically
//! rename them into the spool directory — never shared appends (concurrent
//! large writes can interleave). Files are 0600, shredded after ingest, and
//! left in place on crash so the next pass replays them (idempotent ids make
//! redelivery safe).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::error::Result;

/// Spool line protocol version.
pub const SPOOL_VERSION: u32 = 1;

/// Per-process sequence: coarse clocks (Windows ~15ms) repeat `nanos`
/// across calls, so the timestamp alone cannot name files.
static SEQ: AtomicU64 = AtomicU64::new(0);

/// Staging write, unix: `create_new` (no clobber, no symlink plant)
/// with 0600 from birth; the rename carries name and mode over.
#[cfg(unix)]
fn write_staging(staging: &Path, content: &str) -> Result<()> {
    use std::io::Write as _;
    use std::os::unix::fs::OpenOptionsExt;
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(staging)?;
    f.write_all(content.as_bytes())?;
    f.sync_all()?;
    Ok(())
}

/// Staging write, non-unix: `create_new`, default ACLs (platform lacks
/// unix modes; the store dir itself carries the trust boundary).
#[cfg(not(unix))]
fn write_staging(staging: &Path, content: &str) -> Result<()> {
    use std::io::Write as _;
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(staging)?;
    f.write_all(content.as_bytes())?;
    f.sync_all()?;
    Ok(())
}

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
    let seq = SEQ.fetch_add(1, Ordering::Relaxed);
    let staging = spool_dir.join(format!(".tmp-{pid}-{nanos}-{seq}"));
    write_staging(&staging, &format!("{line}\n"))?;
    let target = spool_dir.join(format!("spool-{pid}-{nanos}-{seq}.jsonl"));
    std::fs::rename(&staging, &target)?;
    Ok(target)
}

/// True for regular files only: symlinks, dirs, and missing paths fail.
/// Ingest must never follow or shred through a link.
fn is_regular(path: &Path) -> bool {
    std::fs::symlink_metadata(path).is_ok_and(|m| m.file_type().is_file())
}

/// List pending spool files, oldest first. Staging dotfiles are skipped
/// (a sibling process may still be writing them), as are symlinks and
/// non-regular files (see [`is_regular`]).
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
        if name.starts_with("spool-") && name.ends_with(".jsonl") && is_regular(&entry.path()) {
            out.push(entry.path());
        }
    }
    out.sort();
    Ok(out)
}

/// Shred a spool file: overwrite with zeros, then remove. Best-effort
/// overwrite — removal is the guarantee. Refuses symlinks (never shred
/// through a link) and treats an already-removed file as success (a
/// concurrent ingest pass shredded first; the commit was idempotent).
///
/// # Errors
///
/// Returns [`crate::error::Error::Io`] when the file cannot be removed.
pub fn shred(path: &Path) -> Result<()> {
    if !is_regular(path) {
        // Symlink plant or already gone: never follow, never fail.
        return Ok(());
    }
    if let Ok(meta) = std::fs::metadata(path) {
        let len = meta.len().min(1024 * 1024) as usize;
        if len > 0 {
            let _ = std::fs::write(path, vec![0u8; len]);
        }
    }
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e.into()),
    }
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

    #[test]
    fn rapid_spooling_never_collides() {
        // Same process, back-to-back: coarse clocks repeat nanos.
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("spool");
        let mut names = std::collections::HashSet::new();
        for i in 0..200 {
            let p = spool_line(&dir, &format!(r#"{{"v":1,"i":{i}}}"#)).unwrap();
            assert!(names.insert(p), "collision on iteration {i}");
        }
        assert_eq!(pending_spools(&dir).unwrap().len(), 200);
    }

    #[cfg(unix)]
    #[test]
    fn symlinks_skipped_and_never_shredded() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("spool");
        std::fs::create_dir_all(&dir).unwrap();
        let victim = tmp.path().join("victim.txt");
        std::fs::write(&victim, "precious").unwrap();
        std::os::unix::fs::symlink(&victim, dir.join("spool-evil.jsonl")).unwrap();
        assert!(pending_spools(&dir).unwrap().is_empty());
        shred(&dir.join("spool-evil.jsonl")).unwrap();
        assert_eq!(std::fs::read_to_string(&victim).unwrap(), "precious");
    }

    #[test]
    fn shred_missing_is_ok() {
        let tmp = tempfile::tempdir().unwrap();
        shred(&tmp.path().join("spool-gone.jsonl")).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn spool_files_are_owner_only() {
        use std::os::unix::fs::PermissionsExt;
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("spool");
        let p = spool_line(&dir, r#"{"v":1}"#).unwrap();
        let mode = std::fs::metadata(&p).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "spool file mode {mode:o}");
    }
}
