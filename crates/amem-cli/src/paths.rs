//! Store path resolution: OS data dir + `agent-memory`, overridable for tests.

use std::path::PathBuf;

use amem_core::error::{Error, Result};

/// Resolve the store directory: `--store-dir` flag wins, then the
/// `AMEM_STORE_DIR` env var (the only override hooks can pass, since they
/// call bare `amem`), else the OS data dir.
pub fn store_dir(override_dir: Option<PathBuf>) -> Result<PathBuf> {
    if let Some(dir) = override_dir {
        return Ok(dir);
    }
    if let Ok(env) = std::env::var("AMEM_STORE_DIR") {
        if !env.is_empty() {
            return Ok(PathBuf::from(env));
        }
    }
    amem_core::dirs::data_dir()
        .map(|d| d.join("agent-memory"))
        .ok_or_else(|| Error::InvalidArguments("cannot locate OS data directory".to_string()))
}

/// Spool subdirectory of a store dir.
#[must_use]
pub fn spool_dir(store: &std::path::Path) -> PathBuf {
    store.join("spool")
}

/// Probe that `store` is writable: create dirs, round-trip a temp file.
/// Used at startup so locked-down machines fail with a clear error naming
/// the path instead of dying mid-ingest. Random suffix + `create_new` so a
/// planted symlink at a fixed probe name cannot redirect the write.
pub fn ensure_writable(store: &std::path::Path) -> Result<()> {
    std::fs::create_dir_all(store).map_err(|e| {
        Error::InvalidArguments(format!(
            "store dir {} is not writable: {e}",
            store.display()
        ))
    })?;
    let probe = store.join(format!(
        ".amem-writability-probe-{}-{}",
        std::process::id(),
        uuid::Uuid::new_v4().simple()
    ));
    {
        use std::io::Write as _;
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&probe)
            .map_err(|e| {
                Error::InvalidArguments(format!(
                    "store dir {} is not writable: {e}",
                    store.display()
                ))
            })?;
        f.write_all(b"ok").map_err(|e| {
            Error::InvalidArguments(format!(
                "store dir {} is not writable: {e}",
                store.display()
            ))
        })?;
    }
    std::fs::remove_file(&probe).map_err(Error::Io)?;
    Ok(())
}
