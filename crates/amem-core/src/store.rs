//! Embedded store: one `SQLite` database per user, zero external services.
//!
//! Identity is `(device, stream, idx)` (atuin-shaped); content hashes are
//! dedup hints only. WAL mode + `busy_timeout` + single-statement
//! transactions keep concurrent readers unblocked and writers serialized on
//! every OS — no `flock`, no hand-rolled locks.

use std::path::Path;

use rusqlite::{Connection, OpenFlags, params};

use crate::error::{Error, Result};

/// Current schema version. Bump with a forward-only migration.
pub const SCHEMA_VERSION: i64 = 1;

/// `SQLite` busy wait (ms) before a writer reports backpressure.
pub const BUSY_TIMEOUT_MS: i64 = 5000;

/// A record to insert. `idx` is allocated by [`Store::insert_record`].
#[derive(Debug, Clone)]
pub struct NewRecord {
    /// Owning device id (see [`device_id`]).
    pub device: String,
    /// Stream name, e.g. `shell`, `transcript:claude`.
    pub stream: String,
    /// Unix timestamp, nanoseconds.
    pub ts_ns: i64,
    /// Human/machine actor, e.g. `host:user`.
    pub actor: String,
    /// Record kind: `command`, `transcript`, `note`.
    pub kind: String,
    /// Canonical JSON payload.
    pub payload: serde_json::Value,
    /// blake3 hex of the canonical payload (dedup hint + integrity).
    pub hash: String,
}

/// A stored record (row read back).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    /// Record identity triple.
    pub device: String,
    /// Record identity triple.
    pub stream: String,
    /// Record identity triple.
    pub idx: i64,
    /// Unix timestamp, nanoseconds.
    pub ts_ns: i64,
    /// Actor string.
    pub actor: String,
    /// Record kind.
    pub kind: String,
    /// Canonical JSON payload.
    pub payload: String,
    /// Content hash.
    pub hash: String,
}

/// Read (or create, 0600) the stable device id for a store directory.
///
/// # Errors
///
/// Returns [`Error::Io`] if the id file cannot be read or written.
pub fn device_id(dir: &Path) -> Result<String> {
    let path = dir.join("device_id");
    if let Ok(id) = std::fs::read_to_string(&path) {
        let id = id.trim().to_string();
        if !id.is_empty() {
            return Ok(id);
        }
    }
    let id = uuid::Uuid::new_v4().simple().to_string();
    std::fs::write(&path, format!("{id}\n"))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
    }
    Ok(id)
}

/// The embedded store handle. Not `Sync`: share via message passing or one
/// connection per thread (`SQLite` serializes writers itself).
#[derive(Debug)]
pub struct Store {
    conn: Connection,
}

impl Store {
    /// Open (creating) the store under `dir`.
    ///
    /// Applies forward-only migrations; refuses databases from a newer
    /// schema version with a clear error instead of guessing.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Db`] on `SQLite` failures, [`Error::Io`] when the
    /// directory cannot be created, [`Error::InvalidArguments`] when the
    /// on-disk schema is newer than [`SCHEMA_VERSION`].
    pub fn open(dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(dir)?;
        let conn = Connection::open_with_flags(
            dir.join("memory.db"),
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
        )?;
        conn.busy_timeout(std::time::Duration::from_millis(BUSY_TIMEOUT_MS as u64))?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "FULL")?;
        let store = Self { conn };
        store.migrate()?;
        Ok(store)
    }

    fn migrate(&self) -> Result<()> {
        self.conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS schema_version (version INTEGER PRIMARY KEY);",
        )?;
        let current: i64 = self.conn.query_row(
            "SELECT COALESCE(MAX(version), 0) FROM schema_version",
            [],
            |r| r.get(0),
        )?;
        if current > SCHEMA_VERSION {
            return Err(Error::InvalidArguments(format!(
                "store schema v{current} is newer than this binary (v{SCHEMA_VERSION}); upgrade amem"
            )));
        }
        if current < 1 {
            self.conn.execute_batch(
                "CREATE TABLE records (
                    device TEXT NOT NULL,
                    stream TEXT NOT NULL,
                    idx INTEGER NOT NULL,
                    ts_ns INTEGER NOT NULL,
                    actor TEXT NOT NULL DEFAULT '',
                    kind TEXT NOT NULL DEFAULT 'command',
                    payload TEXT NOT NULL,
                    hash TEXT NOT NULL DEFAULT '',
                    version INTEGER NOT NULL DEFAULT 1,
                    extras TEXT NOT NULL DEFAULT '{}',
                    PRIMARY KEY (device, stream, idx)
                );
                CREATE INDEX idx_records_stream_ts ON records (stream, ts_ns);
                CREATE TABLE spool_ids (
                    spool_id TEXT PRIMARY KEY,
                    device TEXT NOT NULL,
                    stream TEXT NOT NULL,
                    idx INTEGER NOT NULL
                );
                INSERT INTO schema_version (version) VALUES (1);",
            )?;
        }
        Ok(())
    }

    /// Insert a record, allocating the next `idx` for its `(device, stream)`.
    /// Idempotent on `(device, stream, idx)` identity when the caller pins
    /// `idx` via [`Store::insert_pinned`]; this path always appends.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Db`] on `SQLite` failures.
    pub fn insert_record(&mut self, rec: &NewRecord) -> Result<i64> {
        let tx = self
            .conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let next: i64 = tx.query_row(
            "SELECT COALESCE(MAX(idx), -1) + 1 FROM records WHERE device = ?1 AND stream = ?2",
            params![rec.device, rec.stream],
            |r| r.get(0),
        )?;
        tx.execute(
            "INSERT INTO records (device, stream, idx, ts_ns, actor, kind, payload, hash)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                rec.device,
                rec.stream,
                next,
                rec.ts_ns,
                rec.actor,
                rec.kind,
                rec.payload.to_string(),
                rec.hash
            ],
        )?;
        tx.commit()?;
        Ok(next)
    }

    /// Insert with a caller-pinned `idx`; a retry of the same identity is a
    /// no-op returning `false`. This is the at-least-once safe path.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Db`] on `SQLite` failures.
    pub fn insert_pinned(&self, rec: &NewRecord, idx: i64) -> Result<bool> {
        let changed = self.conn.execute(
            "INSERT OR IGNORE INTO records (device, stream, idx, ts_ns, actor, kind, payload, hash)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                rec.device,
                rec.stream,
                idx,
                rec.ts_ns,
                rec.actor,
                rec.kind,
                rec.payload.to_string(),
                rec.hash
            ],
        )?;
        Ok(changed == 1)
    }

    /// Insert keyed by an external spool id: the first call allocates the
    /// `(device, stream, idx)` triple and records the mapping; redelivery of
    /// the same spool id resolves to the same triple and is a no-op.
    /// Returns `(idx, inserted?)`. The mapping lookup, idx allocation, and
    /// record insert run in ONE immediate transaction.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Db`] on `SQLite` failures.
    pub fn insert_mapped(&mut self, spool_id: &str, rec: &NewRecord) -> Result<(i64, bool)> {
        let tx = self
            .conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        if let Ok(idx) = tx.query_row(
            "SELECT idx FROM spool_ids WHERE spool_id = ?1 AND device = ?2 AND stream = ?3",
            params![spool_id, rec.device, rec.stream],
            |r| r.get::<_, i64>(0),
        ) {
            tx.execute(
                "INSERT OR IGNORE INTO records (device, stream, idx, ts_ns, actor, kind, payload, hash)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![
                    rec.device,
                    rec.stream,
                    idx,
                    rec.ts_ns,
                    rec.actor,
                    rec.kind,
                    rec.payload.to_string(),
                    rec.hash
                ],
            )?;
            tx.commit()?;
            return Ok((idx, false));
        }
        let next: i64 = tx.query_row(
            "SELECT COALESCE(MAX(idx), -1) + 1 FROM records WHERE device = ?1 AND stream = ?2",
            params![rec.device, rec.stream],
            |r| r.get(0),
        )?;
        tx.execute(
            "INSERT INTO spool_ids (spool_id, device, stream, idx) VALUES (?1, ?2, ?3, ?4)",
            params![spool_id, rec.device, rec.stream, next],
        )?;
        tx.execute(
            "INSERT OR IGNORE INTO records (device, stream, idx, ts_ns, actor, kind, payload, hash)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                rec.device,
                rec.stream,
                next,
                rec.ts_ns,
                rec.actor,
                rec.kind,
                rec.payload.to_string(),
                rec.hash
            ],
        )?;
        tx.commit()?;
        Ok((next, true))
    }

    /// Fetch one record by identity.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] when absent, [`Error::Db`] on failures.
    pub fn get_record(&self, device: &str, stream: &str, idx: i64) -> Result<Record> {
        self.conn
            .query_row(
                "SELECT device, stream, idx, ts_ns, actor, kind, payload, hash
                 FROM records WHERE device = ?1 AND stream = ?2 AND idx = ?3",
                params![device, stream, idx],
                |r| {
                    Ok(Record {
                        device: r.get(0)?,
                        stream: r.get(1)?,
                        idx: r.get(2)?,
                        ts_ns: r.get(3)?,
                        actor: r.get(4)?,
                        kind: r.get(5)?,
                        payload: r.get(6)?,
                        hash: r.get(7)?,
                    })
                },
            )
            .map_err(|e| match e {
                rusqlite::Error::QueryReturnedNoRows => {
                    Error::NotFound(format!("{device}/{stream}/{idx}"))
                }
                other => Error::Db(other),
            })
    }

    /// Count records in a stream (tests, doctor, sync cursors).
    ///
    /// # Errors
    ///
    /// Returns [`Error::Db`] on `SQLite` failures.
    pub fn count_stream(&self, device: &str, stream: &str) -> Result<i64> {
        Ok(self.conn.query_row(
            "SELECT COUNT(*) FROM records WHERE device = ?1 AND stream = ?2",
            params![device, stream],
            |r| r.get(0),
        )?)
    }
}

/// blake3 hex of a canonical payload string (dedup hint + integrity).
#[must_use]
pub fn hash_payload(canonical: &str) -> String {
    blake3::hash(canonical.as_bytes()).to_hex().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn test_store() -> (tempfile::TempDir, Store) {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open(tmp.path()).unwrap();
        (tmp, store)
    }

    fn sample(device: &str) -> NewRecord {
        NewRecord {
            device: device.to_string(),
            stream: "shell".to_string(),
            ts_ns: 1_700_000_000_000_000_000,
            actor: "test:user".to_string(),
            kind: "command".to_string(),
            payload: json!({"cmd": "ls"}),
            hash: hash_payload(r#"{"cmd":"ls"}"#),
        }
    }

    #[test]
    fn append_allocates_monotonic_idx() {
        let (_tmp, mut store) = test_store();
        assert_eq!(store.insert_record(&sample("d1")).unwrap(), 0);
        assert_eq!(store.insert_record(&sample("d1")).unwrap(), 1);
        assert_eq!(store.insert_record(&sample("d2")).unwrap(), 0);
        let rec = store.get_record("d1", "shell", 1).unwrap();
        assert_eq!(rec.kind, "command");
    }

    #[test]
    fn pinned_insert_is_idempotent() {
        let (_tmp, store) = test_store();
        assert!(store.insert_pinned(&sample("d1"), 7).unwrap());
        assert!(!store.insert_pinned(&sample("d1"), 7).unwrap());
        assert_eq!(store.count_stream("d1", "shell").unwrap(), 1);
    }

    #[test]
    fn mapped_insert_redelivery_is_noop() {
        let (_tmp, mut store) = test_store();
        let (idx, fresh) = store.insert_mapped("spool-1", &sample("d1")).unwrap();
        assert_eq!((idx, fresh), (0, true));
        let (idx2, fresh2) = store.insert_mapped("spool-1", &sample("d1")).unwrap();
        assert_eq!((idx2, fresh2), (0, false));
        assert_eq!(store.count_stream("d1", "shell").unwrap(), 1);
    }

    #[test]
    fn get_missing_is_not_found() {
        let (_tmp, store) = test_store();
        assert!(matches!(
            store.get_record("d", "s", 0),
            Err(Error::NotFound(_))
        ));
    }

    #[test]
    fn crash_recovery_reopens_clean() {
        let tmp = tempfile::tempdir().unwrap();
        {
            let mut store = Store::open(tmp.path()).unwrap();
            for _ in 0..50 {
                store.insert_record(&sample("d1")).unwrap();
            }
            // Drop without checkpoint: next open must recover via WAL.
        }
        let mut store = Store::open(tmp.path()).unwrap();
        assert_eq!(store.count_stream("d1", "shell").unwrap(), 50);
        assert_eq!(store.insert_record(&sample("d1")).unwrap(), 50);
    }

    #[test]
    fn device_id_is_stable_and_private() {
        let tmp = tempfile::tempdir().unwrap();
        let a = device_id(tmp.path()).unwrap();
        let b = device_id(tmp.path()).unwrap();
        assert_eq!(a, b);
        assert_eq!(a.len(), 32);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(tmp.path().join("device_id"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(mode, 0o600);
        }
    }

    #[test]
    fn future_schema_refused() {
        let tmp = tempfile::tempdir().unwrap();
        {
            let conn = Connection::open(tmp.path().join("memory.db")).unwrap();
            conn.execute_batch(
                "CREATE TABLE schema_version (version INTEGER PRIMARY KEY);
                 INSERT INTO schema_version VALUES (999);",
            )
            .unwrap();
        }
        assert!(matches!(
            Store::open(tmp.path()),
            Err(Error::InvalidArguments(_))
        ));
    }
}
