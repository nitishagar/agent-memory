//! Embedded store: one `SQLite` database per user, zero external services.
//!
//! Identity is `(device, stream, idx)` (atuin-shaped); content hashes are
//! dedup hints only. WAL mode + `busy_timeout` + single-statement
//! transactions keep concurrent readers unblocked and writers serialized on
//! every OS — no `flock`, no hand-rolled locks.
//!
//! No tamper-evidence: `hash` is never verified on read, so anyone with
//! DB write access can rewrite payloads undetectably. The trust boundary
//! is the OS file permissions on the store dir (0700/0600 on unix).

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
/// Creation is atomic: the id file is opened `create_new` (0600 from
/// birth on unix), so concurrent first runs converge — losers re-read
/// the winner. Only a missing file triggers generation; any other read
/// error propagates instead of clobbering a good id.
///
/// # Errors
///
/// Returns [`Error::Io`] if the id file cannot be read or written.
pub fn device_id(dir: &Path) -> Result<String> {
    let path = dir.join("device_id");
    // Three attempts: lost race + stale crash remnant + fresh create.
    for _ in 0..3 {
        match std::fs::read_to_string(&path) {
            Ok(text) if !text.trim().is_empty() => return Ok(text.trim().to_string()),
            Ok(_) => {
                // Empty: either a live writer (wait) or a crash between
                // `create_new` and write (take over). A live writer lands
                // within milliseconds; a full second of emptiness is stale.
                if wait_for_id(&path) {
                    let text = std::fs::read_to_string(&path)?;
                    return Ok(text.trim().to_string());
                }
                let _ = std::fs::remove_file(&path);
                continue;
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(Error::Io(e)),
        }
        std::fs::create_dir_all(dir)?;
        let id = uuid::Uuid::new_v4().simple().to_string();
        match create_private(&path, format!("{id}\n").as_bytes()) {
            Ok(()) => return Ok(id),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(Error::Io(e)),
        }
    }
    // A lost race lands here with the winner's id on disk.
    wait_for_id(&path);
    let text = std::fs::read_to_string(&path)?;
    Ok(text.trim().to_string())
}

/// Wait up to a second for a concurrent writer to land the id.
/// True when the file is non-empty afterwards.
fn wait_for_id(path: &Path) -> bool {
    for _ in 0..100 {
        match std::fs::read_to_string(path) {
            Ok(text) if !text.trim().is_empty() => return true,
            Ok(_) => std::thread::sleep(std::time::Duration::from_millis(10)),
            Err(_) => return false,
        }
    }
    false
}

/// Create a fresh private file, failing if it exists (unix: 0600).
#[cfg(unix)]
fn create_private(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write as _;
    use std::os::unix::fs::OpenOptionsExt;
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    f.write_all(bytes)?;
    f.sync_all()?;
    Ok(())
}

/// Create a fresh file, failing if it exists (non-unix: default ACLs).
#[cfg(not(unix))]
fn create_private(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write as _;
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)?;
    f.write_all(bytes)?;
    f.sync_all()?;
    Ok(())
}

/// Best-effort owner-only permissions on the store tree (unix).
/// `SQLite` creates `memory.db` 0644 under a normal umask; history must
/// not be world-readable on shared machines.
#[cfg(unix)]
fn lock_down(dir: &Path) {
    use std::os::unix::fs::PermissionsExt;
    for (path, mode) in [
        (dir.to_path_buf(), 0o700),
        (dir.join("spool"), 0o700),
        (dir.join("memory.db"), 0o600),
        (dir.join("memory.db-wal"), 0o600),
        (dir.join("memory.db-shm"), 0o600),
        (dir.join("device_id"), 0o600),
    ] {
        if path.exists() {
            let _ = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode));
        }
    }
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
        #[cfg(unix)]
        lock_down(dir);
        let conn = Connection::open_with_flags(
            dir.join("memory.db"),
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
        )?;
        conn.busy_timeout(std::time::Duration::from_millis(BUSY_TIMEOUT_MS as u64))?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "FULL")?;
        #[cfg(unix)]
        lock_down(dir);
        let store = Self { conn };
        store.migrate()?;
        Ok(store)
    }

    /// Open an existing store strictly read-only: creates nothing, migrates
    /// nothing. For `doctor`-style probes on initialized stores.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Db`] (including "unable to open" when `memory.db`
    /// is absent — callers map that to "not initialized").
    pub fn open_readonly(dir: &Path) -> Result<Self> {
        let conn =
            Connection::open_with_flags(dir.join("memory.db"), OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        conn.busy_timeout(std::time::Duration::from_millis(BUSY_TIMEOUT_MS as u64))?;
        Ok(Self { conn })
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
            // `IF NOT EXISTS` throughout: `execute_batch` auto-commits per
            // statement, so a crash mid-migration reopens with tables
            // present but version 0 — the rerun must converge, not brick.
            self.conn.execute_batch(
                "CREATE TABLE IF NOT EXISTS records (
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
                CREATE INDEX IF NOT EXISTS idx_records_stream_ts ON records (stream, ts_ns);
                CREATE TABLE IF NOT EXISTS spool_ids (
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

    /// List every record for a device, oldest first. Export walks this.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Db`] on `SQLite` failure.
    pub fn list_all(&self, device: &str) -> Result<Vec<Record>> {
        let mut stmt = self.conn.prepare(
            "SELECT device, stream, idx, ts_ns, actor, kind, payload, hash
             FROM records WHERE device = ?1 ORDER BY ts_ns, stream, idx",
        )?;
        let rows = stmt.query_map(params![device], |r| {
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
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Error::Db)
    }

    /// Substring search over payloads newest-first. `stream` matches
    /// exactly unless it contains `%` (then it is a `LIKE` glob, `%`
    /// alone = all streams). `text` is always matched literally (`LIKE`
    /// metacharacters escaped), so callers cannot widen either filter by
    /// accident. The MCP server builds on this.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Db`] on `SQLite` failure.
    pub fn search_stream(
        &self,
        device: &str,
        stream_like: &str,
        text: &str,
        limit: i64,
    ) -> Result<Vec<Record>> {
        let escaped = text
            .replace('\\', "\\\\")
            .replace('%', "\\%")
            .replace('_', "\\_");
        let pattern = format!("%{escaped}%");
        // Exact stream unless the caller asked for a glob: a restrictive
        // MCP caller passing `transcript:claude_` must not over-match.
        let (sql, stream_param) = if stream_like.contains('%') {
            (
                "SELECT device, stream, idx, ts_ns, actor, kind, payload, hash
                 FROM records WHERE device = ?1 AND stream LIKE ?2
                 AND payload LIKE ?3 ESCAPE '\\' ORDER BY ts_ns DESC LIMIT ?4",
                stream_like.to_string(),
            )
        } else {
            (
                "SELECT device, stream, idx, ts_ns, actor, kind, payload, hash
                 FROM records WHERE device = ?1 AND stream = ?2
                 AND payload LIKE ?3 ESCAPE '\\' ORDER BY ts_ns DESC LIMIT ?4",
                stream_like.to_string(),
            )
        };
        let mut stmt = self.conn.prepare(sql)?;
        let rows = stmt.query_map(params![device, stream_param, pattern, limit], |r| {
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
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Error::Db)
    }

    /// Resolve an external id (spool uuid, transcript stable id, note id)
    /// to `(stream, idx)`. Exact match first; otherwise a unique PREFIX
    /// resolves (git-short-hash style). No match is [`Error::NotFound`],
    /// several prefix matches [`Error::Ambiguous`]. `stream` narrows both.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`], [`Error::Ambiguous`], or [`Error::Db`].
    pub fn resolve_external(
        &self,
        device: &str,
        ext_id: &str,
        stream: Option<&str>,
    ) -> Result<(String, i64)> {
        // An empty id must never prefix-match the whole mapping table.
        if ext_id.is_empty() {
            return Err(Error::NotFound(String::new()));
        }
        let exact_sql = match stream {
            Some(_) => {
                "SELECT stream, idx FROM spool_ids
                 WHERE spool_id = ?1 AND device = ?2 AND stream = ?3"
            }
            None => {
                "SELECT stream, idx FROM spool_ids
                 WHERE spool_id = ?1 AND device = ?2"
            }
        };
        let exact: std::result::Result<(String, i64), rusqlite::Error> = match stream {
            Some(s) => self
                .conn
                .query_row(exact_sql, params![ext_id, device, s], id_row),
            None => self
                .conn
                .query_row(exact_sql, params![ext_id, device], id_row),
        };
        match exact {
            Ok(hit) => return Ok(hit),
            Err(rusqlite::Error::QueryReturnedNoRows) => {}
            Err(e) => return Err(Error::Db(e)),
        }
        // Unique-prefix fallback. `LIKE` metacharacters in the input are
        // escaped so the prefix is always literal.
        let escaped = ext_id
            .replace('\\', "\\\\")
            .replace('%', "\\%")
            .replace('_', "\\_");
        let pattern = format!("{escaped}%");
        let prefix_sql = match stream {
            Some(_) => {
                "SELECT stream, idx FROM spool_ids
                 WHERE spool_id LIKE ?1 ESCAPE '\\' AND device = ?2 AND stream = ?3"
            }
            None => {
                "SELECT stream, idx FROM spool_ids
                 WHERE spool_id LIKE ?1 ESCAPE '\\' AND device = ?2"
            }
        };
        let mut stmt = self.conn.prepare(prefix_sql)?;
        let mut hits: Vec<(String, i64)> = Vec::new();
        match stream {
            Some(s) => {
                for row in stmt.query_map(params![pattern, device, s], id_row)? {
                    hits.push(row.map_err(Error::Db)?);
                }
            }
            None => {
                for row in stmt.query_map(params![pattern, device], id_row)? {
                    hits.push(row.map_err(Error::Db)?);
                }
            }
        }
        match hits.len() {
            0 => Err(Error::NotFound(ext_id.to_string())),
            1 => Ok(hits.swap_remove(0)),
            _ => Err(Error::Ambiguous(format!(
                "{ext_id}: matches {} ids, use a longer prefix",
                hits.len()
            ))),
        }
    }

    /// Reverse id lookup for export: the external id behind a triple, if
    /// the record was inserted through a mapping ([`Store::insert_mapped`]).
    ///
    /// # Errors
    ///
    /// Returns [`Error::Db`] on `SQLite` failure.
    pub fn external_id(&self, device: &str, stream: &str, idx: i64) -> Result<Option<String>> {
        match self.conn.query_row(
            "SELECT spool_id FROM spool_ids WHERE device = ?1 AND stream = ?2 AND idx = ?3",
            params![device, stream, idx],
            |r| r.get(0),
        ) {
            Ok(id) => Ok(Some(id)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(Error::Db(e)),
        }
    }

    /// Newest-first listing with offset pagination. `stream_like` follows
    /// the [`Store::search_stream`] exact-unless-`%` rule.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Db`] on `SQLite` failure.
    pub fn list_stream(
        &self,
        device: &str,
        stream_like: &str,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<Record>> {
        let sql = if stream_like.contains('%') {
            "SELECT device, stream, idx, ts_ns, actor, kind, payload, hash
             FROM records WHERE device = ?1 AND stream LIKE ?2
             ORDER BY ts_ns DESC, stream, idx LIMIT ?3 OFFSET ?4"
        } else {
            "SELECT device, stream, idx, ts_ns, actor, kind, payload, hash
             FROM records WHERE device = ?1 AND stream = ?2
             ORDER BY ts_ns DESC, stream, idx LIMIT ?3 OFFSET ?4"
        };
        let mut stmt = self.conn.prepare(sql)?;
        let rows = stmt.query_map(params![device, stream_like, limit, offset], |r| {
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
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Error::Db)
    }

    /// Override the `SQLite` busy wait on this connection. Interactive
    /// writers (MCP) use 0 to fail fast with [`Error::OutcomeUnknown`]
    /// instead of blocking behind a bulk ingest.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Db`] on `SQLite` failure.
    pub fn set_busy_timeout(&self, ms: u64) -> Result<()> {
        Ok(self
            .conn
            .busy_timeout(std::time::Duration::from_millis(ms))?)
    }

    /// Guarded note rewrite (stream `notes` only) in one `IMMEDIATE`
    /// transaction: the stored checksum must equal `expected_hash` or the
    /// write is refused — never last-write-wins. Returns the new checksum.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`], [`Error::Conflict`] on a stale guard,
    /// [`Error::OutcomeUnknown`] on lock contention, or [`Error::Db`].
    pub fn note_update_guarded(
        &mut self,
        device: &str,
        idx: i64,
        expected_hash: &str,
        title: &str,
        text: &str,
    ) -> Result<String> {
        let tx = self
            .conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(|e| map_busy(e, "note update"))?;
        let stored: Option<(String, String)> = tx
            .query_row(
                "SELECT hash, payload FROM records WHERE device = ?1 AND stream = 'notes' AND idx = ?2",
                params![device, idx],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .map(Some)
            .or_else(|e| match e {
                rusqlite::Error::QueryReturnedNoRows => Ok(None),
                other => Err(Error::Db(other)),
            })?;
        let Some((stored, payload)) = stored else {
            return Err(Error::NotFound(format!("notes/{idx}")));
        };
        if stored != expected_hash {
            return Err(Error::Conflict(format!(
                "notes/{idx}: checksum mismatch; re-read and resubmit"
            )));
        }
        let id = serde_json::from_str::<serde_json::Value>(&payload)
            .ok()
            .and_then(|v| {
                v.get("id")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string)
            })
            .unwrap_or_default();
        let (canonical, hash) = note_payload(&id, title, text);
        tx.execute(
            "UPDATE records SET payload = ?1, hash = ?2, version = version + 1
             WHERE device = ?3 AND stream = 'notes' AND idx = ?4",
            params![canonical, hash, device, idx],
        )?;
        tx.commit()?;
        Ok(hash)
    }

    /// Guarded note delete (stream `notes` only). The id mapping is kept
    /// so a later re-create reuses the same `idx`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`], [`Error::Conflict`] on a stale guard,
    /// [`Error::OutcomeUnknown`] on lock contention, or [`Error::Db`].
    pub fn note_delete_guarded(
        &mut self,
        device: &str,
        idx: i64,
        expected_hash: &str,
    ) -> Result<()> {
        let tx = self
            .conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(|e| map_busy(e, "note delete"))?;
        let stored: Option<String> = tx
            .query_row(
                "SELECT hash FROM records WHERE device = ?1 AND stream = 'notes' AND idx = ?2",
                params![device, idx],
                |r| r.get(0),
            )
            .map(Some)
            .or_else(|e| match e {
                rusqlite::Error::QueryReturnedNoRows => Ok(None),
                other => Err(Error::Db(other)),
            })?;
        let Some(stored) = stored else {
            return Err(Error::NotFound(format!("notes/{idx}")));
        };
        if stored != expected_hash {
            return Err(Error::Conflict(format!(
                "notes/{idx}: checksum mismatch; re-read and resubmit"
            )));
        }
        tx.execute(
            "DELETE FROM records WHERE device = ?1 AND stream = 'notes' AND idx = ?2",
            params![device, idx],
        )?;
        tx.commit()?;
        Ok(())
    }
}

/// Map a `(stream, idx)` id-mapping row.
fn id_row(r: &rusqlite::Row) -> rusqlite::Result<(String, i64)> {
    Ok((r.get(0)?, r.get(1)?))
}

/// `SQLITE_BUSY` on an interactive write means another writer (e.g. a bulk
/// ingest) holds the lock: the outcome is unknown, never silently retried.
fn map_busy(e: rusqlite::Error, what: &str) -> Error {
    if let rusqlite::Error::SqliteFailure(f, _) = &e {
        if f.code == rusqlite::ErrorCode::DatabaseBusy {
            return Error::OutcomeUnknown(format!(
                "{what}: store locked by another writer; inspect state before retrying"
            ));
        }
    }
    Error::Db(e)
}

/// Map a residual [`Error::Db`] carrying `SQLITE_BUSY` (fired through a
/// fail-fast write path) to [`Error::OutcomeUnknown`]; every other error
/// passes through untouched.
#[must_use]
pub fn busy_to_unknown(e: Error, what: &str) -> Error {
    if let Error::Db(rusqlite::Error::SqliteFailure(f, _)) = &e {
        if f.code == rusqlite::ErrorCode::DatabaseBusy {
            return Error::OutcomeUnknown(format!(
                "{what}: store locked by another writer; inspect state before retrying"
            ));
        }
    }
    e
}

/// Canonical note payload + its blake3 checksum. `id` is the external id
/// (preserved across guarded rewrites).
#[must_use]
pub fn note_payload(id: &str, title: &str, text: &str) -> (String, String) {
    let canonical = serde_json::json!({
        "id": id,
        "title": title,
        "text": text,
        "text_len": text.len(),
    })
    .to_string();
    let hash = hash_payload(&canonical);
    (canonical, hash)
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
    fn search_stream_exact_unless_glob() {
        let tmp = tempfile::tempdir().unwrap();
        let mut store = Store::open(tmp.path()).unwrap();
        let mut a = sample("d1");
        a.stream = "transcript:claude".to_string();
        a.payload = json!({"text": "needle here"});
        store.insert_record(&a).unwrap();
        let mut b = sample("d1");
        b.stream = "transcript:claudex".to_string();
        b.payload = json!({"text": "needle here"});
        store.insert_record(&b).unwrap();
        // Underscore must not act as a wildcard in exact mode.
        let hits = store
            .search_stream("d1", "transcript:claude_", "needle", 10)
            .unwrap();
        assert!(hits.is_empty());
        let hits = store
            .search_stream("d1", "transcript:claude", "needle", 10)
            .unwrap();
        assert_eq!(hits.len(), 1);
        let hits = store
            .search_stream("d1", "transcript:%", "needle", 10)
            .unwrap();
        assert_eq!(hits.len(), 2);
        // Text metacharacters are literal.
        let hits = store.search_stream("d1", "%", "need_e", 10).unwrap();
        assert!(hits.is_empty());
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

    #[test]
    fn torn_migration_converges_on_reopen() {
        // Crash after CREATE TABLE but before the version row: tables
        // present, version 0. Reopen must finish, not brick.
        let tmp = tempfile::tempdir().unwrap();
        {
            let conn = Connection::open(tmp.path().join("memory.db")).unwrap();
            conn.execute_batch(
                "CREATE TABLE schema_version (version INTEGER PRIMARY KEY);
                 CREATE TABLE records (device TEXT NOT NULL, stream TEXT NOT NULL,
                    idx INTEGER NOT NULL, ts_ns INTEGER NOT NULL,
                    actor TEXT NOT NULL DEFAULT '', kind TEXT NOT NULL DEFAULT 'command',
                    payload TEXT NOT NULL, hash TEXT NOT NULL DEFAULT '',
                    version INTEGER NOT NULL DEFAULT 1, extras TEXT NOT NULL DEFAULT '{}',
                    PRIMARY KEY (device, stream, idx));",
            )
            .unwrap();
        }
        let mut store = Store::open(tmp.path()).unwrap();
        assert_eq!(store.insert_record(&sample("d1")).unwrap(), 0);
    }

    #[test]
    fn empty_device_id_recovers() {
        // Crash between create_new and write leaves a 0-byte id file.
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("device_id"), "").unwrap();
        let id = device_id(tmp.path()).unwrap();
        assert_eq!(id.len(), 32);
        assert_eq!(device_id(tmp.path()).unwrap(), id);
    }

    #[test]
    fn readonly_open_creates_nothing() {
        let tmp = tempfile::tempdir().unwrap();
        assert!(Store::open_readonly(tmp.path()).is_err());
        assert!(!tmp.path().join("memory.db").exists());
        let mut store = Store::open(tmp.path()).unwrap();
        store.insert_record(&sample("d1")).unwrap();
        drop(store);
        let ro = Store::open_readonly(tmp.path()).unwrap();
        assert_eq!(ro.count_stream("d1", "shell").unwrap(), 1);
    }

    fn note(device: &str, id: &str, title: &str, text: &str) -> (String, NewRecord) {
        let (canonical, hash) = note_payload(id, title, text);
        let rec = NewRecord {
            device: device.to_string(),
            stream: "notes".to_string(),
            ts_ns: 1_700_000_000_000_000_000,
            actor: "test:user".to_string(),
            kind: "note".to_string(),
            payload: serde_json::from_str(&canonical).unwrap(),
            hash,
        };
        (canonical, rec)
    }

    #[test]
    fn resolve_external_exact_prefix_and_ambiguity() {
        let (_tmp, mut store) = test_store();
        let (_, rec) = note("d1", "note-alpha", "t", "x");
        store.insert_mapped("note-alpha", &rec).unwrap();
        let (_, rec2) = note("d1", "note-beta", "t", "y");
        store.insert_mapped("note-beta", &rec2).unwrap();
        let mut shell = sample("d1");
        shell.payload = json!({"cmd": "echo hi"});
        store.insert_mapped("shell-1", &shell).unwrap();
        // Exact hit, with and without narrowing.
        assert_eq!(
            store
                .resolve_external("d1", "note-alpha", Some("notes"))
                .unwrap(),
            ("notes".to_string(), 0)
        );
        assert_eq!(
            store.resolve_external("d1", "shell-1", None).unwrap(),
            ("shell".to_string(), 0)
        );
        // Unique prefix resolves; shared prefix is ambiguous.
        assert_eq!(
            store.resolve_external("d1", "note-a", None).unwrap(),
            ("notes".to_string(), 0)
        );
        assert!(matches!(
            store.resolve_external("d1", "note-", None),
            Err(Error::Ambiguous(_))
        ));
        // Stream narrowing disambiguates nothing here (same stream) but a
        // wrong stream misses.
        assert!(matches!(
            store.resolve_external("d1", "note-a", Some("shell")),
            Err(Error::NotFound(_))
        ));
        assert!(matches!(
            store.resolve_external("d1", "missing", None),
            Err(Error::NotFound(_))
        ));
        // Empty ids never resolve (no prefix match-all).
        assert!(matches!(
            store.resolve_external("d1", "", None),
            Err(Error::NotFound(_))
        ));
    }

    #[test]
    fn list_stream_paginates_newest_first() {
        let (_tmp, mut store) = test_store();
        for i in 0..5 {
            let mut rec = sample("d1");
            rec.ts_ns += i;
            rec.payload = json!({"cmd": format!("cmd-{i}")});
            store.insert_record(&rec).unwrap();
        }
        let page1 = store.list_stream("d1", "shell", 2, 0).unwrap();
        assert_eq!(page1.len(), 2);
        assert!(page1[0].payload.contains("cmd-4"));
        let page2 = store.list_stream("d1", "shell", 2, 2).unwrap();
        assert!(page2[0].payload.contains("cmd-2"));
        let glob = store.list_stream("d1", "%", 10, 0).unwrap();
        assert_eq!(glob.len(), 5);
    }

    #[test]
    fn guarded_update_rejects_stale_and_accepts_reread() {
        let (_tmp, mut store) = test_store();
        let (_, rec) = note("d1", "n1", "t", "v1");
        let (idx, inserted) = store.insert_mapped("n1", &rec).unwrap();
        assert!(inserted);
        let before = store.get_record("d1", "notes", idx).unwrap();
        // Stale guard refused.
        let err = store
            .note_update_guarded("d1", idx, "deadbeef", "t", "v2")
            .unwrap_err();
        assert!(matches!(err, Error::Conflict(_)), "{err:?}");
        assert_eq!(store.get_record("d1", "notes", idx).unwrap(), before);
        // Re-read + resubmit succeeds with a fresh checksum.
        let new_hash = store
            .note_update_guarded("d1", idx, &before.hash, "t", "v2")
            .unwrap();
        assert_ne!(new_hash, before.hash);
        let after = store.get_record("d1", "notes", idx).unwrap();
        assert_eq!(after.hash, new_hash);
        assert!(after.payload.contains("v2"));
        // Recorder streams are immutable through this path: a shell row at
        // a notes-free idx is untouched, and `notes` has no such row.
        let shell_before = sample("d1");
        let shell_idx = store.insert_record(&shell_before).unwrap();
        assert!(matches!(
            store.note_update_guarded("d1", shell_idx + 100, &before.hash, "t", "x"),
            Err(Error::NotFound(_))
        ));
        assert_eq!(
            store.get_record("d1", "shell", shell_idx).unwrap().payload,
            shell_before.payload.to_string()
        );
    }

    #[test]
    fn guarded_delete_keeps_mapping_for_recreate() {
        let (_tmp, mut store) = test_store();
        let (_, rec) = note("d1", "n1", "t", "v1");
        let (idx, _) = store.insert_mapped("n1", &rec).unwrap();
        let before = store.get_record("d1", "notes", idx).unwrap();
        assert!(matches!(
            store.note_delete_guarded("d1", idx, "stale"),
            Err(Error::Conflict(_))
        ));
        store.note_delete_guarded("d1", idx, &before.hash).unwrap();
        assert!(matches!(
            store.get_record("d1", "notes", idx),
            Err(Error::NotFound(_))
        ));
        // Mapping survives: re-create reuses the idx.
        let (_, rec2) = note("d1", "n1", "t", "v2");
        let (idx2, inserted) = store.insert_mapped("n1", &rec2).unwrap();
        assert_eq!((idx2, inserted), (idx, false));
        assert!(
            store
                .get_record("d1", "notes", idx)
                .unwrap()
                .payload
                .contains("v2")
        );
    }

    #[test]
    fn busy_writer_reports_outcome_unknown() {
        let (tmp, mut store) = test_store();
        let (_, rec) = note("d1", "n1", "t", "v1");
        let mut writer = Store::open(tmp.path()).unwrap();
        let (idx, _) = writer.insert_mapped("n1", &rec).unwrap();
        let before = writer.get_record("d1", "notes", idx).unwrap();
        drop(writer);
        // Second connection holds a write txn; fail-fast writer must not block.
        let mut holder = Store::open(tmp.path()).unwrap();
        let held = holder
            .conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .unwrap();
        store.set_busy_timeout(0).unwrap();
        let err = store
            .note_update_guarded("d1", idx, &before.hash, "t", "v2")
            .unwrap_err();
        assert!(matches!(err, Error::OutcomeUnknown(_)), "{err:?}");
        drop(held);
        drop(holder);
        let new_hash = store
            .note_update_guarded("d1", idx, &before.hash, "t", "v2")
            .unwrap();
        assert_ne!(new_hash, before.hash);
    }

    #[cfg(unix)]
    #[test]
    fn store_tree_is_owner_only() {
        use std::os::unix::fs::PermissionsExt;
        let tmp = tempfile::tempdir().unwrap();
        let mut store = Store::open(tmp.path()).unwrap();
        store.insert_record(&sample("d1")).unwrap();
        drop(store);
        device_id(tmp.path()).unwrap();
        for (rel, want) in [("", 0o700), ("memory.db", 0o600), ("device_id", 0o600)] {
            let mode = std::fs::metadata(tmp.path().join(rel))
                .unwrap()
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(mode, want, "{rel} mode {mode:o}");
        }
    }
}
