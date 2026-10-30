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
pub const SCHEMA_VERSION: i64 = 2;

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

/// One conflict branch: the winner triple stays canonical in `records`,
/// the loser envelope is preserved verbatim (no-loss).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Branch {
    /// Branch row id.
    pub id: i64,
    /// External id both sides claim (empty for pure PK conflicts).
    pub ext_id: String,
    /// Stream of the conflict.
    pub stream: String,
    /// Canonical (first-seen) side.
    pub winner_device: String,
    /// Canonical (first-seen) side.
    pub winner_idx: i64,
    /// Canonical (first-seen) side.
    pub winner_hash: String,
    /// Full loser envelope JSON.
    pub loser_envelope: String,
    /// `pk` (same triple, different hash) or `id` (same external id).
    pub reason: String,
    /// Detection time, Unix ns.
    pub ts_ns: i64,
}

/// One quarantined envelope: held out of recall until restored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Quarantined {
    /// Envelope identity triple.
    pub device: String,
    /// Envelope identity triple.
    pub stream: String,
    /// Envelope identity triple.
    pub idx: i64,
    /// Full envelope JSON.
    pub envelope: String,
    /// Why it was quarantined (hash mismatch, skew, manual, ...).
    pub reason: String,
    /// Quarantine time, Unix ns.
    pub ts_ns: i64,
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
        if current < 2 {
            // Sync substrate: quarantine (moved OUT of `records`, so recall
            // cannot leak it), conflict branches (loser envelopes preserved),
            // and per-endpoint sync cursors. Same crash-convergence rule.
            self.conn.execute_batch(
                "CREATE TABLE IF NOT EXISTS quarantine (
                    device TEXT NOT NULL,
                    stream TEXT NOT NULL,
                    idx INTEGER NOT NULL,
                    envelope TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    ts_ns INTEGER NOT NULL,
                    PRIMARY KEY (device, stream, idx)
                );
                CREATE TABLE IF NOT EXISTS branches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ext_id TEXT NOT NULL,
                    stream TEXT NOT NULL,
                    winner_device TEXT NOT NULL,
                    winner_idx INTEGER NOT NULL,
                    winner_hash TEXT NOT NULL,
                    loser_envelope TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    ts_ns INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS sync_state (
                    endpoint TEXT PRIMARY KEY,
                    generation TEXT NOT NULL,
                    updated_ns INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS sync_marks (
                    endpoint TEXT NOT NULL,
                    device TEXT NOT NULL,
                    stream TEXT NOT NULL,
                    up_idx INTEGER NOT NULL DEFAULT -1,
                    down_idx INTEGER NOT NULL DEFAULT -1,
                    PRIMARY KEY (endpoint, device, stream)
                );
                INSERT INTO schema_version (version) VALUES (2);",
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

    /// Substring search over payloads newest-first. `device` and
    /// `stream` match exactly unless they contain `%` (then a `LIKE`
    /// glob, `%` alone = all). Recall is union-wide by default: synced
    /// teammates' records are searchable, quarantine stays hidden (it
    /// lives in a separate table). `text` is always matched literally
    /// (`LIKE` metacharacters escaped), so callers cannot widen any
    /// filter by accident. The MCP server builds on this.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Db`] on `SQLite` failure.
    pub fn search_stream(
        &self,
        device_like: &str,
        stream_like: &str,
        text: &str,
        limit: i64,
    ) -> Result<Vec<Record>> {
        let escaped = text
            .replace('\\', "\\\\")
            .replace('%', "\\%")
            .replace('_', "\\_");
        let pattern = format!("%{escaped}%");
        // Exact match unless the caller asked for a glob: a restrictive
        // MCP caller passing `transcript:claude_` must not over-match.
        let device_op = if device_like.contains('%') {
            "LIKE"
        } else {
            "="
        };
        let stream_op = if stream_like.contains('%') {
            "LIKE"
        } else {
            "="
        };
        let sql = format!(
            "SELECT device, stream, idx, ts_ns, actor, kind, payload, hash
             FROM records WHERE device {device_op} ?1 AND stream {stream_op} ?2
             AND payload LIKE ?3 ESCAPE '\\' ORDER BY ts_ns DESC LIMIT ?4"
        );
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt.query_map(params![device_like, stream_like, pattern, limit], |r| {
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

    /// Exact external-id matches across ALL devices: `(device, stream,
    /// idx, hash)` rows. Sync conflict scans use this; interactive resolve
    /// stays device-scoped ([`Store::resolve_external`]).
    ///
    /// # Errors
    ///
    /// Returns [`Error::Db`] on `SQLite` failure.
    pub fn resolve_external_any(
        &self,
        ext_id: &str,
        stream: Option<&str>,
    ) -> Result<Vec<(String, String, i64, String)>> {
        if ext_id.is_empty() {
            return Ok(Vec::new());
        }
        let sql = match stream {
            Some(_) => {
                "SELECT m.device, m.stream, m.idx, r.hash FROM spool_ids m
                 JOIN records r ON r.device = m.device AND r.stream = m.stream AND r.idx = m.idx
                 WHERE m.spool_id = ?1 AND m.stream = ?2"
            }
            None => {
                "SELECT m.device, m.stream, m.idx, r.hash FROM spool_ids m
                 JOIN records r ON r.device = m.device AND r.stream = m.stream AND r.idx = m.idx
                 WHERE m.spool_id = ?1"
            }
        };
        let mut stmt = self.conn.prepare(sql)?;
        let mut out = Vec::new();
        match stream {
            Some(s) => {
                for row in stmt.query_map(params![ext_id, s], |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, i64>(2)?,
                        r.get::<_, String>(3)?,
                    ))
                })? {
                    out.push(row.map_err(Error::Db)?);
                }
            }
            None => {
                for row in stmt.query_map(params![ext_id], |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, i64>(2)?,
                        r.get::<_, String>(3)?,
                    ))
                })? {
                    out.push(row.map_err(Error::Db)?);
                }
            }
        }
        Ok(out)
    }

    /// Highest `idx` in a series, or -1 when the series is empty.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Db`] on `SQLite` failure.
    pub fn max_idx(&self, device: &str, stream: &str) -> Result<i64> {
        Ok(self.conn.query_row(
            "SELECT COALESCE(MAX(idx), -1) FROM records WHERE device = ?1 AND stream = ?2",
            params![device, stream],
            |r| r.get(0),
        )?)
    }

    /// Records strictly above `after_idx`, ascending (upload/download paging).
    ///
    /// # Errors
    ///
    /// Returns [`Error::Db`] on `SQLite` failure.
    pub fn records_since(
        &self,
        device: &str,
        stream: &str,
        after_idx: i64,
        limit: i64,
    ) -> Result<Vec<Record>> {
        let mut stmt = self.conn.prepare(
            "SELECT device, stream, idx, ts_ns, actor, kind, payload, hash
             FROM records WHERE device = ?1 AND stream = ?2 AND idx > ?3
             ORDER BY idx ASC LIMIT ?4",
        )?;
        let rows = stmt.query_map(params![device, stream, after_idx, limit], |r| {
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

    /// Apply one downloaded record at its pinned `(device, stream, idx)`
    /// plus its external-id mapping. Re-apply is a no-op (idempotent).
    ///
    /// # Errors
    ///
    /// Returns [`Error::Db`] on `SQLite` failure.
    pub fn apply_remote(&mut self, rec: &NewRecord, idx: i64, ext_id: &str) -> Result<bool> {
        let tx = self
            .conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let changed = tx.execute(
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
        tx.execute(
            "INSERT OR IGNORE INTO spool_ids (spool_id, device, stream, idx) VALUES (?1, ?2, ?3, ?4)",
            params![ext_id, rec.device, rec.stream, idx],
        )?;
        tx.commit()?;
        Ok(changed == 1)
    }

    /// Move a record out of recall into quarantine, dropping its id mapping.
    /// The caller supplies the envelope (built before the move).
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] when the triple is absent, [`Error::Db`]
    /// on `SQLite` failure.
    pub fn quarantine_move(
        &mut self,
        device: &str,
        stream: &str,
        idx: i64,
        envelope: &str,
        reason: &str,
        ts_ns: i64,
    ) -> Result<()> {
        let tx = self
            .conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let gone = tx.execute(
            "DELETE FROM records WHERE device = ?1 AND stream = ?2 AND idx = ?3",
            params![device, stream, idx],
        )?;
        if gone == 0 {
            return Err(Error::NotFound(format!("{device}/{stream}/{idx}")));
        }
        tx.execute(
            "DELETE FROM spool_ids WHERE device = ?1 AND stream = ?2 AND idx = ?3",
            params![device, stream, idx],
        )?;
        tx.execute(
            "INSERT OR REPLACE INTO quarantine (device, stream, idx, envelope, reason, ts_ns)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![device, stream, idx, envelope, reason, ts_ns],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// File a downloaded envelope directly into quarantine (failed validation).
    ///
    /// # Errors
    ///
    /// Returns [`Error::Db`] on `SQLite` failure.
    pub fn quarantine_put(
        &self,
        device: &str,
        stream: &str,
        idx: i64,
        envelope: &str,
        reason: &str,
        ts_ns: i64,
    ) -> Result<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO quarantine (device, stream, idx, envelope, reason, ts_ns)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![device, stream, idx, envelope, reason, ts_ns],
        )?;
        Ok(())
    }

    /// Fetch one quarantined envelope.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] when absent, [`Error::Db`] on failure.
    pub fn quarantine_get(&self, device: &str, stream: &str, idx: i64) -> Result<Quarantined> {
        self.conn
            .query_row(
                "SELECT device, stream, idx, envelope, reason, ts_ns FROM quarantine
                 WHERE device = ?1 AND stream = ?2 AND idx = ?3",
                params![device, stream, idx],
                |r| {
                    Ok(Quarantined {
                        device: r.get(0)?,
                        stream: r.get(1)?,
                        idx: r.get(2)?,
                        envelope: r.get(3)?,
                        reason: r.get(4)?,
                        ts_ns: r.get(5)?,
                    })
                },
            )
            .map_err(|e| match e {
                rusqlite::Error::QueryReturnedNoRows => {
                    Error::NotFound(format!("quarantine {device}/{stream}/{idx}"))
                }
                other => Error::Db(other),
            })
    }

    /// Drop one quarantine row (after branching its envelope elsewhere).
    ///
    /// # Errors
    ///
    /// Returns [`Error::Db`] on `SQLite` failure.
    pub fn quarantine_delete(&self, device: &str, stream: &str, idx: i64) -> Result<()> {
        self.conn.execute(
            "DELETE FROM quarantine WHERE device = ?1 AND stream = ?2 AND idx = ?3",
            params![device, stream, idx],
        )?;
        Ok(())
    }

    /// List every quarantined envelope, oldest first.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Db`] on `SQLite` failure.
    pub fn quarantine_list(&self) -> Result<Vec<Quarantined>> {
        let mut stmt = self.conn.prepare(
            "SELECT device, stream, idx, envelope, reason, ts_ns FROM quarantine ORDER BY ts_ns, device, stream, idx",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok(Quarantined {
                device: r.get(0)?,
                stream: r.get(1)?,
                idx: r.get(2)?,
                envelope: r.get(3)?,
                reason: r.get(4)?,
                ts_ns: r.get(5)?,
            })
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Error::Db)
    }

    /// Restore a quarantined triple into `records` with its mapping, then
    /// drop the quarantine row. Restoring over an identical row is a no-op.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] when the quarantine row is absent,
    /// [`Error::Conflict`] when a DIFFERENT row occupies the triple (the
    /// caller must branch, never overwrite), [`Error::Db`] on failure.
    pub fn quarantine_restore(
        &mut self,
        device: &str,
        stream: &str,
        idx: i64,
        rec: &NewRecord,
        ext_id: &str,
    ) -> Result<()> {
        let tx = self
            .conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let have: Option<String> = tx
            .query_row(
                "SELECT hash FROM records WHERE device = ?1 AND stream = ?2 AND idx = ?3",
                params![device, stream, idx],
                |r| r.get(0),
            )
            .map(Some)
            .or_else(|e| match e {
                rusqlite::Error::QueryReturnedNoRows => Ok(None),
                other => Err(Error::Db(other)),
            })?;
        if let Some(have) = have {
            if have != rec.hash {
                return Err(Error::Conflict(format!(
                    "{device}/{stream}/{idx}: occupied by a different record; branch it"
                )));
            }
        } else {
            tx.execute(
                "INSERT INTO records (device, stream, idx, ts_ns, actor, kind, payload, hash)
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
            tx.execute(
                "INSERT OR IGNORE INTO spool_ids (spool_id, device, stream, idx) VALUES (?1, ?2, ?3, ?4)",
                params![ext_id, rec.device, rec.stream, idx],
            )?;
        }
        let gone = tx.execute(
            "DELETE FROM quarantine WHERE device = ?1 AND stream = ?2 AND idx = ?3",
            params![device, stream, idx],
        )?;
        if gone == 0 {
            return Err(Error::NotFound(format!(
                "quarantine {device}/{stream}/{idx}"
            )));
        }
        tx.commit()?;
        Ok(())
    }

    /// Record a conflict branch: the winner stays canonical, the loser
    /// envelope is preserved verbatim. Returns the branch row id.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Db`] on `SQLite` failure.
    #[allow(clippy::too_many_arguments)]
    pub fn branch_put(
        &self,
        ext_id: &str,
        stream: &str,
        winner_device: &str,
        winner_idx: i64,
        winner_hash: &str,
        loser_envelope: &str,
        reason: &str,
        ts_ns: i64,
    ) -> Result<i64> {
        self.conn.execute(
            "INSERT INTO branches (ext_id, stream, winner_device, winner_idx, winner_hash, loser_envelope, reason, ts_ns)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                ext_id,
                stream,
                winner_device,
                winner_idx,
                winner_hash,
                loser_envelope,
                reason,
                ts_ns
            ],
        )?;
        Ok(self.conn.last_insert_rowid())
    }

    /// List every conflict branch, oldest first.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Db`] on `SQLite` failure.
    pub fn branch_list(&self) -> Result<Vec<Branch>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, ext_id, stream, winner_device, winner_idx, winner_hash, loser_envelope, reason, ts_ns
             FROM branches ORDER BY id",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok(Branch {
                id: r.get(0)?,
                ext_id: r.get(1)?,
                stream: r.get(2)?,
                winner_device: r.get(3)?,
                winner_idx: r.get(4)?,
                winner_hash: r.get(5)?,
                loser_envelope: r.get(6)?,
                reason: r.get(7)?,
                ts_ns: r.get(8)?,
            })
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Error::Db)
    }

    /// Pinned sync generation for an endpoint, if this client ever synced it.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Db`] on `SQLite` failure.
    pub fn sync_state_get(&self, endpoint: &str) -> Result<Option<(String, i64)>> {
        match self.conn.query_row(
            "SELECT generation, updated_ns FROM sync_state WHERE endpoint = ?1",
            params![endpoint],
            |r| Ok((r.get(0)?, r.get(1)?)),
        ) {
            Ok(pair) => Ok(Some(pair)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(Error::Db(e)),
        }
    }

    /// Pin the sync generation for an endpoint.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Db`] on `SQLite` failure.
    pub fn sync_state_put(&self, endpoint: &str, generation: &str, ts_ns: i64) -> Result<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO sync_state (endpoint, generation, updated_ns) VALUES (?1, ?2, ?3)",
            params![endpoint, generation, ts_ns],
        )?;
        Ok(())
    }

    /// Upload/download cursors for one series, defaulting to (-1, -1).
    ///
    /// # Errors
    ///
    /// Returns [`Error::Db`] on `SQLite` failure.
    pub fn sync_marks_get(&self, endpoint: &str, device: &str, stream: &str) -> Result<(i64, i64)> {
        match self.conn.query_row(
            "SELECT up_idx, down_idx FROM sync_marks WHERE endpoint = ?1 AND device = ?2 AND stream = ?3",
            params![endpoint, device, stream],
            |r| Ok((r.get(0)?, r.get(1)?)),
        ) {
            Ok(pair) => Ok(pair),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok((-1, -1)),
            Err(e) => Err(Error::Db(e)),
        }
    }

    /// Advance the upload cursor for one series (monotonic: regressions ignored).
    ///
    /// # Errors
    ///
    /// Returns [`Error::Db`] on `SQLite` failure.
    pub fn sync_marks_set_up(
        &self,
        endpoint: &str,
        device: &str,
        stream: &str,
        up_idx: i64,
    ) -> Result<()> {
        self.conn.execute(
            "INSERT INTO sync_marks (endpoint, device, stream, up_idx, down_idx) VALUES (?1, ?2, ?3, ?4, -1)
             ON CONFLICT (endpoint, device, stream) DO UPDATE SET up_idx = MAX(up_idx, ?4)",
            params![endpoint, device, stream, up_idx],
        )?;
        Ok(())
    }

    /// Advance the download cursor for one series (monotonic: regressions ignored).
    ///
    /// # Errors
    ///
    /// Returns [`Error::Db`] on `SQLite` failure.
    pub fn sync_marks_set_down(
        &self,
        endpoint: &str,
        device: &str,
        stream: &str,
        down_idx: i64,
    ) -> Result<()> {
        self.conn.execute(
            "INSERT INTO sync_marks (endpoint, device, stream, up_idx, down_idx) VALUES (?1, ?2, ?3, -1, ?4)
             ON CONFLICT (endpoint, device, stream) DO UPDATE SET down_idx = MAX(down_idx, ?4)",
            params![endpoint, device, stream, down_idx],
        )?;
        Ok(())
    }

    /// Reset all cursors for an endpoint (generation recovery re-syncs all).
    ///
    /// # Errors
    ///
    /// Returns [`Error::Db`] on `SQLite` failure.
    pub fn sync_marks_reset(&self, endpoint: &str) -> Result<()> {
        self.conn.execute(
            "DELETE FROM sync_marks WHERE endpoint = ?1",
            params![endpoint],
        )?;
        Ok(())
    }

    /// Every cursor row for an endpoint: `(device, stream, up_idx, down_idx)`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Db`] on `SQLite` failure.
    pub fn sync_marks_list(&self, endpoint: &str) -> Result<Vec<(String, String, i64, i64)>> {
        let mut stmt = self.conn.prepare(
            "SELECT device, stream, up_idx, down_idx FROM sync_marks WHERE endpoint = ?1",
        )?;
        let rows = stmt.query_map(params![endpoint], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, i64>(2)?,
                r.get::<_, i64>(3)?,
            ))
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Error::Db)
    }

    /// Distinct streams holding a device's records (upload planning).
    ///
    /// # Errors
    ///
    /// Returns [`Error::Db`] on `SQLite` failure.
    pub fn local_streams(&self, device: &str) -> Result<Vec<String>> {
        let mut stmt = self
            .conn
            .prepare("SELECT DISTINCT stream FROM records WHERE device = ?1 ORDER BY stream")?;
        let rows = stmt.query_map(params![device], |r| r.get::<_, String>(0))?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Error::Db)
    }

    /// Total record count across all devices (divergence accounting).
    ///
    /// # Errors
    ///
    /// Returns [`Error::Db`] on `SQLite` failure.
    pub fn count_all(&self) -> Result<i64> {
        Ok(self
            .conn
            .query_row("SELECT COUNT(*) FROM records", [], |r| r.get(0))?)
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
    /// transaction. The stored checksum must equal `expected_hash` or the
    /// write is refused — never last-write-wins. The new version is
    /// APPENDED at a fresh `idx` (old versions stay as history) and the id
    /// mapping repoints: sync's immutable union needs append-only series.
    /// Returns `(new_idx, new_checksum)`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`], [`Error::Conflict`] on a stale guard
    /// (checksum mismatch or the mapping already moved on),
    /// [`Error::OutcomeUnknown`] on lock contention, or [`Error::Db`].
    pub fn note_update_guarded(
        &mut self,
        device: &str,
        idx: i64,
        expected_hash: &str,
        title: &str,
        text: &str,
    ) -> Result<(i64, String)> {
        let tx = self
            .conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(|e| map_busy(e, "note update"))?;
        let stored: Option<(String, String, i64, String)> = tx
            .query_row(
                "SELECT hash, payload, version, actor FROM records
                 WHERE device = ?1 AND stream = 'notes' AND idx = ?2",
                params![device, idx],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .map(Some)
            .or_else(|e| match e {
                rusqlite::Error::QueryReturnedNoRows => Ok(None),
                other => Err(Error::Db(other)),
            })?;
        let Some((stored, payload, version, actor)) = stored else {
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
        // The mapping must still point here: a concurrent append wins.
        let mapped: Option<i64> = tx
            .query_row(
                "SELECT idx FROM spool_ids WHERE spool_id = ?1 AND device = ?2 AND stream = 'notes'",
                params![id, device],
                |r| r.get(0),
            )
            .map(Some)
            .or_else(|e| match e {
                rusqlite::Error::QueryReturnedNoRows => Ok(None),
                other => Err(Error::Db(other)),
            })?;
        if mapped != Some(idx) {
            return Err(Error::Conflict(format!(
                "notes/{idx}: a newer version exists; re-read and resubmit"
            )));
        }
        let next: i64 = tx.query_row(
            "SELECT COALESCE(MAX(idx), -1) + 1 FROM records WHERE device = ?1 AND stream = 'notes'",
            params![device],
            |r| r.get(0),
        )?;
        let (canonical, hash) = note_payload(&id, title, text);
        tx.execute(
            "INSERT INTO records (device, stream, idx, ts_ns, actor, kind, payload, hash, version)
             VALUES (?1, 'notes', ?2, ?3, ?4, 'note', ?5, ?6, ?7)",
            params![
                device,
                next,
                crate::clock::now_nanos(),
                actor,
                canonical,
                hash,
                version + 1
            ],
        )?;
        tx.execute(
            "UPDATE spool_ids SET idx = ?1 WHERE spool_id = ?2 AND device = ?3 AND stream = 'notes'",
            params![next, id, device],
        )?;
        tx.commit()?;
        Ok((next, hash))
    }

    /// Guarded note delete (stream `notes` only). The addressed version
    /// row and the id mapping are removed; older versions stay as history
    /// (unresolvable by id). A later re-create allocates a fresh `idx`.
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
        tx.execute(
            "DELETE FROM spool_ids WHERE device = ?1 AND stream = 'notes' AND idx = ?2",
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
    hash_bytes_hex(canonical.as_bytes())
}

/// blake3 hex of arbitrary bytes (fingerprints, non-UTF8 checksums).
#[must_use]
pub fn hash_bytes_hex(bytes: &[u8]) -> String {
    blake3::hash(bytes).to_hex().to_string()
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
    fn search_device_percent_covers_union() {
        let tmp = tempfile::tempdir().unwrap();
        let mut store = Store::open(tmp.path()).unwrap();
        let mut a = sample("d1");
        a.payload = json!({"cmd": "teammate-cmd"});
        store.insert_record(&a).unwrap();
        let mut b = sample("d2");
        b.payload = json!({"cmd": "teammate-cmd"});
        store.insert_record(&b).unwrap();
        // Union-wide recall: both devices' records surface.
        let hits = store.search_stream("%", "%", "teammate-cmd", 10).unwrap();
        assert_eq!(hits.len(), 2);
        // Exact device still scopes.
        let hits = store.search_stream("d1", "%", "teammate-cmd", 10).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].device, "d1");
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
    fn migration_v1_upgrades_to_v2() {
        let tmp = tempfile::tempdir().unwrap();
        {
            let conn = Connection::open(tmp.path().join("memory.db")).unwrap();
            conn.execute_batch(
                "CREATE TABLE schema_version (version INTEGER PRIMARY KEY);
                 INSERT INTO schema_version VALUES (1);
                 CREATE TABLE records (device TEXT NOT NULL, stream TEXT NOT NULL,
                    idx INTEGER NOT NULL, ts_ns INTEGER NOT NULL,
                    actor TEXT NOT NULL DEFAULT '', kind TEXT NOT NULL DEFAULT 'command',
                    payload TEXT NOT NULL, hash TEXT NOT NULL DEFAULT '',
                    version INTEGER NOT NULL DEFAULT 1, extras TEXT NOT NULL DEFAULT '{}',
                    PRIMARY KEY (device, stream, idx));
                 CREATE TABLE spool_ids (spool_id TEXT PRIMARY KEY, device TEXT NOT NULL,
                    stream TEXT NOT NULL, idx INTEGER NOT NULL);",
            )
            .unwrap();
        }
        let store = Store::open(tmp.path()).unwrap();
        for table in ["quarantine", "branches", "sync_state", "sync_marks"] {
            let n: i64 = store
                .conn
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_master WHERE name = ?1",
                    rusqlite::params![table],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(n, 1, "{table} missing after upgrade");
        }
        let v: i64 = store
            .conn
            .query_row("SELECT MAX(version) FROM schema_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(v, 2);
    }

    #[test]
    fn quarantine_roundtrip_hides_from_recall() {
        let (_tmp, mut store) = test_store();
        let mut rec = sample("d1");
        rec.payload = json!({"cmd": "quarantine-me"});
        let idx = store.insert_record(&rec).unwrap();
        assert_eq!(
            store
                .search_stream("d1", "shell", "quarantine-me", 10)
                .unwrap()
                .len(),
            1
        );
        store
            .quarantine_move("d1", "shell", idx, "{\"id\":\"e\"}", "manual", 7)
            .unwrap();
        assert!(
            store
                .search_stream("d1", "shell", "quarantine-me", 10)
                .unwrap()
                .is_empty()
        );
        assert!(matches!(
            store.get_record("d1", "shell", idx),
            Err(Error::NotFound(_))
        ));
        let q = store.quarantine_list().unwrap();
        assert_eq!(q.len(), 1);
        assert_eq!(q[0].reason, "manual");
        let back = NewRecord {
            device: "d1".to_string(),
            stream: "shell".to_string(),
            ts_ns: rec.ts_ns,
            actor: rec.actor.clone(),
            kind: rec.kind.clone(),
            payload: rec.payload.clone(),
            hash: rec.hash.clone(),
        };
        store
            .quarantine_restore("d1", "shell", idx, &back, "e")
            .unwrap();
        assert_eq!(
            store
                .search_stream("d1", "shell", "quarantine-me", 10)
                .unwrap()
                .len(),
            1
        );
        assert!(store.quarantine_list().unwrap().is_empty());
    }

    #[test]
    fn quarantine_restore_conflicts_never_overwrite() {
        let (_tmp, mut store) = test_store();
        let idx = store.insert_record(&sample("d1")).unwrap();
        store
            .quarantine_move("d1", "shell", idx, "{\"id\":\"e\"}", "manual", 7)
            .unwrap();
        // A different record lands on the triple meanwhile.
        let mut other = sample("d1");
        other.payload = json!({"cmd": "other"});
        other.hash = hash_payload(r#"{"cmd":"other"}"#);
        store.insert_pinned(&other, idx).unwrap();
        let err = store
            .quarantine_restore("d1", "shell", idx, &sample("d1"), "e")
            .unwrap_err();
        assert!(matches!(err, Error::Conflict(_)), "{err:?}");
        // Occupant untouched, quarantine row kept.
        assert_eq!(
            store.get_record("d1", "shell", idx).unwrap().hash,
            other.hash
        );
        assert_eq!(store.quarantine_list().unwrap().len(), 1);
    }

    #[test]
    fn branches_and_sync_state_roundtrip() {
        let (_tmp, store) = test_store();
        let id = store
            .branch_put("x", "notes", "d1", 0, "h1", "{\"loser\":true}", "id", 9)
            .unwrap();
        assert_eq!(id, 1);
        let bs = store.branch_list().unwrap();
        assert_eq!(bs.len(), 1);
        assert_eq!(bs[0].loser_envelope, "{\"loser\":true}");
        assert_eq!(store.sync_state_get("https://s").unwrap(), None);
        store.sync_state_put("https://s", "gen-1", 10).unwrap();
        assert_eq!(
            store.sync_state_get("https://s").unwrap(),
            Some(("gen-1".to_string(), 10))
        );
        assert_eq!(
            store.sync_marks_get("https://s", "d1", "shell").unwrap(),
            (-1, -1)
        );
        store
            .sync_marks_set_up("https://s", "d1", "shell", 5)
            .unwrap();
        store
            .sync_marks_set_up("https://s", "d1", "shell", 3)
            .unwrap();
        store
            .sync_marks_set_down("https://s", "d1", "shell", 4)
            .unwrap();
        assert_eq!(
            store.sync_marks_get("https://s", "d1", "shell").unwrap(),
            (5, 4)
        );
        store.sync_marks_reset("https://s").unwrap();
        assert_eq!(
            store.sync_marks_get("https://s", "d1", "shell").unwrap(),
            (-1, -1)
        );
    }

    #[test]
    fn apply_remote_idempotent_and_paged() {
        let (_tmp, mut store) = test_store();
        for i in 0..3 {
            let mut rec = sample("d2");
            rec.payload = json!({"cmd": format!("r{i}")});
            assert!(store.apply_remote(&rec, i, &format!("e{i}")).unwrap());
        }
        let mut dup = sample("d2");
        dup.payload = json!({"cmd": "r0"});
        assert!(!store.apply_remote(&dup, 0, "e0").unwrap());
        assert_eq!(store.max_idx("d2", "shell").unwrap(), 2);
        assert_eq!(store.max_idx("d2", "missing").unwrap(), -1);
        let page = store.records_since("d2", "shell", 0, 10).unwrap();
        assert_eq!(page.len(), 2);
        assert_eq!(page[0].idx, 1);
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
        // Re-read + resubmit appends a new version; the old row stays.
        let (new_idx, new_hash) = store
            .note_update_guarded("d1", idx, &before.hash, "t", "v2")
            .unwrap();
        assert_ne!(new_idx, idx);
        assert_ne!(new_hash, before.hash);
        let after = store.get_record("d1", "notes", new_idx).unwrap();
        assert_eq!(after.hash, new_hash);
        assert!(after.payload.contains("v2"));
        assert_eq!(store.get_record("d1", "notes", idx).unwrap(), before);
        assert_eq!(
            store.resolve_external("d1", "n1", Some("notes")).unwrap(),
            ("notes".to_string(), new_idx)
        );
        // The old idx is now stale: mapping moved on.
        assert!(matches!(
            store.note_update_guarded("d1", idx, &before.hash, "t", "v3"),
            Err(Error::Conflict(_))
        ));
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
    fn guarded_delete_drops_mapping_recreate_allocates_fresh() {
        let (_tmp, mut store) = test_store();
        let (_, rec) = note("d1", "n1", "t", "v1");
        let (idx, _) = store.insert_mapped("n1", &rec).unwrap();
        let (_, other) = note("d1", "n2", "t", "keep");
        store.insert_mapped("n2", &other).unwrap();
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
        // Mapping dropped: resolve misses, re-create allocates fresh.
        assert!(matches!(
            store.resolve_external("d1", "n1", Some("notes")),
            Err(Error::NotFound(_))
        ));
        let (_, rec2) = note("d1", "n1", "t", "v2");
        let (idx2, inserted) = store.insert_mapped("n1", &rec2).unwrap();
        assert_ne!(idx2, idx);
        assert!(inserted);
        assert!(
            store
                .get_record("d1", "notes", idx2)
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
        let (new_idx, new_hash) = store
            .note_update_guarded("d1", idx, &before.hash, "t", "v2")
            .unwrap();
        assert_ne!(new_idx, idx);
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
