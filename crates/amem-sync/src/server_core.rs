//! Server-side union store: the same core backs the axum server and the
//! fault-harness simulator, so matrix coverage applies to production paths.

use std::collections::BTreeMap;
use std::path::Path;

use crate::envelope::Envelope;
use crate::protocol::{
    ConflictDto, MAX_BATCH, MAX_IDX, MAX_RECORD_BYTES, PostBody, PostOutcome, StatusDto,
};

/// Union database: raw envelopes keyed by `(device, stream, idx)`.
pub struct ServerCore {
    conn: rusqlite::Connection,
}

impl ServerCore {
    /// Open (or create) the union database, minting a generation UUID on
    /// first boot.
    ///
    /// # Errors
    ///
    /// Returns [`amem_core::error::Error::Db`] on `SQLite` failure.
    pub fn open(path: &Path) -> amem_core::error::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let conn = rusqlite::Connection::open(path)?;
        conn.busy_timeout(std::time::Duration::from_secs(5))?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS union_records (
                device TEXT NOT NULL,
                stream TEXT NOT NULL,
                idx INTEGER NOT NULL,
                envelope TEXT NOT NULL,
                hash TEXT NOT NULL,
                PRIMARY KEY (device, stream, idx)
            );
            CREATE TABLE IF NOT EXISTS union_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);",
        )?;
        let generation: Option<String> = conn
            .query_row(
                "SELECT value FROM union_meta WHERE key = 'generation'",
                [],
                |r| r.get(0),
            )
            .map(Some)
            .or_else(|e| match e {
                rusqlite::Error::QueryReturnedNoRows => Ok(None),
                other => Err(amem_core::error::Error::Db(other)),
            })?;
        if generation.is_none() {
            // OR IGNORE: concurrent first-opens race here; one wins.
            conn.execute(
                "INSERT OR IGNORE INTO union_meta (key, value) VALUES ('generation', ?1)",
                rusqlite::params![uuid::Uuid::new_v4().to_string()],
            )?;
        }
        Ok(Self { conn })
    }

    /// Current status: generation + per-series high-water marks.
    ///
    /// # Errors
    ///
    /// Returns [`amem_core::error::Error::Db`] on `SQLite` failure.
    pub fn status(&self) -> amem_core::error::Result<StatusDto> {
        let generation: String = self.conn.query_row(
            "SELECT value FROM union_meta WHERE key = 'generation'",
            [],
            |r| r.get(0),
        )?;
        let mut stmt = self.conn.prepare(
            "SELECT device, stream, MAX(idx) FROM union_records GROUP BY device, stream",
        )?;
        let mut streams: BTreeMap<String, BTreeMap<String, i64>> = BTreeMap::new();
        for row in stmt.query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, i64>(2)?,
            ))
        })? {
            let (device, stream, max) = row?;
            streams.entry(device).or_default().insert(stream, max);
        }
        Ok(StatusDto {
            generation,
            streams,
        })
    }

    /// Page of raw envelopes at `idx >= start`, ascending, at most `count`.
    ///
    /// # Errors
    ///
    /// Returns [`amem_core::error::Error::Db`] on `SQLite` failure.
    pub fn next(
        &self,
        device: &str,
        stream: &str,
        start: i64,
        count: i64,
    ) -> amem_core::error::Result<Vec<String>> {
        let mut stmt = self.conn.prepare(
            "SELECT envelope FROM union_records
             WHERE device = ?1 AND stream = ?2 AND idx >= ?3
             ORDER BY idx ASC LIMIT ?4",
        )?;
        let rows = stmt.query_map(
            rusqlite::params![device, stream, start, count.max(0)],
            |r| r.get::<_, String>(0),
        )?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(amem_core::error::Error::Db)
    }

    /// Atomic idempotent batch insert: the whole batch lands or nothing
    /// does; re-posts are duplicates; same-triple-different-hash rows are
    /// conflicts (first wins, reported).
    ///
    /// # Errors
    ///
    /// Returns [`amem_core::error::Error::InvalidArguments`] on oversize
    /// records/batches or malformed envelopes, [`amem_core::error::Error::Db`]
    /// on `SQLite` failure.
    pub fn post(&mut self, body: &PostBody) -> amem_core::error::Result<PostOutcome> {
        use amem_core::error::Error;
        if body.records.len() > MAX_BATCH {
            return Err(Error::InvalidArguments(format!(
                "batch of {} exceeds cap {MAX_BATCH}",
                body.records.len()
            )));
        }
        // Validate everything BEFORE opening the write txn: atomicity means
        // a malformed tail must not land a partial head.
        let mut parsed: Vec<(Envelope, String)> = Vec::with_capacity(body.records.len());
        for raw in &body.records {
            if raw.len() > MAX_RECORD_BYTES {
                return Err(Error::InvalidArguments(format!(
                    "record of {} bytes exceeds cap {MAX_RECORD_BYTES}",
                    raw.len()
                )));
            }
            let env = Envelope::from_raw(raw).map_err(|_| {
                Error::InvalidArguments(
                    "malformed envelope in batch; whole batch rejected".to_string(),
                )
            })?;
            if !env.verify_hash() {
                return Err(Error::InvalidArguments(format!(
                    "{}/{}/{}: envelope hash mismatch; whole batch rejected",
                    env.device, env.stream, env.idx
                )));
            }
            // Identity bounds: negative/huge idx rows would sit in the
            // union invisible forever (clients page from 0 and bound by
            // the status max), and empty names break triple handling.
            if env.idx < 0 || env.idx > MAX_IDX {
                return Err(Error::InvalidArguments(format!(
                    "{}/{}/{}: idx out of bounds; whole batch rejected",
                    env.device, env.stream, env.idx
                )));
            }
            if env.device.is_empty() || env.stream.is_empty() {
                return Err(Error::InvalidArguments(
                    "empty device/stream; whole batch rejected".to_string(),
                ));
            }
            parsed.push((env, raw.clone()));
        }
        let tx = self
            .conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let mut accepted = 0u64;
        let mut duplicates = 0u64;
        let mut conflicts = Vec::new();
        for (env, raw) in &parsed {
            let have: Option<String> = tx
                .query_row(
                    "SELECT hash FROM union_records WHERE device = ?1 AND stream = ?2 AND idx = ?3",
                    rusqlite::params![env.device, env.stream, env.idx],
                    |r| r.get(0),
                )
                .map(Some)
                .or_else(|e| match e {
                    rusqlite::Error::QueryReturnedNoRows => Ok(None),
                    other => Err(Error::Db(other)),
                })?;
            match have {
                None => {
                    tx.execute(
                        "INSERT INTO union_records (device, stream, idx, envelope, hash)
                         VALUES (?1, ?2, ?3, ?4, ?5)",
                        rusqlite::params![env.device, env.stream, env.idx, raw, env.hash],
                    )?;
                    accepted += 1;
                }
                Some(h) if h == env.hash => duplicates += 1,
                Some(_) => conflicts.push(ConflictDto {
                    device: env.device.clone(),
                    stream: env.stream.clone(),
                    idx: env.idx,
                }),
            }
        }
        tx.commit()?;
        Ok(PostOutcome {
            accepted,
            duplicates,
            conflicts,
        })
    }

    /// Truncate the union, KEEPING the generation. Simulator/rollback-test
    /// helper only — models a backup-restore rollback the client must
    /// refuse. No production route calls this. Returns `Result` so a
    /// failed injection fails the test instead of passing vacuously.
    ///
    /// # Errors
    ///
    /// Returns [`amem_core::error::Error::Db`] on `SQLite` failure.
    pub fn truncate_union(&mut self) -> amem_core::error::Result<()> {
        self.conn.execute_batch("DELETE FROM union_records;")?;
        Ok(())
    }
}
