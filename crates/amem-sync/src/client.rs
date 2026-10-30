//! Sync client flow over any [`Transport`](crate::transport::Transport):
//! generation gate, rollback gate, upload, download+apply, cursor advance.
//!
//! Resume is exact: server batches are atomic, so the upload cursor
//! restarts from the server high-water (nothing applied is re-sent);
//! downloads advance per record. Kill -9 anywhere resumes clean.

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use amem_core::error::{Error, Result};
use amem_core::store::{self, NewRecord};

use crate::envelope::{Envelope, future_skew_ms};
use crate::protocol::{MAX_RECORD_BYTES, PAGE_SIZE, PostBody};
use crate::transport::{Transport, TransportError};

/// Sync outcome counts.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Summary {
    /// Records uploaded.
    pub uploaded: u64,
    /// Records downloaded and applied.
    pub downloaded: u64,
    /// Re-sent uploads the server already had.
    pub duplicates: u64,
    /// Upload conflicts reported (local branched, server kept first).
    pub conflicts: u64,
    /// Downloads that failed validation (hash/skew/size).
    pub quarantined: u64,
    /// Conflict branches created.
    pub branches: u64,
}

/// Sync client bound to one store dir + endpoint.
pub struct SyncClient<T: Transport> {
    transport: T,
    store_dir: PathBuf,
    endpoint: String,
    device: String,
}

fn sync_err(e: &TransportError) -> Error {
    Error::Sync(e.to_string())
}

impl<T: Transport + Sync> SyncClient<T> {
    /// Bind a client to a store + endpoint.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] on an uninitialized store.
    pub fn open(store_dir: &Path, endpoint: &str, transport: T) -> Result<Self> {
        if !store_dir.join("memory.db").exists() {
            return Err(Error::NotFound(
                "store not initialized; nothing to sync".to_string(),
            ));
        }
        let device = store::device_id(store_dir)?;
        Ok(Self {
            transport,
            store_dir: store_dir.to_path_buf(),
            endpoint: endpoint.to_string(),
            device,
        })
    }

    /// Run one sync: status → gates → upload → download → cursors.
    /// `recover` adopts a changed server generation and re-syncs all.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Sync`] on divergence (generation change without
    /// `recover`), rollback (server high-water regressed), or transport
    /// failure; [`Error::Db`] on store failure.
    pub async fn sync(&self, recover: bool) -> Result<Summary> {
        let mut summary = Summary::default();
        let status = self.transport.status().await.map_err(|e| sync_err(&e))?;
        let mut db = store::Store::open(&self.store_dir)?;

        // Generation gate: first contact pins, change refuses (or recovers).
        match db.sync_state_get(&self.endpoint)? {
            None => {
                db.sync_state_put(&self.endpoint, &status.generation, now_ns())?;
            }
            Some((pinned, _)) if pinned == status.generation => {}
            Some(_) => {
                if !recover {
                    let local = db.count_all()?;
                    let remote: i64 = status
                        .streams
                        .values()
                        .flat_map(|m| m.values())
                        .map(|m| m + 1)
                        .sum();
                    return Err(Error::Sync(format!(
                        "divergence: server generation changed (local={local} remote={remote}); \
                         re-run with --recover --confirm to adopt it and re-sync"
                    )));
                }
                db.sync_state_put(&self.endpoint, &status.generation, now_ns())?;
                db.sync_marks_reset(&self.endpoint)?;
            }
        }

        // Rollback gate: same generation, but the server lost records.
        for (device, stream, up, down) in db.sync_marks_list(&self.endpoint)? {
            let server_max = status
                .streams
                .get(&device)
                .and_then(|m| m.get(&stream))
                .copied()
                .unwrap_or(-1);
            let seen = up.max(down);
            if server_max < seen {
                return Err(Error::Sync(format!(
                    "rollback: server lost {device}/{stream} records \
                     (had {seen}, now {server_max}); restore the server from backup, \
                     then sync again — refusing to auto-merge"
                )));
            }
        }

        // Branch dedupe set: (stream, winner triple, winner hash, loser hash, reason).
        let mut branched: HashSet<(String, String, i64, String, String, String)> = HashSet::new();
        for b in db.branch_list()? {
            let loser_hash = serde_json::from_str::<serde_json::Value>(&b.loser_envelope)
                .ok()
                .and_then(|v| v.get("hash").and_then(|h| h.as_str()).map(str::to_string))
                .unwrap_or_default();
            branched.insert((
                b.stream.clone(),
                b.winner_device.clone(),
                b.winner_idx,
                b.winner_hash.clone(),
                loser_hash,
                b.reason.clone(),
            ));
        }

        self.upload(&mut db, &mut summary).await?;
        self.download(&mut db, &status, &mut summary, &mut branched)
            .await?;
        Ok(summary)
    }

    /// Upload own-device series strictly above the upload cursor.
    #[allow(clippy::too_many_lines)]
    async fn upload(&self, db: &mut store::Store, summary: &mut Summary) -> Result<()> {
        for stream in db.local_streams(&self.device)? {
            let (marked_up, _) = db.sync_marks_get(&self.endpoint, &self.device, &stream)?;
            // Resume rule: the local mark is authoritative. A crash between
            // POST and mark-advance re-sends at most one batch, which lands
            // as duplicates/conflicts (never dup rows: the server is
            // idempotent). Starting from the server high-water instead would
            // skip conflicted idx the server filled with other content.
            let mut up = marked_up;
            let local_max = db.max_idx(&self.device, &stream)?;
            while up < local_max {
                let batch = db.records_since(&self.device, &stream, up, PAGE_SIZE)?;
                if batch.is_empty() {
                    break;
                }
                let mut raws: Vec<String> = Vec::with_capacity(batch.len());
                let mut batch_max = up;
                for rec in &batch {
                    batch_max = batch_max.max(rec.idx);
                    let ext = db
                        .external_id(&rec.device, &rec.stream, rec.idx)?
                        .unwrap_or_else(|| format!("{}#{}", rec.stream, rec.idx));
                    if let Ok(env) = Envelope::from_record(rec, &ext) {
                        raws.push(env.to_raw());
                    } else {
                        // Corrupt local row: quarantine (server keeps a
                        // hole here; downloads step over holes).
                        let envelope = serde_json::json!({
                            "id": ext, "device": rec.device, "stream": rec.stream,
                            "idx": rec.idx, "ts_ns": rec.ts_ns, "actor": rec.actor,
                            "kind": rec.kind,
                            "payload": serde_json::from_str::<serde_json::Value>(&rec.payload)
                                .unwrap_or(serde_json::Value::Null),
                            "hash": rec.hash, "v": 1,
                        })
                        .to_string();
                        db.quarantine_move(
                            &rec.device,
                            &rec.stream,
                            rec.idx,
                            &envelope,
                            "corrupt: stored hash mismatch",
                            now_ns(),
                        )?;
                        summary.quarantined += 1;
                        up = rec.idx;
                        db.sync_marks_set_up(&self.endpoint, &self.device, &stream, up)?;
                    }
                }
                if raws.is_empty() {
                    continue;
                }
                let outcome = self
                    .transport
                    .post(&PostBody {
                        records: raws,
                        client_caps: Vec::new(),
                    })
                    .await
                    .map_err(|e| sync_err(&e))?;
                summary.uploaded += outcome.accepted;
                summary.duplicates += outcome.duplicates;
                // Conflicts are counted here but branched on the download
                // side (single branch site): the server winner arrives as a
                // download and mismatches the local row. The cursor advances
                // past conflicted idx — the server keeps first-wins forever.
                summary.conflicts += outcome.conflicts.len() as u64;
                up = batch_max;
                db.sync_marks_set_up(&self.endpoint, &self.device, &stream, up)?;
            }
        }
        Ok(())
    }

    /// Download every series strictly above the download cursor.
    #[allow(clippy::too_many_lines)]
    async fn download(
        &self,
        db: &mut store::Store,
        status: &crate::protocol::StatusDto,
        summary: &mut Summary,
        branched: &mut HashSet<(String, String, i64, String, String, String)>,
    ) -> Result<()> {
        let now_ms = now_ns().div_euclid(1_000_000);
        for (device, streams) in &status.streams {
            for (stream, server_max) in streams {
                let (_, mut down) = db.sync_marks_get(&self.endpoint, device, stream)?;
                while down < *server_max {
                    let page = self
                        .transport
                        .next(device, stream, down + 1, PAGE_SIZE)
                        .await
                        .map_err(|e| sync_err(&e))?;
                    if page.records.is_empty() {
                        break;
                    }
                    // Reorder tolerance: apply ascending no matter the wire order.
                    let mut raws = page.records;
                    raws.sort_by_key(|r| {
                        serde_json::from_str::<serde_json::Value>(r)
                            .ok()
                            .and_then(|v| v.get("idx").and_then(serde_json::Value::as_i64))
                            .unwrap_or(i64::MAX)
                    });
                    for raw in &raws {
                        Self::apply_one(db, raw, now_ms, summary, branched)?;
                        if let Some(idx) = envelope_idx(raw) {
                            down = down.max(idx);
                            db.sync_marks_set_down(&self.endpoint, device, stream, down)?;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Validate + apply one downloaded envelope: quarantine, branch, or store.
    #[allow(clippy::too_many_lines)]
    fn apply_one(
        db: &mut store::Store,
        raw: &str,
        now_ms: i64,
        summary: &mut Summary,
        branched: &mut HashSet<(String, String, i64, String, String, String)>,
    ) -> Result<()> {
        let now = now_ns();
        // Size gate first (cheap, before parse).
        if raw.len() > MAX_RECORD_BYTES {
            let (device, stream, idx) = triple_of(raw);
            db.quarantine_put(&device, &stream, idx, raw, "oversize", now)?;
            summary.quarantined += 1;
            return Ok(());
        }
        let Ok(env) = Envelope::from_raw(raw) else {
            let (device, stream, idx) = triple_of(raw);
            db.quarantine_put(&device, &stream, idx, raw, "malformed", now)?;
            summary.quarantined += 1;
            return Ok(());
        };
        if !env.verify_hash() {
            db.quarantine_put(&env.device, &env.stream, env.idx, raw, "hash mismatch", now)?;
            summary.quarantined += 1;
            return Ok(());
        }
        if let Some(excess) = future_skew_ms(env.hlc.wall_ms, now_ms) {
            db.quarantine_put(
                &env.device,
                &env.stream,
                env.idx,
                raw,
                &format!("clock skew: envelope {excess}ms beyond tolerance"),
                now,
            )?;
            summary.quarantined += 1;
            return Ok(());
        }
        // Same triple present? Duplicate, or PK conflict (branch, keep local).
        match db.get_record(&env.device, &env.stream, env.idx) {
            Ok(local) => {
                if local.hash == env.hash {
                    return Ok(()); // Duplicate delivery: already applied.
                }
                let key = (
                    env.stream.clone(),
                    env.device.clone(),
                    env.idx,
                    local.hash.clone(),
                    env.hash.clone(),
                    "pk".to_string(),
                );
                if branched.insert(key) {
                    db.branch_put(
                        &env.id,
                        &env.stream,
                        &env.device,
                        env.idx,
                        &local.hash,
                        raw,
                        "pk",
                        now,
                    )?;
                    summary.branches += 1;
                }
                return Ok(());
            }
            Err(Error::NotFound(_)) => {}
            Err(e) => return Err(e),
        }
        // Same external id anywhere with different content? Id conflict:
        // the record still applies under its own PK (immutable union) AND
        // a branch flags the divergence. Exact-id only (prefixes are an
        // interactive convenience, never a conflict signal).
        for (hit_device, _hit_stream, hit_idx, hit_hash) in
            db.resolve_external_any(&env.id, Some(&env.stream))?
        {
            if hit_device == env.device || hit_hash == env.hash {
                continue;
            }
            let key = (
                env.stream.clone(),
                hit_device.clone(),
                hit_idx,
                hit_hash.clone(),
                env.hash.clone(),
                "id".to_string(),
            );
            if branched.insert(key) {
                db.branch_put(
                    &env.id,
                    &env.stream,
                    &hit_device,
                    hit_idx,
                    &hit_hash,
                    raw,
                    "id",
                    now,
                )?;
                summary.branches += 1;
            }
        }
        let rec = NewRecord {
            device: env.device.clone(),
            stream: env.stream.clone(),
            ts_ns: env.ts_ns,
            actor: env.actor.clone(),
            kind: env.kind.clone(),
            payload: env.payload.clone(),
            hash: env.hash.clone(),
        };
        if db.apply_remote(&rec, env.idx, &env.id)? {
            summary.downloaded += 1;
        }
        Ok(())
    }
}

/// Current time, Unix ns.
fn now_ns() -> i64 {
    amem_core::clock::now_nanos()
}

/// Best-effort triple from possibly-malformed bytes (quarantine needs a key).
fn triple_of(raw: &str) -> (String, String, i64) {
    let v: serde_json::Value = serde_json::from_str(raw).unwrap_or(serde_json::Value::Null);
    (
        v.get("device")
            .and_then(|d| d.as_str())
            .unwrap_or("?")
            .to_string(),
        v.get("stream")
            .and_then(|s| s.as_str())
            .unwrap_or("?")
            .to_string(),
        v.get("idx")
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(-1),
    )
}

/// Best-effort idx for cursor advance.
fn envelope_idx(raw: &str) -> Option<i64> {
    serde_json::from_str::<serde_json::Value>(raw)
        .ok()
        .and_then(|v| v.get("idx").and_then(serde_json::Value::as_i64))
}
