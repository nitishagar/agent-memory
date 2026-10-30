//! Sync client flow over any [`Transport`](crate::transport::Transport):
//! generation gate, rollback gate, upload, download+apply, cursor advance.
//!
//! Resume is exact: the LOCAL mark is authoritative for uploads (a crash
//! between POST and mark-advance re-sends at most one batch, which lands
//! as duplicates/conflicts — never dup rows, the server is idempotent);
//! downloads advance per validated record. Kill -9 anywhere resumes clean.

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use amem_core::error::{Error, Result};
use amem_core::store::{self, NewRecord};

use crate::envelope::{Envelope, derive_hlc, future_skew_ms};
use crate::protocol::{MAX_RECORD_BYTES, PAGE_SIZE, PostBody};
use crate::transport::{Transport, TransportError};

/// What `apply_one` did with a downloaded envelope.
enum Applied {
    /// Cursor may advance to this validated `idx`.
    Advance(i64),
    /// Skew-filed, cursor held: the next sync retries, so a healed clock
    /// self-heals the record. Stalls the series silently (not an error).
    HeldSkew,
    /// Unfileable-or-unadvancable junk (unparseable, out-of-bounds idx).
    /// Filed where possible; a page of ONLY junk is a protocol violation.
    HeldJunk,
}

/// Refuse status fan-out past this many series (hostile-server guard).
const MAX_SERIES: usize = 100_000;

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
            // Canonical key (host case, trailing slash, default port):
            // pins, generations, and cursors must not split by spelling.
            endpoint: crate::tls::canonical_endpoint(endpoint),
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
                        "divergence: {endpoint} now serves generation {} (local={local} remote={remote}); \
                         verify you trust this server, then re-run with --recover --confirm to adopt \
                         its union and re-push your {local} local record(s)",
                        short_gen(&status.generation),
                        endpoint = self.endpoint,
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

        self.upload(&mut db, &mut summary, recover).await?;
        self.download(&mut db, &status, &mut summary, &mut branched)
            .await?;
        Ok(summary)
    }

    /// Upload series strictly above the upload cursor: own-device series
    /// normally, every local series on `recover` (foreign records whose
    /// owner never returns still survive a server rebuild).
    #[allow(clippy::too_many_lines)]
    async fn upload(
        &self,
        db: &mut store::Store,
        summary: &mut Summary,
        recover: bool,
    ) -> Result<()> {
        let series: Vec<(String, String)> = if recover {
            db.all_series()?
        } else {
            db.local_streams(&self.device)?
                .into_iter()
                .map(|stream| (self.device.clone(), stream))
                .collect()
        };
        for (device, stream) in &series {
            let (marked_up, _) = db.sync_marks_get(&self.endpoint, device, stream)?;
            // Resume rule: the local mark is authoritative. A crash between
            // POST and mark-advance re-sends at most one batch, which lands
            // as duplicates/conflicts (never dup rows: the server is
            // idempotent). Starting from the server high-water instead would
            // skip conflicted idx the server filled with other content.
            let mut up = marked_up;
            let local_max = db.max_idx(device, stream)?;
            while up < local_max {
                let batch = db.records_since(device, stream, up, PAGE_SIZE)?;
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
                    let raw = Envelope::from_record(rec, &ext).map(|env| env.to_raw());
                    // Oversize rows quarantine LOCALLY (same path as corrupt
                    // rows): the server 400s the whole batch, and without
                    // the cursor advance every retry re-sends the identical
                    // poison batch — sync wedged forever.
                    let poison = match &raw {
                        Err(_) => Some("corrupt: stored hash mismatch"),
                        Ok(raw) if raw.len() > MAX_RECORD_BYTES => {
                            Some("oversize: envelope exceeds server cap")
                        }
                        Ok(_) => None,
                    };
                    if let Some(reason) = poison {
                        // Corrupt/oversize local row: quarantine (server
                        // keeps a hole here; downloads step over holes).
                        // Non-JSON payload bytes (manual sqlite tampering)
                        // ride along as `raw_payload` so restore writes
                        // back the original, not `Null`.
                        let parsed_payload =
                            serde_json::from_str::<serde_json::Value>(&rec.payload).ok();
                        let mut envelope = serde_json::json!({
                            "id": ext, "device": rec.device, "stream": rec.stream,
                            "idx": rec.idx, "ts_ns": rec.ts_ns, "actor": rec.actor,
                            "kind": rec.kind,
                            "payload": parsed_payload.clone().unwrap_or(serde_json::Value::Null),
                            "hash": rec.hash, "v": 1,
                        });
                        if parsed_payload.is_none() {
                            envelope["raw_payload"] =
                                serde_json::Value::String(rec.payload.clone());
                        }
                        let envelope = envelope.to_string();
                        db.quarantine_move(
                            &rec.device,
                            &rec.stream,
                            rec.idx,
                            &envelope,
                            reason,
                            now_ns(),
                        )?;
                        summary.quarantined += 1;
                        up = rec.idx;
                        db.sync_marks_set_up(&self.endpoint, &rec.device, stream, up)?;
                    } else if let Ok(raw) = raw {
                        raws.push(raw);
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
                db.sync_marks_set_up(&self.endpoint, device, stream, up)?;
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
        // A hostile status could fan out over millions of series; refuse.
        let series: usize = status
            .streams
            .values()
            .map(std::collections::BTreeMap::len)
            .sum();
        if series > MAX_SERIES {
            return Err(Error::Sync(format!(
                "server lists {series} series (cap {MAX_SERIES}); refusing fan-out"
            )));
        }
        let now_ms = now_ns().div_euclid(1_000_000);
        for (device, streams) in &status.streams {
            for (stream, server_max) in streams {
                // Our server never emits max > MAX_IDX (post rejects):
                // anything higher is hostile bytes, not a series.
                if *server_max > crate::protocol::MAX_IDX {
                    return Err(Error::Sync(format!(
                        "protocol violation: {device}/{stream} claims max {server_max}; refusing"
                    )));
                }
                let (_, mut down) = db.sync_marks_get(&self.endpoint, device, stream)?;
                // Skew-held tail: stop paging this series (retry next sync),
                // but do NOT error — a wrong local clock must not fail sync.
                let mut stalled_on_skew = false;
                while down < *server_max && !stalled_on_skew {
                    let page = self
                        .transport
                        .next(device, stream, down.saturating_add(1), PAGE_SIZE)
                        .await
                        .map_err(|e| sync_err(&e))?;
                    if page.records.is_empty() {
                        break;
                    }
                    // Reorder tolerance: apply ascending no matter the wire order.
                    let mut raws = page.records;
                    raws.sort_by_key(|r| envelope_idx(r).unwrap_or(i64::MAX));
                    let mut advanced = false;
                    let mut saw_skew = false;
                    for raw in &raws {
                        match Self::apply_one(db, raw, now_ms, *server_max, summary, branched)? {
                            Applied::Advance(idx) => {
                                if idx > down {
                                    advanced = true;
                                    down = idx;
                                    db.sync_marks_set_down(&self.endpoint, device, stream, down)?;
                                }
                            }
                            Applied::HeldSkew => saw_skew = true,
                            Applied::HeldJunk => {}
                        }
                    }
                    // A non-empty page that advances nothing and holds no
                    // skew is pure junk (idx-less/out-of-bounds): error,
                    // never spin forever re-requesting it.
                    if !advanced && !saw_skew {
                        return Err(Error::Sync(format!(
                            "protocol violation: {device}/{stream} page advanced nothing \
                             (junk or out-of-bounds idx); refusing to spin"
                        )));
                    }
                    if !advanced {
                        stalled_on_skew = true;
                    }
                }
                // The server converged mid-sync (truncated between status
                // and next): report it now, not next sync — unless we
                // deliberately stalled on skew-held records.
                if down < *server_max && !stalled_on_skew {
                    return Err(Error::Sync(format!(
                        "rollback mid-sync: {device}/{stream} ended at {down}, \
                         status promised {server_max}; retry, and restore the \
                         server from backup if it repeats"
                    )));
                }
            }
        }
        Ok(())
    }

    /// Validate + apply one downloaded envelope: quarantine, branch, or store.
    ///
    /// The cursor advances ONLY over validated envelopes with an in-bounds
    /// `idx` (`0 <= idx <= server_max`): hostile bytes can file quarantine
    /// rows but can never drag the cursor over unseen records. Skew-held
    /// envelopes return [`Applied::HeldSkew`] (retry next sync);
    /// unparseable/out-of-bounds ones [`Applied::HeldJunk`].
    #[allow(clippy::too_many_lines)]
    fn apply_one(
        db: &mut store::Store,
        raw: &str,
        now_ms: i64,
        server_max: i64,
        summary: &mut Summary,
        branched: &mut HashSet<(String, String, i64, String, String, String)>,
    ) -> Result<Applied> {
        let now = now_ns();
        // Advance past deterministic failures with a usable idx; hold
        // (junk) when the bytes don't even name an in-bounds index.
        let advance_or_hold = |idx: Option<i64>| match idx {
            Some(i) if (0..=server_max).contains(&i) => Applied::Advance(i),
            _ => Applied::HeldJunk,
        };
        // Size gate first (cheap, before parse).
        if raw.len() > MAX_RECORD_BYTES {
            let (device, stream, idx) = triple_of(raw);
            db.quarantine_put(&device, &stream, idx, raw, "oversize", now)?;
            summary.quarantined += 1;
            return Ok(advance_or_hold(envelope_idx(raw)));
        }
        let Ok(env) = Envelope::from_raw(raw) else {
            let (device, stream, idx) = triple_of(raw);
            db.quarantine_put(&device, &stream, idx, raw, "malformed", now)?;
            summary.quarantined += 1;
            return Ok(advance_or_hold(envelope_idx(raw)));
        };
        if !env.verify_hash() {
            db.quarantine_put(&env.device, &env.stream, env.idx, raw, "hash mismatch", now)?;
            summary.quarantined += 1;
            return Ok(advance_or_hold(Some(env.idx)));
        }
        // HLC is DERIVED from (ts_ns, idx, device): an envelope whose clock
        // disagrees with its own fields is forged or corrupt. This also
        // makes the skew gate below trustworthy (wall_ms == ts wall).
        if env.hlc != derive_hlc(env.ts_ns, env.idx, &env.device) {
            db.quarantine_put(&env.device, &env.stream, env.idx, raw, "hlc mismatch", now)?;
            summary.quarantined += 1;
            return Ok(advance_or_hold(Some(env.idx)));
        }
        if env.device.is_empty() || env.stream.is_empty() {
            db.quarantine_put(
                &env.device,
                &env.stream,
                env.idx,
                raw,
                "empty device/stream",
                now,
            )?;
            summary.quarantined += 1;
            return Ok(advance_or_hold(Some(env.idx)));
        }
        // Bounds BEFORE anything trusts the idx: negative or
        // above-the-server's-own-max indexes are hostile bytes.
        if !(0..=server_max).contains(&env.idx) {
            db.quarantine_put(
                &env.device,
                &env.stream,
                env.idx,
                raw,
                "idx out of bounds",
                now,
            )?;
            summary.quarantined += 1;
            return Ok(Applied::HeldJunk);
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
            return Ok(Applied::HeldSkew);
        }
        // Quarantine is sticky across cursor resets (recover must not
        // resurrect shelved records) — EXCEPT stale skew rows: a healed
        // clock deletes the stale filing and applies the record.
        match db.quarantine_get(&env.device, &env.stream, env.idx) {
            Ok(q) if q.reason.starts_with("clock skew") => {
                db.quarantine_delete(&env.device, &env.stream, env.idx)?;
            }
            Ok(_) => return Ok(Applied::Advance(env.idx)),
            Err(Error::NotFound(_)) => {}
            Err(e) => return Err(e),
        }
        // Same triple present? Duplicate, or PK conflict (branch, keep local).
        match db.get_record(&env.device, &env.stream, env.idx) {
            Ok(local) => {
                if local.hash == env.hash {
                    return Ok(Applied::Advance(env.idx)); // Duplicate delivery.
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
                    let loser_hash = store::hash_payload(raw);
                    if db
                        .branch_put(
                            &env.id,
                            &env.stream,
                            &env.device,
                            env.idx,
                            &local.hash,
                            raw,
                            &loser_hash,
                            "pk",
                            now,
                        )?
                        .is_some()
                    {
                        summary.branches += 1;
                    }
                }
                return Ok(Applied::Advance(env.idx));
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
                let loser_hash = store::hash_payload(raw);
                if db
                    .branch_put(
                        &env.id,
                        &env.stream,
                        &hit_device,
                        hit_idx,
                        &hit_hash,
                        raw,
                        &loser_hash,
                        "id",
                        now,
                    )?
                    .is_some()
                {
                    summary.branches += 1;
                }
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
        if db.apply_remote(&rec, env.idx, &env.id, Some(raw))? {
            summary.downloaded += 1;
        }
        Ok(Applied::Advance(env.idx))
    }
}

/// Current time, Unix ns.
fn now_ns() -> i64 {
    amem_core::clock::now_nanos()
}

/// First 8 chars of a generation UUID (divergence messages).
fn short_gen(generation: &str) -> String {
    generation.chars().take(8).collect()
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
