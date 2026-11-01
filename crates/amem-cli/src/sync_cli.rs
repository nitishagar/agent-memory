//! Sync-adjacent CLI: conflict branches, quarantine, un-quarantine.
//!
//! Ids accept either a `device/stream/idx` triple or an external id
//! (exact; ambiguous across devices refuses with the candidate list).

use std::path::Path;

use amem_core::error::{Error, Result};
use amem_core::store::{NewRecord, Store};

/// Resolve a CLI id to a triple: `device/stream/idx` or an exact external id.
pub fn resolve_triple(db: &Store, id: &str) -> Result<(String, String, i64)> {
    if let Some((device, rest)) = id.split_once('/')
        && let Some((stream, idx_s)) = rest.split_once('/')
        && let Ok(idx) = idx_s.parse::<i64>()
    {
        return Ok((device.to_string(), stream.to_string(), idx));
    }
    let hits = db.resolve_external_any(id, None)?;
    match hits.len() {
        0 => Err(Error::NotFound(id.to_string())),
        1 => {
            let (device, stream, idx, _) = hits.into_iter().next().unwrap();
            Ok((device, stream, idx))
        }
        _ => {
            let mut show: Vec<String> = hits
                .iter()
                .take(5)
                .map(|(d, s, i, _)| format!("{d}/{s}/{i}"))
                .collect();
            if hits.len() > 5 {
                show.push(format!("… and {} more", hits.len() - 5));
            }
            Err(Error::Ambiguous(format!(
                "{id}: in {} places ({}); use device/stream/idx",
                hits.len(),
                show.join(", ")
            )))
        }
    }
}

/// `amem branches`: every conflict branch as JSON.
pub fn branches(store_dir: &Path) -> Result<()> {
    let db = open_store(store_dir)?;
    let list = db.branch_list()?;
    let arr: Vec<serde_json::Value> = list
        .iter()
        .map(|b| {
            serde_json::json!({
                "id": b.id, "ext_id": b.ext_id, "stream": b.stream,
                "winner": format!("{}/{}/{}", b.winner_device, b.stream, b.winner_idx),
                "winner_hash": b.winner_hash, "reason": b.reason, "ts_ns": b.ts_ns,
            })
        })
        .collect();
    println!("{}", serde_json::json!({"branches": arr}));
    Ok(())
}

/// `amem quarantine [<id>]`: list (no id) or move a record out of recall.
pub fn quarantine(store_dir: &Path, id: Option<&str>, reason: &str) -> Result<()> {
    let mut db = open_store(store_dir)?;
    let Some(id) = id else {
        let list = db.quarantine_list()?;
        let arr: Vec<serde_json::Value> = list
            .iter()
            .map(|q| {
                serde_json::json!({
                    "triple": format!("{}/{}/{}", q.device, q.stream, q.idx),
                    "id": envelope_id(&q.envelope),
                    "reason": q.reason, "ts_ns": q.ts_ns,
                })
            })
            .collect();
        println!("{}", serde_json::json!({"quarantined": arr}));
        return Ok(());
    };
    let (device, stream, idx) = resolve_triple(&db, id)?;
    let rec = db.get_record(&device, &stream, idx)?;
    let ext = db
        .external_id(&device, &stream, idx)?
        .unwrap_or_else(|| format!("{stream}#{idx}"));
    // A corrupt row is the MAIN quarantine use case: when the envelope
    // builder refuses (hash mismatch), file a manual envelope preserving
    // the fields instead of failing.
    let envelope = match amem_sync::envelope::Envelope::from_record(&rec, &ext) {
        Ok(env) => env.to_raw(),
        Err(_) => serde_json::json!({
            "id": ext, "device": rec.device, "stream": rec.stream,
            "idx": idx, "ts_ns": rec.ts_ns, "actor": rec.actor,
            "kind": rec.kind,
            "payload": serde_json::from_str::<serde_json::Value>(&rec.payload)
                .unwrap_or(serde_json::Value::Null),
            "hash": rec.hash, "v": 1,
        })
        .to_string(),
    };
    db.quarantine_move(&device, &stream, idx, &envelope, reason, now_ns())?;
    println!(
        "{}",
        serde_json::json!({"quarantined": format!("{device}/{stream}/{idx}")})
    );
    Ok(())
}

/// `amem unquarantine <id>`: restore into recall; a divergent occupant
/// keeps the triple and the restored envelope branches instead.
pub fn unquarantine(store_dir: &Path, id: &str) -> Result<()> {
    let mut db = open_store(store_dir)?;
    let target = find_quarantined(&db, id)?;
    let env = amem_sync::envelope::Envelope::from_raw(&target.envelope)
        .map_err(|_| Error::InvalidArguments("quarantined envelope is malformed".to_string()))?;
    if !env.verify_hash() {
        return Err(Error::InvalidArguments(
            "quarantined envelope fails hash check; refusing to restore".to_string(),
        ));
    }
    // The envelope must name the triple it was filed under: mixed-triple
    // restores would plant a record the cursors never account for.
    if env.device != target.device || env.stream != target.stream || env.idx != target.idx {
        return Err(Error::InvalidArguments(format!(
            "quarantined envelope names {}/{}/{} but was filed under {}/{}/{}; refusing to restore",
            env.device, env.stream, env.idx, target.device, target.stream, target.idx,
        )));
    }
    // Skew re-check: a future-dated envelope restored into recall would
    // sort first forever. Fix the clock (or the data), then retry.
    let now_ms = now_ns().div_euclid(1_000_000);
    if let Some(excess) = amem_sync::envelope::future_skew_ms(env.hlc.wall_ms, now_ms) {
        return Err(Error::InvalidArguments(format!(
            "quarantined envelope is {excess}ms future-dated; refusing to restore into recall"
        )));
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
    match db.quarantine_restore(&target.device, &target.stream, target.idx, &rec, &env.id) {
        Ok(()) => {
            // Restored id may now collide with a teammate's same-id note:
            // branch the divergence like a download would.
            branch_restored_conflicts(&db, &env, &target.envelope)?;
            // A below-water restore re-opens its upload series on every
            // endpoint (else the idx is a permanent server hole).
            for endpoint in db.sync_state_list()? {
                let (up, _) = db.sync_marks_get(&endpoint, &target.device, &target.stream)?;
                if up >= target.idx {
                    db.sync_marks_reset_series(&endpoint, &target.device, &target.stream)?;
                }
            }
            println!(
                "{}",
                serde_json::json!({"restored": format!("{}/{}/{}", target.device, target.stream, target.idx)})
            );
            Ok(())
        }
        Err(Error::Conflict(_)) => {
            // Divergent occupant wins the triple; the restored envelope is
            // preserved as a branch (no-loss), then the quarantine row drops.
            let occupant = db.get_record(&target.device, &target.stream, target.idx)?;
            let loser_hash = amem_core::store::hash_payload(&target.envelope);
            let branch_id = db.branch_put(
                &env.id,
                &env.stream,
                &target.device,
                target.idx,
                &occupant.hash,
                &target.envelope,
                &loser_hash,
                "restore",
                now_ns(),
            )?;
            db.quarantine_delete(&target.device, &target.stream, target.idx)?;
            println!(
                "{}",
                serde_json::json!({"branched": branch_id, "reason": "restore-conflict"})
            );
            Ok(())
        }
        Err(e) => Err(e),
    }
}

/// After a restore, flag same-id divergences the download path would have
/// branched (deduped against existing branches).
fn branch_restored_conflicts(
    db: &Store,
    env: &amem_sync::envelope::Envelope,
    loser_envelope: &str,
) -> Result<()> {
    if env.id.is_empty() {
        return Ok(());
    }
    let mut seen = std::collections::HashSet::new();
    for b in db.branch_list()? {
        seen.insert((
            b.stream.clone(),
            b.winner_device.clone(),
            b.winner_idx,
            b.winner_hash.clone(),
            b.loser_hash.clone(),
            b.reason.clone(),
        ));
    }
    let loser_hash = amem_core::store::hash_payload(loser_envelope);
    for (hit_device, hit_stream, hit_idx, hit_hash) in
        db.resolve_external_any(&env.id, Some(&env.stream))?
    {
        if (hit_device == env.device && hit_stream == env.stream && hit_idx == env.idx)
            || hit_hash == env.hash
        {
            continue;
        }
        let key = (
            env.stream.clone(),
            hit_device.clone(),
            hit_idx,
            hit_hash.clone(),
            loser_hash.clone(),
            "id".to_string(),
        );
        if seen.insert(key) {
            db.branch_put(
                &env.id,
                &env.stream,
                &hit_device,
                hit_idx,
                &hit_hash,
                loser_envelope,
                &loser_hash,
                "id",
                now_ns(),
            )?;
        }
    }
    Ok(())
}

/// Best-effort envelope id (quarantine list + lookup).
fn envelope_id(envelope: &str) -> String {
    serde_json::from_str::<serde_json::Value>(envelope)
        .ok()
        .and_then(|v| v.get("id").and_then(|i| i.as_str()).map(str::to_string))
        .unwrap_or_default()
}

/// Find a quarantine row by triple or by envelope id.
fn find_quarantined(db: &Store, id: &str) -> Result<amem_core::store::Quarantined> {
    if let Some((device, rest)) = id.split_once('/')
        && let Some((stream, idx_s)) = rest.split_once('/')
        && let Ok(idx) = idx_s.parse::<i64>()
    {
        return db.quarantine_get(device, stream, idx);
    }
    let mut hits = Vec::new();
    for q in db.quarantine_list()? {
        if envelope_id(&q.envelope) == id {
            hits.push(q);
        }
    }
    match hits.len() {
        0 => Err(Error::NotFound(id.to_string())),
        1 => Ok(hits.into_iter().next().unwrap()),
        _ => {
            let mut show: Vec<String> = hits
                .iter()
                .take(5)
                .map(|q| format!("{}/{}/{}", q.device, q.stream, q.idx))
                .collect();
            if hits.len() > 5 {
                show.push(format!("… and {} more", hits.len() - 5));
            }
            Err(Error::Ambiguous(format!(
                "{id}: quarantined {} times ({}); use device/stream/idx",
                hits.len(),
                show.join(", ")
            )))
        }
    }
}

fn open_store(store_dir: &Path) -> Result<Store> {
    if !store_dir.join("memory.db").exists() {
        return Err(Error::NotFound("store not initialized".to_string()));
    }
    Store::open(store_dir)
}

fn now_ns() -> i64 {
    amem_core::clock::now_nanos()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Below-water restore re-opens the upload series (P0-3: no permanent
    /// server hole), while above-water series are untouched.
    #[test]
    fn unquarantine_resets_below_water_marks() {
        let tmp = tempfile::tempdir().unwrap();
        let store = tmp.path().join("s");
        let db = Store::open(&store).unwrap();
        let device = amem_core::store::device_id(&store).unwrap();
        for (stream, idx) in [("shell", 0i64), ("shell", 1), ("notes", 0)] {
            let payload = serde_json::json!({"cmd": format!("c{idx}")});
            let rec = NewRecord {
                device: device.clone(),
                stream: stream.to_string(),
                ts_ns: 1_700_000_000_000_000_000 + idx,
                actor: "t:u".to_string(),
                kind: "command".to_string(),
                payload: payload.clone(),
                hash: amem_core::store::hash_payload(&payload.to_string()),
            };
            db.insert_pinned(&rec, idx).unwrap();
        }
        // Pretend a sync happened: marks above the shell hole.
        db.sync_state_put("https://s", "gen", 1).unwrap();
        db.sync_marks_set_up("https://s", &device, "shell", 1)
            .unwrap();
        db.sync_marks_set_up("https://s", &device, "notes", 0)
            .unwrap();
        drop(db);

        // Quarantine shell/0 (the hole), then restore it.
        quarantine(&store, Some(&format!("{device}/shell/0")), "test").unwrap();
        unquarantine(&store, &format!("{device}/shell/0")).unwrap();

        let db = Store::open(&store).unwrap();
        // shell series re-opened (re-upload is idempotent); notes untouched.
        assert_eq!(
            db.sync_marks_get("https://s", &device, "shell").unwrap(),
            (-1, -1)
        );
        assert_eq!(
            db.sync_marks_get("https://s", &device, "notes").unwrap().0,
            0
        );
        assert!(db.get_record(&device, "shell", 0).is_ok());
    }
}
