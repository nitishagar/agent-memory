//! Sync-adjacent CLI: conflict branches, quarantine, un-quarantine.
//!
//! Ids accept either a `device/stream/idx` triple or an external id
//! (exact; ambiguous across devices refuses with the candidate list).

use std::path::Path;

use amem_core::error::{Error, Result};
use amem_core::store::{NewRecord, Store};

/// Resolve a CLI id to a triple: `device/stream/idx` or an exact external id.
pub fn resolve_triple(db: &Store, id: &str) -> Result<(String, String, i64)> {
    if let Some((device, rest)) = id.split_once('/') {
        if let Some((stream, idx_s)) = rest.split_once('/') {
            if let Ok(idx) = idx_s.parse::<i64>() {
                return Ok((device.to_string(), stream.to_string(), idx));
            }
        }
    }
    let hits = db.resolve_external_any(id, None)?;
    match hits.len() {
        0 => Err(Error::NotFound(id.to_string())),
        1 => {
            let (device, stream, idx, _) = hits.into_iter().next().unwrap();
            Ok((device, stream, idx))
        }
        _ => Err(Error::Ambiguous(format!(
            "{id}: in {} places; use device/stream/idx",
            hits.len()
        ))),
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
    let envelope = amem_sync::envelope::Envelope::from_record(&rec, &ext)?.to_raw();
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
            let branch_id = db.branch_put(
                &env.id,
                &env.stream,
                &target.device,
                target.idx,
                &occupant.hash,
                &target.envelope,
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

/// Find a quarantine row by triple or by envelope id.
fn find_quarantined(db: &Store, id: &str) -> Result<amem_core::store::Quarantined> {
    if let Some((device, rest)) = id.split_once('/') {
        if let Some((stream, idx_s)) = rest.split_once('/') {
            if let Ok(idx) = idx_s.parse::<i64>() {
                return db.quarantine_get(device, stream, idx);
            }
        }
    }
    let mut hits = Vec::new();
    for q in db.quarantine_list()? {
        let env_id = serde_json::from_str::<serde_json::Value>(&q.envelope)
            .ok()
            .and_then(|v| v.get("id").and_then(|i| i.as_str()).map(str::to_string))
            .unwrap_or_default();
        if env_id == id {
            hits.push(q);
        }
    }
    match hits.len() {
        0 => Err(Error::NotFound(id.to_string())),
        1 => Ok(hits.into_iter().next().unwrap()),
        _ => Err(Error::Ambiguous(format!(
            "{id}: quarantined {} times; use device/stream/idx",
            hits.len()
        ))),
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
