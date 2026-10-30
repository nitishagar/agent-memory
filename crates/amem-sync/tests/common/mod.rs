//! Shared fault-harness helpers: seeding, union dumps, client wiring.
//!
//! Each integration target compiles this module separately; not every
//! target uses every helper.
#![allow(dead_code)]

use std::path::Path;

use amem_core::store::{self, NewRecord};

/// Seed `n` shell records; returns the device id.
pub fn seed_shell(dir: &Path, n: i64, prefix: &str) -> String {
    let mut db = store::Store::open(dir).unwrap();
    let device = store::device_id(dir).unwrap();
    for i in 0..n {
        let payload = serde_json::json!({"cmd": format!("{prefix}-{i}"), "exit": 0});
        let rec = NewRecord {
            device: device.clone(),
            stream: "shell".to_string(),
            ts_ns: 1_700_000_000_000_000_000 + i,
            actor: "t:u".to_string(),
            kind: "command".to_string(),
            payload: payload.clone(),
            hash: store::hash_payload(&payload.to_string()),
        };
        db.insert_mapped(&format!("{prefix}-spool-{i}"), &rec)
            .unwrap();
    }
    device
}

/// Sorted dump of every record across `devices`: `device|stream|idx|hash|payload`.
pub fn dump(dir: &Path, devices: &[String]) -> Vec<String> {
    let db = store::Store::open(dir).unwrap();
    let mut out = Vec::new();
    for d in devices {
        for r in db.list_all(d).unwrap() {
            out.push(format!(
                "{}|{}|{}|{}|{}",
                r.device, r.stream, r.idx, r.hash, r.payload
            ));
        }
    }
    out.sort();
    out
}

/// Total record count across `devices`.
pub fn count(dir: &Path, devices: &[String]) -> usize {
    dump(dir, devices).len()
}
