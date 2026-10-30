//! Sync envelope: the versioned, server-agnostic record wire format.
//!
//! `{id,device,stream,idx,hlc,ts_ns,actor,kind,payload,hash,v:1,
//! e2e_envelope:null,client_caps:[]}` plus flattened extras. Forward
//! compatible: unknown fields are retained, never rejected.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Hybrid logical clock, DERIVED deterministically from the record — no
/// migration, no clock state. `wall_ms` is capture time, `ctr` disambiguates
/// same-ms records within a series, `node` is the owner device. Total order
/// `(wall_ms, ctr, node)`; idempotent across re-uploads.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Hlc {
    /// Capture wall clock, Unix ms.
    pub wall_ms: i64,
    /// Series counter (low 16 bits of `idx`).
    pub ctr: u16,
    /// Owner device id.
    pub node: String,
}

/// Future-skew tolerance: envelopes dated more than this beyond local
/// wall time are quarantined, never applied.
pub const MAX_SKEW_MS: i64 = 300_000;

/// Derive the deterministic HLC for a record triple.
#[must_use]
pub fn derive_hlc(ts_ns: i64, idx: i64, device: &str) -> Hlc {
    Hlc {
        wall_ms: ts_ns.div_euclid(1_000_000),
        ctr: u16::try_from(idx.rem_euclid(65_536)).unwrap_or(u16::MAX),
        node: device.to_string(),
    }
}

/// Future skew in ms when `wall_ms` exceeds `now_ms + MAX_SKEW_MS`.
#[must_use]
pub fn future_skew_ms(wall_ms: i64, now_ms: i64) -> Option<i64> {
    let excess = wall_ms.saturating_sub(now_ms).saturating_sub(MAX_SKEW_MS);
    if excess > 0 { Some(excess) } else { None }
}

/// One syncable record. `extra` retains unknown future fields verbatim.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Envelope {
    /// External id (spool uuid, transcript stable id, note id).
    pub id: String,
    /// Owner device.
    pub device: String,
    /// Stream name.
    pub stream: String,
    /// Series index.
    pub idx: i64,
    /// Derived hybrid logical clock.
    pub hlc: Hlc,
    /// Capture time, Unix ns.
    pub ts_ns: i64,
    /// Actor string.
    pub actor: String,
    /// Record kind.
    pub kind: String,
    /// Canonical JSON payload.
    pub payload: serde_json::Value,
    /// blake3 hex over the canonical payload serialization.
    pub hash: String,
    /// Envelope version (1 in MVP; higher tolerated + retained).
    #[serde(default = "default_v")]
    pub v: u32,
    /// Reserved E2E encryption envelope (null in MVP).
    #[serde(default)]
    pub e2e_envelope: Option<serde_json::Value>,
    /// Sender capabilities (forward-compat substrate).
    #[serde(default)]
    pub client_caps: Vec<String>,
    /// Flattened extras + unknown future fields, retained verbatim.
    #[serde(flatten, default)]
    pub extra: HashMap<String, serde_json::Value>,
}

fn default_v() -> u32 {
    1
}

impl Envelope {
    /// Build the envelope for a stored record. The hash is verified
    /// against a fresh recomputation so a corrupt store row never syncs.
    ///
    /// # Errors
    ///
    /// Returns [`amem_core::error::Error::InvalidArguments`] when the stored
    /// hash does not match the payload (corrupt row — quarantined by caller).
    pub fn from_record(
        rec: &amem_core::store::Record,
        ext_id: &str,
    ) -> amem_core::error::Result<Self> {
        let payload: serde_json::Value = serde_json::from_str(&rec.payload)?;
        let recomputed = amem_core::store::hash_payload(&payload.to_string());
        if recomputed != rec.hash {
            return Err(amem_core::error::Error::InvalidArguments(format!(
                "{}/{}/{}: stored hash mismatch, refusing to sync corrupt row",
                rec.device, rec.stream, rec.idx
            )));
        }
        Ok(Self {
            id: ext_id.to_string(),
            device: rec.device.clone(),
            stream: rec.stream.clone(),
            idx: rec.idx,
            hlc: derive_hlc(rec.ts_ns, rec.idx, &rec.device),
            ts_ns: rec.ts_ns,
            actor: rec.actor.clone(),
            kind: rec.kind.clone(),
            payload,
            hash: rec.hash.clone(),
            v: 1,
            e2e_envelope: None,
            client_caps: Vec::new(),
            extra: HashMap::new(),
        })
    }

    /// Verify the envelope hash over its canonical payload serialization.
    #[must_use]
    pub fn verify_hash(&self) -> bool {
        amem_core::store::hash_payload(&self.payload.to_string()) == self.hash
    }

    /// Canonical wire bytes (raw form the server stores verbatim).
    #[must_use]
    pub fn to_raw(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|_| "{}".to_string())
    }

    /// Parse wire bytes, tolerating unknown fields.
    ///
    /// # Errors
    ///
    /// Returns [`amem_core::error::Error::Json`] on malformed JSON.
    pub fn from_raw(raw: &str) -> amem_core::error::Result<Self> {
        Ok(serde_json::from_str(raw)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hlc_derives_deterministically() {
        let a = derive_hlc(1_700_000_000_123_456_789, 70000, "d1");
        assert_eq!(a.wall_ms, 1_700_000_000_123);
        assert_eq!(a.ctr, 4464);
        assert_eq!(a, derive_hlc(1_700_000_000_123_456_789, 70000, "d1"));
    }

    #[test]
    fn skew_only_flags_the_future() {
        assert_eq!(future_skew_ms(1000, 1000), None);
        assert_eq!(future_skew_ms(0, 1_000_000_000), None);
        assert!(future_skew_ms(1_000_000_000, 1000).is_some());
    }
}
