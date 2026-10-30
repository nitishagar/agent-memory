//! Protocol DTOs shared by the client, the simulated transport, and the
//! HTTP server: one definition, three bindings.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Protocol version served.
pub const PROTOCOL_V1: &str = "v1";

/// `GET /v1/status`: server generation + per-series high-water marks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatusDto {
    /// Server generation UUID (changes on wipe/re-init).
    pub generation: String,
    /// `device -> stream -> max idx`.
    pub streams: BTreeMap<String, BTreeMap<String, i64>>,
}

/// `POST /v1/records` body: RAW envelopes (unknown fields retained).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PostBody {
    /// Raw envelope JSON strings.
    pub records: Vec<String>,
    /// Sender capabilities.
    #[serde(default)]
    pub client_caps: Vec<String>,
}

/// One conflicting triple reported by `POST /v1/records`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConflictDto {
    /// Owner device.
    pub device: String,
    /// Stream name.
    pub stream: String,
    /// Series index.
    pub idx: i64,
}

/// `POST /v1/records` outcome: atomic + idempotent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PostOutcome {
    /// Rows newly inserted.
    pub accepted: u64,
    /// Re-posts of identical rows (no-op).
    pub duplicates: u64,
    /// Same triple, different hash (server kept first).
    pub conflicts: Vec<ConflictDto>,
}

/// `GET /v1/next` page: raw envelopes ascending from `start`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NextDto {
    /// Raw envelope JSON strings, `idx` ascending.
    pub records: Vec<String>,
}

/// Upload/download page size (atuin matches at 100).
pub const PAGE_SIZE: i64 = 100;

/// Largest single record the server accepts (1 MiB raw JSON).
pub const MAX_RECORD_BYTES: usize = 1024 * 1024;

/// Largest POST batch the server accepts.
pub const MAX_BATCH: usize = 100;
