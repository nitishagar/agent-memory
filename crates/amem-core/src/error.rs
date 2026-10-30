//! Typed errors shared by the CLI, store, and (later) MCP surface.
//!
//! The five primary codes mirror the memory-tool contract; internal failures
//! carry their source for logs while surfacing a stable code to callers.

use thiserror::Error;

/// All failures produced by `amem-core`.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    /// Requested record does not exist.
    #[error("not found: {0}")]
    NotFound(String),
    /// Query matched more than one record and needs narrowing.
    #[error("ambiguous: {0}")]
    Ambiguous(String),
    /// Unguarded write collided with an existing id.
    #[error("already exists: {0}")]
    AlreadyExists(String),
    /// Guarded write lost to a concurrent change; re-read and retry.
    #[error("conflict: {0}")]
    Conflict(String),
    /// Caller-side usage error (bad args, bad endpoint, corrupt input file).
    #[error("invalid arguments: {0}")]
    InvalidArguments(String),
    /// The operation may have landed (e.g. residual lock contention).
    /// Inspect state before retrying; never blindly resubmit.
    #[error("outcome unknown (may have landed): {0}")]
    OutcomeUnknown(String),
    /// Sync-subsystem failure (transport, protocol, server).
    #[error("sync error: {0}")]
    Sync(String),
    /// Operation exceeded its time budget.
    #[error("timed out: {0}")]
    Timeout(String),
    /// OS / filesystem failure.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    /// `SQLite` failure.
    #[error("database error: {0}")]
    Db(#[from] rusqlite::Error),
    /// JSON encode/decode failure.
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
}

impl Error {
    /// Stable machine-readable code for the MCP/CLI surface.
    #[must_use]
    pub fn code(&self) -> &'static str {
        match self {
            Self::NotFound(_) => "NOT_FOUND",
            Self::Ambiguous(_) => "AMBIGUOUS",
            Self::AlreadyExists(_) => "ALREADY_EXISTS",
            Self::Conflict(_) => "CONFLICT",
            Self::InvalidArguments(_) => "INVALID_ARGUMENTS",
            Self::OutcomeUnknown(_) => "OUTCOME_UNKNOWN",
            Self::Sync(_) => "SYNC_ERROR",
            Self::Timeout(_) => "TIMEOUT",
            Self::Io(_) => "IO_ERROR",
            Self::Db(_) => "DB_ERROR",
            Self::Json(_) => "JSON_ERROR",
        }
    }
}

/// Crate result alias.
pub type Result<T> = std::result::Result<T, Error>;
