//! Agent memory recorder: local-first shell + transcript memory over MCP.
//!
//! Storage, ingestion, redaction, and git context live here. The CLI
//! (`amem-cli`) and MCP server call into this crate; nothing here touches
//! the network.

pub mod clock;
pub mod error;
pub mod git;
pub mod ingest;
pub mod redact;
pub mod spool;
pub mod store;
pub mod transcripts;

/// Crate version, for store schema compatibility checks.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
