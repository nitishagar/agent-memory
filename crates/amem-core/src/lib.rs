//! Agent memory recorder: local-first shell + transcript memory over MCP.
//!
//! Storage, ingestion, redaction, and git context live here. The CLI
//! (`amem-cli`) and MCP server call into this crate; nothing here touches
//! the network.

/// Crate version, for store schema compatibility checks.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
