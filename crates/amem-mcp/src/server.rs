//! `rmcp` stdio server: thin tool wrappers over [`Mem`](crate::Mem).
//!
//! Domain failures are tool-level errors (`isError`) carrying the typed
//! `CODE: detail | retry: guidance` payload, so agents can self-correct
//! without a protocol round-trip failure.

use std::path::Path;

use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{CallToolResult, ContentBlock};
use rmcp::{ServiceExt, schemars, tool, tool_handler, tool_router, transport::stdio};

use crate::{Format, Mem, render_error};

fn ok_text(text: String) -> CallToolResult {
    CallToolResult::success(vec![ContentBlock::text(text)])
}

fn tool_err(text: String) -> CallToolResult {
    CallToolResult::error(vec![ContentBlock::text(text)])
}

/// Agent-memory MCP server over one store dir.
#[derive(Debug, Clone)]
pub struct AmemServer {
    mem: Mem,
}

impl AmemServer {
    #[must_use]
    pub const fn new(mem: Mem) -> Self {
        Self { mem }
    }
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
struct SearchArgs {
    /// Substring to find in stored payloads.
    query: String,
    /// Stream filter, exact unless it contains `%` (`%` = all streams).
    stream: Option<String>,
    /// Max hits (1-200).
    limit: Option<i64>,
    /// `text` (default) or `json`.
    format: Option<String>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
struct ReadArgs {
    /// External id (exact or unique prefix).
    id: String,
    /// Narrow to one stream.
    stream: Option<String>,
    /// `text` (default) or `json`.
    format: Option<String>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
struct ListArgs {
    /// Stream filter, exact unless it contains `%` (`%` = all streams).
    stream: Option<String>,
    /// Max records (1-200).
    limit: Option<i64>,
    /// Skip this many newest records.
    offset: Option<i64>,
    /// `text` (default) or `json`.
    format: Option<String>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
struct WriteArgs {
    /// Note id; minted when absent. Must not belong to a recorder stream.
    id: Option<String>,
    /// Note title.
    title: Option<String>,
    /// Note body (required, non-empty).
    text: String,
    /// Checksum from `mem_read`; required to overwrite an existing note.
    checksum: Option<String>,
    /// `text` (default) or `json`.
    format: Option<String>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
struct DeleteArgs {
    /// Note id (exact or unique prefix).
    id: String,
    /// Narrow to one stream.
    stream: Option<String>,
    /// Checksum from `mem_read` (mandatory guard).
    checksum: String,
    /// `text` (default) or `json`.
    format: Option<String>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
struct ContextArgs {
    /// Max events in the brief (1-500).
    limit: Option<i64>,
    /// `text` (default) or `json`.
    format: Option<String>,
}

#[tool_router]
impl AmemServer {
    #[tool(description = "Substring-search memory (shell, transcripts, notes), newest first.")]
    fn mem_search(&self, Parameters(a): Parameters<SearchArgs>) -> CallToolResult {
        let format = Format::parse(a.format.as_deref());
        let format = match format {
            Ok(f) => f,
            Err(e) => return tool_err(render_error(&e, Format::Text)),
        };
        match self
            .mem
            .search(&a.query, a.stream.as_deref(), a.limit.unwrap_or(20), format)
        {
            Ok(out) => ok_text(out),
            Err(e) => tool_err(render_error(&e, format)),
        }
    }

    #[tool(description = "Read one full memory record by id, with its checksum guard.")]
    fn mem_read(&self, Parameters(a): Parameters<ReadArgs>) -> CallToolResult {
        let format = match Format::parse(a.format.as_deref()) {
            Ok(f) => f,
            Err(e) => return tool_err(render_error(&e, Format::Text)),
        };
        match self.mem.read(&a.id, a.stream.as_deref(), format) {
            Ok(out) => ok_text(out),
            Err(e) => tool_err(render_error(&e, format)),
        }
    }

    #[tool(description = "List memory records newest-first with pagination.")]
    fn mem_list(&self, Parameters(a): Parameters<ListArgs>) -> CallToolResult {
        let format = match Format::parse(a.format.as_deref()) {
            Ok(f) => f,
            Err(e) => return tool_err(render_error(&e, Format::Text)),
        };
        match self.mem.list(
            a.stream.as_deref(),
            a.limit.unwrap_or(50),
            a.offset.unwrap_or(0),
            format,
        ) {
            Ok(out) => ok_text(out),
            Err(e) => tool_err(render_error(&e, format)),
        }
    }

    #[tool(
        description = "Create a note, or overwrite one with its mem_read checksum. Recorder streams are read-only."
    )]
    fn mem_write(&self, Parameters(a): Parameters<WriteArgs>) -> CallToolResult {
        let format = match Format::parse(a.format.as_deref()) {
            Ok(f) => f,
            Err(e) => return tool_err(render_error(&e, Format::Text)),
        };
        match self.mem.write(
            a.id.as_deref(),
            a.title.as_deref().unwrap_or(""),
            &a.text,
            a.checksum.as_deref(),
            format,
        ) {
            Ok(out) => ok_text(out),
            Err(e) => tool_err(render_error(&e, format)),
        }
    }

    #[tool(
        description = "Delete a note. Requires the mem_read checksum; recorder streams are read-only."
    )]
    fn mem_delete(&self, Parameters(a): Parameters<DeleteArgs>) -> CallToolResult {
        let format = match Format::parse(a.format.as_deref()) {
            Ok(f) => f,
            Err(e) => return tool_err(render_error(&e, Format::Text)),
        };
        match self
            .mem
            .delete(&a.id, a.stream.as_deref(), &a.checksum, format)
        {
            Ok(out) => ok_text(out),
            Err(e) => tool_err(render_error(&e, format)),
        }
    }

    #[tool(description = "Chronological brief of recent activity: the 'what did I do' answer.")]
    fn mem_context(&self, Parameters(a): Parameters<ContextArgs>) -> CallToolResult {
        let format = match Format::parse(a.format.as_deref()) {
            Ok(f) => f,
            Err(e) => return tool_err(render_error(&e, Format::Text)),
        };
        match self.mem.context(a.limit.unwrap_or(100), format) {
            Ok(out) => ok_text(out),
            Err(e) => tool_err(render_error(&e, format)),
        }
    }
}

#[tool_handler(
    name = "agent-memory",
    version = "0.1.0",
    instructions = "Local-first memory for coding agents: search shell history and agent transcripts, keep guarded notes."
)]
impl rmcp::ServerHandler for AmemServer {}

fn io_other<E: std::fmt::Display>(e: E) -> amem_core::error::Error {
    amem_core::error::Error::Io(std::io::Error::other(e.to_string()))
}

/// Serve MCP over stdio until the client disconnects. Synchronous wrapper:
/// the CLI stays runtime-free; the current-thread runtime lives here.
///
/// # Errors
///
/// Returns [`amem_core::error::Error::NotFound`] on an uninitialized store,
/// [`amem_core::error::Error::Io`] on transport failure.
pub fn serve_stdio(store_dir: &Path) -> amem_core::error::Result<()> {
    let mem = Mem::open(store_dir)?;
    let server = AmemServer::new(mem);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    rt.block_on(async move {
        let service = server.serve(stdio()).await.map_err(io_other)?;
        service.waiting().await.map_err(io_other)?;
        Ok(())
    })
}
