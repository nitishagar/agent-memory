//! MCP tools over the agent-memory store (`mem_search/read/list/write/delete/context`).
//!
//! Text-default + JSON (`format` arg), typed errors with retry guidance.
//! Every method opens a fresh [`Store`](amem_core::store::Store): reads are
//! side-effect-free, and writers fail fast (`busy_timeout(0)`) with
//! `OUTCOME_UNKNOWN` instead of blocking behind a bulk ingest.

pub mod server;

use std::path::{Path, PathBuf};

use amem_core::display::{first_line, render_hit, sanitize};
use amem_core::error::{Error, Result};
use amem_core::store::{self, NewRecord, Record};

/// Output format for tool results and errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    Text,
    Json,
}

impl Format {
    /// Parse the `format` arg; anything but `text`/`json` is a usage error.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidArguments`] on an unknown format name.
    pub fn parse(raw: Option<&str>) -> Result<Self> {
        match raw {
            None | Some("text") => Ok(Self::Text),
            Some("json") => Ok(Self::Json),
            Some(other) => Err(Error::InvalidArguments(format!(
                "format must be 'text' or 'json', got '{other}'"
            ))),
        }
    }
}

/// Retry guidance per error code: every failure tells the agent its next move.
#[must_use]
pub fn retry_guidance(e: &Error) -> &'static str {
    match e {
        Error::NotFound(_) => "check the id (mem_list to browse); ids are exact or unique prefixes",
        Error::Ambiguous(_) => "use a longer id prefix or pass stream to narrow",
        Error::AlreadyExists(_) => {
            "pass the current checksum (from mem_read) to overwrite, or pick another id"
        }
        Error::Conflict(_) => "re-read with mem_read to get the current checksum, then resubmit",
        Error::OutcomeUnknown(_) => {
            "re-read the record first: if your change is present it applied — do not resend blindly; else resubmit"
        }
        Error::InvalidArguments(_) => "fix the request and retry",
        _ => "retry once; if it persists inspect the store with `amem doctor`",
    }
}

/// Render a typed error for the wire: `CODE: detail | retry: guidance` in
/// text mode, `{"error": {code, message, retry}}` in JSON mode.
#[must_use]
pub fn render_error(e: &Error, format: Format) -> String {
    let retry = retry_guidance(e);
    match format {
        Format::Text => format!("{}: {e} | retry: {retry}", e.code()),
        Format::Json => serde_json::json!({
            "error": {"code": e.code(), "message": e.to_string(), "retry": retry},
        })
        .to_string(),
    }
}

/// Tool backend: stateless over a store dir, one fresh connection per call.
#[derive(Debug, Clone)]
pub struct Mem {
    store_dir: PathBuf,
    device: String,
}

impl Mem {
    /// Open the backend on an initialized store.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] when the store is not initialized.
    pub fn open(store_dir: &Path) -> Result<Self> {
        if !store_dir.join("memory.db").exists() {
            return Err(Error::NotFound(
                "store not initialized; run `amem init` first".to_string(),
            ));
        }
        let device = store::device_id(store_dir)?;
        Ok(Self {
            store_dir: store_dir.to_path_buf(),
            device,
        })
    }

    fn read_store(&self) -> Result<store::Store> {
        store::Store::open_readonly(&self.store_dir)
    }

    fn write_store(&self) -> Result<store::Store> {
        let store = store::Store::open(&self.store_dir)?;
        store.set_busy_timeout(0)?;
        Ok(store)
    }

    /// `mem_search`: substring search over payloads, newest first.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Db`] on store failure.
    pub fn search(
        &self,
        query: &str,
        stream: Option<&str>,
        limit: i64,
        format: Format,
    ) -> Result<String> {
        let db = self.read_store()?;
        let hits = db.search_stream(
            &self.device,
            stream.unwrap_or("%"),
            query,
            limit.clamp(1, 200),
        )?;
        Ok(match format {
            Format::Text => {
                if hits.is_empty() {
                    "no matches".to_string()
                } else {
                    hits.iter().map(render_hit).collect::<Vec<_>>().join("\n")
                }
            }
            Format::Json => serde_json::json!({
                "results": hits.iter().map(record_json).collect::<Vec<_>>(),
            })
            .to_string(),
        })
    }

    /// `mem_read`: one full record by external id (exact or unique prefix).
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`], [`Error::Ambiguous`], or [`Error::Db`].
    pub fn read(&self, id: &str, stream: Option<&str>, format: Format) -> Result<String> {
        let db = self.read_store()?;
        let (stream, idx) = db.resolve_external(&self.device, id, stream)?;
        let rec = db.get_record(&self.device, &stream, idx)?;
        Ok(match format {
            Format::Text => {
                let payload: serde_json::Value =
                    serde_json::from_str(&rec.payload).unwrap_or(serde_json::Value::Null);
                format!(
                    "id: {id}\nstream: {}\nidx: {}\nts_ns: {}\nactor: {}\nkind: {}\nchecksum: {}\nbody:\n{}",
                    sanitize(&rec.stream),
                    rec.idx,
                    rec.ts_ns,
                    sanitize(&rec.actor),
                    sanitize(&rec.kind),
                    rec.hash,
                    sanitize(&payload.to_string()),
                )
            }
            Format::Json => record_json(&rec).to_string(),
        })
    }

    /// `mem_list`: newest-first listing with offset pagination.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Db`] on store failure.
    pub fn list(
        &self,
        stream: Option<&str>,
        limit: i64,
        offset: i64,
        format: Format,
    ) -> Result<String> {
        let db = self.read_store()?;
        let hits = db.list_stream(
            &self.device,
            stream.unwrap_or("%"),
            limit.clamp(1, 200),
            offset.max(0),
        )?;
        Ok(match format {
            Format::Text => {
                if hits.is_empty() {
                    "no records".to_string()
                } else {
                    hits.iter().map(render_hit).collect::<Vec<_>>().join("\n")
                }
            }
            Format::Json => serde_json::json!({
                "results": hits.iter().map(record_json).collect::<Vec<_>>(),
                "offset": offset.max(0),
            })
            .to_string(),
        })
    }
}

impl Mem {
    /// `mem_write`: create a note, or guarded-overwrite one.
    ///
    /// No `id` mints one. No `checksum` over an existing id is
    /// [`Error::AlreadyExists`]; a `checksum` over a missing id is
    /// [`Error::NotFound`]; a stale `checksum` is [`Error::Conflict`].
    /// Only the `notes` stream is writable — recorder streams refuse with
    /// [`Error::InvalidArguments`].
    ///
    /// # Errors
    ///
    /// Returns the typed errors above, [`Error::OutcomeUnknown`] on lock
    /// contention, or [`Error::Db`].
    pub fn write(
        &self,
        id: Option<&str>,
        title: &str,
        text: &str,
        checksum: Option<&str>,
        format: Format,
    ) -> Result<String> {
        if text.trim().is_empty() {
            return Err(Error::InvalidArguments(
                "note text must not be empty".to_string(),
            ));
        }
        if id.is_some_and(str::is_empty) {
            return Err(Error::InvalidArguments(
                "note id must not be empty".to_string(),
            ));
        }
        let mut db = self.write_store()?;
        let id = id.map_or_else(|| format!("note-{}", uuid::Uuid::new_v4()), str::to_string);
        let existing = match db.resolve_external(&self.device, &id, Some("notes")) {
            Ok((_, idx)) => Some(idx),
            Err(Error::NotFound(_)) => None,
            Err(e) => return Err(e),
        };
        // An id owned by a recorder stream is not writable under any guard.
        if existing.is_none() {
            if let Ok((stream, _)) = db.resolve_external(&self.device, &id, None) {
                return Err(Error::InvalidArguments(format!(
                    "id {id} belongs to recorder stream {stream}; only notes are writable"
                )));
            }
        }
        let out = match (existing, checksum) {
            (None, Some(_)) => {
                return Err(Error::NotFound(format!(
                    "{id}: no such note; drop checksum to create"
                )));
            }
            (None, None) => {
                let (canonical, hash) = store::note_payload(&id, title, text);
                let rec = NewRecord {
                    device: self.device.clone(),
                    stream: "notes".to_string(),
                    ts_ns: amem_core::clock::now_nanos(),
                    actor: "mcp".to_string(),
                    kind: "note".to_string(),
                    payload: serde_json::from_str(&canonical)?,
                    hash,
                };
                let (idx, inserted) = db
                    .insert_mapped(&id, &rec)
                    .map_err(|e| store::busy_to_unknown(e, "note create"))?;
                if !inserted {
                    return Err(Error::AlreadyExists(format!(
                        "{id}: created concurrently; re-read and resubmit with checksum"
                    )));
                }
                (idx, rec.hash, true)
            }
            (Some(_), None) => {
                return Err(Error::AlreadyExists(format!(
                    "{id}: exists; pass its checksum (from mem_read) to overwrite"
                )));
            }
            (Some(idx), Some(sum)) => {
                let hash = db
                    .note_update_guarded(&self.device, idx, sum, title, text)
                    .map_err(|e| store::busy_to_unknown(e, "note update"))?;
                (idx, hash, false)
            }
        };
        let (idx, hash, created) = out;
        Ok(match format {
            Format::Text => format!(
                "{} notes#{idx} checksum {hash}",
                if created { "created" } else { "updated" }
            ),
            Format::Json => serde_json::json!({
                "id": id,
                "stream": "notes",
                "idx": idx,
                "checksum": hash,
                "created": created,
            })
            .to_string(),
        })
    }

    /// `mem_delete`: guarded note delete. The checksum is mandatory.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`], [`Error::Ambiguous`],
    /// [`Error::InvalidArguments`] for recorder streams,
    /// [`Error::Conflict`] on a stale guard, [`Error::OutcomeUnknown`]
    /// on lock contention, or [`Error::Db`].
    pub fn delete(
        &self,
        id: &str,
        stream: Option<&str>,
        checksum: &str,
        format: Format,
    ) -> Result<String> {
        let mut db = self.write_store()?;
        let (found_stream, idx) = db.resolve_external(&self.device, id, stream)?;
        if found_stream != "notes" {
            return Err(Error::InvalidArguments(format!(
                "stream {found_stream} is append-only; only notes are deletable"
            )));
        }
        db.note_delete_guarded(&self.device, idx, checksum)
            .map_err(|e| store::busy_to_unknown(e, "note delete"))?;
        Ok(match format {
            Format::Text => format!("deleted notes#{idx}"),
            Format::Json => serde_json::json!({
                "id": id, "stream": "notes", "idx": idx, "deleted": true,
            })
            .to_string(),
        })
    }

    /// `mem_context`: a chronological brief of recent activity across all
    /// streams — the "what did I do" answer. Oldest first, one line each.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Db`] on store failure.
    pub fn context(&self, limit: i64, format: Format) -> Result<String> {
        let db = self.read_store()?;
        let mut hits = db.list_stream(&self.device, "%", limit.clamp(1, 500), 0)?;
        hits.reverse();
        Ok(match format {
            Format::Text => {
                if hits.is_empty() {
                    "no recorded activity yet".to_string()
                } else {
                    let mut out = format!("# memory brief ({} events)\n", hits.len());
                    for rec in &hits {
                        out.push_str(&render_hit(rec));
                        out.push('\n');
                    }
                    out.pop();
                    out
                }
            }
            Format::Json => serde_json::json!({
                "events": hits.iter().map(record_json).collect::<Vec<_>>(),
            })
            .to_string(),
        })
    }
}

/// One record as JSON (checksums included: readers need them as guards).
fn record_json(rec: &Record) -> serde_json::Value {
    let payload: serde_json::Value =
        serde_json::from_str(&rec.payload).unwrap_or(serde_json::Value::Null);
    serde_json::json!({
        "stream": rec.stream,
        "idx": rec.idx,
        "ts_ns": rec.ts_ns,
        "actor": rec.actor,
        "kind": rec.kind,
        "checksum": rec.hash,
        "payload": payload,
        "summary": first_line(&render_hit(rec)),
    })
}
