//! Transcript ingestion: agent files → redacted `transcript:*` records.
//!
//! Each source parses under the shared pass deadline; per-file isolation
//! holds throughout (one bad file warns, never aborts). Events dedupe by
//! [`stable_id`] through [`Store::insert_mapped`], so re-ingesting an
//! unchanged file is a no-op. Text and project paths redact before
//! storage, exactly like shell commands.

use std::path::Path;
use std::time::Instant;

use crate::clock::now_nanos;
use crate::error::{Error, Result};
use crate::redact::Redactor;
use crate::store::{self, NewRecord, Store};
use crate::transcripts::{ParsedEvent, Source, Warning, find_jsonl, stable_id};

/// Per-source transcript ingest counters.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct TranscriptStats {
    /// Files parsed.
    pub files: u64,
    /// Events parsed.
    pub events: u64,
    /// New records committed.
    pub ingested: u64,
    /// Events already stored (re-ingest no-op).
    pub skipped_dup: u64,
}

/// Ingest one source from `root` (directory for JSONL sources, the
/// database file for `SQLite` sources). Missing roots are "not
/// installed", not errors.
///
/// # Errors
///
/// Returns [`Error::Timeout`] when `deadline` expires, [`Error::Db`] on
/// store failure. Unparsable files and rows degrade to warnings.
pub fn ingest_source(
    store: &mut Store,
    device: &str,
    actor: &str,
    redactor: &Redactor,
    source: Source,
    root: &Path,
    deadline: Instant,
) -> Result<(TranscriptStats, Vec<Warning>)> {
    let mut stats = TranscriptStats::default();
    let mut warnings = Vec::new();
    if !root.exists() {
        return Ok((stats, warnings));
    }
    let files: Vec<std::path::PathBuf> = match source {
        Source::Claude | Source::Pi | Source::Codex => find_jsonl(root, 4),
        Source::Cursor | Source::Opencode | Source::Zcode => {
            if root.is_file() {
                vec![root.to_path_buf()]
            } else {
                warnings.push(Warning {
                    path: root.to_path_buf(),
                    reason: format!("{}: expected a database file", source.name()),
                });
                Vec::new()
            }
        }
    };
    for file in files {
        let parsed: Result<(Vec<ParsedEvent>, Vec<Warning>)> = match source {
            Source::Claude => super::claude::parse_file(&file, deadline).map(|(e, w, _)| (e, w)),
            Source::Pi => super::pi::parse_file(&file, deadline).map(|(e, w, _)| (e, w)),
            Source::Codex => super::codex::parse_file(&file, deadline).map(|(e, w, _)| (e, w)),
            Source::Cursor => super::cursor::parse_db(&file, deadline),
            Source::Opencode => super::opencode::parse_db(&file, source, deadline),
            Source::Zcode => super::zcode::parse_db(&file, deadline),
        };
        let (events, mut file_warnings) = match parsed {
            Ok(pair) => pair,
            Err(Error::Timeout(_)) => {
                return Err(Error::Timeout(format!(
                    "transcript pass exceeded deadline on {}",
                    file.display()
                )));
            }
            Err(e) => {
                warnings.push(Warning {
                    path: file.clone(),
                    reason: format!("unreadable: {e}"),
                });
                continue;
            }
        };
        warnings.append(&mut file_warnings);
        stats.files += 1;
        stats.events += events.len() as u64;
        for parsed in events {
            if commit_event(store, device, actor, redactor, source, &parsed)? {
                stats.ingested += 1;
            } else {
                stats.skipped_dup += 1;
            }
            if Instant::now() > deadline {
                return Err(Error::Timeout(format!(
                    "transcript pass exceeded deadline committing {}",
                    file.display()
                )));
            }
        }
    }
    Ok((stats, warnings))
}

/// Commit one event; returns false when already stored.
fn commit_event(
    store: &mut Store,
    device: &str,
    actor: &str,
    redactor: &Redactor,
    source: Source,
    parsed: &ParsedEvent,
) -> Result<bool> {
    let event = &parsed.event;
    let (text, text_redacted) = redactor.redact(&event.text);
    let (project, project_redacted) = redactor.redact(&event.project);
    // Session/tool/model are free-form transcript strings: a crafted
    // `tool_use.name` must not smuggle a secret past redaction. Normal
    // ids never match secret patterns, so identity joins are unaffected.
    let (session, session_redacted) = redactor.redact(&event.session_id);
    let (tool, tool_redacted) = event.tool_name.as_deref().map_or_else(
        || (None, false),
        |t| {
            let (r, hit) = redactor.redact(t);
            (Some(r), hit)
        },
    );
    let (model, model_redacted) = event.model.as_deref().map_or_else(
        || (None, false),
        |m| {
            let (r, hit) = redactor.redact(m);
            (Some(r), hit)
        },
    );
    let payload = serde_json::json!({
        "source": source.name(),
        "session": session,
        "project": project,
        "role": event.role.name(),
        "text": text,
        "text_len": event.text.len(),
        "tool": tool,
        "model": model,
        "redacted": text_redacted || project_redacted || session_redacted || tool_redacted || model_redacted,
    });
    let canonical = payload.to_string();
    let rec = NewRecord {
        device: device.to_string(),
        stream: format!("transcript:{}", source.name()),
        ts_ns: event
            .ts_ms
            .map_or_else(now_nanos, |ms| ms.saturating_mul(1_000_000)),
        actor: actor.to_string(),
        kind: "transcript".to_string(),
        hash: store::hash_payload(&canonical),
        payload,
    };
    let id = stable_id(source, &event.session_id, &parsed.seq);
    let (_, inserted) = store.insert_mapped(&id, &rec)?;
    Ok(inserted)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transcripts::cursor;

    const CLAUDE_BASIC: &str = include_str!("../../tests/fixtures/transcripts/claude_basic.jsonl");
    const CURSOR_BASIC: &str = include_str!("../../tests/fixtures/transcripts/cursor_basic.sql");

    fn harness() -> (tempfile::TempDir, Store, String) {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open(&tmp.path().join("store")).unwrap();
        let device = store::device_id(&tmp.path().join("store")).unwrap();
        (tmp, store, device)
    }

    #[test]
    fn jsonl_source_ingests_and_redacts() {
        let (tmp, mut store, device) = harness();
        let root = tmp.path().join("claude");
        std::fs::create_dir_all(root.join("slug")).unwrap();
        std::fs::write(root.join("slug").join("sess.jsonl"), CLAUDE_BASIC).unwrap();
        // A secret in a second session must redact before storage.
        let secret_line = serde_json::json!({"type":"user","message":{"role":"user","content":"deploy token=topsecret9"},
            "uuid":"99999999-9999-4999-8999-999999999999","timestamp":"2026-10-30T00:00:09.000Z",
            "sessionId":"bbbbbbbb-bbbb-4bbb-bbbb-bbbbbbbbbbbb","cwd":"/home/u/proj"}).to_string();
        std::fs::write(
            root.join("slug").join("sess2.jsonl"),
            format!("{secret_line}\n"),
        )
        .unwrap();
        let redactor = Redactor::new();
        let (stats, warnings) = ingest_source(
            &mut store,
            &device,
            "t:u",
            &redactor,
            Source::Claude,
            &root,
            Instant::now() + crate::transcripts::PASS_DEADLINE,
        )
        .unwrap();
        assert!(warnings.is_empty(), "{warnings:?}");
        assert_eq!(stats.files, 2);
        assert_eq!(stats.events, 6);
        assert_eq!(stats.ingested, 6);
        assert_eq!(store.count_stream(&device, "transcript:claude").unwrap(), 6);
        // Re-ingest: exactly-once.
        let (stats2, _) = ingest_source(
            &mut store,
            &device,
            "t:u",
            &redactor,
            Source::Claude,
            &root,
            Instant::now() + crate::transcripts::PASS_DEADLINE,
        )
        .unwrap();
        assert_eq!(stats2.ingested, 0);
        assert_eq!(stats2.skipped_dup, 6);
        assert_eq!(store.count_stream(&device, "transcript:claude").unwrap(), 6);
        // The secret never reached the store.
        let db = Store::open(&tmp.path().join("store")).unwrap();
        for idx in 0..6 {
            let rec = db.get_record(&device, "transcript:claude", idx).unwrap();
            assert!(!rec.payload.contains("topsecret9"), "{}", rec.payload);
        }
    }

    #[test]
    fn sqlite_source_ingests() {
        let (tmp, mut store, device) = harness();
        let db_path = tmp.path().join("state.vscdb");
        rusqlite::Connection::open(&db_path)
            .unwrap()
            .execute_batch(CURSOR_BASIC)
            .unwrap();
        let redactor = Redactor::new();
        let (stats, warnings) = ingest_source(
            &mut store,
            &device,
            "t:u",
            &redactor,
            Source::Cursor,
            &db_path,
            Instant::now() + crate::transcripts::PASS_DEADLINE,
        )
        .unwrap();
        assert!(warnings.is_empty(), "{warnings:?}");
        assert_eq!((stats.files, stats.events, stats.ingested), (1, 3, 3));
    }

    #[test]
    fn missing_root_is_not_installed() {
        let (_tmp, mut store, device) = harness();
        let redactor = Redactor::new();
        let (stats, warnings) = ingest_source(
            &mut store,
            &device,
            "t:u",
            &redactor,
            Source::Pi,
            std::path::Path::new("/nonexistent-pi-root"),
            Instant::now() + crate::transcripts::PASS_DEADLINE,
        )
        .unwrap();
        assert_eq!(stats, TranscriptStats::default());
        assert!(warnings.is_empty(), "{warnings:?}");
        assert!(
            cursor::parse_db(
                std::path::Path::new("/nonexistent"),
                Instant::now() + crate::transcripts::PASS_DEADLINE
            )
            .is_err()
        );
    }
}
