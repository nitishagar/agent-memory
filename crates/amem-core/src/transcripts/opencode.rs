//! `OpenCode` transcripts: `opencode.db` (`SQLite`, `session→message→part`).
//!
//! Sessions carry the project (`directory`); messages carry the role and
//! model; parts carry the content. Only `text` and `tool` parts become
//! events — `reasoning`, `step-*`, `patch`, `file`, `compaction`, and
//! `agent` parts are replay noise for memory purposes. Selected columns
//! are named explicitly so fork schemas with extra columns (`ZCode`) parse
//! through this same core via `source`.

use std::path::Path;
use std::time::Instant;

use crate::error::{Error, Result};
use crate::transcripts::{
    MAX_VALUE_BYTES, ParsedEvent, Role, Source, Warning, make_event, open_agent_db,
};

/// Parse one session database for `source` (`OpenCode` or `ZCode`).
/// `data` values over [`MAX_VALUE_BYTES`] skip in-SQL; the `deadline`
/// is honored per session and per message.
///
/// # Errors
///
/// Returns [`Error::Db`] when the file is not a database at all and
/// [`Error::Timeout`] on an expired `deadline`. Missing tables and bad
/// rows degrade to warnings + partial events.
pub fn parse_db(
    path: &Path,
    source: Source,
    deadline: Instant,
) -> Result<(Vec<ParsedEvent>, Vec<Warning>)> {
    let mut events = Vec::new();
    let mut warnings = Vec::new();
    let conn = open_agent_db(path)?;
    warn_oversize(&conn, path, &mut warnings);
    let sessions = match load_sessions(&conn) {
        Ok(s) => s,
        Err(e) => {
            warnings.push(Warning {
                path: path.to_path_buf(),
                reason: format!("session table unreadable: {e}"),
            });
            return Ok((events, warnings));
        }
    };
    let mut msg_stmt = match conn.prepare(
        "SELECT id, time_created, data FROM message WHERE session_id = ?1
         AND length(data) <= ?2 ORDER BY time_created, id",
    ) {
        Ok(s) => s,
        Err(e) => {
            warnings.push(Warning {
                path: path.to_path_buf(),
                reason: format!("message table unreadable: {e}"),
            });
            return Ok((events, warnings));
        }
    };
    let mut part_stmt = match conn.prepare(
        "SELECT id, time_created, data FROM part WHERE message_id = ?1
         AND length(data) <= ?2 ORDER BY time_created, id",
    ) {
        Ok(s) => s,
        Err(e) => {
            warnings.push(Warning {
                path: path.to_path_buf(),
                reason: format!("part table unreadable: {e}"),
            });
            return Ok((events, warnings));
        }
    };
    for session in sessions {
        if Instant::now() > deadline {
            return Err(Error::Timeout(format!(
                "transcript pass exceeded deadline on {}",
                path.display()
            )));
        }
        let messages = match load_messages(&mut msg_stmt, &session.id) {
            Ok(m) => m,
            Err(e) => {
                warnings.push(Warning {
                    path: path.to_path_buf(),
                    reason: format!("session {}: messages unreadable: {e}", session.id),
                });
                continue;
            }
        };
        for message in messages {
            if Instant::now() > deadline {
                return Err(Error::Timeout(format!(
                    "transcript pass exceeded deadline on {}",
                    path.display()
                )));
            }
            let parts = match load_parts(&mut part_stmt, &message.id) {
                Ok(p) => p,
                Err(e) => {
                    warnings.push(Warning {
                        path: path.to_path_buf(),
                        reason: format!("message {}: parts unreadable: {e}", message.id),
                    });
                    continue;
                }
            };
            parse_message(
                source,
                &session,
                &message,
                &parts,
                &mut events,
                &mut warnings,
                path,
            );
        }
    }
    Ok((events, warnings))
}

/// Count oversize `data` values without reading them.
fn warn_oversize(conn: &rusqlite::Connection, path: &Path, warnings: &mut Vec<Warning>) {
    for table in ["message", "part"] {
        let n: i64 = conn
            .query_row(
                &format!("SELECT COUNT(*) FROM {table} WHERE length(data) > ?1"),
                [MAX_VALUE_BYTES],
                |r| r.get(0),
            )
            .unwrap_or(0);
        if n > 0 {
            warnings.push(Warning {
                path: path.to_path_buf(),
                reason: format!("{table}: {n} oversize values skipped"),
            });
        }
    }
}

struct Session {
    id: String,
    directory: String,
}

struct Message {
    id: String,
    time_ms: Option<i64>,
    data: serde_json::Value,
    data_bad: bool,
}

struct Part {
    id: String,
    time_ms: Option<i64>,
    data: serde_json::Value,
    data_bad: bool,
}

fn parse_data(raw: Option<String>) -> (serde_json::Value, bool) {
    let raw = raw.unwrap_or_default();
    if raw.trim().is_empty() {
        return (serde_json::Value::Null, false);
    }
    match serde_json::from_str(&raw) {
        Ok(v) => (v, false),
        Err(_) => (serde_json::Value::Null, true),
    }
}

fn load_sessions(conn: &rusqlite::Connection) -> rusqlite::Result<Vec<Session>> {
    let mut stmt = conn.prepare("SELECT id, directory FROM session ORDER BY time_created, id")?;
    let rows = stmt.query_map([], |r| {
        Ok(Session {
            id: r.get(0)?,
            directory: r.get::<_, Option<String>>(1)?.unwrap_or_default(),
        })
    })?;
    rows.collect()
}

fn load_messages(stmt: &mut rusqlite::Statement, session: &str) -> rusqlite::Result<Vec<Message>> {
    let rows = stmt.query_map(rusqlite::params![session, MAX_VALUE_BYTES], |r| {
        let (data, data_bad) = parse_data(r.get::<_, Option<String>>(2)?);
        Ok(Message {
            id: r.get(0)?,
            time_ms: r.get::<_, Option<i64>>(1)?,
            data,
            data_bad,
        })
    })?;
    rows.collect()
}

fn load_parts(stmt: &mut rusqlite::Statement, message: &str) -> rusqlite::Result<Vec<Part>> {
    let rows = stmt.query_map(rusqlite::params![message, MAX_VALUE_BYTES], |r| {
        let (data, data_bad) = parse_data(r.get::<_, Option<String>>(2)?);
        Ok(Part {
            id: r.get(0)?,
            time_ms: r.get::<_, Option<i64>>(1)?,
            data,
            data_bad,
        })
    })?;
    rows.collect()
}

fn str_field<'a>(v: &'a serde_json::Value, key: &str) -> &'a str {
    v.get(key).and_then(serde_json::Value::as_str).unwrap_or("")
}

fn parse_message(
    source: Source,
    session: &Session,
    message: &Message,
    parts: &[Part],
    events: &mut Vec<ParsedEvent>,
    warnings: &mut Vec<Warning>,
    path: &Path,
) {
    if message.data_bad {
        warnings.push(Warning {
            path: path.to_path_buf(),
            reason: format!("message {}: unparseable data", message.id),
        });
        return;
    }
    let role = match str_field(&message.data, "role") {
        "user" => Role::User,
        "assistant" => Role::Assistant,
        other => {
            if !other.is_empty() {
                warnings.push(Warning {
                    path: path.to_path_buf(),
                    reason: format!("message {}: unknown role {other}", message.id),
                });
            }
            return;
        }
    };
    let model = message
        .data
        .get("modelID")
        .and_then(serde_json::Value::as_str)
        .or_else(|| {
            message
                .data
                .get("model")
                .and_then(|m| m.get("modelID"))
                .and_then(serde_json::Value::as_str)
        })
        .map(str::to_string);
    for part in parts {
        if part.data_bad {
            warnings.push(Warning {
                path: path.to_path_buf(),
                reason: format!("part {}: unparseable data", part.id),
            });
            continue;
        }
        let ts_ms = part.time_ms.or(message.time_ms);
        match str_field(&part.data, "type") {
            "text" => {
                let text = str_field(&part.data, "text");
                if text.trim().is_empty() {
                    continue;
                }
                events.push(ParsedEvent {
                    seq: part.id.clone(),
                    event: make_event(
                        source,
                        session.id.clone(),
                        session.directory.clone(),
                        ts_ms,
                        role,
                        text,
                        None,
                        model.clone(),
                    ),
                });
            }
            "tool" => {
                let name = str_field(&part.data, "tool").to_string();
                let state = &part.data["state"];
                let input = state
                    .get("input")
                    .map_or_else(String::new, ToString::to_string);
                let output = match state.get("output") {
                    Some(serde_json::Value::String(s)) => s.clone(),
                    Some(v) => v.to_string(),
                    None => String::new(),
                };
                let text = if input.is_empty() {
                    output
                } else if output.is_empty() {
                    format!("in: {input}")
                } else {
                    format!("in: {input}\nout: {output}")
                };
                events.push(ParsedEvent {
                    seq: part.id.clone(),
                    event: make_event(
                        source,
                        session.id.clone(),
                        session.directory.clone(),
                        ts_ms,
                        Role::Tool,
                        &text,
                        if name.is_empty() { None } else { Some(name) },
                        model.clone(),
                    ),
                });
            }
            _ => {} // reasoning, step-*, patch, file, compaction, agent: skipped
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    const BASIC_SQL: &str = include_str!("../../tests/fixtures/transcripts/opencode_basic.sql");

    pub(crate) fn db_from_sql(sql: &str) -> (tempfile::TempDir, std::path::PathBuf) {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("opencode.db");
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute_batch(sql).unwrap();
        drop(conn);
        (tmp, path)
    }

    #[test]
    fn golden_session_message_parts() {
        let (_tmp, path) = db_from_sql(BASIC_SQL);
        let (events, warnings) = parse_db(
            &path,
            Source::Opencode,
            Instant::now() + crate::transcripts::PASS_DEADLINE,
        )
        .unwrap();
        assert!(warnings.is_empty(), "{warnings:?}");
        // user-text + assistant-text + tool (step-finish skipped).
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].event.role, Role::User);
        assert_eq!(events[0].event.text, "hi");
        assert_eq!(events[0].event.session_id, "ses_00000000000000000000000001");
        assert_eq!(events[0].event.project, "/home/u/proj");
        assert_eq!(events[0].event.ts_ms, Some(1_793_318_400_000));
        assert_eq!(events[1].event.role, Role::Assistant);
        assert_eq!(events[1].event.text, "hello");
        assert_eq!(events[1].event.model.as_deref(), Some("m"));
        assert_eq!(events[2].event.role, Role::Tool);
        assert_eq!(events[2].event.tool_name.as_deref(), Some("read"));
        assert!(events[2].event.text.contains("contents"));
    }

    #[test]
    fn bad_data_warns_per_row() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("bad.db");
        rusqlite::Connection::open(&path)
            .unwrap()
            .execute_batch(
                "CREATE TABLE session(id TEXT PRIMARY KEY, directory TEXT, time_created INTEGER);
                 CREATE TABLE message(id TEXT PRIMARY KEY, session_id TEXT, time_created INTEGER, data TEXT);
                 CREATE TABLE part(id TEXT PRIMARY KEY, message_id TEXT, session_id TEXT, time_created INTEGER, data TEXT);
                 INSERT INTO session VALUES('s1','/p',999);
                 INSERT INTO message VALUES('m1','s1',1000,'not json');
                 INSERT INTO message VALUES('m2','s1',1001,'{\"role\":\"user\"}');
                 INSERT INTO part VALUES('p1','m2','s1',1001,'also not json');",
            )
            .unwrap();
        let (events, warnings) = parse_db(
            &path,
            Source::Opencode,
            std::time::Instant::now() + crate::transcripts::PASS_DEADLINE,
        )
        .unwrap();
        assert!(events.is_empty(), "{events:?}");
        assert_eq!(warnings.len(), 2);
    }

    #[test]
    fn missing_tables_warn_not_error() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("bare.db");
        rusqlite::Connection::open(&path)
            .unwrap()
            .execute_batch("CREATE TABLE t(x);")
            .unwrap();
        let (events, warnings) = parse_db(
            &path,
            Source::Opencode,
            Instant::now() + crate::transcripts::PASS_DEADLINE,
        )
        .unwrap();
        assert!(events.is_empty(), "{events:?}");
        assert_eq!(warnings.len(), 1);
    }
}
