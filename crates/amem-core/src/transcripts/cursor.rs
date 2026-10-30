//! Cursor transcripts: `<User>/globalStorage/state.vscdb` (`SQLite`).
//!
//! Composer sessions live in `cursorDiskKV`: `composerData:<id>` rows hold
//! session state + message order, `bubbleId:<composer>:<bubble>` rows hold
//! the messages. Always opened read-only; WAL sidecars replay for reads.
//! Newer writers may leave `fullConversationHeadersOnly` empty — bubbles
//! then order by `createdAt`. Empty-text bubbles still matter when they
//! carry `toolFormerData`.

use std::collections::HashMap;
use std::path::Path;
use std::time::Instant;

use crate::error::{Error, Result};
use crate::transcripts::{
    MAX_VALUE_BYTES, ParsedEvent, Role, Source, Warning, make_event, open_agent_db, parse_iso_ms,
};

/// Session metadata collected from `composerData:<id>` rows.
struct Composer {
    order: Vec<String>,
    project: String,
}

/// Parse one `state.vscdb`. Returns events grouped by composer (caller
/// sorts globally if needed) plus warnings for unreadable rows. Values
/// over [`MAX_VALUE_BYTES`] skip in-SQL (never materialized); the
/// `deadline` is honored every thousand rows.
///
/// # Errors
///
/// Returns [`Error::Db`] when the file is not a database at all and
/// [`Error::Timeout`] on an expired `deadline`. Missing tables and bad
/// rows degrade to warnings + partial events.
pub fn parse_db(path: &Path, deadline: Instant) -> Result<(Vec<ParsedEvent>, Vec<Warning>)> {
    let mut events = Vec::new();
    let mut warnings = Vec::new();
    let conn = open_agent_db(path)?;
    let mut composers: HashMap<String, Composer> = HashMap::new();
    let mut bubbles: HashMap<String, Vec<serde_json::Value>> = HashMap::new();

    let skipped_large: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM cursorDiskKV WHERE length(value) > ?1",
            [MAX_VALUE_BYTES],
            |r| r.get(0),
        )
        .unwrap_or(0);
    if skipped_large > 0 {
        warnings.push(Warning {
            path: path.to_path_buf(),
            reason: format!("{skipped_large} oversize values skipped"),
        });
    }
    let Ok(mut stmt) =
        conn.prepare("SELECT key, value FROM cursorDiskKV WHERE length(value) <= ?1")
    else {
        warnings.push(Warning {
            path: path.to_path_buf(),
            reason: "cursorDiskKV table missing".to_string(),
        });
        return Ok((events, warnings));
    };
    // Values are BLOBs on real stores, TEXT in hand-built fixtures:
    // accept both.
    let rows = stmt.query_map([MAX_VALUE_BYTES], |r| {
        let key: String = r.get(0)?;
        let value: rusqlite::types::Value = r.get(1)?;
        let bytes = match value {
            rusqlite::types::Value::Blob(b) => b,
            rusqlite::types::Value::Text(s) => s.into_bytes(),
            _ => Vec::new(),
        };
        Ok((key, bytes))
    });
    let Ok(rows) = rows else {
        warnings.push(Warning {
            path: path.to_path_buf(),
            reason: "cursorDiskKV unreadable".to_string(),
        });
        return Ok((events, warnings));
    };
    for (n, row) in rows.flatten().enumerate() {
        if n % 1000 == 0 && Instant::now() > deadline {
            return Err(Error::Timeout(format!(
                "transcript pass exceeded deadline on {}",
                path.display()
            )));
        }
        let (key, value) = row;
        // Skip unknown keys before parsing: real stores carry large
        // `composer.content.*` blobs that are never messages.
        if !key.starts_with("composerData:") && !key.starts_with("bubbleId:") {
            continue;
        }
        let text = String::from_utf8_lossy(&value);
        let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&text) else {
            warnings.push(Warning {
                path: path.to_path_buf(),
                reason: format!("key {key}: bad JSON"),
            });
            continue;
        };
        if let Some(id) = key.strip_prefix("composerData:") {
            composers.insert(id.to_string(), composer_of(&parsed));
        } else if let Some(rest) = key.strip_prefix("bubbleId:") {
            if let Some((composer_id, _)) = rest.split_once(':') {
                if composer_id.is_empty() {
                    warnings.push(Warning {
                        path: path.to_path_buf(),
                        reason: format!("key {key}: empty composer id"),
                    });
                    continue;
                }
                bubbles
                    .entry(composer_id.to_string())
                    .or_default()
                    .push(parsed);
            }
        }
    }

    let mut composer_ids: Vec<&String> = bubbles.keys().collect();
    composer_ids.sort();
    for composer_id in composer_ids {
        let rows = &bubbles[composer_id];
        let composer = composers.get(composer_id);
        for bubble in order_bubbles(rows, composer, &mut warnings, path) {
            parse_bubble(
                composer_id,
                composer,
                bubble,
                &mut events,
                &mut warnings,
                path,
            );
        }
    }
    Ok((events, warnings))
}

fn str_field<'a>(v: &'a serde_json::Value, key: &str) -> &'a str {
    v.get(key).and_then(serde_json::Value::as_str).unwrap_or("")
}

fn composer_of(v: &serde_json::Value) -> Composer {
    let order = v
        .get("fullConversationHeadersOnly")
        .and_then(serde_json::Value::as_array)
        .map(|a| {
            a.iter()
                .map(|h| str_field(h, "bubbleId").to_string())
                .filter(|s| !s.is_empty())
                .collect()
        })
        .unwrap_or_default();
    let project = v
        .get("gitWorktree")
        .and_then(|w| w.get("worktreePath"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_string();
    Composer { order, project }
}

/// Order bubbles by the composer's header list when present, else by
/// `createdAt`. Header-unknown bubbles append in `createdAt` order.
/// Duplicate bubble ids warn (first wins) instead of silently collapsing.
fn order_bubbles<'a>(
    rows: &'a [serde_json::Value],
    composer: Option<&Composer>,
    warnings: &mut Vec<Warning>,
    path: &Path,
) -> Vec<&'a serde_json::Value> {
    let mut by_id: HashMap<&str, &serde_json::Value> = HashMap::new();
    for b in rows {
        let id = str_field(b, "bubbleId");
        if by_id.contains_key(id) {
            warnings.push(Warning {
                path: path.to_path_buf(),
                reason: format!("duplicate bubbleId {id}; first wins"),
            });
        } else {
            by_id.insert(id, b);
        }
    }
    let mut out = Vec::with_capacity(rows.len());
    if let Some(c) = composer {
        for id in &c.order {
            if let Some(b) = by_id.remove(id.as_str()) {
                out.push(b);
            }
        }
    }
    let mut rest: Vec<&serde_json::Value> = by_id.into_values().collect();
    rest.sort_by_key(|b| str_field(b, "createdAt").to_string());
    out.extend(rest);
    out
}

fn parse_bubble(
    composer_id: &str,
    composer: Option<&Composer>,
    bubble: &serde_json::Value,
    events: &mut Vec<ParsedEvent>,
    warnings: &mut Vec<Warning>,
    path: &Path,
) {
    let bubble_id = str_field(bubble, "bubbleId");
    if bubble_id.is_empty() {
        warnings.push(Warning {
            path: path.to_path_buf(),
            reason: format!("composer {composer_id}: bubble without id"),
        });
        return;
    }
    let mut project = str_field(bubble, "workspaceProjectDir").to_string();
    if project.is_empty() {
        project = composer.map_or_else(String::new, |c| c.project.clone());
    }
    let ts_ms = parse_iso_ms(str_field(bubble, "createdAt"));
    let text = str_field(bubble, "text").to_string();
    let tool = bubble.get("toolFormerData");
    if text.trim().is_empty() && tool.is_none() {
        return;
    }
    let (role, model) = match bubble.get("type").and_then(serde_json::Value::as_i64) {
        Some(1) => (Role::User, None),
        Some(2) => (
            Role::Assistant,
            bubble
                .get("modelInfo")
                .and_then(|m| m.get("modelName"))
                .and_then(serde_json::Value::as_str)
                .map(str::to_string),
        ),
        _ => return, // unknown bubble kind: tolerated, skipped
    };
    if !text.trim().is_empty() {
        events.push(ParsedEvent {
            seq: bubble_id.to_string(),
            event: make_event(
                Source::Cursor,
                composer_id.to_string(),
                project.clone(),
                ts_ms,
                role,
                &text,
                None,
                model.clone(),
            ),
        });
    }
    if let Some(t) = tool {
        let name = str_field(t, "name").to_string();
        let args = t
            .get("rawArgs")
            .map_or_else(String::new, |a| a.as_str().unwrap_or_default().to_string());
        events.push(ParsedEvent {
            seq: format!("{bubble_id}:tool"),
            event: make_event(
                Source::Cursor,
                composer_id.to_string(),
                project,
                ts_ms,
                Role::Tool,
                &args,
                if name.is_empty() { None } else { Some(name) },
                model,
            ),
        });
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use super::*;
    use crate::transcripts::PASS_DEADLINE;

    const BASIC_SQL: &str = include_str!("../../tests/fixtures/transcripts/cursor_basic.sql");

    fn db_from_sql(sql: &str) -> (tempfile::TempDir, std::path::PathBuf) {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("state.vscdb");
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute_batch(sql).unwrap();
        drop(conn);
        (tmp, path)
    }

    #[test]
    fn golden_orders_by_headers_and_reads_tools() {
        let (_tmp, path) = db_from_sql(BASIC_SQL);
        let (events, warnings) = parse_db(&path, Instant::now() + PASS_DEADLINE).unwrap();
        assert!(warnings.is_empty(), "{warnings:?}");
        // user + assistant + tool bubble (empty text, tool payload).
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].event.role, Role::User);
        assert_eq!(events[0].event.text, "add retries");
        assert_eq!(
            events[0].event.session_id,
            "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
        );
        assert_eq!(events[0].event.project, "/home/u/proj");
        assert_eq!(events[1].event.role, Role::Assistant);
        assert_eq!(events[1].event.model.as_deref(), Some("gpt-5.2"));
        assert_eq!(events[2].event.role, Role::Tool);
        assert_eq!(events[2].event.tool_name.as_deref(), Some("read_file"));
        assert!(events[2].event.text.contains("path"));
    }

    #[test]
    fn missing_table_warns_not_errors() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("empty.vscdb");
        rusqlite::Connection::open(&path)
            .unwrap()
            .execute_batch("CREATE TABLE t(x);")
            .unwrap();
        let (events, warnings) = parse_db(&path, Instant::now() + PASS_DEADLINE).unwrap();
        assert!(events.is_empty());
        assert_eq!(warnings.len(), 1);
    }

    #[test]
    fn not_a_database_errors() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("junk.vscdb");
        std::fs::write(&path, b"definitely not sqlite").unwrap();
        assert!(parse_db(&path, Instant::now() + PASS_DEADLINE).is_err());
    }

    #[test]
    fn oversize_values_skip_without_reading() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("big.vscdb");
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute_batch(
            "CREATE TABLE cursorDiskKV(key TEXT UNIQUE ON CONFLICT REPLACE, value BLOB);",
        )
        .unwrap();
        let huge_len = usize::try_from(crate::transcripts::MAX_VALUE_BYTES + 1).unwrap();
        let huge = vec![9u8; huge_len];
        conn.execute(
            "INSERT INTO cursorDiskKV VALUES ('bubbleId:c:b', ?1)",
            rusqlite::params![huge],
        )
        .unwrap();
        drop(conn);
        let (events, warnings) = parse_db(&path, Instant::now() + PASS_DEADLINE).unwrap();
        assert!(events.is_empty());
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].reason.contains("oversize"));
    }

    #[test]
    fn expired_deadline_aborts() {
        let (_tmp, path) = db_from_sql(BASIC_SQL);
        let err = parse_db(
            &path,
            Instant::now()
                .checked_sub(std::time::Duration::from_secs(1))
                .unwrap(),
        )
        .unwrap_err();
        assert!(matches!(err, crate::error::Error::Timeout(_)));
    }
}
