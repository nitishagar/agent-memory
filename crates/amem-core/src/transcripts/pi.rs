//! Pi transcripts: `~/.pi/agent/sessions/--<cwd>--/<ts>_<uuid>.jsonl`.
//!
//! One v3 JSONL file per session: a `session` header line, then a tree of
//! `message` entries (`id`/`parentId`). Roles: `user`, `assistant`,
//! `toolResult` (plus replay/noise roles that are skipped). Two clocks:
//! entry `timestamp` (ISO) and nested message `timestamp` (ms-epoch) —
//! the nested ms clock wins when present. Matches the local v3 layout,
//! which mirrors <https://pi.dev/docs/latest/session-format>.

use std::path::Path;
use std::time::Instant;

use crate::error::Result;
use crate::transcripts::{
    ParsedEvent, ReadStats, Role, Source, Warning, fallback_seq, fallback_session, is_torn_tail,
    make_event, parse_iso_ms, read_jsonl_bounded,
};

/// Parse one session file.
///
/// # Errors
///
/// Returns on file I/O failure or an expired `deadline`. Bad lines warn
/// (torn tails skip silently); unknown entry types are skipped.
pub fn parse_file(
    path: &Path,
    deadline: Instant,
) -> Result<(Vec<ParsedEvent>, Vec<Warning>, ReadStats)> {
    let mut events = Vec::new();
    let mut warnings = Vec::new();
    let mut line_no: u64 = 0;
    let mut session = file_session_id(path);
    if session.is_empty() {
        session = fallback_session(path);
    }
    let mut project = String::new();
    let mut saw_header = false;
    let stats = read_jsonl_bounded(path, deadline, |line, last| {
        line_no += 1;
        let value: serde_json::Value = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(_) if is_torn_tail(last) => return Ok(true),
            Err(e) => {
                warnings.push(Warning {
                    path: path.to_path_buf(),
                    reason: format!("line {line_no}: bad JSON ({e})"),
                });
                return Ok(true);
            }
        };
        match str_field(&value, "type") {
            "session" => {
                saw_header = true;
                if let Some(id) = value.get("id").and_then(serde_json::Value::as_str) {
                    session = id.to_string();
                }
                project = str_field(&value, "cwd").to_string();
                if let Some(v) = value.get("version").and_then(serde_json::Value::as_i64)
                    && v != 3
                {
                    warnings.push(Warning {
                        path: path.to_path_buf(),
                        reason: format!("line {line_no}: session version {v}, parsed as v3"),
                    });
                }
            }
            "message" => parse_message(&value, &session, &project, line_no, line, &mut events),
            "" => {
                warnings.push(Warning {
                    path: path.to_path_buf(),
                    reason: format!("line {line_no}: missing type"),
                });
            }
            _ => {} // model_change, usage, compaction, ...: tolerated, skipped
        }
        Ok(true)
    })?;
    if !saw_header {
        warnings.push(Warning {
            path: path.to_path_buf(),
            reason: "no session header; identity from filename".to_string(),
        });
    }
    Ok((events, warnings, stats))
}

/// Session id from the `<ts>_<uuid>.jsonl` filename (fallback when the
/// header line is missing).
fn file_session_id(path: &Path) -> String {
    path.file_stem()
        .map(|s| s.to_string_lossy().into_owned())
        .as_deref()
        .and_then(|stem| stem.rsplit('_').next())
        .unwrap_or_default()
        .to_string()
}

fn str_field<'a>(v: &'a serde_json::Value, key: &str) -> &'a str {
    v.get(key).and_then(serde_json::Value::as_str).unwrap_or("")
}

/// Timestamps: nested ms-epoch wins, entry ISO is the fallback.
fn ts_of(entry: &serde_json::Value, msg: &serde_json::Value) -> Option<i64> {
    msg.get("timestamp")
        .and_then(serde_json::Value::as_i64)
        .or_else(|| {
            entry
                .get("timestamp")
                .and_then(serde_json::Value::as_str)
                .and_then(parse_iso_ms)
        })
}

/// Flatten message content (string or block array) to text.
/// Tool calls are returned separately.
fn split_content(content: &serde_json::Value) -> (String, Vec<(String, String, String)>) {
    match content {
        serde_json::Value::String(s) => (s.clone(), Vec::new()),
        serde_json::Value::Array(blocks) => {
            let mut texts = Vec::new();
            let mut calls = Vec::new();
            for b in blocks {
                match str_field(b, "type") {
                    "text" => texts.push(str_field(b, "text").to_string()),
                    "toolCall" => calls.push((
                        str_field(b, "id").to_string(),
                        str_field(b, "name").to_string(),
                        b.get("arguments")
                            .map_or_else(String::new, ToString::to_string),
                    )),
                    _ => {} // thinking, image, ...: skipped
                }
            }
            (texts.join("\n"), calls)
        }
        _ => (String::new(), Vec::new()),
    }
}

/// Per-message envelope shared by the role arms.
struct Ctx {
    session: String,
    project: String,
    ts_ms: Option<i64>,
    seq: String,
}

impl Ctx {
    fn push(
        &self,
        events: &mut Vec<ParsedEvent>,
        seq: String,
        role: Role,
        text: &str,
        tool_name: Option<String>,
        model: Option<String>,
    ) {
        events.push(ParsedEvent {
            seq,
            event: make_event(
                Source::Pi,
                self.session.clone(),
                self.project.clone(),
                self.ts_ms,
                role,
                text,
                tool_name,
                model,
            ),
        });
    }
}

fn parse_message(
    entry: &serde_json::Value,
    session: &str,
    project: &str,
    line_no: u64,
    line: &str,
    events: &mut Vec<ParsedEvent>,
) {
    let Some(msg) = entry.get("message") else {
        return;
    };
    let entry_id = str_field(entry, "id");
    let seq = if entry_id.is_empty() {
        fallback_seq(line_no, line)
    } else {
        entry_id.to_string()
    };
    let ctx = Ctx {
        session: session.to_string(),
        project: project.to_string(),
        ts_ms: ts_of(entry, msg),
        seq,
    };
    match str_field(msg, "role") {
        "user" => {
            let (text, _) = split_content(&msg["content"]);
            if text.trim().is_empty() {
                return;
            }
            ctx.push(events, ctx.seq.clone(), Role::User, &text, None, None);
        }
        "assistant" => parse_assistant(msg, &ctx, events),
        "toolResult" => {
            let (text, _) = split_content(&msg["content"]);
            let tool_name = str_field(msg, "toolName").to_string();
            let is_error = msg
                .get("isError")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false);
            let text = if is_error && !text.is_empty() {
                format!("[error]\n{text}")
            } else {
                text
            };
            let call_id = str_field(msg, "toolCallId");
            ctx.push(
                events,
                format!("{}:{call_id}", ctx.seq),
                Role::Tool,
                &text,
                if tool_name.is_empty() {
                    None
                } else {
                    Some(tool_name)
                },
                None,
            );
        }
        _ => {} // system, compactionSummary, ...: replay noise, skipped
    }
}

fn parse_assistant(msg: &serde_json::Value, ctx: &Ctx, events: &mut Vec<ParsedEvent>) {
    let (text, calls) = split_content(&msg["content"]);
    let model = msg
        .get("model")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string);
    if !text.trim().is_empty() {
        ctx.push(
            events,
            ctx.seq.clone(),
            Role::Assistant,
            &text,
            None,
            model.clone(),
        );
    }
    for (call_id, name, args) in calls {
        let tool_name = if name.is_empty() { None } else { Some(name) };
        let call_seq = if call_id.is_empty() {
            ctx.seq.clone()
        } else {
            call_id
        };
        ctx.push(
            events,
            format!("{}:{call_seq}", ctx.seq),
            Role::Tool,
            &args,
            tool_name,
            model.clone(),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BASIC: &str = include_str!("../../tests/fixtures/transcripts/pi_basic.jsonl");

    fn parse_str(name: &str, content: &str) -> (Vec<ParsedEvent>, Vec<Warning>) {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join(name);
        std::fs::write(&path, content).unwrap();
        let (events, warnings, _) =
            parse_file(&path, Instant::now() + crate::transcripts::PASS_DEADLINE).unwrap();
        (events, warnings)
    }

    #[test]
    fn golden_header_user_assistant_tool() {
        let (events, warnings) = parse_str("2026-10-30T00-00-00-000Z_01936f10.jsonl", BASIC);
        assert!(warnings.is_empty(), "{warnings:?}");
        // user + assistant-text + toolCall + toolResult (model_change skipped).
        assert_eq!(events.len(), 4);
        assert_eq!(events[0].event.role, Role::User);
        assert_eq!(events[0].event.text, "hi");
        assert_eq!(
            events[0].event.session_id,
            "01936f10-1111-7111-8111-111111111111"
        );
        assert_eq!(events[0].event.project, "/home/u/proj");
        assert_eq!(events[0].event.ts_ms, Some(1_793_318_401_000));
        assert_eq!(events[1].event.role, Role::Assistant);
        assert_eq!(events[1].event.model.as_deref(), Some("claude-sonnet-4-5"));
        assert_eq!(events[2].event.role, Role::Tool);
        assert_eq!(events[2].event.tool_name.as_deref(), Some("read"));
        assert_eq!(events[3].event.role, Role::Tool);
        assert_eq!(events[3].event.text, "contents");
    }

    #[test]
    fn missing_header_warns_and_uses_filename() {
        let body: String = BASIC.lines().skip(1).collect::<Vec<_>>().join("\n");
        let (events, warnings) = parse_str("2026-10-30T00-00-00-000Z_fallback99.jsonl", &body);
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].reason.contains("no session header"));
        assert!(!events.is_empty(), "{events:?}");
        assert_eq!(events[0].event.session_id, "fallback99");
    }

    #[test]
    fn empty_file_warns_only() {
        let (events, warnings) = parse_str("x_empty.jsonl", "");
        assert!(events.is_empty(), "{events:?}");
        assert_eq!(warnings.len(), 1);
    }
}
