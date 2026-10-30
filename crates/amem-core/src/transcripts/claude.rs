//! Claude Code transcripts: `~/.claude/projects/<slug>/<uuid>.jsonl`.
//!
//! Append-only JSONL, one object per line. Message lines (`user`,
//! `assistant`) carry a raw Anthropic `message`; tool calls and results
//! join by `tool_use_id`. Per-line `cwd` is the authoritative project
//! (the directory slug is lossy). Unknown `type` values are skipped:
//! the aux-line vocabulary drifts without notice.

use std::path::Path;
use std::time::Instant;

use crate::error::Result;
use crate::transcripts::{
    ParsedEvent, ReadStats, Role, Source, Warning, fallback_seq, fallback_session, is_torn_tail,
    make_event, parse_iso_ms, read_jsonl_bounded,
};

/// Parse one session file. Returns events (in file order), warnings for
/// skipped lines, and reader stats. `deadline` bounds the read.
///
/// # Errors
///
/// Returns on file I/O failure or an expired `deadline`. Bad lines never
/// error: torn tails skip silently, other bad lines warn.
pub fn parse_file(
    path: &Path,
    deadline: Instant,
) -> Result<(Vec<ParsedEvent>, Vec<Warning>, ReadStats)> {
    let fallback_session = fallback_session(path);
    let mut events = Vec::new();
    let mut warnings = Vec::new();
    let mut line_no: u64 = 0;
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
        parse_line(
            &value,
            &fallback_session,
            line_no,
            line,
            &mut events,
            &mut warnings,
            path,
        );
        Ok(true)
    })?;
    Ok((events, warnings, stats))
}

fn str_field<'a>(v: &'a serde_json::Value, key: &str) -> &'a str {
    v.get(key).and_then(serde_json::Value::as_str).unwrap_or("")
}

/// Flatten Anthropic content (string or block array) to text.
/// `tool_result` blocks are returned separately for Tool events.
fn split_content(content: &serde_json::Value) -> (String, Vec<(String, String, bool)>) {
    match content {
        serde_json::Value::String(s) => (s.clone(), Vec::new()),
        serde_json::Value::Array(blocks) => {
            let mut texts = Vec::new();
            let mut results = Vec::new();
            for b in blocks {
                let kind = str_field(b, "type");
                if kind == "tool_result" {
                    let id = str_field(b, "tool_use_id").to_string();
                    let text = text_of(&b["content"]);
                    let is_error = b
                        .get("is_error")
                        .and_then(serde_json::Value::as_bool)
                        .unwrap_or(false);
                    results.push((id, text, is_error));
                } else if kind == "text" {
                    texts.push(str_field(b, "text").to_string());
                }
            }
            (texts.join("\n"), results)
        }
        _ => (String::new(), Vec::new()),
    }
}

/// Extract display text from a `content` that may be a string, a block
/// array, or anything else (stringified as last resort).
fn text_of(content: &serde_json::Value) -> String {
    match content {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Array(blocks) => blocks
            .iter()
            .map(|b| {
                if b.is_string() {
                    b.as_str().unwrap_or_default().to_string()
                } else {
                    str_field(b, "text").to_string()
                }
            })
            .collect::<Vec<_>>()
            .join("\n"),
        serde_json::Value::Null => String::new(),
        other => other.to_string(),
    }
}

/// Per-line envelope shared by all message arms.
struct Header {
    session: String,
    project: String,
    ts_ms: Option<i64>,
    seq: String,
}

impl Header {
    fn of(v: &serde_json::Value, fallback_session: &str, line_no: u64, line: &str) -> Self {
        let s = str_field(v, "sessionId");
        let s = if s.is_empty() {
            str_field(v, "session_id")
        } else {
            s
        };
        let session = if s.is_empty() {
            fallback_session.to_string()
        } else {
            s.to_string()
        };
        let project = str_field(v, "cwd").to_string();
        let ts_ms = v
            .get("timestamp")
            .and_then(serde_json::Value::as_str)
            .and_then(parse_iso_ms);
        let uuid = str_field(v, "uuid");
        let seq = if uuid.is_empty() {
            fallback_seq(line_no, line)
        } else {
            uuid.to_string()
        };
        Self {
            session,
            project,
            ts_ms,
            seq,
        }
    }

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
                Source::Claude,
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

fn parse_line(
    v: &serde_json::Value,
    fallback_session: &str,
    line_no: u64,
    line: &str,
    events: &mut Vec<ParsedEvent>,
    warnings: &mut Vec<Warning>,
    path: &Path,
) {
    let header = Header::of(v, fallback_session, line_no, line);
    match str_field(v, "type") {
        "user" => parse_user(v, &header, events),
        "assistant" => parse_assistant(v, &header, events),
        "system" => parse_system(v, &header, events),
        "" => {
            warnings.push(Warning {
                path: path.to_path_buf(),
                reason: format!("line {line_no}: missing type"),
            });
        }
        _ => {} // future-widget, cost-state, ...: tolerated, skipped
    }
}

fn parse_user(v: &serde_json::Value, header: &Header, events: &mut Vec<ParsedEvent>) {
    let Some(msg) = v.get("message") else { return };
    let (text, results) = split_content(&msg["content"]);
    if !text.trim().is_empty() {
        header.push(events, header.seq.clone(), Role::User, &text, None, None);
    }
    for (tool_id, text, is_error) in results {
        let text = if is_error && !text.is_empty() {
            format!("[error]\n{text}")
        } else {
            text
        };
        header.push(
            events,
            format!("{}:{tool_id}", header.seq),
            Role::Tool,
            &text,
            None,
            None,
        );
    }
}

fn parse_assistant(v: &serde_json::Value, header: &Header, events: &mut Vec<ParsedEvent>) {
    let Some(msg) = v.get("message") else { return };
    let model = msg
        .get("model")
        .and_then(serde_json::Value::as_str)
        .or_else(|| v.get("requestedModel").and_then(serde_json::Value::as_str))
        .map(str::to_string);
    let mut texts = Vec::new();
    let mut calls = Vec::new();
    if let Some(blocks) = msg.get("content").and_then(serde_json::Value::as_array) {
        for b in blocks {
            match str_field(b, "type") {
                "text" => texts.push(str_field(b, "text").to_string()),
                "tool_use" => calls.push((
                    str_field(b, "id").to_string(),
                    str_field(b, "name").to_string(),
                    b.get("input").map_or_else(String::new, ToString::to_string),
                )),
                _ => {} // thinking, server_tool_use, ...: skipped
            }
        }
    }
    let text = texts.join("\n");
    if !text.trim().is_empty() {
        header.push(
            events,
            header.seq.clone(),
            Role::Assistant,
            &text,
            None,
            model.clone(),
        );
    }
    for (call_id, name, input) in calls {
        let tool_name = if name.is_empty() { None } else { Some(name) };
        header.push(
            events,
            format!("{}:{call_id}", header.seq),
            Role::Tool,
            &input,
            tool_name,
            model.clone(),
        );
    }
}

fn parse_system(v: &serde_json::Value, header: &Header, events: &mut Vec<ParsedEvent>) {
    let text = match str_field(v, "subtype") {
        "turn_duration" => {
            let ms = v
                .get("durationMs")
                .and_then(serde_json::Value::as_i64)
                .unwrap_or(0);
            let n = v
                .get("messageCount")
                .and_then(serde_json::Value::as_i64)
                .unwrap_or(0);
            format!("turn: {ms}ms, {n} messages")
        }
        "compact_boundary" | "compaction" | "away_summary" => str_field(v, "content").to_string(),
        _ => return,
    };
    if text.is_empty() {
        return;
    }
    header.push(events, header.seq.clone(), Role::System, &text, None, None);
}

#[cfg(test)]
mod tests {
    use super::*;

    const BASIC: &str = include_str!("../../tests/fixtures/transcripts/claude_basic.jsonl");

    fn parse_str(name: &str, content: &str) -> (Vec<ParsedEvent>, Vec<Warning>) {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join(name);
        std::fs::write(&path, content).unwrap();
        let (events, warnings, _) =
            parse_file(&path, Instant::now() + crate::transcripts::PASS_DEADLINE).unwrap();
        (events, warnings)
    }

    #[test]
    fn golden_user_assistant_tool_system() {
        let (events, warnings) = parse_str("golden.jsonl", BASIC);
        assert!(warnings.is_empty(), "{warnings:?}");
        // user + assistant-text + tool_use + tool_result + system.
        assert_eq!(events.len(), 5);
        assert_eq!(events[0].event.role, Role::User);
        assert_eq!(events[0].event.text, "list files");
        assert_eq!(
            events[0].event.session_id,
            "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaaa"
        );
        assert_eq!(events[0].event.project, "/home/u/proj");
        assert_eq!(events[1].event.role, Role::Assistant);
        assert_eq!(events[1].event.text, "Listing.");
        assert_eq!(events[1].event.model.as_deref(), Some("claude-sonnet-5-5"));
        assert_eq!(events[2].event.role, Role::Tool);
        assert_eq!(events[2].event.tool_name.as_deref(), Some("Bash"));
        assert!(events[2].event.text.contains("ls"));
        assert_eq!(events[3].event.role, Role::Tool);
        assert_eq!(events[3].event.text, "a.txt\n");
        assert_eq!(events[4].event.role, Role::System);
        assert!(events[4].event.text.contains("1500ms"));
        // Distinct stable seqs.
        let mut seqs: Vec<&str> = events.iter().map(|e| e.seq.as_str()).collect();
        seqs.sort_unstable();
        seqs.dedup();
        assert_eq!(seqs.len(), 5);
    }

    #[test]
    fn torn_tail_skips_silently_bad_line_warns() {
        let content = format!("{BASIC}{{\"type\": \"user\", \"broken\": ");
        let (events, warnings) = parse_str("torn.jsonl", &content);
        assert_eq!(events.len(), 5);
        assert!(warnings.is_empty(), "{warnings:?}");
        let content = format!("{BASIC}not json\n{BASIC}");
        let (events, warnings) = parse_str("badline.jsonl", &content);
        assert_eq!(events.len(), 10);
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].reason.contains("bad JSON"));
    }

    #[test]
    fn empty_file_yields_nothing() {
        let (events, warnings) = parse_str("empty.jsonl", "");
        assert!(events.is_empty() && warnings.is_empty());
    }
}
