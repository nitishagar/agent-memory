//! Codex transcripts: `$CODEX_HOME/sessions/YYYY/MM/DD/rollout-*.jsonl`.
//!
//! Envelope lines (`{timestamp, ordinal?, type, payload}`) carry
//! `session_meta` (identity, first line), `response_item` (the
//! authoritative transcript), and `event_msg` (UI/token noise, skipped).
//! Pre-2025-09 files use a bare era: a meta line, then raw response
//! items. No per-line session id — readers carry it forward. `.zst`
//! variants are out of scope (no decompression dependency).

use std::path::Path;
use std::time::Instant;

use crate::error::Result;
use crate::transcripts::{
    ParsedEvent, ReadStats, Role, Source, Warning, is_torn_tail, make_event, parse_iso_ms,
    read_jsonl_bounded,
};

/// Parse one rollout file.
///
/// # Errors
///
/// Returns on file I/O failure or an expired `deadline`. Bad lines warn
/// (torn tails skip silently); unknown item types are skipped.
pub fn parse_file(
    path: &Path,
    deadline: Instant,
) -> Result<(Vec<ParsedEvent>, Vec<Warning>, ReadStats)> {
    let mut events = Vec::new();
    let mut warnings = Vec::new();
    let mut line_no: u64 = 0;
    let mut session = String::new();
    let mut project = String::new();
    let mut model: Option<String> = None;
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
        let mut ctx = Ctx {
            session: &mut session,
            project: &mut project,
            model: &mut model,
            line_no,
            path,
        };
        parse_line(&value, &mut ctx, &mut events, &mut warnings);
        Ok(true)
    })?;
    Ok((events, warnings, stats))
}

/// Carried-forward session state (Codex lines lack session ids).
struct Ctx<'a> {
    session: &'a mut String,
    project: &'a mut String,
    model: &'a mut Option<String>,
    line_no: u64,
    path: &'a Path,
}

impl Ctx<'_> {
    fn seq(&self, v: &serde_json::Value) -> String {
        v.get("ordinal")
            .and_then(serde_json::Value::as_i64)
            .map_or_else(|| format!("line-{}", self.line_no), |o| format!("ord-{o}"))
    }

    #[allow(clippy::too_many_arguments)]
    fn push(
        &self,
        events: &mut Vec<ParsedEvent>,
        seq: String,
        ts_ms: Option<i64>,
        role: Role,
        text: &str,
        tool_name: Option<String>,
        model: Option<String>,
    ) {
        events.push(ParsedEvent {
            seq,
            event: make_event(
                Source::Codex,
                self.session.clone(),
                self.project.clone(),
                ts_ms,
                role,
                text,
                tool_name,
                model,
            ),
        });
    }
}

fn str_field<'a>(v: &'a serde_json::Value, key: &str) -> &'a str {
    v.get(key).and_then(serde_json::Value::as_str).unwrap_or("")
}

fn ts_ms(v: &serde_json::Value) -> Option<i64> {
    v.get("timestamp")
        .and_then(serde_json::Value::as_str)
        .and_then(parse_iso_ms)
}

fn parse_line(
    v: &serde_json::Value,
    ctx: &mut Ctx,
    events: &mut Vec<ParsedEvent>,
    warnings: &mut Vec<Warning>,
) {
    // Envelope era: `{type, payload}`. Bare era: raw items (disjoint
    // type vocabulary) or a bare `{id, cwd, ...}` meta line.
    if let Some(payload) = v.get("payload") {
        parse_envelope(v, payload, ctx, events);
    } else if v.get("type").and_then(serde_json::Value::as_str).is_some() {
        let ts_ms = ts_ms(v);
        let seq = ctx.seq(v);
        parse_item(v, ts_ms, &seq, ctx, events);
    } else if v.get("id").is_some() && v.get("cwd").is_some() {
        *ctx.session = str_field(v, "id").to_string();
        *ctx.project = str_field(v, "cwd").to_string();
    } else {
        warnings.push(Warning {
            path: ctx.path.to_path_buf(),
            reason: format!("line {}: unrecognized line", ctx.line_no),
        });
    }
}

fn parse_envelope(
    v: &serde_json::Value,
    payload: &serde_json::Value,
    ctx: &mut Ctx,
    events: &mut Vec<ParsedEvent>,
) {
    let ts_ms = ts_ms(v);
    let seq = ctx.seq(v);
    match str_field(v, "type") {
        "session_meta" => {
            let id = str_field(payload, "session_id");
            let id = if id.is_empty() {
                str_field(payload, "id")
            } else {
                id
            };
            if !id.is_empty() {
                *ctx.session = id.to_string();
            }
            let cwd = str_field(payload, "cwd");
            if !cwd.is_empty() {
                *ctx.project = cwd.to_string();
            }
        }
        "response_item" => parse_item(payload, ts_ms, &seq, ctx, events),
        "turn_context" => {
            if let Some(m) = payload.get("model").and_then(serde_json::Value::as_str) {
                *ctx.model = Some(m.to_string());
            }
            let cwd = str_field(payload, "cwd");
            if !cwd.is_empty() {
                *ctx.project = cwd.to_string();
            }
        }
        "compacted" => {
            let n = payload
                .get("replacement_history")
                .and_then(serde_json::Value::as_array)
                .map_or(0, Vec::len);
            ctx.push(
                events,
                seq,
                ts_ms,
                Role::System,
                &format!("context compacted ({n} replacement items)"),
                None,
                None,
            );
        }
        _ => {} // event_msg, world_state, ...: UI noise or non-conversation
    }
}

/// One response item (envelope payload or bare line).
fn parse_item(
    item: &serde_json::Value,
    ts_ms: Option<i64>,
    seq: &str,
    ctx: &mut Ctx,
    events: &mut Vec<ParsedEvent>,
) {
    match str_field(item, "type") {
        "message" => {
            let (role, model) = match str_field(item, "role") {
                "user" => (Role::User, None),
                "assistant" => (Role::Assistant, ctx.model.clone()),
                "system" | "developer" => (Role::System, None),
                _ => return,
            };
            let text = message_text(item);
            if text.trim().is_empty() {
                return;
            }
            ctx.push(events, seq.to_string(), ts_ms, role, &text, None, model);
        }
        "function_call" | "custom_tool_call" => {
            let name = str_field(item, "name");
            let args = item
                .get("arguments")
                .map_or_else(String::new, |a| a.as_str().unwrap_or_default().to_string());
            let text = if args.is_empty() {
                item.to_string()
            } else {
                args
            };
            ctx.push(
                events,
                format!("{}:{}", seq, str_field(item, "call_id")),
                ts_ms,
                Role::Tool,
                &text,
                if name.is_empty() {
                    None
                } else {
                    Some(name.to_string())
                },
                None,
            );
        }
        "local_shell_call" => {
            let cmd = item
                .get("action")
                .and_then(|a| a.get("command"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or("");
            ctx.push(
                events,
                format!("{}:{}", seq, str_field(item, "call_id")),
                ts_ms,
                Role::Tool,
                cmd,
                Some("shell".to_string()),
                None,
            );
        }
        "function_call_output" | "custom_tool_call_output" | "tool_search_call_output" => {
            let output = match item.get("output") {
                Some(serde_json::Value::String(s)) => s.clone(),
                Some(v) => v.to_string(),
                None => String::new(),
            };
            ctx.push(
                events,
                format!("{}:{}", seq, str_field(item, "call_id")),
                ts_ms,
                Role::Tool,
                &output,
                None,
                None,
            );
        }
        "web_search_call" | "tool_search_call" => {
            ctx.push(
                events,
                seq.to_string(),
                ts_ms,
                Role::Tool,
                &item.to_string(),
                Some(str_field(item, "type").to_string()),
                None,
            );
        }
        _ => {} // reasoning (encrypted), agent_message dupes, ...: skipped
    }
}

/// Join `input_text`/`output_text` content blocks.
fn message_text(item: &serde_json::Value) -> String {
    item.get("content")
        .and_then(serde_json::Value::as_array)
        .map(|blocks| {
            blocks
                .iter()
                .filter_map(|b| match str_field(b, "type") {
                    "input_text" | "output_text" | "text" => Some(str_field(b, "text").to_string()),
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join("\n")
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    const BASIC: &str = include_str!("../../tests/fixtures/transcripts/codex_basic.jsonl");
    const LEGACY: &str = include_str!("../../tests/fixtures/transcripts/codex_legacy.jsonl");

    fn parse_str(name: &str, content: &str) -> (Vec<ParsedEvent>, Vec<Warning>) {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join(name);
        std::fs::write(&path, content).unwrap();
        let (events, warnings, _) =
            parse_file(&path, Instant::now() + crate::transcripts::PASS_DEADLINE).unwrap();
        (events, warnings)
    }

    #[test]
    fn golden_envelope_era() {
        let (events, warnings) = parse_str("rollout-test.jsonl", BASIC);
        assert!(warnings.is_empty(), "{warnings:?}");
        // user + function_call + output + assistant + compacted marker
        // (token_count event_msg skipped).
        assert_eq!(events.len(), 5);
        assert_eq!(events[0].event.role, Role::User);
        assert_eq!(events[0].event.text, "hello");
        assert_eq!(
            events[0].event.session_id,
            "019f97ce-aaaa-7aaa-aaaa-aaaaaaaaaaaa"
        );
        assert_eq!(events[0].event.project, "/home/u/demo");
        assert_eq!(events[1].event.role, Role::Tool);
        assert_eq!(events[1].event.tool_name.as_deref(), Some("Bash"));
        assert!(events[1].event.text.contains("ls"));
        assert_eq!(events[2].event.role, Role::Tool);
        assert_eq!(events[2].event.text, "a.txt");
        assert_eq!(events[3].event.role, Role::Assistant);
        assert_eq!(events[4].event.role, Role::System);
        assert!(events[4].event.text.contains("compacted"));
    }

    #[test]
    fn legacy_bare_era() {
        let (events, warnings) = parse_str("rollout-old.jsonl", LEGACY);
        assert!(warnings.is_empty(), "{warnings:?}");
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].event.text, "old hello");
        assert_eq!(
            events[0].event.session_id,
            "019f97ce-bbbb-7bbb-bbbb-bbbbbbbbbbbb"
        );
        assert_eq!(events[0].event.project, "/home/u/old");
        assert_eq!(events[1].event.role, Role::Assistant);
    }

    #[test]
    fn empty_file_yields_nothing() {
        let (events, warnings) = parse_str("empty.jsonl", "");
        assert!(events.is_empty() && warnings.is_empty());
    }
}
