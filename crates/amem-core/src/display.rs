//! Terminal-safe rendering for stored strings.
//!
//! Transcript and note content is attacker-controlled: hits, warnings, and
//! MCP text must not carry escape sequences or forged lines to a terminal.

/// Sanitize a hostile string for display: control characters (including
/// ESC and `\r`) become U+FFFD, output capped at 300 chars.
#[must_use]
pub fn sanitize(s: &str) -> String {
    let clean: String = s
        .chars()
        .map(|c| if c.is_control() { '\u{FFFD}' } else { c })
        .take(300)
        .collect();
    if s.chars().count() > 300 {
        format!("{clean}…")
    } else {
        clean
    }
}

/// First line of `s`, capped at 160 chars and sanitized.
#[must_use]
pub fn first_line(s: &str) -> String {
    let line = s.lines().next().unwrap_or_default();
    if line.chars().count() > 160 {
        sanitize(&format!("{}…", line.chars().take(160).collect::<String>()))
    } else {
        sanitize(line)
    }
}

/// One human-readable search hit line. Shapes differ per stream, so the
/// payload is sniffed: shell records show `cmd`, notes show
/// `title: text`, transcripts show `source/session/role` plus the first
/// text line. Multibyte-safe: the session short is char- (not byte-)
/// sliced, and every field is sanitized.
#[must_use]
pub fn render_hit(rec: &crate::store::Record) -> String {
    let payload: serde_json::Value =
        serde_json::from_str(&rec.payload).unwrap_or(serde_json::Value::Null);
    if rec.stream == "shell" {
        let cmd = payload
            .get("cmd")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("?");
        let exit = payload.get("exit").and_then(serde_json::Value::as_i64);
        return format!(
            "[shell #{}] {} (exit {})",
            rec.idx,
            first_line(cmd),
            exit.map_or_else(|| "?".to_string(), |e| e.to_string())
        );
    }
    if rec.stream == "notes" {
        let title = payload
            .get("title")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("?");
        let text = payload
            .get("text")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("");
        return format!(
            "[notes #{}] {}: {}",
            rec.idx,
            sanitize(title),
            first_line(text)
        );
    }
    let source = payload
        .get("source")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("?");
    let session = payload
        .get("session")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("?");
    let role = payload
        .get("role")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("?");
    let text = payload
        .get("text")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("");
    let short: String = session.chars().take(8).collect();
    format!(
        "[{} #{}] {} {}: {}",
        sanitize(source),
        rec.idx,
        sanitize(&short),
        sanitize(role),
        first_line(text)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn controls_become_replacement_char() {
        assert_eq!(sanitize("a\x1b[0m\r\nb"), "a�[0m��b");
    }

    #[test]
    fn long_output_truncates_with_ellipsis() {
        let s = "x".repeat(400);
        let out = sanitize(&s);
        assert_eq!(out.chars().count(), 301);
        assert!(out.ends_with('…'));
    }

    #[test]
    fn first_line_strips_later_lines() {
        assert_eq!(first_line("one\ntwo"), "one");
    }
}
