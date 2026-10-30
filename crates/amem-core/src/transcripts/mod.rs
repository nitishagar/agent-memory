//! Agent transcript ingestion (read-only): six sources, one event shape.
//!
//! The recorder NEVER writes into agent files. Every parser tolerates
//! unknown fields, mid-write (torn-tail) files, and corrupt lines: bad
//! input yields a [`Warning`], never a crash. Streaming budgets bound
//! time and memory per pass.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use crate::error::{Error, Result};

#[cfg(test)]
mod budgets;
pub mod claude;
pub mod codex;
pub mod cursor;
pub mod ingest;
pub mod opencode;
pub mod pi;
pub mod zcode;

/// Transcript sources (strictness answer 4: six minimum).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Source {
    /// `~/.claude/projects/<slug>/<uuid>.jsonl`.
    Claude,
    /// `$CODEX_HOME/sessions/YYYY/MM/DD/rollout-*.jsonl`.
    Codex,
    /// Cursor `globalStorage/state.vscdb` (`SQLite` WAL).
    Cursor,
    /// `OpenCode` `opencode.db` (`SQLite`).
    Opencode,
    /// `~/.pi/agent/sessions/.../*.jsonl` (v3).
    Pi,
    /// `ZCode` `db.sqlite` (`OpenCode`-fork schema) + subagent JSONL.
    Zcode,
}

impl Source {
    /// Stable short name for records and logs.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Claude => "claude",
            Self::Codex => "codex",
            Self::Cursor => "cursor",
            Self::Opencode => "opencode",
            Self::Pi => "pi",
            Self::Zcode => "zcode",
        }
    }

    /// All six sources, plan order (claude → cursor → pi → opencode → zcode → codex).
    #[must_use]
    pub const fn all() -> &'static [Self] {
        &[
            Self::Claude,
            Self::Cursor,
            Self::Pi,
            Self::Opencode,
            Self::Zcode,
            Self::Codex,
        ]
    }
}

/// Speaker role inside a transcript.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// Human prompt.
    User,
    /// Model turn.
    Assistant,
    /// Tool call or result.
    Tool,
    /// System/compaction/summary marker.
    System,
}

impl Role {
    /// Stable short name for records.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::User => "user",
            Self::Assistant => "assistant",
            Self::Tool => "tool",
            Self::System => "system",
        }
    }
}

/// One normalized transcript event. Parsers fill what their format has;
/// unknowns stay `None`/empty rather than guessing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TranscriptEvent {
    /// Which agent produced this.
    pub source: Source,
    /// Session identity (uuid, composer id, `ses_*`…).
    pub session_id: String,
    /// Project identity (cwd, slug, folder URI…).
    pub project: String,
    /// Milliseconds since epoch when the format carries time.
    pub ts_ms: Option<i64>,
    /// Speaker.
    pub role: Role,
    /// Normalized text (tool I/O included for [`Role::Tool`]).
    pub text: String,
    /// Tool name for [`Role::Tool`] events.
    pub tool_name: Option<String>,
    /// Model id when the format records it.
    pub model: Option<String>,
}

/// A skipped file/line and why. Surfaced to logs, never fatal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Warning {
    /// File that produced the warning.
    pub path: PathBuf,
    /// Human-readable reason.
    pub reason: String,
}

/// Maximum JSONL line length read per line (64KB); longer lines are
/// skipped and counted, never buffered whole.
pub const MAX_LINE_BYTES: usize = 64 * 1024;
/// Maximum bytes read from one file per pass (256MB).
pub const MAX_FILE_BYTES: u64 = 256 * 1024 * 1024;
/// Maximum event text kept per transcript event; `SQLite` values are
/// unbounded, so long tool outputs truncate with a marker + full length.
pub const MAX_EVENT_TEXT_BYTES: usize = 64 * 1024;
/// Maximum raw `SQLite` value read per row (8MB); larger values skip
/// with a warning instead of materializing hostile blobs. `i64`: the
/// `SQLite` integer type.
pub const MAX_VALUE_BYTES: i64 = 8 * 1024 * 1024;
/// Wall-clock budget for one ingest pass (5 minutes).
pub const PASS_DEADLINE: Duration = Duration::from_secs(300);

/// Outcome counters for one streaming JSONL read.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ReadStats {
    /// Lines handed to the parser.
    pub lines: u64,
    /// Lines skipped (overlong, torn tail, empty).
    pub skipped: u64,
    /// Bytes consumed.
    pub bytes: u64,
}

/// Read a JSONL transcript line-by-line under the streaming budgets.
///
/// Each line (sans trailing newline) is passed to `on_line`; returning
/// `Ok(false)` stops early. Rules: lines over [`MAX_LINE_BYTES`] are
/// skipped whole; reading stops at [`MAX_FILE_BYTES`]; a final line
/// without a trailing newline that fails to parse is the torn tail of
/// a mid-write file — `on_line` still sees it, and a parse failure
/// there must be treated as skippable by the caller (see
/// [`is_torn_tail`]). The `deadline` aborts with [`Error::Timeout`].
///
/// # Errors
///
/// Returns [`Error::Timeout`] past `deadline`, [`Error::Io`] on file
/// failure, or whatever `on_line` returns.
pub fn read_jsonl_bounded(
    path: &std::path::Path,
    deadline: Instant,
    mut on_line: impl FnMut(&str, bool) -> Result<bool>,
) -> Result<ReadStats> {
    use std::io::BufReader;
    let file = std::fs::File::open(path).map_err(Error::Io)?;
    let mut reader = BufReader::new(file);
    let mut stats = ReadStats::default();
    let mut buf = Vec::with_capacity(8 * 1024);
    loop {
        if Instant::now() > deadline {
            return Err(Error::Timeout(format!(
                "transcript pass exceeded {}s on {}",
                PASS_DEADLINE.as_secs(),
                path.display()
            )));
        }
        buf.clear();
        // Bounded line scan: never buffer past MAX_LINE_BYTES+1, so a
        // gigabyte newline-free line costs 64KB, not the machine.
        let (terminated, overlong, consumed) = read_capped_line(&mut reader, &mut buf)?;
        if consumed == 0 {
            break; // EOF
        }
        stats.bytes += consumed;
        if stats.bytes > MAX_FILE_BYTES {
            break;
        }
        if buf.ends_with(b"\r") {
            buf.pop();
        }
        if overlong {
            stats.skipped += 1;
            continue;
        }
        if buf.is_empty() {
            continue;
        }
        stats.lines += 1;
        let line = String::from_utf8_lossy(&buf);
        let last = !terminated && at_eof(&mut reader)?;
        if !on_line(&line, last)? {
            break;
        }
    }
    Ok(stats)
}

/// Read one line, buffering at most `MAX_LINE_BYTES + 1` bytes.
/// Returns `(saw_newline, line_exceeded_cap, bytes_consumed)`; overlong
/// lines are consumed through their newline without buffering the excess.
fn read_capped_line(
    reader: &mut std::io::BufReader<std::fs::File>,
    buf: &mut Vec<u8>,
) -> Result<(bool, bool, u64)> {
    use std::io::BufRead as _;
    let mut overlong = false;
    let mut consumed: u64 = 0;
    loop {
        let chunk = reader.fill_buf().map_err(Error::Io)?;
        if chunk.is_empty() {
            return Ok((false, overlong, consumed));
        }
        if overlong {
            // Already over cap: discard through the newline.
            if let Some(i) = chunk.iter().position(|&b| b == b'\n') {
                reader.consume(i + 1);
                consumed += (i + 1) as u64;
                return Ok((true, true, consumed));
            }
            let n = chunk.len();
            reader.consume(n);
            consumed += n as u64;
        } else {
            let room = MAX_LINE_BYTES + 1 - buf.len();
            let n = chunk.len().min(room);
            if let Some(i) = chunk[..n].iter().position(|&b| b == b'\n') {
                buf.extend_from_slice(&chunk[..i]);
                reader.consume(i + 1);
                consumed += (i + 1) as u64;
                return Ok((true, false, consumed));
            }
            buf.extend_from_slice(&chunk[..n]);
            reader.consume(n);
            consumed += n as u64;
            if buf.len() > MAX_LINE_BYTES {
                overlong = true;
                buf.clear();
            }
        }
    }
}

/// True when the reader is positioned at end-of-file.
fn at_eof(reader: &mut std::io::BufReader<std::fs::File>) -> Result<bool> {
    use std::io::BufRead as _;
    Ok(reader.fill_buf().map_err(Error::Io)?.is_empty())
}

/// True when an unterminated final line failed to parse: the agent was
/// mid-write. Callers skip these without a warning.
#[must_use]
pub const fn is_torn_tail(last_line: bool) -> bool {
    last_line
}

/// Truncate event text at a char boundary with a marker + full length.
/// Returns the (possibly truncated) text.
#[must_use]
pub fn truncate_text(text: &str, max_bytes: usize) -> String {
    if text.len() <= max_bytes {
        return text.to_string();
    }
    let full_len = text.len();
    let mut end = max_bytes;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…[truncated {full_len} bytes]", &text[..end])
}

/// Stable dedupe key for one transcript event: re-ingesting a file must
/// not duplicate records. `seq` is the format-native event id (message
/// uuid, bubble id, ordinal, or line number as last resort). Components
/// are separator-escaped: `session`/`seq` are attacker-controlled
/// transcript strings, and `("a", "b:c")` must never collide with
/// `("a:b", "c")`.
#[must_use]
pub fn stable_id(source: Source, session: &str, seq: &str) -> String {
    fn esc(s: &str) -> String {
        s.replace('\\', "\\\\").replace(':', "\\:")
    }
    format!("ts:{}:{}:{}", source.name(), esc(session), esc(seq))
}

/// Fallback session identity from a path: parent directory name plus
/// file stem. Same-stem files in different project dirs stay distinct.
#[must_use]
pub fn fallback_session(path: &std::path::Path) -> String {
    let stem = path
        .file_stem()
        .map_or_else(String::new, |s| s.to_string_lossy().into_owned());
    let parent = path
        .parent()
        .and_then(|p| p.file_name())
        .map_or_else(String::new, |s| s.to_string_lossy().into_owned());
    if parent.is_empty() {
        stem
    } else {
        format!("{parent}/{stem}")
    }
}

/// Fallback sequence key when a line carries no native id: line number
/// plus a content hash, so inserting a line above shifts numbers without
/// duplicating every event below on re-ingest.
#[must_use]
pub fn fallback_seq(line_no: u64, line: &str) -> String {
    let hash = format!("{}", blake3::hash(line.as_bytes()));
    format!("line-{line_no}-{}", &hash[..8])
}

/// One parsed event plus its native sequence key (for [`stable_id`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedEvent {
    /// Format-native event id.
    pub seq: String,
    /// Normalized event.
    pub event: TranscriptEvent,
}

/// Build a [`TranscriptEvent`], truncating text to
/// [`MAX_EVENT_TEXT_BYTES`].
#[must_use]
#[allow(clippy::too_many_arguments)]
pub fn make_event(
    source: Source,
    session: String,
    project: String,
    ts_ms: Option<i64>,
    role: Role,
    text: &str,
    tool_name: Option<String>,
    model: Option<String>,
) -> TranscriptEvent {
    TranscriptEvent {
        source,
        session_id: session,
        project,
        ts_ms,
        role,
        text: truncate_text(text, MAX_EVENT_TEXT_BYTES),
        tool_name,
        model,
    }
}

/// Cap on transcript files collected per source per pass: roots are
/// walked without a visited set, so this bounds hostile fan-out.
pub const MAX_FILES_PER_SOURCE: usize = 100_000;

/// Find `*.jsonl` transcript files under `root` (recursive to `max_depth`).
/// Missing roots yield an empty list (source not installed); unreadable
/// dirs are skipped, never fatal. Symlinks are never followed and
/// non-regular files (FIFOs, sockets) are never returned: a planted FIFO
/// must not hang the pass, and a planted link must not pull outside
/// files into the wrong source.
#[must_use]
pub fn find_jsonl(root: &std::path::Path, max_depth: usize) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![(root.to_path_buf(), 0)];
    while let Some((dir, depth)) = stack.pop() {
        if depth > max_depth || out.len() >= MAX_FILES_PER_SOURCE {
            continue;
        }
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            if out.len() >= MAX_FILES_PER_SOURCE {
                break;
            }
            let path = entry.path();
            let Ok(meta) = std::fs::symlink_metadata(&path) else {
                continue;
            };
            if meta.file_type().is_symlink() {
                continue;
            }
            if meta.is_dir() {
                stack.push((path, depth + 1));
            } else if meta.is_file() && path.extension().is_some_and(|e| e == "jsonl") {
                out.push(path);
            }
        }
    }
    out.sort();
    out
}

/// Open an agent database strictly read-only. WAL sidecars replay for
/// reads (unlike `immutable=1`, which would hide recent commits); the
/// agent's writer is never blocked or disturbed.
///
/// # Errors
///
/// Returns [`Error::Db`] when the file is missing or not a database.
pub fn open_agent_db(path: &std::path::Path) -> Result<rusqlite::Connection> {
    let conn =
        rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    conn.busy_timeout(Duration::from_secs(5))?;
    // Force the header read now: `SQLite` opens lazily, so a non-database
    // file would otherwise error only at first query.
    conn.query_row("SELECT 1", [], |_| Ok(()))?;
    Ok(conn)
}

/// Parse an ISO-8601 timestamp to unix milliseconds.
/// Accepts `Z` and numeric offsets, optional fractional seconds.
/// Returns `None` for anything unparseable (callers keep `ts_ms: None`).
#[must_use]
pub fn parse_iso_ms(text: &str) -> Option<i64> {
    let (date, rest) = text.split_once('T')?;
    let mut parts = date.split('-');
    let year: i64 = parts.next()?.parse().ok()?;
    let month: i64 = parts.next()?.parse().ok()?;
    let day: i64 = parts.next()?.parse().ok()?;
    // Years outside living memory are hostile or garbage; the bound also
    // keeps the civil-date arithmetic overflow-free.
    if parts.next().is_some()
        || !(1970..=3000).contains(&year)
        || !(1..=12).contains(&month)
        || !(1..=31).contains(&day)
    {
        return None;
    }
    // Split time+fraction from zone.
    let (time_part, zone) = if let Some(t) = rest.strip_suffix('Z') {
        (t, 0)
    } else {
        let at = rest.rfind(['+', '-'])?;
        if at < 8 {
            return None;
        }
        let (t, z) = rest.split_at(at);
        (t, parse_zone(z)?)
    };
    let (clock, frac_ms) = match time_part.split_once('.') {
        Some((c, f)) => {
            let digits: String = f.chars().take_while(char::is_ascii_digit).collect();
            if digits.is_empty() || digits.len() > 9 {
                return None;
            }
            let mut ms = format!("{digits:0<3}");
            ms.truncate(3);
            (c, ms.parse::<i64>().ok()?)
        }
        None => (time_part, 0),
    };
    let mut tp = clock.split(':');
    let hour: i64 = tp.next()?.parse().ok()?;
    let minute: i64 = tp.next()?.parse().ok()?;
    let second: i64 = tp.next()?.parse().ok()?;
    if tp.next().is_some() || hour > 23 || minute > 59 || second > 60 {
        return None;
    }
    let days = days_from_civil(year, month, day);
    Some(((days * 86_400 + hour * 3600 + minute * 60 + second) * 1000 + frac_ms) - zone)
}

/// Parse a numeric zone (`+HH:MM`, `-HHMM`, `+HH`) to milliseconds.
fn parse_zone(z: &str) -> Option<i64> {
    let neg = z.starts_with('-');
    let digits: String = z.chars().filter(char::is_ascii_digit).collect();
    let (h, m) = match digits.len() {
        2 => (digits.parse::<i64>().ok()?, 0),
        4 => (
            digits[..2].parse::<i64>().ok()?,
            digits[2..].parse::<i64>().ok()?,
        ),
        _ => return None,
    };
    if h > 14 || m > 59 {
        return None;
    }
    let ms = (h * 3600 + m * 60) * 1000;
    Some(if neg { -ms } else { ms })
}

/// Days since 1970-01-01 (Howard Hinnant's `days_from_civil`).
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// Default on-box root for a source, honoring overrides.
///
/// Returns `None` when the home directory cannot be determined; parsers
/// treat a missing root as "source not installed", not an error.
#[must_use]
pub fn default_root(source: Source) -> Option<PathBuf> {
    let home = dirs::home_dir()?;
    let data = dirs::data_dir().unwrap_or_else(|| home.join(".local/share"));
    let config = dirs::config_dir().unwrap_or_else(|| home.join(".config"));
    Some(match source {
        Source::Claude => home.join(".claude/projects"),
        Source::Codex => std::env::var_os("CODEX_HOME")
            .map_or_else(|| home.join(".codex"), PathBuf::from)
            .join("sessions"),
        Source::Cursor => config.join("Cursor/User/globalStorage/state.vscdb"),
        Source::Opencode => std::env::var_os("OPENCODE_DATA")
            .map_or_else(|| data.join("opencode"), PathBuf::from)
            .join("opencode.db"),
        Source::Pi => home.join(".pi/agent/sessions"),
        Source::Zcode => home.join(".zcode/cli/db/db.sqlite"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_file(name: &str, bytes: &[u8]) -> (tempfile::TempDir, PathBuf) {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join(name);
        std::fs::write(&path, bytes).unwrap();
        (tmp, path)
    }

    #[test]
    fn bounded_reader_counts_and_skips_overlong() {
        let big = "y".repeat(MAX_LINE_BYTES + 1);
        let content = format!("{{\"a\":1}}\n{big}\n{{\"b\":2}}\n");
        let (_tmp, path) = tmp_file("t.jsonl", content.as_bytes());
        let mut seen = Vec::new();
        let stats = read_jsonl_bounded(path.as_path(), Instant::now() + PASS_DEADLINE, |l, _| {
            seen.push(l.to_string());
            Ok(true)
        })
        .unwrap();
        assert_eq!(seen, vec!["{\"a\":1}", "{\"b\":2}"]);
        assert_eq!(stats.lines, 2);
        assert_eq!(stats.skipped, 1);
    }

    #[test]
    fn bounded_reader_flags_torn_tail() {
        let content = b"{\"a\":1}\n{\"torn\": tru";
        let (_tmp, path) = tmp_file("torn.jsonl", content);
        let mut last_flags = Vec::new();
        read_jsonl_bounded(path.as_path(), Instant::now() + PASS_DEADLINE, |_, last| {
            last_flags.push(last);
            Ok(true)
        })
        .unwrap();
        assert_eq!(last_flags, vec![false, true]);
        assert!(is_torn_tail(last_flags[1]));
    }

    #[test]
    fn bounded_reader_enforces_deadline() {
        let (_tmp, path) = tmp_file("d.jsonl", b"{\"a\":1}\n");
        let past = Instant::now()
            .checked_sub(std::time::Duration::from_secs(1))
            .unwrap();
        let err = read_jsonl_bounded(path.as_path(), past, |_, _| Ok(true)).unwrap_err();
        assert!(matches!(err, Error::Timeout(_)));
    }

    #[test]
    fn default_roots_shape() {
        for s in Source::all() {
            let root = default_root(*s);
            assert!(root.is_some(), "{} has no root", s.name());
        }
        assert_eq!(Source::all().len(), 6);
    }

    #[test]
    fn iso_parser_covers_observed_shapes() {
        // Values cross-checked against `date -u +%s`.
        assert_eq!(
            parse_iso_ms("2026-10-30T00:00:00.000Z"),
            Some(1_793_318_400_000)
        );
        assert_eq!(
            parse_iso_ms("2026-10-30T00:00:01Z"),
            Some(1_793_318_401_000)
        );
        assert_eq!(
            parse_iso_ms("2026-10-30T05:30:00.000+05:30"),
            Some(1_793_318_400_000)
        );
        assert_eq!(
            parse_iso_ms("2026-07-08T01:40:35.865Z"),
            Some(1_783_474_835_865)
        );
        assert_eq!(parse_iso_ms("not-a-time"), None);
        assert_eq!(parse_iso_ms("2026-13-01T00:00:00Z"), None);
        assert_eq!(parse_iso_ms("2026-10-30 00:00:00"), None);
    }

    #[test]
    fn stable_ids_escape_separators() {
        let a = stable_id(Source::Claude, "a", "b:c");
        let b = stable_id(Source::Claude, "a:b", "c");
        assert_ne!(a, b);
        let c = stable_id(Source::Claude, "a\\", "b");
        let d = stable_id(Source::Claude, "a", "\\b");
        assert_ne!(c, d);
    }

    #[test]
    fn fallback_seq_binds_content() {
        let a = fallback_seq(3, "hello");
        let b = fallback_seq(3, "world");
        assert_ne!(a, b);
        assert_eq!(a, fallback_seq(3, "hello"));
    }

    #[test]
    fn iso_rejects_absurd_years() {
        assert_eq!(parse_iso_ms("9999999999999999-01-01T00:00:00Z"), None);
        assert_eq!(parse_iso_ms("1969-12-31T23:59:59Z"), None);
        assert_eq!(parse_iso_ms("3001-01-01T00:00:00Z"), None);
        assert!(parse_iso_ms("2026-01-01T00:00:00Z").is_some());
    }

    #[cfg(unix)]
    #[test]
    fn discovery_skips_symlinks() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("root");
        std::fs::create_dir_all(&root).unwrap();
        let real = root.join("real.jsonl");
        std::fs::write(&real, "{}\n").unwrap();
        std::os::unix::fs::symlink(&real, root.join("link.jsonl")).unwrap();
        std::os::unix::fs::symlink(&root, root.join("loop")).unwrap();
        let found = find_jsonl(&root, 4);
        assert_eq!(found, vec![real]);
    }

    #[test]
    fn giant_line_costs_kilobytes_not_gigabytes() {
        // 10MB newline-free line: skipped whole, bounded memory.
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("giant.jsonl");
        let mut content = "A".repeat(10 * 1024 * 1024);
        content.push('\n');
        content.push_str("{\"ok\":1}\n");
        std::fs::write(&path, content).unwrap();
        let mut seen = Vec::new();
        let stats = read_jsonl_bounded(path.as_path(), Instant::now() + PASS_DEADLINE, |l, _| {
            seen.push(l.to_string());
            Ok(true)
        })
        .unwrap();
        assert_eq!(seen, vec!["{\"ok\":1}"]);
        assert_eq!(stats.skipped, 1);
    }

    #[test]
    fn truncate_marks_and_measures() {
        assert_eq!(truncate_text("short", 64), "short");
        let out = truncate_text(&"x".repeat(100), 64);
        assert!(out.starts_with(&"x".repeat(64)));
        assert!(out.contains("[truncated 100 bytes]"));
        // Multibyte boundary: never splits a codepoint.
        let out = truncate_text(&"é".repeat(100), 64);
        assert!(out.contains("[truncated 200 bytes]"));
        assert!(out.len() < 200);
    }
}
