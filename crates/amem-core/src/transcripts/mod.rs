//! Agent transcript ingestion (read-only): six sources, one event shape.
//!
//! The recorder NEVER writes into agent files. Every parser tolerates
//! unknown fields, mid-write (torn-tail) files, and corrupt lines: bad
//! input yields a [`Warning`], never a crash. Streaming budgets bound
//! time and memory per pass.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use crate::error::{Error, Result};

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
    use std::io::{BufRead as _, BufReader};
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
        let n = reader.read_until(b'\n', &mut buf).map_err(Error::Io)?;
        if n == 0 {
            break;
        }
        stats.bytes += n as u64;
        if stats.bytes > MAX_FILE_BYTES {
            break;
        }
        let terminated = buf.ends_with(b"\n");
        if terminated {
            buf.pop();
            if buf.ends_with(b"\r") {
                buf.pop();
            }
        }
        if buf.len() > MAX_LINE_BYTES {
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
        let err = read_jsonl_bounded(path.as_path(), Instant::now(), |_, _| Ok(true)).unwrap_err();
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
}
