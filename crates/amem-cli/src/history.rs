//! `history start/end/both`: the hook-facing capture CLI.
//!
//! Fast path by design: append one spool line, exit. No locks, no network,
//! no store access — hooks must never wait on anything fallible.

use std::path::PathBuf;

use amem_core::error::{Error, Result};
use amem_core::spool;

/// Read a command line from stdin, tolerating non-UTF-8 bytes.
///
/// History sources never yield a trailing newline, but PowerShell terminates
/// piped native-stdin input with one, so exactly one trailing CRLF/LF is
/// stripped. Stdin is the only unbounded channel: argv and env strings both
/// hit the OS `MAX_ARG_STRLEN` ceiling (~128KB) with `E2BIG`.
fn read_cmd_stdin() -> Result<String> {
    use std::io::Read as _;
    let mut buf = Vec::new();
    std::io::stdin().read_to_end(&mut buf).map_err(Error::Io)?;
    let mut cmd = String::from_utf8_lossy(&buf).into_owned();
    if cmd.ends_with('\n') {
        cmd.pop();
        if cmd.ends_with('\r') {
            cmd.pop();
        }
    }
    Ok(cmd)
}

/// Shells the hooks emit. Anything else is a caller bug, rejected here
/// rather than stored as a junk stream facet.
const KNOWN_SHELLS: &[&str] = &["bash", "zsh", "fish", "powershell"];

fn check_shell(shell: &str) -> Result<()> {
    if KNOWN_SHELLS.contains(&shell) {
        Ok(())
    } else {
        Err(Error::InvalidArguments(format!(
            "unknown shell '{shell}': expected {}",
            KNOWN_SHELLS.join(", ")
        )))
    }
}

/// Emit a `start` line; prints the pairing id to stdout.
pub fn start(
    spool_dir: &std::path::Path,
    shell: &str,
    cmd: Option<String>,
    cmd_from_env: Option<String>,
    cmd_from_stdin: bool,
    cwd: Option<PathBuf>,
    session: Option<String>,
) -> Result<()> {
    check_shell(shell)?;
    let cmd = if cmd_from_stdin {
        read_cmd_stdin()?
    } else {
        match (cmd, cmd_from_env) {
            (Some(c), _) => c,
            (None, Some(var)) => std::env::var(&var)
                .map_err(|_| Error::InvalidArguments(format!("env {var} is not set")))?,
            (None, None) => {
                return Err(Error::InvalidArguments(
                    "history start needs --cmd, --cmd-from-env, or --cmd-from-stdin".to_string(),
                ));
            }
        }
    };
    let id = uuid::Uuid::new_v4().simple().to_string();
    let cwd = match cwd {
        Some(c) => c,
        None => std::env::current_dir().map_err(Error::Io)?,
    };
    let session = session
        .or_else(|| std::env::var("AMEM_SESSION").ok())
        .unwrap_or_else(|| "default".to_string());
    let line = serde_json::json!({
        "v": 1, "op": "start", "id": id, "shell": shell, "cmd": cmd,
        "cwd": cwd.to_string_lossy(), "ts_ns": amem_core::clock::now_nanos(),
        "session": session,
    });
    spool::spool_line(spool_dir, &line.to_string())?;
    println!("{id}");
    Ok(())
}

/// Emit an `end` line pairing a previous `start` id.
pub fn end(spool_dir: &std::path::Path, id: &str, exit: i64, dur_ns: Option<i64>) -> Result<()> {
    let line = serde_json::json!({
        "v": 1, "op": "end", "id": id, "exit": exit, "dur_ns": dur_ns,
        "ts_ns": amem_core::clock::now_nanos(),
    });
    spool::spool_line(spool_dir, &line.to_string())?;
    Ok(())
}

/// Emit a paired start+end for shells that report completed commands
/// (PowerShell prompt hook): same id, `ts_ns` now.
pub fn both(
    spool_dir: &std::path::Path,
    shell: &str,
    exit: i64,
    dur_ns: Option<i64>,
    cmd_from_env: &str,
    cmd_from_stdin: bool,
) -> Result<()> {
    check_shell(shell)?;
    let cmd = if cmd_from_stdin {
        read_cmd_stdin()?
    } else {
        std::env::var(cmd_from_env)
            .map_err(|_| Error::InvalidArguments(format!("env {cmd_from_env} is not set")))?
    };
    let id = uuid::Uuid::new_v4().simple().to_string();
    let cwd = std::env::current_dir().map_err(Error::Io)?;
    let session = std::env::var("AMEM_SESSION").unwrap_or_else(|_| "default".to_string());
    let ts = amem_core::clock::now_nanos();
    let start = serde_json::json!({
        "v": 1, "op": "start", "id": id, "shell": shell, "cmd": cmd,
        "cwd": cwd.to_string_lossy(), "ts_ns": ts, "session": session,
    });
    spool::spool_line(spool_dir, &start.to_string())?;
    end(spool_dir, &id, exit, dur_ns)
}
