//! Ingest pipeline: spool files → redacted records → store.
//!
//! Pairing: `start`/`end` hook lines join by id. Young unpaired starts are
//! rewritten to a fresh spool file for a later pass; starts older than
//! [`PENDING_MAX_AGE_SECS`] are recorded incomplete; orphan ends (no start)
//! are counted and dropped — an exit code without a command is not memory.
//! Capture edge rules (spec): empty skipped, multi-line kept flagged,
//! NUL-containing dropped (atuin rule), other controls verbatim, 1MB+
//! truncated with a marker plus the full length.

use std::collections::HashMap;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::clock::now_nanos;
use crate::error::Result;
use crate::git;
use crate::redact::Redactor;
use crate::spool;
use crate::store::{self, NewRecord, Store};

/// Maximum command bytes stored; longer commands truncate with a marker.
pub const MAX_COMMAND_BYTES: usize = 1_000_000;

/// Age after which an unpaired start is recorded incomplete instead of kept.
pub const PENDING_MAX_AGE_SECS: u64 = 24 * 3600;
/// Same bound in nanoseconds (i64: matches record timestamps).
const PENDING_MAX_AGE_NS: i64 = 86_400_000_000_000;

/// Raw hook line (v1 protocol).
#[derive(Debug, Deserialize)]
struct SpoolLine {
    op: String,
    id: String,
    #[serde(default)]
    shell: String,
    #[serde(default)]
    cmd: String,
    #[serde(default)]
    cwd: String,
    #[serde(default)]
    ts_ns: i64,
    #[serde(default)]
    session: String,
    #[serde(default)]
    exit: Option<i64>,
    #[serde(default)]
    dur_ns: Option<i64>,
}

/// Per-pass ingest statistics (also the CLI's observable output).
#[derive(Debug, Default, Clone, Serialize, PartialEq, Eq)]
pub struct IngestStats {
    /// Records committed to the store.
    pub ingested: u64,
    /// Empty/whitespace commands skipped.
    pub skipped_empty: u64,
    /// NUL-containing commands dropped.
    pub dropped_nul: u64,
    /// Commands truncated at 1MB (recorded, flagged).
    pub truncated: u64,
    /// Ends without a matching start.
    pub orphan_end: u64,
    /// Starts kept for a later pass (young).
    pub pending_kept: u64,
    /// Starts recorded incomplete (aged out).
    pub incomplete: u64,
    /// Malformed spool lines skipped.
    pub malformed: u64,
}

/// Test-only crash hook: abort after committing `n` records (unclean death).
#[derive(Debug, Clone, Copy, Default)]
pub struct IngestOptions {
    /// Abort the process after this many commits (0 = disabled).
    pub crash_after: u64,
}

/// Run one ingest pass over `spool_dir` into `store`.
///
/// # Errors
///
/// Returns on store/IO failures. Git probe failures degrade to unknown
/// context per record (never fail the pass).
pub fn ingest_once(
    store: &mut Store,
    spool_dir: &Path,
    device: &str,
    actor: &str,
    redactor: &Redactor,
    opts: IngestOptions,
) -> Result<IngestStats> {
    let mut stats = IngestStats::default();
    let mut starts_by_id: HashMap<String, SpoolLine> = HashMap::new();
    let mut ends_by_id: HashMap<String, SpoolLine> = HashMap::new();
    let mut git_cache: HashMap<String, Option<git::GitInfo>> = HashMap::new();

    // Read all spool files first; shred only after every record below is
    // committed. A crash anywhere replays from intact spool files, and
    // `insert_mapped` makes redelivery idempotent.
    let spool_files = spool::pending_spools(spool_dir)?;
    for path in &spool_files {
        let text = std::fs::read_to_string(path).unwrap_or_default();
        for line in text.lines() {
            match serde_json::from_str::<SpoolLine>(line) {
                Ok(s) if s.op == "start" => {
                    starts_by_id.insert(s.id.clone(), s);
                }
                Ok(s) if s.op == "end" => {
                    ends_by_id.insert(s.id.clone(), s);
                }
                _ => stats.malformed += 1,
            }
        }
    }

    let now_ns = now_nanos();
    let mut kept: Vec<SpoolLine> = Vec::new();
    let mut committed: u64 = 0;

    let mut ctx = StartCtx {
        device,
        actor,
        redactor,
        git_cache: &mut git_cache,
    };
    for (id, start) in starts_by_id {
        let end = ends_by_id.remove(&id);
        if end.is_none() {
            let age = now_ns.saturating_sub(start.ts_ns);
            if age < PENDING_MAX_AGE_NS {
                stats.pending_kept += 1;
                kept.push(start);
                continue;
            }
        }
        if commit_start(store, &mut ctx, &id, &start, end.as_ref(), &mut stats)? {
            committed += 1;
            if opts.crash_after > 0 && committed >= opts.crash_after {
                // Deliberate unclean death for crash-recovery tests.
                std::process::abort();
            }
        }
    }
    stats.orphan_end = ends_by_id.len() as u64;

    // Rewrite young pending starts for a later pass (atomic via spool),
    // then shred the originals. Any crash order replays safely: pending ids
    // dedup in memory, committed ids resolve via the spool mapping.
    for s in kept {
        let line = serde_json::json!({
            "v": 1, "op": "start", "id": s.id, "shell": s.shell,
            "cmd": s.cmd, "cwd": s.cwd, "ts_ns": s.ts_ns, "session": s.session,
        });
        spool::spool_line(spool_dir, &line.to_string())?;
    }
    for path in &spool_files {
        spool::shred(path)?;
    }
    Ok(stats)
}

/// Shared context for committing one paired start.
struct StartCtx<'a> {
    device: &'a str,
    actor: &'a str,
    redactor: &'a Redactor,
    git_cache: &'a mut HashMap<String, Option<git::GitInfo>>,
}

/// Apply edge rules, redact, and commit one start. Returns whether a new
/// record was committed (false = skipped, dropped, or crash-replay dup).
fn commit_start(
    store: &mut Store,
    ctx: &mut StartCtx,
    id: &str,
    start: &SpoolLine,
    end: Option<&SpoolLine>,
    stats: &mut IngestStats,
) -> Result<bool> {
    if start.cmd.as_bytes().contains(&0) {
        stats.dropped_nul += 1;
        return Ok(false);
    }
    if start.cmd.trim().is_empty() {
        stats.skipped_empty += 1;
        return Ok(false);
    }
    let (cmd, truncated, full_len) = truncate(&start.cmd);
    if truncated {
        stats.truncated += 1;
    }
    let (cmd, redacted) = ctx.redactor.redact(&cmd);
    let multiline = cmd.contains('\n');
    let git_info = ctx
        .git_cache
        .entry(start.cwd.clone())
        .or_insert_with(|| git::probe(Path::new(&start.cwd)).unwrap_or(None))
        .clone();
    let payload = serde_json::json!({
        "cmd": cmd,
        "cmd_len": full_len,
        "truncated": truncated,
        "multiline": multiline,
        "redacted": redacted,
        "exit": end.and_then(|e| e.exit),
        "dur_ns": end.and_then(|e| e.dur_ns),
        "completed": end.is_some(),
        "cwd": start.cwd,
        "session": start.session,
        "shell": start.shell,
        "git": git_info.map(|g| serde_json::json!({
            "repo": g.repo_id, "branch": g.branch, "commit": g.commit,
            "bare": g.bare, "outer": g.submodule_outer, "local_only": g.local_only,
        })),
    });
    let canonical = payload.to_string();
    let rec = NewRecord {
        device: ctx.device.to_string(),
        stream: "shell".to_string(),
        ts_ns: start.ts_ns,
        actor: ctx.actor.to_string(),
        kind: "command".to_string(),
        hash: store::hash_payload(&canonical),
        payload,
    };
    let (_, inserted) = store.insert_mapped(id, &rec)?;
    if !inserted {
        // Redelivery of an already-committed event (crash replay).
        return Ok(false);
    }
    if end.is_none() {
        stats.incomplete += 1;
    } else {
        stats.ingested += 1;
    }
    Ok(true)
}

fn truncate(cmd: &str) -> (String, bool, usize) {
    let full_len = cmd.len();
    if full_len <= MAX_COMMAND_BYTES {
        return (cmd.to_string(), false, full_len);
    }
    let mut end = MAX_COMMAND_BYTES;
    while !cmd.is_char_boundary(end) {
        end -= 1;
    }
    (
        format!("{}…[truncated {full_len} bytes]", &cmd[..end]),
        true,
        full_len,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn harness() -> (tempfile::TempDir, Store, String, Redactor) {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open(&tmp.path().join("store")).unwrap();
        let device = store::device_id(&tmp.path().join("store")).unwrap();
        (tmp, store, device, Redactor::new())
    }

    fn start(id: &str, cmd: &str) -> String {
        serde_json::json!({"v":1,"op":"start","id":id,"shell":"bash","cmd":cmd,
            "cwd":"/tmp","ts_ns":now_nanos(),"session":"s1"})
        .to_string()
    }

    fn end(id: &str, exit: i64) -> String {
        serde_json::json!({"v":1,"op":"end","id":id,"exit":exit,"dur_ns":1000}).to_string()
    }

    #[test]
    fn pair_ingests_and_shreds() {
        let (tmp, mut store, device, redactor) = harness();
        let spool = tmp.path().join("spool");
        spool::spool_line(&spool, &start("a", "ls -la")).unwrap();
        spool::spool_line(&spool, &end("a", 0)).unwrap();
        let stats = ingest_once(
            &mut store,
            &spool,
            &device,
            "t:u",
            &redactor,
            IngestOptions::default(),
        )
        .unwrap();
        assert_eq!(stats.ingested, 1);
        assert_eq!(store.count_stream(&device, "shell").unwrap(), 1);
        assert!(spool::pending_spools(&spool).unwrap().is_empty());
        let rec = store.get_record(&device, "shell", 0).unwrap();
        assert!(rec.payload.contains("ls -la"));
    }

    #[test]
    fn edge_rules_hold() {
        let (tmp, mut store, device, redactor) = harness();
        let spool = tmp.path().join("spool");
        spool::spool_line(&spool, &start("e1", "   ")).unwrap();
        spool::spool_line(&spool, &end("e1", 0)).unwrap();
        spool::spool_line(&spool, &start("n1", "echo \0 bad")).unwrap();
        spool::spool_line(&spool, &end("n1", 0)).unwrap();
        spool::spool_line(&spool, &start("m1", "echo one\necho two")).unwrap();
        spool::spool_line(&spool, &end("m1", 0)).unwrap();
        spool::spool_line(&spool, &end("orphan", 0)).unwrap();
        let big = "x".repeat(MAX_COMMAND_BYTES + 10);
        spool::spool_line(&spool, &start("b1", &big)).unwrap();
        spool::spool_line(&spool, &end("b1", 0)).unwrap();
        let stats = ingest_once(
            &mut store,
            &spool,
            &device,
            "t:u",
            &redactor,
            IngestOptions::default(),
        )
        .unwrap();
        assert_eq!(stats.skipped_empty, 1);
        assert_eq!(stats.dropped_nul, 1);
        assert_eq!(stats.orphan_end, 1);
        assert_eq!(stats.truncated, 1);
        assert_eq!(stats.ingested, 2); // multi-line + truncated
        let rec = store.get_record(&device, "shell", 0).unwrap();
        assert!(rec.payload.contains("multiline") || rec.payload.contains("truncated"));
    }

    #[test]
    fn secrets_never_reach_store() {
        let (tmp, mut store, device, redactor) = harness();
        let spool = tmp.path().join("spool");
        spool::spool_line(&spool, &start("s1", "deploy token=supersecret1")).unwrap();
        spool::spool_line(&spool, &end("s1", 0)).unwrap();
        ingest_once(
            &mut store,
            &spool,
            &device,
            "t:u",
            &redactor,
            IngestOptions::default(),
        )
        .unwrap();
        let rec = store.get_record(&device, "shell", 0).unwrap();
        assert!(!rec.payload.contains("supersecret1"));
        assert!(rec.payload.contains("[redacted:secret-assign]"));
    }

    #[test]
    fn replay_after_commit_is_exactly_once() {
        let (tmp, mut store, device, redactor) = harness();
        let spool = tmp.path().join("spool");
        let s = start("r1", "echo hi");
        let e = end("r1", 0);
        spool::spool_line(&spool, &s).unwrap();
        spool::spool_line(&spool, &e).unwrap();
        let st = ingest_once(
            &mut store,
            &spool,
            &device,
            "t:u",
            &redactor,
            IngestOptions::default(),
        )
        .unwrap();
        assert_eq!(st.ingested, 1);
        // Simulate crash-before-shred: same lines arrive again.
        spool::spool_line(&spool, &s).unwrap();
        spool::spool_line(&spool, &e).unwrap();
        let st = ingest_once(
            &mut store,
            &spool,
            &device,
            "t:u",
            &redactor,
            IngestOptions::default(),
        )
        .unwrap();
        assert_eq!(st.ingested, 0);
        assert_eq!(store.count_stream(&device, "shell").unwrap(), 1);
    }

    #[test]
    fn young_pending_survives_pass() {
        let (tmp, mut store, device, redactor) = harness();
        let spool = tmp.path().join("spool");
        spool::spool_line(&spool, &start("p1", "sleep 999")).unwrap();
        let stats = ingest_once(
            &mut store,
            &spool,
            &device,
            "t:u",
            &redactor,
            IngestOptions::default(),
        )
        .unwrap();
        assert_eq!(stats.pending_kept, 1);
        assert_eq!(store.count_stream(&device, "shell").unwrap(), 0);
        // Second pass pairs the end.
        spool::spool_line(&spool, &end("p1", 0)).unwrap();
        let stats = ingest_once(
            &mut store,
            &spool,
            &device,
            "t:u",
            &redactor,
            IngestOptions::default(),
        )
        .unwrap();
        assert_eq!(stats.ingested, 1);
    }
}
