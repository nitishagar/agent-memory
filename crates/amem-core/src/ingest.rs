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

/// Spool files larger than this are skipped for the pass (left in
/// place, counted): legit files hold one ≤1MB line, so anything past
/// 16MB is hostile or corrupt, never memory.
pub const MAX_SPOOL_FILE_BYTES: u64 = 16 * 1024 * 1024;

/// Cap on distinct git probes per pass: `cwd` is spool-controlled.
const MAX_GIT_PROBES_PER_PASS: usize = 1000;

/// Raw hook line (v1 protocol).
#[derive(Debug, Deserialize)]
struct SpoolLine {
    #[serde(default)]
    v: u32,
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
    /// Oversize spool files skipped for the pass (left in place).
    pub skipped_large: u64,
    /// Transcript files parsed this pass (all sources).
    pub transcript_files: u64,
    /// Transcript events parsed this pass (all sources).
    pub transcript_events: u64,
    /// New transcript records committed this pass.
    pub transcript_ingested: u64,
    /// Transcript warnings this pass (bad rows/files skipped).
    pub transcript_warnings: u64,
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
    let mut shreddable: Vec<&Path> = Vec::with_capacity(spool_files.len());
    for path in &spool_files {
        let size = std::fs::metadata(path).map_or(0, |m| m.len());
        if size > MAX_SPOOL_FILE_BYTES {
            stats.skipped_large += 1;
            continue;
        }
        // Unreadable is not malformed: leave the file for a later pass
        // (transient errors resolve; shredding would destroy evidence).
        // Counted so the pass stays loud.
        let Ok(text) = std::fs::read_to_string(path) else {
            stats.malformed += 1;
            continue;
        };
        shreddable.push(path);
        for line in text.lines() {
            match serde_json::from_str::<SpoolLine>(line) {
                Ok(s) if s.v == spool::SPOOL_VERSION && s.op == "start" => {
                    starts_by_id.insert(s.id.clone(), s);
                }
                Ok(s) if s.v == spool::SPOOL_VERSION && s.op == "end" => {
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
    // Commit in capture order so `(device, stream, idx)` is causal.
    let mut ordered: Vec<(String, SpoolLine)> = starts_by_id.into_iter().collect();
    ordered.sort_by(|a, b| (a.1.ts_ns, &a.0).cmp(&(b.1.ts_ns, &b.0)));
    for (id, start) in ordered {
        let end = ends_by_id.remove(&id);
        if end.is_none() {
            // Clamp future timestamps for aging: far-future pins nothing.
            let age = now_ns.saturating_sub(start.ts_ns.min(now_ns));
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

    // Young orphan ends are rewritten like pending starts: their start may
    // still be a staging dotfile (skipped by the listing) and arrive next
    // pass. Old orphans are counted and dropped.
    let mut orphans: Vec<SpoolLine> = ends_by_id.into_values().collect();
    orphans.sort_by(|a, b| a.id.cmp(&b.id));
    for end in orphans {
        let age = now_ns.saturating_sub(end.ts_ns.min(now_ns));
        if age < PENDING_MAX_AGE_NS {
            let line = serde_json::json!({
                "v": 1, "op": "end", "id": end.id, "exit": end.exit,
                "dur_ns": end.dur_ns, "ts_ns": end.ts_ns,
            });
            spool::spool_line(spool_dir, &line.to_string())?;
            stats.pending_kept += 1;
        } else {
            stats.orphan_end += 1;
        }
    }

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
    for path in shreddable {
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
    let (cwd, cwd_redacted) = ctx.redactor.redact(&start.cwd);
    let multiline = cmd.contains('\n');
    // `cwd` is spool-controlled: never probe missing dirs, cap probes.
    let git_info =
        if Path::new(&start.cwd).exists() && ctx.git_cache.len() < MAX_GIT_PROBES_PER_PASS {
            ctx.git_cache
                .entry(start.cwd.clone())
                .or_insert_with(|| git::probe(Path::new(&start.cwd)).unwrap_or(None))
                .clone()
        } else {
            ctx.git_cache.get(&start.cwd).cloned().flatten()
        };
    let payload = serde_json::json!({
        "cmd": cmd,
        "cmd_len": full_len,
        "truncated": truncated,
        "multiline": multiline,
        "redacted": redacted || cwd_redacted,
        "exit": end.and_then(|e| e.exit),
        "dur_ns": end.and_then(|e| e.dur_ns),
        "completed": end.is_some(),
        "cwd": cwd,
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

    fn start_ts(id: &str, cmd: &str, ts_ns: i64) -> String {
        serde_json::json!({"v":1,"op":"start","id":id,"shell":"bash","cmd":cmd,
            "cwd":"/tmp","ts_ns":ts_ns,"session":"s1"})
        .to_string()
    }

    fn end(id: &str, exit: i64) -> String {
        serde_json::json!({"v":1,"op":"end","id":id,"exit":exit,"dur_ns":1000}).to_string()
    }

    fn end_ts(id: &str, exit: i64, ts_ns: i64) -> String {
        serde_json::json!({"v":1,"op":"end","id":id,"exit":exit,"dur_ns":1000,"ts_ns":ts_ns})
            .to_string()
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
    fn aged_start_records_incomplete() {
        let (tmp, mut store, device, redactor) = harness();
        let spool = tmp.path().join("spool");
        let old = now_nanos() - PENDING_MAX_AGE_NS - 1;
        spool::spool_line(&spool, &start_ts("old1", "sleep 999", old)).unwrap();
        let stats = ingest_once(
            &mut store,
            &spool,
            &device,
            "t:u",
            &redactor,
            IngestOptions::default(),
        )
        .unwrap();
        assert_eq!(stats.incomplete, 1);
        assert_eq!(stats.pending_kept, 0);
        assert!(spool::pending_spools(&spool).unwrap().is_empty());
        let rec = store.get_record(&device, "shell", 0).unwrap();
        assert!(rec.payload.contains("\"completed\":false"));
    }

    #[test]
    fn young_orphan_end_rewritten_old_dropped() {
        let (tmp, mut store, device, redactor) = harness();
        let spool = tmp.path().join("spool");
        spool::spool_line(&spool, &end_ts("late-start", 0, now_nanos())).unwrap();
        spool::spool_line(&spool, &end_ts("ancient", 0, 1)).unwrap();
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
        assert_eq!(stats.orphan_end, 1);
        assert_eq!(store.count_stream(&device, "shell").unwrap(), 0);
        // The young orphan survives; the late start pairs next pass.
        assert_eq!(spool::pending_spools(&spool).unwrap().len(), 1);
        spool::spool_line(&spool, &start("late-start", "echo late")).unwrap();
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

    #[test]
    fn oversize_spool_left_in_place() {
        let (tmp, mut store, device, redactor) = harness();
        let spool = tmp.path().join("spool");
        std::fs::create_dir_all(&spool).unwrap();
        let big = "z".repeat(usize::try_from(MAX_SPOOL_FILE_BYTES + 1).unwrap());
        std::fs::write(spool.join("spool-huge.jsonl"), big).unwrap();
        let stats = ingest_once(
            &mut store,
            &spool,
            &device,
            "t:u",
            &redactor,
            IngestOptions::default(),
        )
        .unwrap();
        assert_eq!(stats.skipped_large, 1);
        assert_eq!(stats.malformed, 0);
        assert!(spool.join("spool-huge.jsonl").exists());
    }

    #[test]
    fn future_protocol_version_is_malformed() {
        let (tmp, mut store, device, redactor) = harness();
        let spool = tmp.path().join("spool");
        spool::spool_line(
            &spool,
            r#"{"v":2,"op":"start","id":"v2","shell":"bash","cmd":"ls","cwd":"/tmp","ts_ns":1,"session":"s"}"#,
        )
        .unwrap();
        let stats = ingest_once(
            &mut store,
            &spool,
            &device,
            "t:u",
            &redactor,
            IngestOptions::default(),
        )
        .unwrap();
        assert_eq!(stats.malformed, 1);
        assert_eq!(store.count_stream(&device, "shell").unwrap(), 0);
    }

    #[test]
    fn commit_order_follows_capture_time() {
        let (tmp, mut store, device, redactor) = harness();
        let spool = tmp.path().join("spool");
        let base = 1_700_000_000_000_000_000i64;
        // Spool newest-first; idx must still follow ts.
        for (id, ts, cmd) in [
            ("c", base + 2, "third"),
            ("a", base, "first"),
            ("b", base + 1, "second"),
        ] {
            spool::spool_line(&spool, &start_ts(id, cmd, ts)).unwrap();
            spool::spool_line(&spool, &end(id, 0)).unwrap();
        }
        ingest_once(
            &mut store,
            &spool,
            &device,
            "t:u",
            &redactor,
            IngestOptions::default(),
        )
        .unwrap();
        for (idx, cmd) in [(0, "first"), (1, "second"), (2, "third")] {
            let rec = store.get_record(&device, "shell", idx).unwrap();
            assert!(rec.payload.contains(cmd), "idx {idx}: {}", rec.payload);
        }
    }

    #[test]
    fn secrets_in_cwd_redacted() {
        let (tmp, mut store, device, redactor) = harness();
        let spool = tmp.path().join("spool");
        let line = serde_json::json!({"v":1,"op":"start","id":"cwd1","shell":"bash",
            "cmd":"ls","cwd":"/tmp/token=s3cr3t","ts_ns":now_nanos(),"session":"s1"})
        .to_string();
        spool::spool_line(&spool, &line).unwrap();
        spool::spool_line(&spool, &end("cwd1", 0)).unwrap();
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
        assert!(!rec.payload.contains("s3cr3t"), "{}", rec.payload);
        assert!(rec.payload.contains("[redacted:secret-assign]"));
    }

    #[cfg(unix)]
    #[test]
    fn unreadable_spool_kept_not_shredded() {
        use std::os::unix::fs::PermissionsExt;
        let (tmp, mut store, device, redactor) = harness();
        let spool = tmp.path().join("spool");
        let p = spool::spool_line(&spool, &start("u1", "echo hi")).unwrap();
        std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o000)).unwrap();
        if std::fs::read_to_string(&p).is_ok() {
            return; // Running as root: perms don't bite; nothing to prove.
        }
        let stats = ingest_once(
            &mut store,
            &spool,
            &device,
            "t:u",
            &redactor,
            IngestOptions::default(),
        )
        .unwrap();
        assert_eq!(stats.malformed, 1);
        assert_eq!(stats.ingested, 0);
        assert!(p.exists(), "unreadable spool must survive the pass");
        std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o600)).unwrap();
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
