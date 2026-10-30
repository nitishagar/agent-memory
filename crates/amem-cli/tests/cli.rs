//! CLI integration tests: history spooling, ingest, init snippets, doctor,
//! sync-stub failures, and unclean-death recovery — all against temp stores.

use std::path::{Path, PathBuf};
use std::process::Command;

fn amem() -> Command {
    Command::new(env!("CARGO_BIN_EXE_amem"))
}

fn temp_store(name: &str) -> (tempfile::TempDir, PathBuf) {
    let tmp = tempfile::tempdir().unwrap();
    let store = tmp.path().join(name);
    (tmp, store)
}

fn store_count(store: &Path) -> i64 {
    let db = amem_core::store::Store::open(store).unwrap();
    let device = amem_core::store::device_id(store).unwrap();
    db.count_stream(&device, "shell").unwrap()
}

#[test]
fn init_snippets_render_per_shell() {
    for (shell, marker) in [
        ("bash", "preexec_functions"),
        ("zsh", "add-zsh-hook"),
        ("fish", "fish_preexec"),
        ("powershell", "Get-History"),
    ] {
        let out = amem().args(["init", shell]).output().unwrap();
        assert!(out.status.success(), "{shell} init failed");
        let text = String::from_utf8(out.stdout).unwrap();
        assert!(text.contains(marker), "{shell} missing {marker}");
        assert!(
            text.contains(">>> amem init"),
            "{shell} missing guard markers"
        );
    }
    let out = amem().args(["init", "tcsh"]).output().unwrap();
    assert!(!out.status.success());
    // `pwsh` is an alias of `powershell`.
    let a = amem().args(["init", "powershell"]).output().unwrap();
    let b = amem().args(["init", "pwsh"]).output().unwrap();
    assert!(b.status.success());
    assert_eq!(a.stdout, b.stdout);
}

#[test]
fn history_spool_to_ingest_roundtrip() {
    let (_tmp, store) = temp_store("s1");
    let store = store.to_str().unwrap().to_string();
    let out = amem()
        .args([
            "--store-dir",
            &store,
            "history",
            "start",
            "--shell",
            "bash",
            "--cmd",
            "ls",
        ])
        .output()
        .unwrap();
    assert!(out.status.success());
    let id = String::from_utf8(out.stdout).unwrap();
    let id = id.trim().to_string();
    assert_eq!(id.len(), 32);
    let st = amem()
        .args([
            "--store-dir",
            &store,
            "history",
            "end",
            "--id",
            &id,
            "--exit",
            "0",
        ])
        .status()
        .unwrap();
    assert!(st.success());
    let out = amem()
        .args([
            "--store-dir",
            &store,
            "ingest",
            "--once",
            "--no-transcripts",
        ])
        .output()
        .unwrap();
    assert!(out.status.success());
    let stats: serde_json::Value = serde_json::from_slice(&out.stdout).expect("stats are JSON");
    assert_eq!(stats["ingested"], 1);
}

#[test]
fn sync_bogus_endpoints_fail_typed() {
    let (_tmp, store) = temp_store("s2");
    let store = store.to_str().unwrap().to_string();
    // Refused (nothing listens on :9).
    let out = amem()
        .args([
            "--store-dir",
            &store,
            "sync",
            "--endpoint",
            "http://127.0.0.1:9/",
        ])
        .output()
        .unwrap();
    assert!(!out.status.success());
    let err = String::from_utf8(out.stderr).unwrap();
    assert!(err.contains("SYNC_ERROR"), "stderr: {err}");
    // Unresolvable host → distinct DNS error.
    let out = amem()
        .args([
            "--store-dir",
            &store,
            "sync",
            "--endpoint",
            "http://nonexistent.invalid/",
        ])
        .output()
        .unwrap();
    assert!(!out.status.success());
    let err = String::from_utf8(out.stderr).unwrap();
    assert!(err.contains("resolve"), "stderr: {err}");
    // https explicitly deferred.
    let out = amem()
        .args([
            "--store-dir",
            &store,
            "sync",
            "--endpoint",
            "https://example.com/",
        ])
        .output()
        .unwrap();
    assert!(!out.status.success());
    let err = String::from_utf8(out.stderr).unwrap();
    assert!(err.contains("Phase 4"), "stderr: {err}");
}

#[test]
fn sync_ignores_proxy_env() {
    use std::net::TcpListener;
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let (_tmp, store) = temp_store("s3");
    let out = amem()
        .env("https_proxy", "http://bogus.invalid:9999")
        .env("http_proxy", "http://bogus.invalid:9999")
        .args([
            "--store-dir",
            store.to_str().unwrap(),
            "sync",
            "--endpoint",
            &format!("http://127.0.0.1:{port}/"),
        ])
        .output()
        .unwrap();
    assert!(out.status.success(), "proxy env leaked into sync");
    drop(listener);
}

#[test]
fn doctor_reports_store_and_sources() {
    let (_tmp, store) = temp_store("s4");
    let out = amem()
        .args(["--store-dir", store.to_str().unwrap(), "doctor"])
        .output()
        .unwrap();
    assert!(out.status.success());
    let text = String::from_utf8(out.stdout).unwrap();
    assert!(text.contains("amem doctor"));
    assert!(text.contains("transcript sources:"));
    assert!(text.contains("shell rc files:"));
}

#[cfg(unix)]
#[test]
fn read_only_store_fails_clearly() {
    use std::os::unix::fs::PermissionsExt;
    let tmp = tempfile::tempdir().unwrap();
    let store = tmp.path().join("ro");
    std::fs::create_dir(&store).unwrap();
    std::fs::set_permissions(&store, std::fs::Permissions::from_mode(0o555)).unwrap();
    let out = amem()
        .args([
            "--store-dir",
            store.to_str().unwrap(),
            "ingest",
            "--once",
            "--no-transcripts",
        ])
        .output()
        .unwrap();
    assert!(!out.status.success());
    let err = String::from_utf8(out.stderr).unwrap();
    assert!(err.contains("not writable"), "stderr: {err}");
    std::fs::set_permissions(&store, std::fs::Permissions::from_mode(0o755)).unwrap();
}

#[test]
fn crash_mid_pass_recovers_exactly_once() {
    let (_tmp, store) = temp_store("s5");
    let store_s = store.to_str().unwrap().to_string();
    for i in 0..200 {
        let id = format!("crash-{i}");
        let start = serde_json::json!({"v":1,"op":"start","id":id,"shell":"bash",
            "cmd":format!("echo {i}"),"cwd":"/tmp","ts_ns":1_700_000_000_000_000_000i64,"session":"s"});
        let end = serde_json::json!({"v":1,"op":"end","id":id,"exit":0,"dur_ns":1});
        amem_core::spool::spool_line(&store.join("spool"), &start.to_string()).unwrap();
        amem_core::spool::spool_line(&store.join("spool"), &end.to_string()).unwrap();
    }
    // Unclean death after 50 commits.
    let st = amem()
        .args([
            "--store-dir",
            &store_s,
            "ingest",
            "--once",
            "--crash-after",
            "50",
        ])
        .status()
        .unwrap();
    assert!(!st.success(), "crash flag must abort");
    // Clean rerun replays everything exactly once.
    let out = amem()
        .args([
            "--store-dir",
            &store_s,
            "ingest",
            "--once",
            "--no-transcripts",
        ])
        .output()
        .unwrap();
    assert!(out.status.success());
    assert_eq!(store_count(&store), 200);
    // And a third pass is a no-op.
    let out = amem()
        .args([
            "--store-dir",
            &store_s,
            "ingest",
            "--once",
            "--no-transcripts",
        ])
        .output()
        .unwrap();
    let stats: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(stats["ingested"], 0);
}

#[test]
fn search_finds_seeded_command() {
    let (_tmp, store) = temp_store("s12");
    let store_s = store.to_str().unwrap().to_string();
    let st = amem()
        .env("AMEM_E1", "deploy needle-xyz-1")
        .args([
            "--store-dir",
            &store_s,
            "history",
            "both",
            "--shell",
            "bash",
            "--exit",
            "0",
            "--cmd-from-env",
            "AMEM_E1",
        ])
        .status()
        .unwrap();
    assert!(st.success());
    let st = amem()
        .args([
            "--store-dir",
            &store_s,
            "ingest",
            "--once",
            "--no-transcripts",
        ])
        .status()
        .unwrap();
    assert!(st.success());
    let out = amem()
        .args(["--store-dir", &store_s, "search", "needle-xyz-1"])
        .output()
        .unwrap();
    assert!(out.status.success());
    let text = String::from_utf8(out.stdout).unwrap();
    assert!(text.contains("needle-xyz-1"), "stdout: {text}");
    assert!(text.contains("[shell #0]"), "stdout: {text}");
    let out = amem()
        .args(["--store-dir", &store_s, "search", "nothing-matches-this"])
        .output()
        .unwrap();
    assert!(out.status.success());
    assert!(String::from_utf8(out.stdout).unwrap().is_empty());
}

#[test]
fn search_survives_hostile_transcript_session_and_escapes() {
    // Multibyte session (byte 8 splits a char), ESC/CR in text: search
    // must not panic and must not emit raw controls.
    let (tmp, store) = temp_store("s13");
    let store_s = store.to_str().unwrap().to_string();
    let root = tmp.path().join("claude");
    std::fs::create_dir_all(root.join("slug")).unwrap();
    // é = 2 bytes: byte 8 splits the 4th char.
    let line = serde_json::json!({"type":"user",
        "message":{"role":"user","content":"escape-me-\u{1b}]0;pwned\u{7}argent\r"},
        "uuid":"aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaaa",
        "timestamp":"2026-10-30T00:00:00.000Z",
        "sessionId":"éééééééééé","cwd":"/p"})
    .to_string();
    std::fs::write(root.join("slug").join("s.jsonl"), format!("{line}\n")).unwrap();
    let st = amem()
        .args([
            "--store-dir",
            &store_s,
            "ingest",
            "--once",
            "--transcript-root",
            &format!("claude={}", root.display()),
            "--transcript-root",
            "codex=/nonexistent-codex",
            "--transcript-root",
            "cursor=/nonexistent-cursor",
            "--transcript-root",
            "opencode=/nonexistent-opencode",
            "--transcript-root",
            "pi=/nonexistent-pi",
            "--transcript-root",
            "zcode=/nonexistent-zcode",
        ])
        .status()
        .unwrap();
    assert!(st.success());
    let out = amem()
        .args(["--store-dir", &store_s, "search", "escape-me"])
        .output()
        .unwrap();
    assert!(out.status.success());
    let stdout = out.stdout;
    assert!(!stdout.is_empty(), "hostile record not found");
    assert!(!stdout.contains(&0x1b), "ESC leaked to terminal");
    assert!(!stdout.contains(&b'\r'), "CR leaked to terminal");
}

fn start_via_stdin(store: &str, shell: &str, cmd_bytes: &[u8]) -> String {
    start_via_stdin_session(store, shell, cmd_bytes, None)
}

fn start_via_stdin_session(
    store: &str,
    shell: &str,
    cmd_bytes: &[u8],
    session: Option<&str>,
) -> String {
    use std::io::Write as _;
    use std::process::Stdio;
    let mut cmd = amem();
    cmd.args([
        "--store-dir",
        store,
        "history",
        "start",
        "--shell",
        shell,
        "--cmd-from-stdin",
    ]);
    if let Some(s) = session {
        cmd.env("AMEM_SESSION", s);
    }
    let mut child = cmd
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    child.stdin.as_mut().unwrap().write_all(cmd_bytes).unwrap();
    let out = child.wait_with_output().unwrap();
    assert!(out.status.success());
    String::from_utf8(out.stdout).unwrap().trim().to_string()
}

#[test]
fn history_rejects_unknown_shell() {
    let (_tmp, store) = temp_store("s8");
    let out = amem()
        .args([
            "--store-dir",
            store.to_str().unwrap(),
            "history",
            "start",
            "--shell",
            "tcsh",
            "--cmd",
            "ls",
        ])
        .output()
        .unwrap();
    assert!(!out.status.success());
    let err = String::from_utf8(out.stderr).unwrap();
    assert!(err.contains("unknown shell"), "stderr: {err}");
}

#[test]
fn doctor_creates_nothing() {
    let (tmp, store) = temp_store("s9");
    assert!(!store.exists());
    let out = amem()
        .args(["--store-dir", store.to_str().unwrap(), "doctor"])
        .env("HOME", tmp.path())
        .output()
        .unwrap();
    assert!(out.status.success());
    assert!(!store.exists(), "doctor created the store");
    let text = String::from_utf8(out.stdout).unwrap();
    assert!(text.contains("not initialized"), "stdout: {text}");
}

#[test]
fn sync_errors_never_echo_credentials() {
    let (_tmp, store) = temp_store("s10");
    let out = amem()
        .args([
            "--store-dir",
            store.to_str().unwrap(),
            "sync",
            "--endpoint",
            "http://user:secret@127.0.0.1:9/",
        ])
        .output()
        .unwrap();
    assert!(!out.status.success());
    let err = String::from_utf8(out.stderr).unwrap();
    assert!(!err.contains("secret"), "stderr leaks: {err}");
    assert!(err.contains("127.0.0.1:9"), "stderr: {err}");
}

#[test]
fn bad_redact_pattern_warns_instead_of_wedging() {
    let (_tmp, store) = temp_store("s11");
    std::fs::create_dir_all(&store).unwrap();
    std::fs::write(store.join("redact.json"), "{\"extra\": [\"([\"]}").unwrap();
    let out = amem()
        .args([
            "--store-dir",
            store.to_str().unwrap(),
            "ingest",
            "--once",
            "--no-transcripts",
        ])
        .output()
        .unwrap();
    let err = String::from_utf8(out.stderr).unwrap();
    let stdout = String::from_utf8(out.stdout).unwrap();
    assert!(
        out.status.success(),
        "ingest wedged by bad pattern: stdout={stdout} stderr={err}"
    );
    assert!(err.contains("skipping redaction pattern"), "stderr: {err}");
}

#[test]
fn history_start_stdin_beats_argv_ceiling() {
    // 200KB > MAX_ARG_STRLEN (~128KB): argv/env transport would E2BIG.
    let (_tmp, store) = temp_store("s6");
    let store_s = store.to_str().unwrap().to_string();
    let big = "x".repeat(200_000);
    let id = start_via_stdin(&store_s, "bash", big.as_bytes());
    assert_eq!(id.len(), 32);
    let st = amem()
        .args([
            "--store-dir",
            &store_s,
            "history",
            "end",
            "--id",
            &id,
            "--exit",
            "0",
        ])
        .status()
        .unwrap();
    assert!(st.success());
    let out = amem()
        .args([
            "--store-dir",
            &store_s,
            "ingest",
            "--once",
            "--no-transcripts",
        ])
        .output()
        .unwrap();
    let stats: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(stats["ingested"], 1);
    assert_eq!(stats["truncated"], 0); // 200KB < 1MB store ceiling
    let db = amem_core::store::Store::open(&store).unwrap();
    let device = amem_core::store::device_id(&store).unwrap();
    let rec = db.get_record(&device, "shell", 0).unwrap();
    let payload: serde_json::Value = serde_json::from_str(&rec.payload).unwrap();
    assert_eq!(payload["cmd_len"], 200_000);
}

#[test]
fn history_start_stdin_strips_one_trailing_newline() {
    // PowerShell newline-terminates piped native-stdin input.
    let (_tmp, store) = temp_store("s7");
    let store_s = store.to_str().unwrap().to_string();
    let id = start_via_stdin(&store_s, "powershell", b"echo hi\n");
    let st = amem()
        .args([
            "--store-dir",
            &store_s,
            "history",
            "end",
            "--id",
            &id,
            "--exit",
            "0",
        ])
        .status()
        .unwrap();
    assert!(st.success());
    let out = amem()
        .args([
            "--store-dir",
            &store_s,
            "ingest",
            "--once",
            "--no-transcripts",
        ])
        .output()
        .unwrap();
    assert!(out.status.success());
    let db = amem_core::store::Store::open(&store).unwrap();
    let device = amem_core::store::device_id(&store).unwrap();
    let rec = db.get_record(&device, "shell", 0).unwrap();
    let payload: serde_json::Value = serde_json::from_str(&rec.payload).unwrap();
    assert_eq!(payload["cmd"], "echo hi");
}
