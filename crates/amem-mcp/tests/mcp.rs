//! MCP tool contract: guards, typed errors × formats, busy policy, recall.

use amem_core::store::{self, NewRecord};
use amem_mcp::{Format, Mem, render_error, retry_guidance};
use serde_json::json;

fn seeded() -> (tempfile::TempDir, std::path::PathBuf, Mem) {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("store");
    let mut db = store::Store::open(&dir).unwrap();
    let device = store::device_id(&dir).unwrap();
    let mut shell = NewRecord {
        device: device.clone(),
        stream: "shell".to_string(),
        ts_ns: 1_700_000_000_000_000_000,
        actor: "t:u".to_string(),
        kind: "command".to_string(),
        payload: json!({"cmd": "cargo build", "exit": 0}),
        hash: store::hash_payload(r#"{"cmd":"cargo build","exit":0}"#),
    };
    db.insert_mapped("spool-1", &shell).unwrap();
    shell.payload = json!({"cmd": "cargo test", "exit": 1});
    shell.ts_ns += 1;
    db.insert_mapped("spool-2", &shell).unwrap();
    drop(db);
    let mem = Mem::open(&dir).unwrap();
    (tmp, dir, mem)
}

fn checksum_of(mem: &Mem, id: &str) -> String {
    let out = mem.read(id, None, Format::Json).unwrap();
    let v: serde_json::Value = serde_json::from_str(&out).unwrap();
    v["checksum"].as_str().unwrap().to_string()
}

#[test]
fn guards_stale_write_rejected_reread_resubmit_succeeds() {
    let (_tmp, _dir, mem) = seeded();
    let created = mem
        .write(Some("n1"), "title", "v1", None, Format::Json)
        .unwrap();
    let v: serde_json::Value = serde_json::from_str(&created).unwrap();
    assert_eq!(v["created"], true);
    let sum = checksum_of(&mem, "n1");
    // Unguarded overwrite refused.
    let err = mem
        .write(Some("n1"), "t", "v2", None, Format::Text)
        .unwrap_err();
    assert_eq!(err.code(), "ALREADY_EXISTS");
    // Stale guard refused; store unchanged.
    let err = mem
        .write(Some("n1"), "t", "v2", Some("stale"), Format::Text)
        .unwrap_err();
    assert_eq!(err.code(), "CONFLICT");
    assert_eq!(checksum_of(&mem, "n1"), sum);
    // Re-read + resubmit succeeds.
    let updated = mem
        .write(Some("n1"), "t", "v2", Some(&sum), Format::Text)
        .unwrap();
    assert!(updated.starts_with("updated notes#"), "{updated}");
    assert_ne!(checksum_of(&mem, "n1"), sum);
    // Guarded delete: stale refused, fresh succeeds.
    let err = mem.delete("n1", None, "stale", Format::Text).unwrap_err();
    assert_eq!(err.code(), "CONFLICT");
    let fresh = checksum_of(&mem, "n1");
    assert_eq!(
        mem.delete("n1", None, &fresh, Format::Text).unwrap(),
        "deleted notes#1"
    );
    assert_eq!(
        mem.read("n1", None, Format::Text).unwrap_err().code(),
        "NOT_FOUND"
    );
}

#[test]
fn recorder_streams_reject_mutation() {
    let (_tmp, _dir, mem) = seeded();
    let err = mem
        .write(Some("spool-1"), "t", "evil", None, Format::Text)
        .unwrap_err();
    assert_eq!(err.code(), "INVALID_ARGUMENTS");
    let err = mem
        .delete("spool-1", None, "whatever", Format::Text)
        .unwrap_err();
    assert_eq!(err.code(), "INVALID_ARGUMENTS");
    // Recorder data intact.
    assert!(
        mem.search("cargo build", None, 10, Format::Text)
            .unwrap()
            .contains("cargo build")
    );
}

#[test]
fn errors_all_codes_text_and_json() {
    let (_tmp, dir, mem) = seeded();
    mem.write(Some("note-alpha"), "t", "x", None, Format::Text)
        .unwrap();
    mem.write(Some("note-beta"), "t", "y", None, Format::Text)
        .unwrap();
    let sum = checksum_of(&mem, "note-alpha");
    // Produce each of the 5 codes.
    let not_found = mem.read("missing", None, Format::Text).unwrap_err();
    let ambiguous = mem.read("note-", None, Format::Text).unwrap_err();
    let exists = mem
        .write(Some("note-alpha"), "t", "z", None, Format::Text)
        .unwrap_err();
    let conflict = mem
        .write(Some("note-alpha"), "t", "z", Some("stale"), Format::Text)
        .unwrap_err();
    // OUTCOME_UNKNOWN via a held write txn (fail-fast writer).
    let unknown = {
        let conn = rusqlite::Connection::open(dir.join("memory.db")).unwrap();
        conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        let e = mem
            .write(Some("note-alpha"), "t", "z", Some(&sum), Format::Text)
            .unwrap_err();
        conn.execute_batch("ROLLBACK").unwrap();
        e
    };
    for (e, code) in [
        (not_found, "NOT_FOUND"),
        (ambiguous, "AMBIGUOUS"),
        (exists, "ALREADY_EXISTS"),
        (conflict, "CONFLICT"),
        (unknown, "OUTCOME_UNKNOWN"),
    ] {
        assert_eq!(e.code(), code);
        assert!(!retry_guidance(&e).is_empty());
        let text = render_error(&e, Format::Text);
        assert!(text.starts_with(code), "{text}");
        assert!(text.contains("retry:"), "{text}");
        let js = render_error(&e, Format::Json);
        let v: serde_json::Value = serde_json::from_str(&js).unwrap();
        assert_eq!(v["error"]["code"], code);
        assert!(!v["error"]["retry"].as_str().unwrap().is_empty());
    }
    // Bad format is itself a typed usage error.
    assert!(Format::parse(Some("yaml")).is_err());
}

#[test]
fn busy_collide_store_consistent() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("store");
    let mut db = store::Store::open(&dir).unwrap();
    let device = store::device_id(&dir).unwrap();
    let (canonical, hash) = store::note_payload("n1", "t", "v1");
    let rec = NewRecord {
        device,
        stream: "notes".to_string(),
        ts_ns: 1,
        actor: "t".to_string(),
        kind: "note".to_string(),
        payload: serde_json::from_str(&canonical).unwrap(),
        hash,
    };
    db.insert_mapped("n1", &rec).unwrap();
    drop(db);
    let mem = Mem::open(&dir).unwrap();
    let sum = checksum_of(&mem, "n1");
    // Racing a bulk ingest (held IMMEDIATE): typed OUTCOME_UNKNOWN, fast.
    let conn = rusqlite::Connection::open(dir.join("memory.db")).unwrap();
    conn.execute_batch("BEGIN IMMEDIATE").unwrap();
    let err = mem
        .write(Some("n1"), "t", "v2", Some(&sum), Format::Text)
        .unwrap_err();
    assert_eq!(err.code(), "OUTCOME_UNKNOWN");
    conn.execute_batch("ROLLBACK").unwrap();
    drop(conn);
    // Store consistent: the raced write did not half-land.
    assert_eq!(checksum_of(&mem, "n1"), sum);
    assert!(mem.read("n1", None, Format::Text).unwrap().contains("v1"));
    // After the lock clears, the same guarded write succeeds.
    mem.write(Some("n1"), "t", "v2", Some(&sum), Format::Text)
        .unwrap();
    assert!(mem.read("n1", None, Format::Text).unwrap().contains("v2"));
}

#[test]
fn context_brief_recalls_seeded_events() {
    let (_tmp, _dir, mem) = seeded();
    mem.write(Some("n1"), "plan", "ship mvp", None, Format::Text)
        .unwrap();
    let brief = mem.context(100, Format::Text).unwrap();
    assert!(brief.contains("cargo build"), "{brief}");
    assert!(brief.contains("cargo test"), "{brief}");
    assert!(brief.contains("ship mvp"), "{brief}");
    // Chronological: build before test.
    let b = brief.find("cargo build").unwrap();
    let t = brief.find("cargo test").unwrap();
    assert!(b < t, "{brief}");
    let js = mem.context(100, Format::Json).unwrap();
    let v: serde_json::Value = serde_json::from_str(&js).unwrap();
    assert_eq!(v["events"].as_array().unwrap().len(), 3);
}

#[test]
fn search_and_list_json_shapes_parse() {
    let (_tmp, _dir, mem) = seeded();
    let js = mem.search("cargo", None, 10, Format::Json).unwrap();
    let v: serde_json::Value = serde_json::from_str(&js).unwrap();
    assert_eq!(v["results"].as_array().unwrap().len(), 2);
    assert!(v["results"][0]["checksum"].is_string());
    let js = mem.list(Some("shell"), 1, 1, Format::Json).unwrap();
    let v: serde_json::Value = serde_json::from_str(&js).unwrap();
    assert_eq!(v["results"].as_array().unwrap().len(), 1);
    assert_eq!(v["offset"], 1);
    assert!(
        mem.list(None, 10, 0, Format::Text)
            .unwrap()
            .contains("cargo test")
    );
}
