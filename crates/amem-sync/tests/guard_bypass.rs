//! Guard bypass: sync apply of a conflicting note branches the loser and
//! never silently overwrites the store.

mod common;

use amem_sync::client::SyncClient;
use amem_sync::sim::SimulatedTransport;

const EP: &str = "https://sync.test:8443";

fn seed_note(dir: &std::path::Path, id: &str, text: &str) {
    let mut db = amem_core::store::Store::open(dir).unwrap();
    let device = amem_core::store::device_id(dir).unwrap();
    let (canonical, hash) = amem_core::store::note_payload(id, "t", text);
    let rec = amem_core::store::NewRecord {
        device,
        stream: "notes".to_string(),
        ts_ns: 1_700_000_000_000_000_000,
        actor: "t:u".to_string(),
        kind: "note".to_string(),
        payload: serde_json::from_str(&canonical).unwrap(),
        hash,
    };
    db.insert_mapped(id, &rec).unwrap();
}

#[tokio::test]
async fn conflicting_note_branches_loser_keeps_both() {
    let tmp = tempfile::tempdir().unwrap();
    let sim = SimulatedTransport::open(&tmp.path().join("server.db")).unwrap();
    let (dir_a, dir_b) = (tmp.path().join("a"), tmp.path().join("b"));
    let da = common::seed_shell(&dir_a, 1, "a");
    let db = common::seed_shell(&dir_b, 1, "b");
    seed_note(&dir_a, "deploy", "A: freeze fridays");
    seed_note(&dir_b, "deploy", "B: ship fridays");
    let a = SyncClient::open(&dir_a, EP, sim.clone()).unwrap();
    let b = SyncClient::open(&dir_b, EP, sim.clone()).unwrap();
    a.sync(false).await.unwrap();
    let s = b.sync(false).await.unwrap();
    assert!(s.branches >= 1, "{s:?}");
    // B's local note is byte-identical (never overwritten)...
    let store = amem_core::store::Store::open(&dir_b).unwrap();
    let (stream, idx) = store
        .resolve_external(&db, "deploy", Some("notes"))
        .unwrap();
    let local = store.get_record(&db, &stream, idx).unwrap();
    assert!(local.payload.contains("ship fridays"), "{}", local.payload);
    // ...A's note arrived under its own triple (immutable union)...
    let remote = store.get_record(&da, "notes", 0).unwrap();
    assert!(
        remote.payload.contains("freeze fridays"),
        "{}",
        remote.payload
    );
    // ...and the conflict is branched with the loser envelope preserved.
    let branches = store.branch_list().unwrap();
    assert_eq!(branches.len(), 1);
    assert_eq!(branches[0].ext_id, "deploy");
    assert_eq!(branches[0].reason, "id");
    assert!(branches[0].loser_envelope.contains("freeze fridays"));
    drop(store);
    // A side converges symmetrically (its own note wins locally).
    a.sync(false).await.unwrap();
    let store = amem_core::store::Store::open(&dir_a).unwrap();
    assert_eq!(store.branch_list().unwrap().len(), 1);
    let (_s, i) = store
        .resolve_external(&da, "deploy", Some("notes"))
        .unwrap();
    assert!(
        store
            .get_record(&da, "notes", i)
            .unwrap()
            .payload
            .contains("freeze fridays")
    );
}

#[tokio::test]
async fn pk_conflict_branches_without_overwrite() {
    // Same triple, different hash (cloned device id): the download must not
    // replace the local row.
    let tmp = tempfile::tempdir().unwrap();
    let sim = SimulatedTransport::open(&tmp.path().join("server.db")).unwrap();
    let dir_a = tmp.path().join("a");
    let dir_b = tmp.path().join("b");
    let da = common::seed_shell(&dir_a, 1, "a");
    // B clones A's device id, then diverges the same triple.
    std::fs::create_dir_all(&dir_b).unwrap();
    std::fs::copy(dir_a.join("device_id"), dir_b.join("device_id")).unwrap();
    {
        let mut db = amem_core::store::Store::open(&dir_b).unwrap();
        let payload = serde_json::json!({"cmd": "b-diverged", "exit": 0});
        let rec = amem_core::store::NewRecord {
            device: da.clone(),
            stream: "shell".to_string(),
            ts_ns: 1_700_000_000_000_000_001,
            actor: "t:u".to_string(),
            kind: "command".to_string(),
            payload: payload.clone(),
            hash: amem_core::store::hash_payload(&payload.to_string()),
        };
        db.insert_mapped("b-spool-0", &rec).unwrap();
    }
    let a = SyncClient::open(&dir_a, EP, sim.clone()).unwrap();
    let b = SyncClient::open(&dir_b, EP, sim.clone()).unwrap();
    a.sync(false).await.unwrap();
    let s = b.sync(false).await.unwrap();
    // B's upload conflicts (server kept A's row); B branches, keeps its own.
    assert!(s.conflicts >= 1, "{s:?}");
    assert!(s.branches >= 1, "{s:?}");
    let store = amem_core::store::Store::open(&dir_b).unwrap();
    assert!(
        store
            .get_record(&da, "shell", 0)
            .unwrap()
            .payload
            .contains("b-diverged")
    );
    let branches = store.branch_list().unwrap();
    assert_eq!(branches.len(), 1);
    assert_eq!(branches[0].reason, "pk");
}
