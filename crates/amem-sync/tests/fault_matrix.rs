//! Fault matrix: two simulated clients + injector over one union.
//!
//! Asserts no-loss / no-dupe / no-half-batch, rollback-refusal, and
//! quarantine-flagging. Written before the server; the axum server binds
//! the same `ServerCore`, so this coverage carries to production paths.

mod common;

use amem_sync::client::{Summary, SyncClient};
use amem_sync::protocol::PostBody;
use amem_sync::server_core::ServerCore;
use amem_sync::sim::{Faults, SimulatedTransport};

const EP: &str = "https://sync.test:8443";

fn setup() -> (tempfile::TempDir, SimulatedTransport) {
    let tmp = tempfile::tempdir().unwrap();
    let sim = SimulatedTransport::open(&tmp.path().join("server.db")).unwrap();
    (tmp, sim)
}

#[tokio::test]
async fn clean_two_way_union() {
    let (tmp, sim) = setup();
    let (da, db);
    let (dir_a, dir_b) = (tmp.path().join("a"), tmp.path().join("b"));
    da = common::seed_shell(&dir_a, 3, "a");
    db = common::seed_shell(&dir_b, 2, "b");
    let devices = vec![da.clone(), db.clone()];
    let a = SyncClient::open(&dir_a, EP, sim.clone()).unwrap();
    let b = SyncClient::open(&dir_b, EP, sim.clone()).unwrap();
    let s1 = a.sync(false).await.unwrap();
    assert_eq!((s1.uploaded, s1.downloaded), (3, 0));
    let s2 = b.sync(false).await.unwrap();
    assert_eq!((s2.uploaded, s2.downloaded), (2, 3));
    let s3 = a.sync(false).await.unwrap();
    assert_eq!((s3.uploaded, s3.downloaded), (0, 2));
    // Byte-equal unions, no dupes.
    assert_eq!(common::count(&dir_a, &devices), 5);
    assert_eq!(
        common::dump(&dir_a, &devices),
        common::dump(&dir_b, &devices)
    );
    // Steady state is a no-op.
    assert_eq!(a.sync(false).await.unwrap(), Summary::default());
}

#[tokio::test]
async fn dupe_post_idempotent() {
    let (tmp, sim) = setup();
    let dir_a = tmp.path().join("a");
    let dir_b = tmp.path().join("b");
    let da = common::seed_shell(&dir_a, 3, "a");
    let db = common::seed_shell(&dir_b, 1, "b");
    let devices = vec![da, db];
    sim.arm(Faults {
        dupe_post: true,
        ..Default::default()
    });
    let a = SyncClient::open(&dir_a, EP, sim.clone()).unwrap();
    let b = SyncClient::open(&dir_b, EP, sim.clone()).unwrap();
    a.sync(false).await.unwrap();
    b.sync(false).await.unwrap();
    a.sync(false).await.unwrap();
    assert_eq!(common::count(&dir_a, &devices), 4);
    assert_eq!(
        common::dump(&dir_a, &devices),
        common::dump(&dir_b, &devices)
    );
}

#[tokio::test]
async fn drop_then_resume() {
    let (tmp, sim) = setup();
    let dir_a = tmp.path().join("a");
    let dir_b = tmp.path().join("b");
    let da = common::seed_shell(&dir_a, 2, "a");
    let db = common::seed_shell(&dir_b, 2, "b");
    let devices = vec![da, db];
    let a = SyncClient::open(&dir_a, EP, sim.clone()).unwrap();
    sim.arm(Faults {
        fail_on_call: Some(1),
        ..Default::default()
    });
    let err = a.sync(false).await.unwrap_err();
    assert!(err.to_string().contains("transport"), "{err:?}");
    // Resume completes with no dupes.
    let b = SyncClient::open(&dir_b, EP, sim.clone()).unwrap();
    a.sync(false).await.unwrap();
    b.sync(false).await.unwrap();
    a.sync(false).await.unwrap();
    assert_eq!(common::count(&dir_a, &devices), 4);
    assert_eq!(
        common::dump(&dir_a, &devices),
        common::dump(&dir_b, &devices)
    );
}

#[tokio::test]
async fn reorder_page_applies_in_order() {
    let (tmp, sim) = setup();
    let dir_a = tmp.path().join("a");
    let dir_b = tmp.path().join("b");
    let da = common::seed_shell(&dir_a, 5, "a");
    let db = common::seed_shell(&dir_b, 0, "b");
    let devices = vec![da, db];
    // Drain B's empty device row so the union assertion is exact.
    let a = SyncClient::open(&dir_a, EP, sim.clone()).unwrap();
    let b = SyncClient::open(&dir_b, EP, sim.clone()).unwrap();
    a.sync(false).await.unwrap();
    sim.arm(Faults {
        reorder_page: true,
        ..Default::default()
    });
    let s = b.sync(false).await.unwrap();
    assert_eq!(s.downloaded, 5);
    assert_eq!(
        common::dump(&dir_a, &devices),
        common::dump(&dir_b, &devices)
    );
    // Cursor landed on max: steady state is quiet.
    assert_eq!(b.sync(false).await.unwrap().downloaded, 0);
}

#[tokio::test]
async fn skew_quarantines_and_hides_from_recall() {
    let (tmp, sim) = setup();
    let dir_a = tmp.path().join("a");
    let dir_b = tmp.path().join("b");
    let da = common::seed_shell(&dir_a, 3, "a");
    let db = common::seed_shell(&dir_b, 1, "b");
    let a = SyncClient::open(&dir_a, EP, sim.clone()).unwrap();
    a.sync(false).await.unwrap();
    // Seeds are 2023-dated: push walls ~3y forward, past now + tolerance.
    sim.arm(Faults {
        skew_ms: 100_000_000_000,
        ..Default::default()
    });
    let b = SyncClient::open(&dir_b, EP, sim.clone()).unwrap();
    let s = b.sync(false).await.unwrap();
    assert_eq!(s.quarantined, 3);
    assert_eq!(s.downloaded, 0);
    // Quarantined rows are filed with reasons and hidden from recall.
    let store = amem_core::store::Store::open(&dir_b).unwrap();
    let q = store.quarantine_list().unwrap();
    assert_eq!(q.len(), 3);
    assert!(q.iter().all(|r| r.reason.contains("skew")), "{q:?}");
    assert_eq!(
        store.search_stream(&db, "shell", "a-", 10).unwrap().len(),
        0
    );
    assert_eq!(
        store.search_stream(&db, "shell", "b-", 10).unwrap().len(),
        1
    );
    drop(store);
    // A is unaffected; B keeps only its own record in the union.
    assert_eq!(common::count(&dir_a, std::slice::from_ref(&da)), 3);
    assert_eq!(common::count(&dir_b, std::slice::from_ref(&db)), 1);
    let _ = da;
}

#[tokio::test]
async fn rollback_refused_persistently() {
    let (tmp, sim) = setup();
    let dir_a = tmp.path().join("a");
    let da = common::seed_shell(&dir_a, 3, "a");
    let a = SyncClient::open(&dir_a, EP, sim.clone()).unwrap();
    a.sync(false).await.unwrap();
    sim.arm(Faults {
        rollback_to_empty: true,
        ..Default::default()
    });
    let err = a.sync(false).await.unwrap_err();
    assert!(err.to_string().contains("rollback"), "{err:?}");
    // Refusal is sticky (marks still prove the server lost data) and local
    // state is untouched.
    let err = a.sync(false).await.unwrap_err();
    assert!(err.to_string().contains("rollback"), "{err:?}");
    assert_eq!(common::count(&dir_a, &[da]), 3);
}

#[test]
fn batch_atomic_rejects_whole_batch() {
    let tmp = tempfile::tempdir().unwrap();
    let mut core = ServerCore::open(&tmp.path().join("server.db")).unwrap();
    let good = serde_json::json!({
        "id": "e1", "device": "d1", "stream": "shell", "idx": 0,
        "hlc": {"wall_ms": 1, "ctr": 0, "node": "d1"},
        "ts_ns": 1, "actor": "t", "kind": "command",
        "payload": {"cmd": "ok"}, "hash": amem_core::store::hash_payload(r#"{"cmd":"ok"}"#),
        "v": 1,
    })
    .to_string();
    let out = core
        .post(&PostBody {
            records: vec![good.clone(), "not json".to_string()],
            client_caps: vec![],
        })
        .unwrap_err();
    assert!(out.to_string().contains("malformed"), "{out:?}");
    assert!(core.status().unwrap().streams.is_empty());
    // The good record alone lands fine.
    let ok = core
        .post(&PostBody {
            records: vec![good],
            client_caps: vec![],
        })
        .unwrap();
    assert_eq!((ok.accepted, ok.duplicates), (1, 0));
}
