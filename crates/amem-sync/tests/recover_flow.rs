//! Recovery flow: a generation change refuses auto-merge with divergence
//! counts; `--recover` re-pushes; the second client still refuses until its
//! own recover.

mod common;

use amem_sync::client::SyncClient;
use amem_sync::sim::SimulatedTransport;

const EP: &str = "https://sync.test:8443";

#[tokio::test]
async fn generation_change_refuses_then_recovers() {
    let tmp = tempfile::tempdir().unwrap();
    let sim = SimulatedTransport::open(&tmp.path().join("server.db")).unwrap();
    let (dir_a, dir_b) = (tmp.path().join("a"), tmp.path().join("b"));
    let da = common::seed_shell(&dir_a, 2, "a");
    let db = common::seed_shell(&dir_b, 2, "b");
    let devices = vec![da, db];
    let a = SyncClient::open(&dir_a, EP, sim.clone()).unwrap();
    let b = SyncClient::open(&dir_b, EP, sim.clone()).unwrap();
    a.sync(false).await.unwrap();
    b.sync(false).await.unwrap();
    a.sync(false).await.unwrap();
    assert_eq!(common::count(&dir_a, &devices), 4);

    // Server wiped + re-initialized: new generation, empty union.
    let sim2 = SimulatedTransport::open(&tmp.path().join("server2.db")).unwrap();
    let a2 = SyncClient::open(&dir_a, EP, sim2.clone()).unwrap();
    let err = a2.sync(false).await.unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("divergence"), "{msg}");
    assert!(msg.contains("local=4"), "{msg}");
    assert!(msg.contains("remote=0"), "{msg}");

    // `--recover` re-pushes own records (hosts write their own store).
    let s = a2.sync(true).await.unwrap();
    assert_eq!(s.uploaded, 2);
    assert_eq!(s.downloaded, 0);

    // B still refuses (it pins the old generation) until its own recover.
    let b2 = SyncClient::open(&dir_b, EP, sim2.clone()).unwrap();
    let err = b2.sync(false).await.unwrap_err();
    assert!(err.to_string().contains("divergence"), "{err:?}");
    let s = b2.sync(true).await.unwrap();
    assert_eq!(s.uploaded, 2);
    // B already held A's records from before the wipe: nothing new to pull.
    assert_eq!(s.downloaded, 0);

    // Both converge on the rebuilt union.
    a2.sync(false).await.unwrap();
    assert_eq!(common::count(&dir_a, &devices), 4);
    assert_eq!(
        common::dump(&dir_a, &devices),
        common::dump(&dir_b, &devices)
    );
}
