//! Forward compatibility: v2-tagged envelopes (plus unknown fields)
//! round-trip through the union byte-identical, and clients apply them.

mod common;

use amem_sync::client::SyncClient;
use amem_sync::envelope::Envelope;
use amem_sync::protocol::PostBody;
use amem_sync::server_core::ServerCore;
use amem_sync::sim::SimulatedTransport;

const EP: &str = "https://sync.test:8443";

#[test]
fn v2_envelope_roundtrips_verbatim() {
    let tmp = tempfile::tempdir().unwrap();
    let mut core = ServerCore::open(&tmp.path().join("server.db")).unwrap();
    let mut raw: serde_json::Value = serde_json::json!({
        "id": "future-1", "device": "d9", "stream": "shell", "idx": 0,
        "hlc": {"wall_ms": 1_700_000_000_000i64, "ctr": 0, "node": "d9"},
        "ts_ns": 1_700_000_000_000_000_000i64, "actor": "t", "kind": "command",
        "payload": {"cmd": "from the future"},
        "hash": amem_core::store::hash_payload(r#"{"cmd":"from the future"}"#),
        "v": 2,
        "e2e_envelope": {"alg": "x-chacha20", "nonce": "abc"},
        "client_caps": ["notes-v2"],
        "future_x": {"nested": [1, 2, 3]},
    });
    // Canonical key order: the union stores bytes, so freeze the form.
    let raw_s = serde_json::to_string(&raw).unwrap();
    raw = serde_json::from_str(&raw_s).unwrap();
    let raw_s = serde_json::to_string(&raw).unwrap();
    let out = core
        .post(&PostBody {
            records: vec![raw_s.clone()],
            client_caps: vec!["notes-v2".to_string()],
        })
        .unwrap();
    assert_eq!(out.accepted, 1);
    let page = core.next("d9", "shell", 0, 10).unwrap();
    assert_eq!(page.len(), 1);
    assert_eq!(page[0], raw_s);
    // Unknown fields parse into `extra`, known futures into their slots.
    let env = Envelope::from_raw(&page[0]).unwrap();
    assert_eq!(env.v, 2);
    assert!(env.e2e_envelope.is_some());
    assert_eq!(env.client_caps, vec!["notes-v2".to_string()]);
    assert!(env.extra.contains_key("future_x"));
    assert!(env.verify_hash());
}

#[tokio::test]
async fn client_applies_unknown_versions() {
    let tmp = tempfile::tempdir().unwrap();
    let sim = SimulatedTransport::open(&tmp.path().join("server.db")).unwrap();
    // Seed the union directly with a v2 envelope (a newer client did it).
    let raw = serde_json::json!({
        "id": "future-2", "device": "d9", "stream": "shell", "idx": 0,
        "hlc": {"wall_ms": 1_700_000_000_000i64, "ctr": 0, "node": "d9"},
        "ts_ns": 1_700_000_000_000_000_000i64, "actor": "t", "kind": "command",
        "payload": {"cmd": "new client wrote this"},
        "hash": amem_core::store::hash_payload(r#"{"cmd":"new client wrote this"}"#),
        "v": 2, "weird": true,
    })
    .to_string();
    {
        let mut core = ServerCore::open(&tmp.path().join("server.db")).unwrap();
        core.post(&PostBody {
            records: vec![raw],
            client_caps: vec![],
        })
        .unwrap();
    }
    let dir = tmp.path().join("a");
    common::seed_shell(&dir, 1, "a");
    let c = SyncClient::open(&dir, EP, sim).unwrap();
    let s = c.sync(false).await.unwrap();
    assert_eq!(s.downloaded, 1);
    let db = amem_core::store::Store::open(&dir).unwrap();
    let rec = db.get_record("d9", "shell", 0).unwrap();
    assert!(rec.payload.contains("new client wrote this"));
}
