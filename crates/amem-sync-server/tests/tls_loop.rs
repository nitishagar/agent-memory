//! TLS loop: real axum+rustls server, real hyper client, TOFU pinning,
//! two clients converging — over 127.0.0.1 with an ephemeral port.

use amem_sync::client::SyncClient;
use amem_sync::tls::HyperTransport;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

/// A port that is (almost surely) not the test server: neighbor port.
fn listen_port(endpoint: &str) -> u16 {
    let port: u16 = endpoint.rsplit(':').next().unwrap().parse().unwrap();
    port.wrapping_add(1)
}

fn seed(dir: &std::path::Path, n: i64, prefix: &str) -> String {
    let mut db = amem_core::store::Store::open(dir).unwrap();
    let device = amem_core::store::device_id(dir).unwrap();
    for i in 0..n {
        let payload = serde_json::json!({"cmd": format!("{prefix}-{i}"), "exit": 0});
        let rec = amem_core::store::NewRecord {
            device: device.clone(),
            stream: "shell".to_string(),
            ts_ns: 1_700_000_000_000_000_000 + i,
            actor: "t:u".to_string(),
            kind: "command".to_string(),
            payload: payload.clone(),
            hash: amem_core::store::hash_payload(&payload.to_string()),
        };
        db.insert_mapped(&format!("{prefix}-spool-{i}"), &rec)
            .unwrap();
    }
    device
}

#[tokio::test]
async fn tls_loop_two_clients_converge() {
    let tmp = tempfile::tempdir().unwrap();
    let srv_dir = tmp.path().join("srv");
    let fp = amem_sync_server::cert::init_cert(&srv_dir).unwrap();
    assert_eq!(fp.len(), 64);
    // Ephemeral port reservation (bind, read, drop, rebind in server).
    let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = probe.local_addr().unwrap().port();
    drop(probe);
    let listen = format!("127.0.0.1:{port}");
    let dir = srv_dir.clone();
    let listen2 = listen.clone();
    tokio::spawn(async move {
        amem_sync_server::server::serve_dir(&dir, &listen2)
            .await
            .unwrap();
    });
    // Wait for the listener (TLS handshake, not TCP, proves readiness).
    let endpoint = format!("https://{listen}");
    let dir_a = tmp.path().join("a");
    let dir_b = tmp.path().join("b");
    let da = seed(&dir_a, 2, "a");
    let db = seed(&dir_b, 1, "b");
    let mut transport_a = None;
    for _ in 0..100 {
        match HyperTransport::connect(&endpoint, &dir_a, true, None).await {
            Ok(t) => {
                transport_a = Some(t);
                break;
            }
            Err(_) => tokio::time::sleep(std::time::Duration::from_millis(50)).await,
        }
    }
    let transport_a = transport_a.expect("server came up");
    // Pin file written on TOFU.
    assert!(dir_a.join("sync_pins.json").exists());
    let a = SyncClient::open(&dir_a, &endpoint, transport_a).unwrap();
    let sa = a.sync(false).await.unwrap();
    assert_eq!((sa.uploaded, sa.downloaded), (2, 0));
    // Second client pins too, then both converge.
    let transport_b = HyperTransport::connect(&endpoint, &dir_b, true, None)
        .await
        .unwrap();
    let b = SyncClient::open(&dir_b, &endpoint, transport_b).unwrap();
    let sb = b.sync(false).await.unwrap();
    assert_eq!((sb.uploaded, sb.downloaded), (1, 2));
    let sa2 = a.sync(false).await.unwrap();
    assert_eq!(sa2.downloaded, 1);
    for dir in [&dir_a, &dir_b] {
        let store = amem_core::store::Store::open(dir).unwrap();
        assert_eq!(store.max_idx(&da, "shell").unwrap(), 1);
        assert_eq!(store.max_idx(&db, "shell").unwrap(), 0);
    }
    // Pinned reconnect (no --trust-new) works; wrong pin fails closed.
    let t = HyperTransport::connect(&endpoint, &dir_a, false, None).await;
    assert!(t.is_ok(), "pinned reconnect must succeed");
    std::fs::write(dir_a.join("sync_pins.json"), r#"{"x":"junk"}"#).unwrap();
    // Unknown server (different port, valid shape) refuses without --trust-new.
    let other_ep = format!("https://127.0.0.1:{other}", other = listen_port(&endpoint));
    let err = HyperTransport::connect(&other_ep, &dir_a, false, None)
        .await
        .err()
        .expect("must fail");
    assert!(err.0.contains("unknown sync server"), "{err:?}");
    // Path prefixes are rejected at parse (the server only routes /v1/*).
    let err = HyperTransport::connect(&format!("{endpoint}/other"), &dir_a, false, None)
        .await
        .err()
        .expect("must fail");
    assert!(err.0.contains("path prefixes"), "{err:?}");
}

#[tokio::test]
async fn plaintext_is_not_http() {
    // A cleartext HTTP request to the TLS port must not get an HTTP reply.
    let tmp = tempfile::tempdir().unwrap();
    let srv_dir = tmp.path().join("srv");
    amem_sync_server::cert::init_cert(&srv_dir).unwrap();
    let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = probe.local_addr().unwrap().port();
    drop(probe);
    let listen = format!("127.0.0.1:{port}");
    let dir = srv_dir.clone();
    let listen2 = listen.clone();
    tokio::spawn(async move {
        amem_sync_server::server::serve_dir(&dir, &listen2)
            .await
            .unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let mut tcp = tokio::net::TcpStream::connect(format!("127.0.0.1:{port}"))
        .await
        .unwrap();
    tcp.write_all(b"GET /v1/status HTTP/1.1\r\nHost: x\r\n\r\n")
        .await
        .unwrap();
    let mut buf = [0u8; 64];
    let n = tokio::time::timeout(std::time::Duration::from_secs(3), tcp.read(&mut buf))
        .await
        .map_or(0, |r| r.unwrap_or(0));
    let head = String::from_utf8_lossy(&buf[..n]);
    assert!(
        !head.starts_with("HTTP/"),
        "plaintext got an HTTP reply: {head:?}"
    );
}

#[tokio::test]
async fn expect_fingerprint_verifies_first_contact() {
    let tmp = tempfile::tempdir().unwrap();
    let srv_dir = tmp.path().join("srv");
    let fp = amem_sync_server::cert::init_cert(&srv_dir).unwrap();
    let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = probe.local_addr().unwrap().port();
    drop(probe);
    let listen = format!("127.0.0.1:{port}");
    let dir = srv_dir.clone();
    let listen2 = listen.clone();
    tokio::spawn(async move {
        amem_sync_server::server::serve_dir(&dir, &listen2)
            .await
            .unwrap();
    });
    let endpoint = format!("https://{listen}");
    let dir_a = tmp.path().join("a");
    amem_core::store::Store::open(&dir_a).unwrap();
    // Wrong expectation: refused, nothing pinned. Retry until the server
    // is up (connection-refused means "not yet", TOFU-refused means "up").
    let mut wrong = fp.clone();
    wrong.replace_range(0..1, if fp.starts_with('0') { "1" } else { "0" });
    let mut err = None;
    for _ in 0..100 {
        match HyperTransport::connect(&endpoint, &dir_a, true, Some(&wrong)).await {
            Ok(_) => panic!("wrong fingerprint must not pin"),
            Err(e) if e.0.contains("TOFU refused") => {
                err = Some(e);
                break;
            }
            Err(_) => tokio::time::sleep(std::time::Duration::from_millis(50)).await,
        }
    }
    let err = err.expect("server came up");
    assert!(err.0.contains("TOFU refused"), "{err:?}");
    assert!(!dir_a.join("sync_pins.json").exists());
    // Right expectation (with SHA256: prefix + uppercase tolerated): pins.
    let given = format!("SHA256:{}", fp.to_uppercase());
    HyperTransport::connect(&endpoint, &dir_a, true, Some(&given))
        .await
        .expect("must pin");
    assert!(dir_a.join("sync_pins.json").exists());
}
