//! axum routes over a manual rustls accept loop (no cleartext listener
//! exists anywhere in this binary: plaintext to the port fails the TLS
//! handshake by construction).

use std::path::Path;
use std::sync::Arc;

use amem_sync::protocol::{NextDto, PROTOCOL_V1, PostBody, PostOutcome, StatusDto};
use amem_sync::server_core::ServerCore;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json};
use serde::Deserialize;

/// POST body cap: 100 × 1 MiB records + JSON slack.
///
/// NOTE: each POST still buffers up to this in RAM, and bodies are capped
/// per-request only — there is no per-device quota or rate limit. The MVP
/// server trusts its network (see `docs/security.html`); do not expose it
/// beyond your team without a front proxy that authenticates + throttles.
const BODY_CAP: usize = 110 * 1024 * 1024;
/// Page cap, matching the client `PAGE_SIZE` (100): more headroom only
/// serves a client asking for 1000 × 1 MiB pages to OOM the server.
const PAGE_CAP: i64 = 100;
/// Max concurrent TLS connections (slow-drip/idle FD exhaustion guard).
const MAX_CONNS: usize = 64;
/// TLS handshake deadline per connection.
const HANDSHAKE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
/// Whole-connection serve deadline (covers slow POST bodies on thin links).
const SERVE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(120);

#[derive(Clone)]
struct App {
    core: Arc<tokio::sync::Mutex<ServerCore>>,
}

#[derive(Debug, Deserialize)]
struct NextQuery {
    device: String,
    stream: String,
    start: i64,
    count: i64,
}

fn typed(status: StatusCode, code: &str, message: &str) -> axum::response::Response {
    let body = serde_json::json!({"error": {"code": code, "message": message}});
    (status, Json(body)).into_response()
}

/// DB failure: generic bytes to the network (schema hints stay local),
/// details to the operator's stderr.
fn db_error(e: &amem_core::error::Error) -> axum::response::Response {
    eprintln!("amem: sync server db error: {e}");
    typed(StatusCode::INTERNAL_SERVER_ERROR, "DB", "internal error")
}

async fn get_status(State(app): State<App>) -> axum::response::Response {
    let core = app.core.lock().await;
    match core.status() {
        Ok(StatusDto {
            generation,
            streams,
        }) => Json(StatusDto {
            generation,
            streams,
        })
        .into_response(),
        Err(e) => db_error(&e),
    }
}

async fn get_next(State(app): State<App>, Query(q): Query<NextQuery>) -> axum::response::Response {
    if q.start < 0 {
        return typed(
            StatusCode::BAD_REQUEST,
            "INVALID_ARGUMENTS",
            "start must be >= 0",
        );
    }
    let core = app.core.lock().await;
    match core.next(&q.device, &q.stream, q.start, q.count.clamp(1, PAGE_CAP)) {
        Ok(records) => Json(NextDto { records }).into_response(),
        Err(e) => db_error(&e),
    }
}

async fn post_records(
    State(app): State<App>,
    Json(body): Json<PostBody>,
) -> axum::response::Response {
    let mut core = app.core.lock().await;
    match core.post(&body) {
        Ok(PostOutcome {
            accepted,
            duplicates,
            conflicts,
        }) => Json(PostOutcome {
            accepted,
            duplicates,
            conflicts,
        })
        .into_response(),
        Err(amem_core::error::Error::InvalidArguments(msg)) => {
            typed(StatusCode::BAD_REQUEST, "INVALID_ARGUMENTS", &msg)
        }
        Err(e) => db_error(&e),
    }
}

fn router(app: App) -> axum::Router {
    axum::Router::new()
        .route(
            &format!("/{PROTOCOL_V1}/status"),
            axum::routing::get(get_status),
        )
        .route(
            &format!("/{PROTOCOL_V1}/next"),
            axum::routing::get(get_next),
        )
        .route(
            &format!("/{PROTOCOL_V1}/records"),
            axum::routing::post(post_records),
        )
        .layer(axum::extract::DefaultBodyLimit::disable())
        .layer(tower_http::limit::RequestBodyLimitLayer::new(BODY_CAP))
        .with_state(app)
}

/// Serve forever on `listen` (e.g. `127.0.0.1:8443`) from `dir`
/// (`server.db` + `cert.pem`/`key.pem`).
///
/// # Errors
///
/// Returns [`amem_core::error::Error::Io`] when the socket, TLS, or store
/// fails to initialize.
pub async fn serve_dir(dir: &Path, listen: &str) -> amem_core::error::Result<()> {
    let core = ServerCore::open(&dir.join("server.db"))?;
    lock_down(dir);
    let app = App {
        core: Arc::new(tokio::sync::Mutex::new(core)),
    };
    let router = router(app);
    let tls_config = Arc::new(crate::cert::load_server_config(dir)?);
    let acceptor = tokio_rustls::TlsAcceptor::from(tls_config);
    let tcp = tokio::net::TcpListener::bind(listen).await?;
    eprintln!("amem: sync server on https://{listen}");
    let conns = Arc::new(tokio::sync::Semaphore::new(MAX_CONNS));
    loop {
        let (stream, peer) = tcp.accept().await?;
        let Ok(permit) = conns.clone().try_acquire_owned() else {
            eprintln!("amem: sync server: connection from {peer} refused (at cap {MAX_CONNS})");
            continue;
        };
        let acceptor = acceptor.clone();
        let router = router.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let tls = match tokio::time::timeout(HANDSHAKE_TIMEOUT, acceptor.accept(stream)).await {
                Ok(Ok(tls)) => tls,
                Ok(Err(_)) => return, // Plaintext or bad handshake: drop, nothing leaks.
                Err(_) => {
                    eprintln!("amem: sync server: handshake from {peer} timed out");
                    return;
                }
            };
            let io = hyper_util::rt::TokioIo::new(tls);
            let svc = hyper::service::service_fn(|req| {
                use tower::ServiceExt as _;
                router.clone().oneshot(req)
            });
            let res = tokio::time::timeout(
                SERVE_TIMEOUT,
                hyper::server::conn::http1::Builder::new().serve_connection(io, svc),
            )
            .await;
            if res.is_err() {
                eprintln!("amem: sync server: connection from {peer} timed out");
            }
        });
    }
}

/// Owner-only permissions for the server dir + union DB, mirroring the
/// client store: team secrets must not leak to other local users.
#[cfg(unix)]
fn lock_down(dir: &Path) {
    use std::os::unix::fs::PermissionsExt as _;
    for (path, mode) in [
        (dir.to_path_buf(), 0o700),
        (dir.join("server.db"), 0o600),
        (dir.join("server.db-wal"), 0o600),
        (dir.join("server.db-shm"), 0o600),
    ] {
        if path.exists() {
            let _ = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode));
        }
    }
}

/// No-op permission pass where unix modes don't apply.
#[cfg(not(unix))]
fn lock_down(_dir: &Path) {}

/// True when `listen` binds loopback only (`127.*`, `::1`, `localhost`).
/// The CLI refuses non-loopback binds without `--public --confirm` (the
/// MVP server has no client auth: network access = full read/write).
#[must_use]
pub fn listen_is_loopback(listen: &str) -> bool {
    use std::net::ToSocketAddrs as _;
    // Bare port or `:port` binds all interfaces.
    let probe = if listen.contains(':') {
        listen.to_string()
    } else {
        format!("127.0.0.1:{listen}")
    };
    match probe.to_socket_addrs() {
        Ok(addrs) => {
            let mut any = false;
            for addr in addrs {
                any = true;
                if !addr.ip().is_loopback() {
                    return false;
                }
            }
            any
        }
        Err(_) => false,
    }
}

/// Blocking wrapper for the CLI (multi-thread runtime for parallel clients).
///
/// # Errors
///
/// Returns [`amem_core::error::Error::Io`] on initialization failure.
pub fn serve_blocking(dir: &Path, listen: &str) -> amem_core::error::Result<()> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    rt.block_on(serve_dir(dir, listen))
}
