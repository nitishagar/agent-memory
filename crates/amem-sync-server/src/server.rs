//! axum routes over a manual rustls accept loop (no cleartext listener
//! exists anywhere in this binary: plaintext to the port fails the TLS
//! handshake by construction).

use std::path::Path;
use std::sync::Arc;

use amem_sync::protocol::{NextDto, PostBody, PostOutcome, StatusDto};
use amem_sync::server_core::ServerCore;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json};
use serde::Deserialize;

/// POST body cap: 100 × 1 MiB records + JSON slack.
const BODY_CAP: usize = 110 * 1024 * 1024;
/// Defensive page cap (clients page at 100).
const PAGE_CAP: i64 = 1000;

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
        Err(e) => typed(StatusCode::INTERNAL_SERVER_ERROR, "DB", &e.to_string()),
    }
}

async fn get_next(State(app): State<App>, Query(q): Query<NextQuery>) -> axum::response::Response {
    let core = app.core.lock().await;
    match core.next(&q.device, &q.stream, q.start, q.count.clamp(1, PAGE_CAP)) {
        Ok(records) => Json(NextDto { records }).into_response(),
        Err(e) => typed(StatusCode::INTERNAL_SERVER_ERROR, "DB", &e.to_string()),
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
        Err(e) => typed(StatusCode::INTERNAL_SERVER_ERROR, "DB", &e.to_string()),
    }
}

fn router(app: App) -> axum::Router {
    axum::Router::new()
        .route("/v1/status", axum::routing::get(get_status))
        .route("/v1/next", axum::routing::get(get_next))
        .route("/v1/records", axum::routing::post(post_records))
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
    let app = App {
        core: Arc::new(tokio::sync::Mutex::new(core)),
    };
    let router = router(app);
    let tls_config = Arc::new(crate::cert::load_server_config(dir)?);
    let acceptor = tokio_rustls::TlsAcceptor::from(tls_config);
    let tcp = tokio::net::TcpListener::bind(listen).await?;
    eprintln!("amem: sync server on https://{listen}");
    loop {
        let (stream, _peer) = tcp.accept().await?;
        let acceptor = acceptor.clone();
        let router = router.clone();
        tokio::spawn(async move {
            let Ok(tls) = acceptor.accept(stream).await else {
                return; // Plaintext or bad handshake: drop, nothing leaks.
            };
            let io = hyper_util::rt::TokioIo::new(tls);
            let svc = hyper::service::service_fn(|req| {
                use tower::ServiceExt as _;
                router.clone().oneshot(req)
            });
            let _ = hyper::server::conn::http1::Builder::new()
                .serve_connection(io, svc)
                .await;
        });
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
