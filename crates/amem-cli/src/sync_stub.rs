//! P1 sync stub: endpoint validation with honest typed failures.
//!
//! Phase 1 ships no sync transport. This stub parses the endpoint, proves
//! raw-socket behavior (no proxy honoring — a plain `TcpStream` cannot read
//! proxy env), and fails closed: DNS problems, refused connections, and
//! `https://` (TLS arrives in Phase 4) each map to a distinct typed error.
//! Local commands never touch this path.

use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

use amem_core::error::{Error, Result};

/// Connection timeout for the reachability probe.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Probe a sync endpoint. `Ok` describes reachability; any failure is typed.
///
/// # Errors
///
/// - [`Error::InvalidArguments`] for non-http(s) URLs or malformed endpoints.
/// - [`Error::Sync`] for DNS failure, refused/timeout connections, and
///   `https://` (explicitly Phase 4 work).
pub fn check(endpoint: &str) -> Result<String> {
    let url = url::Url::parse(endpoint)
        .map_err(|e| Error::InvalidArguments(format!("bad endpoint '{endpoint}': {e}")))?;
    match url.scheme() {
        "http" => {}
        "https" => {
            return Err(Error::Sync(
                "https endpoints need TLS sync (Phase 4); refusing plaintext downgrade".to_string(),
            ));
        }
        other => {
            return Err(Error::InvalidArguments(format!(
                "unsupported endpoint scheme '{other}': expected http or https"
            )));
        }
    }
    let host = url
        .host_str()
        .ok_or_else(|| Error::InvalidArguments(format!("endpoint '{endpoint}' has no host")))?;
    let port = url.port().unwrap_or(80);
    // Explicit resolve first: DNS failure must be distinguishable from refused.
    let mut addrs = format!("{host}:{port}")
        .to_socket_addrs()
        .map_err(|e| Error::Sync(format!("cannot resolve host '{host}': {e}")))?;
    let addr = addrs
        .next()
        .ok_or_else(|| Error::Sync(format!("cannot resolve host '{host}': no addresses")))?;
    match TcpStream::connect_timeout(&addr, CONNECT_TIMEOUT) {
        Ok(_) => Ok(format!(
            "endpoint {host}:{port} reachable (sync transport: Phase 4)"
        )),
        Err(e) => Err(Error::Sync(format!(
            "cannot reach {host}:{port}: {e} (local commands unaffected)"
        ))),
    }
}
