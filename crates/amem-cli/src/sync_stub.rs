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

/// Scrub `user:pass@` from an endpoint string that failed to parse
/// (so it cannot be scrubbed structurally).
fn scrub_unparsed(endpoint: &str) -> String {
    match endpoint.find("://") {
        Some(scheme) => match endpoint[scheme + 3..].find('@') {
            Some(at) => format!(
                "{}://***@{}",
                &endpoint[..scheme],
                &endpoint[scheme + 3 + at + 1..]
            ),
            None => endpoint.to_string(),
        },
        None => endpoint.to_string(),
    }
}

/// Probe a sync endpoint. `Ok` describes reachability; any failure is typed.
///
/// # Errors
///
/// - [`Error::InvalidArguments`] for non-http(s) URLs or malformed endpoints.
/// - [`Error::Sync`] for DNS failure, refused/timeout connections, and
///   `https://` (explicitly Phase 4 work).
pub fn check(endpoint: &str) -> Result<String> {
    let url = url::Url::parse(endpoint).map_err(|e| {
        Error::InvalidArguments(format!("bad endpoint '{}': {e}", scrub_unparsed(endpoint)))
    })?;
    // Never echo credentials: all errors below use the scrubbed form.
    let mut shown = url.clone();
    let _ = shown.set_username("");
    let _ = shown.set_password(None);
    let shown = shown.to_string();
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
        .ok_or_else(|| Error::InvalidArguments(format!("endpoint '{shown}' has no host")))?;
    let port = url.port().unwrap_or(80);
    // `host_str` strips brackets; `ToSocketAddrs` needs them back for v6.
    let dial = if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    };
    // Explicit resolve first: DNS failure must be distinguishable from refused.
    let addrs = dial
        .to_socket_addrs()
        .map_err(|e| Error::Sync(format!("cannot resolve host '{host}': {e}")))?;
    let display = dial;
    let mut last_err = None;
    let mut tried = 0;
    for addr in addrs {
        tried += 1;
        match TcpStream::connect_timeout(&addr, CONNECT_TIMEOUT) {
            Ok(_) => {
                return Ok(format!(
                    "endpoint {display} reachable (sync transport: Phase 4)"
                ));
            }
            Err(e) => last_err = Some(e),
        }
    }
    if tried == 0 {
        return Err(Error::Sync(format!(
            "cannot resolve host '{host}': no addresses"
        )));
    }
    Err(Error::Sync(format!(
        "cannot reach {display}: {} (local commands unaffected)",
        last_err.map_or_else(|| "unknown error".to_string(), |e| e.to_string())
    )))
}
