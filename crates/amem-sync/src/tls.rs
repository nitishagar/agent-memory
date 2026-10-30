//! TLS transport: hyper over rustls with exact-certificate TOFU pins.
//!
//! TLS-only (`https://` endpoints; `http://` is refused, never attempted).
//! No proxy support by design: the client dials the endpoint directly, so
//! `HTTP_PROXY` environments fail closed instead of leaking sync traffic.
//! First contact pins the server certificate verbatim (`sync_pins.json`);
//! later connects trust ONLY the pinned bytes (no `WebPKI`, no CA games).

use std::collections::HashMap;
use std::net::IpAddr;
use std::str::FromStr;
use std::sync::Arc;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::Request;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{verify_tls12_signature, verify_tls13_signature};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, Error as TlsError, SignatureScheme};

use crate::protocol::{NextDto, PROTOCOL_V1, PostBody, PostOutcome, StatusDto};
use crate::transport::{Transport, TransportError};

/// Cap on response bodies (100 × 1 MiB records + slack).
const MAX_BODY_BYTES: u64 = 128 * 1024 * 1024;

/// TCP-connect + TLS-handshake deadline (a black-holed server must not hang sync).
const DIAL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// Whole-request deadline (handshake + HTTP + body).
const CALL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

/// Exact-certificate verifier: the peer must present precisely the pinned
/// DER bytes; handshake signatures still verify against the presented key.
///
/// Expiry, SANs, and revocation are INTENTIONALLY unchecked: the pin — not
/// PKI — is the trust anchor. A compromised cert stays trusted until the
/// operator deletes the pin (accepted risk, documented in `docs/security.html`).
#[derive(Debug)]
struct ExactVerifier {
    pinned: Vec<u8>,
    algs: rustls::crypto::WebPkiSupportedAlgorithms,
}

impl ExactVerifier {
    fn new(pinned: Vec<u8>) -> Self {
        let algs = rustls::crypto::ring::default_provider().signature_verification_algorithms;
        Self { pinned, algs }
    }
}

impl ServerCertVerifier for ExactVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, TlsError> {
        if end_entity.as_ref() == self.pinned.as_slice() {
            Ok(ServerCertVerified::assertion())
        } else {
            Err(TlsError::General(format!(
                "sync certificate pin mismatch: presented {}, pinned {}; \
                 the server identity changed — verify the new fingerprint \
                 out-of-band with the server operator before re-pinning",
                fingerprint(end_entity.as_ref()),
                fingerprint(&self.pinned),
            )))
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, TlsError> {
        verify_tls12_signature(message, cert, dss, &self.algs)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, TlsError> {
        verify_tls13_signature(message, cert, dss, &self.algs)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.algs.supported_schemes()
    }
}

/// TOFU capture verifier: accepts any certificate (verifying the handshake
/// signature so the peer proves key possession). The caller reads the
/// presented chain from `peer_certificates()` after the handshake.
#[derive(Debug)]
struct CaptureVerifier {
    algs: rustls::crypto::WebPkiSupportedAlgorithms,
}

impl CaptureVerifier {
    fn new() -> Self {
        let algs = rustls::crypto::ring::default_provider().signature_verification_algorithms;
        Self { algs }
    }
}

impl ServerCertVerifier for CaptureVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, TlsError> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, TlsError> {
        verify_tls12_signature(message, cert, dss, &self.algs)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, TlsError> {
        verify_tls13_signature(message, cert, dss, &self.algs)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.algs.supported_schemes()
    }
}

/// Explicit crypto provider (ring): the dependency graph enables both
/// ring and aws-lc-rs, so the process default is ambiguous — every config
/// here names its provider instead of relying on install order.
fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

/// blake3 fingerprint of DER bytes (TOFU display + pin comparison aid).
#[must_use]
pub fn fingerprint(der: &[u8]) -> String {
    amem_core::store::hash_bytes_hex(der)
}

/// Normalize a user-supplied fingerprint for comparison: lowercase,
/// `SHA256:` prefix and `:`/whitespace separators stripped.
fn normalize_fingerprint(raw: &str) -> String {
    raw.trim()
        .strip_prefix("SHA256:")
        .or_else(|| raw.trim().strip_prefix("sha256:"))
        .unwrap_or(raw.trim())
        .chars()
        .filter(|c| !c.is_whitespace() && *c != ':')
        .collect::<String>()
        .to_lowercase()
}

/// Parsed endpoint: https-only host:port (no path prefix, no userinfo).
#[derive(Debug, Clone)]
struct Endpoint {
    host: String,
    port: u16,
}

impl Endpoint {
    fn parse(url: &str) -> Result<Self, TransportError> {
        let u = url::Url::parse(url).map_err(|e| TransportError(format!("bad endpoint: {e}")))?;
        if u.scheme() != "https" {
            return Err(TransportError(format!(
                "TLS-only: refusing to sync over '{}'; use https://",
                u.scheme()
            )));
        }
        // Credentials would be silently dropped (the server has no auth):
        // reject rather than sustain an auth illusion. The host:port is
        // echoed (never the credentials) so the operator sees which
        // endpoint was refused.
        if !u.username().is_empty() || u.password().is_some() {
            return Err(TransportError(format!(
                "endpoint userinfo is not supported (the sync server has no password auth); \
                 remove user:pass@ from https://{}:{}",
                u.host_str().unwrap_or("?"),
                u.port().unwrap_or(443),
            )));
        }
        let host = u.host_str().unwrap_or("").to_lowercase();
        if host.is_empty() {
            return Err(TransportError("endpoint has no host".to_string()));
        }
        // The server only routes root /v1/*: a path prefix would 404 every
        // call, so reject it at parse time instead of failing confusingly.
        if u.path() != "/" && !u.path().is_empty() {
            return Err(TransportError(format!(
                "endpoint path prefixes are not supported (got '{}'); use https://host:port",
                u.path()
            )));
        }
        if u.query().is_some() {
            return Err(TransportError(
                "endpoint query strings are not supported".to_string(),
            ));
        }
        Ok(Self {
            host,
            port: u.port().unwrap_or(443),
        })
    }

    /// Canonical key for pins + sync cursors: `HOST` vs `host` and trailing
    /// slashes must not split identity (and rollback protection with it).
    fn canonical(&self) -> String {
        format!("https://{}:{}", self.host, self.port)
    }

    fn server_name(&self) -> Result<ServerName<'static>, TransportError> {
        if let Ok(ip) = IpAddr::from_str(&self.host) {
            return Ok(ServerName::IpAddress(ip.into()));
        }
        ServerName::try_from(self.host.clone())
            .map(|n| n.to_owned())
            .map_err(|_| TransportError(format!("bad TLS server name: {}", self.host)))
    }
}

/// Canonical endpoint key for pins + sync cursors. Non-URL keys (test
/// simulators) pass through unchanged; https URLs normalize to
/// `https://host:port` (lowercased, no path/query/userinfo).
#[must_use]
pub fn canonical_endpoint(url: &str) -> String {
    Endpoint::parse(url).map_or_else(|_| url.to_string(), |e| e.canonical())
}

/// Pin file: `{canonical_endpoint: cert_pem}`.
fn pins_path(store_dir: &std::path::Path) -> std::path::PathBuf {
    store_dir.join("sync_pins.json")
}

/// Load pins. A MISSING file is first contact (empty map); an UNREADABLE
/// or CORRUPT file is a loud error — silently dropping every pin would
/// re-open the TOFU window to whoever answers next.
fn load_pins(store_dir: &std::path::Path) -> Result<HashMap<String, String>, TransportError> {
    let path = pins_path(store_dir);
    let text = match std::fs::read_to_string(&path) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(HashMap::new()),
        Err(e) => {
            return Err(TransportError(format!(
                "cannot read {}: {e}; refusing to sync without pins",
                path.display()
            )));
        }
    };
    serde_json::from_str(&text).map_err(|e| {
        TransportError(format!(
            "sync_pins.json is corrupt ({e}); restore from backup, or delete it to re-pin every server (verifying fingerprints out-of-band)"
        ))
    })
}

fn store_pin(store_dir: &std::path::Path, endpoint: &str, pem: &str) -> Result<(), TransportError> {
    #[cfg(unix)]
    use std::io::Write as _;
    let mut pins = load_pins(store_dir)?;
    pins.insert(endpoint.to_string(), pem.to_string());
    let bytes = serde_json::to_string_pretty(&pins)
        .map_err(|e| TransportError(format!("cannot serialize pin file: {e}")))?;
    // Atomic: temp file + rename, so a crash never leaves truncated JSON
    // (which `load_pins` would then refuse to read). 0600 from creation.
    let path = pins_path(store_dir);
    let tmp = path.with_extension("json.tmp");
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(&tmp)
            .map_err(|e| TransportError(format!("cannot write pin file: {e}")))?;
        f.write_all(bytes.as_bytes())
            .map_err(|e| TransportError(format!("cannot write pin file: {e}")))?;
        f.sync_all()
            .map_err(|e| TransportError(format!("cannot write pin file: {e}")))?;
        drop(f);
    }
    #[cfg(not(unix))]
    {
        std::fs::write(&tmp, &bytes)
            .map_err(|e| TransportError(format!("cannot write pin file: {e}")))?;
    }
    std::fs::rename(&tmp, &path)
        .map_err(|e| TransportError(format!("cannot write pin file: {e}")))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let _ = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600));
    }
    Ok(())
}

/// DER bytes from a PEM CERTIFICATE block.
fn der_from_pem(pem: &str) -> Result<Vec<u8>, TransportError> {
    use rustls::pki_types::pem::PemObject as _;
    rustls::pki_types::CertificateDer::from_pem_slice(pem.as_bytes())
        .map(|c| c.as_ref().to_vec())
        .map_err(|_| TransportError("stored pin is not a valid PEM certificate".to_string()))
}

/// PEM text for DER bytes.
fn pem_from_der(der: &[u8]) -> String {
    const B64: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::from("-----BEGIN CERTIFICATE-----\n");
    let mut line = 0;
    for chunk in der.chunks(3) {
        let n = (u32::from(chunk[0]) << 16)
            | (u32::from(*chunk.get(1).unwrap_or(&0)) << 8)
            | u32::from(*chunk.get(2).unwrap_or(&0));
        let chars = [
            B64[(n >> 18) as usize & 63] as char,
            B64[(n >> 12) as usize & 63] as char,
            if chunk.len() > 1 {
                B64[(n >> 6) as usize & 63] as char
            } else {
                '='
            },
            if chunk.len() > 2 {
                B64[n as usize & 63] as char
            } else {
                '='
            },
        ];
        for c in chars {
            out.push(c);
            line += 1;
            if line == 64 {
                out.push('\n');
                line = 0;
            }
        }
    }
    if line != 0 {
        out.push('\n');
    }
    out.push_str("-----END CERTIFICATE-----\n");
    out
}

/// Hyper + rustls transport with pinned exact-certificate verification.
pub struct HyperTransport {
    endpoint: Endpoint,
    config: Arc<ClientConfig>,
}

impl HyperTransport {
    /// Connect to an `https://` endpoint, enforcing the TOFU pin.
    /// `expect_fingerprint` (from `--expect-fingerprint`) makes first
    /// contact verify instead of blindly trusting: the captured
    /// certificate must match, or the pin is refused and nothing syncs.
    ///
    /// # Errors
    ///
    /// Returns [`TransportError`] on non-TLS URLs, unknown servers (without
    /// `trust_new`), pin/fingerprint mismatch, or connection failure.
    pub async fn connect(
        url: &str,
        store_dir: &std::path::Path,
        trust_new: bool,
        expect_fingerprint: Option<&str>,
    ) -> Result<Self, TransportError> {
        let endpoint = Endpoint::parse(url)?;
        let key = endpoint.canonical();
        let pins = load_pins(store_dir)?;
        if let Some(pem) = pins.get(&key) {
            let pinned = der_from_pem(pem)?;
            if let Some(want) = expect_fingerprint {
                let want = normalize_fingerprint(want);
                if fingerprint(&pinned) != want {
                    return Err(TransportError(format!(
                        "pinned certificate for {key} does not match --expect-fingerprint"
                    )));
                }
            }
            return Self::with_pinned(endpoint, pinned).await;
        }
        if !trust_new {
            return Err(TransportError(
                "unknown sync server; verify its certificate out-of-band, then re-run with --trust-new to pin it"
                    .to_string(),
            ));
        }
        // TOFU bootstrap: capture the certificate, persist the pin, then
        // reconnect through the pinned verifier (which also validates it).
        let capture = Arc::new(CaptureVerifier::new());
        let config = Arc::new(
            ClientConfig::builder_with_provider(provider())
                .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])
                .map_err(|e| TransportError(format!("tls: {e}")))?
                .dangerous()
                .with_custom_certificate_verifier(capture)
                .with_no_client_auth(),
        );
        let der = Self::fetch_cert(&endpoint, &config).await?;
        let fp = fingerprint(&der);
        if let Some(want) = expect_fingerprint {
            let want = normalize_fingerprint(want);
            if fp != want {
                return Err(TransportError(format!(
                    "TOFU refused: server presented {fp}, expected {want}; nothing pinned, nothing synced"
                )));
            }
        }
        eprintln!("amem: TOFU: pinning sync server certificate {fp}");
        store_pin(store_dir, &key, &pem_from_der(&der))?;
        Self::with_pinned(endpoint, der).await
    }

    /// Connect with an explicit pin, verifying it immediately.
    async fn with_pinned(endpoint: Endpoint, pinned: Vec<u8>) -> Result<Self, TransportError> {
        let config = Arc::new(
            ClientConfig::builder_with_provider(provider())
                .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])
                .map_err(|e| TransportError(format!("tls: {e}")))?
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(ExactVerifier::new(pinned)))
                .with_no_client_auth(),
        );
        // Verify the pin now (not lazily at first sync call).
        Self::fetch_cert(&endpoint, &config).await?;
        Ok(Self { endpoint, config })
    }

    /// TLS handshake + `GET /v1/status` (cheap liveness + pin check).
    async fn fetch_cert(
        endpoint: &Endpoint,
        config: &Arc<ClientConfig>,
    ) -> Result<Vec<u8>, TransportError> {
        let endpoint = endpoint.clone();
        let config = config.clone();
        tokio::time::timeout(CALL_TIMEOUT, async move {
            let stream = Self::dial(&endpoint, &config).await?;
            let cert = stream
                .get_ref()
                .1
                .peer_certificates()
                .and_then(|c| c.first().cloned())
                .map(|c| c.as_ref().to_vec());
            // Drain a status call so a non-server TLS endpoint fails here too.
            let _ = Self::round_trip(
                &endpoint,
                stream,
                "GET",
                &format!("/{PROTOCOL_V1}/status"),
                None,
            )
            .await?;
            cert.ok_or_else(|| TransportError("server presented no certificate".to_string()))
        })
        .await
        .map_err(|_| {
            TransportError(format!(
                "sync connect timed out after {}s",
                CALL_TIMEOUT.as_secs()
            ))
        })?
    }

    /// TCP dial + TLS handshake, under a deadline.
    async fn dial(
        endpoint: &Endpoint,
        config: &Arc<ClientConfig>,
    ) -> Result<tokio_rustls::client::TlsStream<tokio::net::TcpStream>, TransportError> {
        let host = endpoint.host.clone();
        let port = endpoint.port;
        let server_name = endpoint.server_name()?;
        let config = config.clone();
        tokio::time::timeout(DIAL_TIMEOUT, async move {
            let tcp = tokio::net::TcpStream::connect((host.as_str(), port))
                .await
                .map_err(|e| TransportError(format!("connect {host}:{port}: {e}")))?;
            tokio_rustls::TlsConnector::from(config)
                .connect(server_name, tcp)
                .await
                .map_err(|e| TransportError(format!("TLS handshake: {e}")))
        })
        .await
        .map_err(|_| {
            TransportError(format!(
                "connect {}:{}: timed out after {}s",
                endpoint.host,
                endpoint.port,
                DIAL_TIMEOUT.as_secs()
            ))
        })?
    }

    /// One request/response round-trip over a fresh connection.
    async fn round_trip(
        endpoint: &Endpoint,
        stream: tokio_rustls::client::TlsStream<tokio::net::TcpStream>,
        method: &str,
        path: &str,
        body: Option<String>,
    ) -> Result<(u16, Vec<u8>), TransportError> {
        let io = hyper_util::rt::TokioIo::new(stream);
        let (mut sender, conn) = hyper::client::conn::http1::handshake(io)
            .await
            .map_err(|e| TransportError(format!("http: {e}")))?;
        tokio::spawn(async move {
            let _ = conn.await;
        });
        let uri = format!("https://{}:{}{path}", endpoint.host, endpoint.port);
        let mut req = Request::builder().method(method).uri(&uri);
        let bytes = body.map_or_else(Bytes::new, |b| Bytes::from(b.into_bytes()));
        if method == "POST" {
            req = req.header("content-type", "application/json");
        }
        let resp = sender
            .send_request(
                req.body(Full::new(bytes))
                    .map_err(|e| TransportError(format!("http: {e}")))?,
            )
            .await
            .map_err(|e| TransportError(format!("http: {e}")))?;
        let status = resp.status().as_u16();
        let capped = http_body_util::Limited::new(
            resp.into_body(),
            usize::try_from(MAX_BODY_BYTES).unwrap_or(usize::MAX),
        );
        let collected = capped
            .collect()
            .await
            .map_err(|e| TransportError(format!("http: {e}")))?;
        Ok((status, collected.to_bytes().to_vec()))
    }

    /// Fresh connection + round-trip, under a whole-call deadline.
    async fn call(
        &self,
        method: &str,
        path: &str,
        body: Option<String>,
    ) -> Result<Vec<u8>, TransportError> {
        let endpoint = self.endpoint.clone();
        let config = self.config.clone();
        let method = method.to_string();
        let path = path.to_string();
        tokio::time::timeout(CALL_TIMEOUT, async move {
            let stream = Self::dial(&endpoint, &config).await?;
            let (status, bytes) = Self::round_trip(&endpoint, stream, &method, &path, body).await?;
            if !(200..300).contains(&status) {
                // Server-controlled bytes: sanitize before they reach the
                // operator's terminal (no ANSI injection via error text).
                let snippet = amem_core::display::sanitize(
                    &String::from_utf8_lossy(&bytes)
                        .chars()
                        .take(200)
                        .collect::<String>(),
                );
                return Err(TransportError(format!(
                    "server refused {method} {path}: HTTP {status}: {snippet}"
                )));
            }
            Ok(bytes)
        })
        .await
        .map_err(|_| {
            TransportError(format!(
                "sync request timed out after {}s (server hung or network stalled)",
                CALL_TIMEOUT.as_secs()
            ))
        })?
    }
}

impl Transport for HyperTransport {
    async fn status(&self) -> Result<StatusDto, TransportError> {
        let bytes = self
            .call("GET", &format!("/{PROTOCOL_V1}/status"), None)
            .await?;
        serde_json::from_slice(&bytes).map_err(|e| TransportError(format!("bad status: {e}")))
    }

    async fn next(
        &self,
        device: &str,
        stream: &str,
        start: i64,
        count: i64,
    ) -> Result<NextDto, TransportError> {
        let path = format!(
            "/{PROTOCOL_V1}/next?device={}&stream={}&start={start}&count={count}",
            urlencode(device),
            urlencode(stream)
        );
        let bytes = self.call("GET", &path, None).await?;
        serde_json::from_slice(&bytes).map_err(|e| TransportError(format!("bad next page: {e}")))
    }

    async fn post(&self, body: &PostBody) -> Result<PostOutcome, TransportError> {
        let json =
            serde_json::to_string(body).map_err(|e| TransportError(format!("bad post: {e}")))?;
        let bytes = self
            .call("POST", &format!("/{PROTOCOL_V1}/records"), Some(json))
            .await?;
        serde_json::from_slice(&bytes).map_err(|e| TransportError(format!("bad outcome: {e}")))
    }
}

/// Percent-encode a query value (minimal: everything outside unreserved).
fn urlencode(s: &str) -> String {
    use std::fmt::Write as _;
    let mut out = String::new();
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(b as char);
        } else {
            let _ = write!(out, "%{b:02X}");
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pem_der_roundtrip() {
        // Hand-rolled base64 must survive every padding shape (len % 3).
        for len in [1usize, 2, 3, 4, 31, 32, 33, 64, 100, 512] {
            let der: Vec<u8> = (0..len)
                .map(|i| u8::try_from(i.wrapping_mul(37).wrapping_add(11) % 256).unwrap_or(0))
                .collect();
            let pem = pem_from_der(&der);
            assert_eq!(der_from_pem(&pem).unwrap(), der, "len={len}");
        }
    }

    #[test]
    fn endpoint_canonicalizes() {
        assert_eq!(
            canonical_endpoint("https://Sync.Test:8443/"),
            "https://sync.test:8443"
        );
        assert_eq!(canonical_endpoint("https://h"), "https://h:443");
        // Non-URLs pass through (simulator keys).
        assert_eq!(canonical_endpoint("sim"), "sim");
        // Rejections.
        assert!(Endpoint::parse("http://h:1").is_err());
        assert!(Endpoint::parse("https://u:p@h:1").is_err());
        assert!(Endpoint::parse("https://h:1/prefix").is_err());
        assert!(Endpoint::parse("https://h:1/?q=1").is_err());
    }
}
