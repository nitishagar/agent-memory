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
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::Request;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{verify_tls12_signature, verify_tls13_signature};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, Error as TlsError, SignatureScheme};

use crate::protocol::{NextDto, PostBody, PostOutcome, StatusDto};
use crate::transport::{Transport, TransportError};

/// Cap on response bodies (100 × 1 MiB records + slack).
const MAX_BODY_BYTES: u64 = 128 * 1024 * 1024;

/// Exact-certificate verifier: the peer must present precisely the pinned
/// DER bytes; handshake signatures still verify against the presented key.
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
            Err(TlsError::General(
                "sync certificate pin mismatch: server identity changed; verify out-of-band, delete the pin, re-pin".to_string(),
            ))
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
/// signature so the peer proves key possession) and records the DER bytes.
#[derive(Debug)]
struct CaptureVerifier {
    captured: Mutex<Option<Vec<u8>>>,
    algs: rustls::crypto::WebPkiSupportedAlgorithms,
}

impl CaptureVerifier {
    fn new() -> Self {
        let algs = rustls::crypto::ring::default_provider().signature_verification_algorithms;
        Self {
            captured: Mutex::new(None),
            algs,
        }
    }
}

impl ServerCertVerifier for CaptureVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, TlsError> {
        *self.captured.lock().unwrap() = Some(end_entity.as_ref().to_vec());
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

/// Parsed endpoint: https-only host:port + path prefix.
#[derive(Debug, Clone)]
struct Endpoint {
    host: String,
    port: u16,
    base: String,
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
        let host = u.host_str().unwrap_or("").to_string();
        if host.is_empty() {
            return Err(TransportError("endpoint has no host".to_string()));
        }
        let mut base = u.path().trim_end_matches('/').to_string();
        if base == "/" {
            base.clear();
        }
        Ok(Self {
            host,
            port: u.port().unwrap_or(443),
            base,
        })
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

/// Pin file: `{endpoint: cert_pem}`.
fn pins_path(store_dir: &std::path::Path) -> std::path::PathBuf {
    store_dir.join("sync_pins.json")
}

fn load_pins(store_dir: &std::path::Path) -> HashMap<String, String> {
    std::fs::read_to_string(pins_path(store_dir))
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_default()
}

fn store_pin(store_dir: &std::path::Path, endpoint: &str, pem: &str) -> Result<(), TransportError> {
    let mut pins = load_pins(store_dir);
    pins.insert(endpoint.to_string(), pem.to_string());
    let bytes = serde_json::to_string_pretty(&pins).unwrap_or_default();
    std::fs::write(pins_path(store_dir), bytes)
        .map_err(|e| TransportError(format!("cannot write pin file: {e}")))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let _ =
            std::fs::set_permissions(pins_path(store_dir), std::fs::Permissions::from_mode(0o600));
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
    ///
    /// # Errors
    ///
    /// Returns [`TransportError`] on non-TLS URLs, unknown servers (without
    /// `trust_new`), pin mismatch, or connection failure.
    pub async fn connect(
        url: &str,
        store_dir: &std::path::Path,
        trust_new: bool,
    ) -> Result<Self, TransportError> {
        let endpoint = Endpoint::parse(url)?;
        let pins = load_pins(store_dir);
        if let Some(pem) = pins.get(url) {
            let pinned = der_from_pem(pem)?;
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
                .with_custom_certificate_verifier(capture.clone())
                .with_no_client_auth(),
        );
        let der = Self::fetch_cert(&endpoint, &config).await?;
        let fp = fingerprint(&der);
        eprintln!("amem: TOFU: pinning sync server certificate {fp}");
        store_pin(store_dir, url, &pem_from_der(&der))?;
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
        let stream = Self::dial(endpoint, config).await?;
        let cert = stream
            .get_ref()
            .1
            .peer_certificates()
            .and_then(|c| c.first().cloned())
            .map(|c| c.as_ref().to_vec());
        // Drain a status call so a non-server TLS endpoint fails here too.
        let _ = Self::round_trip(endpoint, stream, "GET", "/v1/status", None).await?;
        cert.ok_or_else(|| TransportError("server presented no certificate".to_string()))
    }

    /// TCP dial + TLS handshake.
    async fn dial(
        endpoint: &Endpoint,
        config: &Arc<ClientConfig>,
    ) -> Result<tokio_rustls::client::TlsStream<tokio::net::TcpStream>, TransportError> {
        let tcp = tokio::net::TcpStream::connect((endpoint.host.as_str(), endpoint.port))
            .await
            .map_err(|e| {
                TransportError(format!("connect {}:{}: {e}", endpoint.host, endpoint.port))
            })?;
        tokio_rustls::TlsConnector::from(config.clone())
            .connect(endpoint.server_name()?, tcp)
            .await
            .map_err(|e| TransportError(format!("TLS handshake: {e}")))
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
        let uri = format!(
            "https://{}:{}{}{path}",
            endpoint.host, endpoint.port, endpoint.base
        );
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

    /// Fresh connection + round-trip.
    async fn call(
        &self,
        method: &str,
        path: &str,
        body: Option<String>,
    ) -> Result<Vec<u8>, TransportError> {
        let stream = Self::dial(&self.endpoint, &self.config).await?;
        let (status, bytes) = Self::round_trip(&self.endpoint, stream, method, path, body).await?;
        if !(200..300).contains(&status) {
            return Err(TransportError(format!(
                "server refused {} {}: HTTP {status}: {}",
                method,
                path,
                String::from_utf8_lossy(&bytes)
                    .chars()
                    .take(200)
                    .collect::<String>()
            )));
        }
        Ok(bytes)
    }
}

impl Transport for HyperTransport {
    async fn status(&self) -> Result<StatusDto, TransportError> {
        let bytes = self.call("GET", "/v1/status", None).await?;
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
            "/v1/next?device={}&stream={}&start={start}&count={count}",
            urlencode(device),
            urlencode(stream)
        );
        let bytes = self.call("GET", &path, None).await?;
        serde_json::from_slice(&bytes).map_err(|e| TransportError(format!("bad next page: {e}")))
    }

    async fn post(&self, body: &PostBody) -> Result<PostOutcome, TransportError> {
        let json =
            serde_json::to_string(body).map_err(|e| TransportError(format!("bad post: {e}")))?;
        let bytes = self.call("POST", "/v1/records", Some(json)).await?;
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
