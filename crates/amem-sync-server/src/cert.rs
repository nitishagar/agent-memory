//! Self-signed TLS bootstrap: `init-cert` mints `cert.pem`/`key.pem`
//! (0600) for one server dir; clients pin the certificate verbatim (TOFU).

use std::path::Path;
use std::sync::Arc;

/// Filenames inside the server dir.
pub const CERT_FILE: &str = "cert.pem";
/// Filenames inside the server dir.
pub const KEY_FILE: &str = "key.pem";

/// Generate a self-signed certificate (SANs: `localhost`, `127.0.0.1`, `::1`)
/// into `dir`. Returns the blake3 fingerprint of the DER bytes.
///
/// # Errors
///
/// Returns [`amem_core::error::Error::InvalidArguments`] when generation
/// fails, [`amem_core::error::Error::Io`] on write failure.
pub fn init_cert(dir: &Path) -> amem_core::error::Result<String> {
    use amem_core::error::Error;
    std::fs::create_dir_all(dir)?;
    let mut params = rcgen::CertificateParams::new(vec![
        "localhost".to_string(),
        "127.0.0.1".to_string(),
        "::1".to_string(),
    ])
    .map_err(|e| Error::InvalidArguments(format!("cert params: {e}")))?;
    params.is_ca = rcgen::IsCa::NoCa;
    let key_pair = rcgen::KeyPair::generate()
        .map_err(|e| Error::InvalidArguments(format!("key generation: {e}")))?;
    let cert = params
        .self_signed(&key_pair)
        .map_err(|e| Error::InvalidArguments(format!("self-sign: {e}")))?;
    let cert_pem = cert.pem();
    let key_pem = key_pair.serialize_pem();
    std::fs::write(dir.join(CERT_FILE), &cert_pem)?;
    std::fs::write(dir.join(KEY_FILE), &key_pem)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        for f in [CERT_FILE, KEY_FILE] {
            let _ = std::fs::set_permissions(dir.join(f), std::fs::Permissions::from_mode(0o600));
        }
    }
    Ok(amem_core::store::hash_bytes_hex(cert.der()))
}

/// Load `cert.pem`/`key.pem` into a rustls server config.
///
/// # Errors
///
/// Returns [`amem_core::error::Error::Io`] when the files are missing or
/// malformed.
pub fn load_server_config(dir: &Path) -> amem_core::error::Result<rustls::ServerConfig> {
    use amem_core::error::Error;
    let io_err = |what: &str| Error::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, what));
    let cert_bytes = std::fs::read(dir.join(CERT_FILE)).map_err(|_| {
        io_err("server dir has no cert.pem; run `amem sync-server --dir <dir> --init-cert` first")
    })?;
    let key_bytes = std::fs::read(dir.join(KEY_FILE)).map_err(|_| {
        io_err("server dir has no key.pem; run `amem sync-server --dir <dir> --init-cert` first")
    })?;
    let certs: Vec<_> = rustls_pemfile::certs(&mut cert_bytes.as_slice())
        .filter_map(Result::ok)
        .collect();
    if certs.is_empty() {
        return Err(io_err("cert.pem holds no certificate"));
    }
    let key = rustls_pemfile::private_key(&mut key_bytes.as_slice())
        .map_err(|_| io_err("key.pem holds no private key"))?
        .ok_or_else(|| io_err("key.pem holds no private key"))?;
    rustls::ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
        .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])
        .map_err(|e| io_err(&format!("tls: {e}")))?
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| io_err(&format!("cert/key mismatch: {e}")))
}
