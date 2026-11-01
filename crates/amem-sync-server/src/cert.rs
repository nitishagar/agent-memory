//! Self-signed TLS bootstrap: `init-cert` mints `cert.pem`/`key.pem`
//! (0600) for one server dir; clients pin the certificate verbatim (TOFU).

use std::path::Path;
use std::sync::Arc;

use rustls_pki_types::pem::PemObject as _;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};

/// Filenames inside the server dir.
pub const CERT_FILE: &str = "cert.pem";
/// Filenames inside the server dir.
pub const KEY_FILE: &str = "key.pem";

/// Generate a self-signed certificate (SANs: `localhost`, `127.0.0.1`, `::1`)
/// into `dir`. Returns the blake3 fingerprint of the DER bytes. Refuses to
/// overwrite an existing cert (a silent rotation would break every client's
/// pin); delete the files deliberately to rotate.
///
/// Files are created 0600 from the first byte (no world-readable window).
///
/// # Errors
///
/// Returns [`amem_core::error::Error::InvalidArguments`] when generation
/// fails or a cert already exists, [`amem_core::error::Error::Io`] on
/// write failure.
pub fn init_cert(dir: &Path) -> amem_core::error::Result<String> {
    use amem_core::error::Error;
    std::fs::create_dir_all(dir)?;
    for f in [CERT_FILE, KEY_FILE] {
        if dir.join(f).exists() {
            return Err(Error::InvalidArguments(format!(
                "{f} already exists in {}; refusing to overwrite (rotation breaks every \
                 client pin — back up and delete both files deliberately to rotate)",
                dir.display()
            )));
        }
    }
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
    write_private(&dir.join(CERT_FILE), cert_pem.as_bytes())?;
    write_private(&dir.join(KEY_FILE), key_pem.as_bytes())?;
    Ok(amem_core::store::hash_bytes_hex(cert.der()))
}

/// Write a file readable only by the owner from creation (no umask
/// window where a local watcher could grab the private key).
fn write_private(path: &Path, bytes: &[u8]) -> amem_core::error::Result<()> {
    #[cfg(unix)]
    use std::io::Write as _;
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(path)?;
        f.write_all(bytes)?;
        f.sync_all()?;
        Ok(())
    }
    #[cfg(not(unix))]
    {
        std::fs::write(path, bytes)?;
        Ok(())
    }
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
    let certs: Vec<_> = CertificateDer::pem_slice_iter(&cert_bytes)
        .filter_map(Result::ok)
        .collect();
    if certs.is_empty() {
        return Err(io_err("cert.pem holds no certificate"));
    }
    let key = PrivateKeyDer::from_pem_slice(&key_bytes)
        .map_err(|_| io_err("key.pem holds no private key"))?;
    rustls::ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
        .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])
        .map_err(|e| io_err(&format!("tls: {e}")))?
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| io_err(&format!("cert/key mismatch: {e}")))
}
