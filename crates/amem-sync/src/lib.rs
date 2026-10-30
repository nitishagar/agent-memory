//! Self-hosted team sync: versioned envelope protocol over TLS, with a
//! programmable fault simulator so the merge/apply paths are covered before
//! any server exists.

pub mod client;
pub mod envelope;
pub mod protocol;
pub mod server_core;
pub mod sim;
pub mod tls;
pub mod transport;

/// Run one sync over TLS (blocking wrapper: the current-thread runtime
/// lives here so callers stay runtime-free).
///
/// # Errors
///
/// Returns [`amem_core::error::Error::Sync`] on transport/divergence/
/// rollback failures, [`amem_core::error::Error::Db`] on store failure.
pub fn run_sync(
    store_dir: &std::path::Path,
    endpoint: &str,
    recover: bool,
    trust_new: bool,
) -> amem_core::error::Result<client::Summary> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    rt.block_on(async move {
        let transport = tls::HyperTransport::connect(endpoint, store_dir, trust_new)
            .await
            .map_err(|e| amem_core::error::Error::Sync(e.to_string()))?;
        let client = client::SyncClient::open(store_dir, endpoint, transport)?;
        client.sync(recover).await
    })
}
