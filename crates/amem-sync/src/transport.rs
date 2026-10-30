//! Transport abstraction: the sync flow speaks this trait; the fault
//! harness injects failures through it, production runs it over TLS.

use crate::protocol::{NextDto, PostBody, PostOutcome, StatusDto};

/// Transport failure (network, TLS, protocol). Mapped to
/// [`amem_core::error::Error::Sync`] at the client boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransportError(pub String);

impl std::fmt::Display for TransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "transport: {}", self.0)
    }
}

/// Sync transport: status, paging download, atomic idempotent upload.
pub trait Transport {
    /// `GET /v1/status`.
    fn status(&self)
    -> impl std::future::Future<Output = Result<StatusDto, TransportError>> + Send;
    /// `GET /v1/next?device&stream&start&count`.
    fn next(
        &self,
        device: &str,
        stream: &str,
        start: i64,
        count: i64,
    ) -> impl std::future::Future<Output = Result<NextDto, TransportError>> + Send;
    /// `POST /v1/records`.
    fn post(
        &self,
        body: &PostBody,
    ) -> impl std::future::Future<Output = Result<PostOutcome, TransportError>> + Send;
}
