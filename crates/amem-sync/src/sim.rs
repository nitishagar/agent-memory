//! Simulated transport: a [`ServerCore`](crate::server_core::ServerCore)
//! behind a programmable fault injector for the harness.

use std::sync::{Arc, Mutex};

use crate::protocol::{NextDto, PostBody, PostOutcome, StatusDto};
use crate::server_core::ServerCore;
use crate::transport::{Transport, TransportError};

/// Faults the injector can arm. `fail_on_call` / `dupe_post` /
/// `reorder_page` / `rollback_to_empty` fire once then disarm; `skew_ms`
/// persists until cleared.
#[derive(Debug, Clone, Default)]
pub struct Faults {
    /// Submit the next POST body twice (at-least-once delivery).
    pub dupe_post: bool,
    /// Fail the Nth transport call (1-based) with a transport error.
    pub fail_on_call: Option<u64>,
    /// Reverse the next `next()` page (reordered delivery).
    pub reorder_page: bool,
    /// Rewrite every downloaded envelope wall clock by this delta.
    pub skew_ms: i64,
    /// Truncate the union on the next call (rollback; generation kept).
    pub rollback_to_empty: bool,
}

/// Shared simulator: one union, many clients, injectable faults.
#[derive(Clone)]
pub struct SimulatedTransport {
    inner: Arc<Mutex<SimInner>>,
}

struct SimInner {
    core: ServerCore,
    faults: Faults,
    calls: u64,
}

impl SimulatedTransport {
    /// Create a simulator over a fresh union database.
    ///
    /// # Errors
    ///
    /// Returns [`amem_core::error::Error::Db`] on `SQLite` failure.
    pub fn open(path: &std::path::Path) -> amem_core::error::Result<Self> {
        Ok(Self {
            inner: Arc::new(Mutex::new(SimInner {
                core: ServerCore::open(path)?,
                faults: Faults::default(),
                calls: 0,
            })),
        })
    }

    /// Arm faults for upcoming calls (replaces the previous program).
    ///
    /// # Panics
    ///
    /// Panics when the simulator mutex is poisoned (a previous holder panicked).
    pub fn arm(&self, faults: Faults) {
        self.inner.lock().unwrap().faults = faults;
    }

    /// Pre-call injection: rollback + call-count failure. Returns the
    /// failure when this call must not reach the core.
    fn pre(inner: &mut SimInner) -> Option<TransportError> {
        inner.calls += 1;
        if inner.faults.rollback_to_empty {
            inner.faults.rollback_to_empty = false;
            inner.core.truncate_union();
        }
        if inner.faults.fail_on_call == Some(inner.calls) {
            inner.faults.fail_on_call = None;
            return Some(TransportError(format!(
                "injected failure on call {}",
                inner.calls
            )));
        }
        None
    }
}

impl Transport for SimulatedTransport {
    async fn status(&self) -> Result<StatusDto, TransportError> {
        let mut inner = self.inner.lock().unwrap();
        if let Some(e) = Self::pre(&mut inner) {
            return Err(e);
        }
        inner
            .core
            .status()
            .map_err(|e| TransportError(e.to_string()))
    }

    async fn next(
        &self,
        device: &str,
        stream: &str,
        start: i64,
        count: i64,
    ) -> Result<NextDto, TransportError> {
        let mut inner = self.inner.lock().unwrap();
        if let Some(e) = Self::pre(&mut inner) {
            return Err(e);
        }
        let mut records = inner
            .core
            .next(device, stream, start, count)
            .map_err(|e| TransportError(e.to_string()))?;
        if inner.faults.reorder_page {
            inner.faults.reorder_page = false;
            records.reverse();
        }
        let skew = inner.faults.skew_ms;
        if skew != 0 {
            records = records
                .into_iter()
                .map(|raw| skew_envelope(&raw, skew))
                .collect();
        }
        Ok(NextDto { records })
    }

    async fn post(&self, body: &PostBody) -> Result<PostOutcome, TransportError> {
        let mut inner = self.inner.lock().unwrap();
        if let Some(e) = Self::pre(&mut inner) {
            return Err(e);
        }
        // Core errors (malformed batch) surface as transport errors: the
        // client treats them as failed calls, advancing nothing.
        let out = inner
            .core
            .post(body)
            .map_err(|e| TransportError(e.to_string()))?;
        if inner.faults.dupe_post {
            inner.faults.dupe_post = false;
            let _ = inner.core.post(body);
        }
        Ok(out)
    }
}

/// Rewrite an envelope's wall clock by `skew` (hash untouched: it covers
/// the payload only). Unparseable input passes through for the server's
/// own validation to reject.
fn skew_envelope(raw: &str, skew: i64) -> String {
    let mut v: serde_json::Value = match serde_json::from_str(raw) {
        Ok(v) => v,
        Err(_) => return raw.to_string(),
    };
    if let Some(wall) = v.get_mut("hlc").and_then(|h| h.get_mut("wall_ms")) {
        if let Some(w) = wall.as_i64() {
            *wall = serde_json::Value::from(w.saturating_add(skew));
        }
    }
    v.to_string()
}
