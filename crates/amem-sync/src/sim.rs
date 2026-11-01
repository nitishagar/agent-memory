//! Simulated transport: a [`ServerCore`](crate::server_core::ServerCore)
//! behind a programmable fault injector for the harness.

use std::sync::{Arc, Mutex};

use crate::protocol::{NextDto, PostBody, PostOutcome, StatusDto};
use crate::server_core::ServerCore;
use crate::transport::{Transport, TransportError};

/// Faults the injector can arm. `fail_on_call` / `dupe_post` /
/// `reorder_page` / `rollback_to_empty` / `junk_page` fire once then
/// disarm; `skew_ms` persists until cleared.
#[derive(Debug, Clone, Default)]
#[allow(clippy::struct_excessive_bools)] // fault flags are the point
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
    /// Replace the next `next()` page with idx-less junk (hostile server).
    pub junk_page: bool,
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

    /// Post raw envelopes straight into the sim's union (test seeding),
    /// through the sim's own mutex — never open a second `ServerCore` on
    /// the live DB file (the core's `SELECT`-then-`INSERT` is only
    /// race-safe behind the single app mutex).
    ///
    /// # Panics
    ///
    /// Panics when the simulator mutex is poisoned (a previous holder panicked).
    ///
    /// # Errors
    ///
    /// Returns [`TransportError`] when the batch is malformed or oversize.
    pub fn post_raw_for_test(&self, records: Vec<String>) -> Result<PostOutcome, TransportError> {
        let mut inner = self.inner.lock().unwrap();
        inner
            .core
            .post(&PostBody {
                records,
                client_caps: Vec::new(),
            })
            .map_err(|e| TransportError(e.to_string()))
    }

    /// Pre-call injection: rollback + call-count failure. Returns the
    /// failure when this call must not reach the core.
    fn pre(inner: &mut SimInner) -> Option<TransportError> {
        inner.calls += 1;
        if inner.faults.rollback_to_empty {
            inner.faults.rollback_to_empty = false;
            inner
                .core
                .truncate_union()
                .expect("rollback injection must apply");
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
    // The bodies are synchronous (fault injection around a sync core); each
    // wraps its original body in an immediately-invoked closure inside
    // `std::future::ready` to satisfy the `+ Send` future the trait wants
    // without an artificial await.
    fn status(
        &self,
    ) -> impl std::future::Future<Output = Result<StatusDto, TransportError>> + Send {
        std::future::ready((|| {
            let mut inner = self.inner.lock().unwrap();
            if let Some(e) = Self::pre(&mut inner) {
                return Err(e);
            }
            inner
                .core
                .status()
                .map_err(|e| TransportError(e.to_string()))
        })())
    }

    fn next(
        &self,
        device: &str,
        stream: &str,
        start: i64,
        count: i64,
    ) -> impl std::future::Future<Output = Result<NextDto, TransportError>> + Send {
        std::future::ready((|| {
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
            if inner.faults.junk_page {
                inner.faults.junk_page = false;
                records = vec!["{\"a\":1}".to_string(), "not json at all".to_string()];
            }
            let skew = inner.faults.skew_ms;
            if skew != 0 {
                records = records
                    .into_iter()
                    .map(|raw| skew_envelope(&raw, skew))
                    .collect();
            }
            Ok(NextDto { records })
        })())
    }

    fn post(
        &self,
        body: &PostBody,
    ) -> impl std::future::Future<Output = Result<PostOutcome, TransportError>> + Send {
        std::future::ready((|| {
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
        })())
    }
}

/// Shift an envelope's clock coherently by `skew` ms: BOTH `hlc.wall_ms`
/// and `ts_ns` move, so the envelope stays HLC-consistent and exercises
/// the real skew gate (a wall-only shift is now an "hlc mismatch").
/// Hash untouched (it covers the payload only). Unparseable input passes
/// through for the client's own validation to reject.
fn skew_envelope(raw: &str, skew: i64) -> String {
    let mut v: serde_json::Value = match serde_json::from_str(raw) {
        Ok(v) => v,
        Err(_) => return raw.to_string(),
    };
    if let Some(wall) = v.get_mut("hlc").and_then(|h| h.get_mut("wall_ms"))
        && let Some(w) = wall.as_i64()
    {
        *wall = serde_json::Value::from(w.saturating_add(skew));
    }
    if let Some(ts) = v.get("ts_ns").and_then(serde_json::Value::as_i64) {
        v["ts_ns"] = serde_json::Value::from(ts.saturating_add(skew.saturating_mul(1_000_000)));
    }
    v.to_string()
}
