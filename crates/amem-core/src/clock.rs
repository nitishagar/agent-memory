//! Wall-clock helpers shared by CLI and ingest.

use std::time::{SystemTime, UNIX_EPOCH};

/// Current time as Unix nanoseconds, saturating on clock failure.
#[must_use]
pub fn now_nanos() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX))
}
