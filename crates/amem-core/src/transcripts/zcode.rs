//! `ZCode` transcripts: `~/.zcode/cli/db/db.sqlite`.
//!
//! `ZCode`'s backend is an `OpenCode` fork: the canonical `session`,
//! `message`, and `part` tables parse through the shared
//! [`crate::transcripts::opencode`] core (named-column selects tolerate
//! `ZCode`'s extra columns). Sub-agent `transcript.jsonl` sidecars and
//! `rollout/model-io-*.jsonl` dumps are complementary, not sources of
//! truth, and are out of scope for the MVP parser.

use std::path::Path;
use std::time::Instant;

use crate::error::Result;
use crate::transcripts::{ParsedEvent, Source, Warning, opencode};

/// Parse one `ZCode` `db.sqlite`. See [`opencode::parse_db`].
///
/// # Errors
///
/// Returns [`Error::Db`] when the file is not a database at all and
/// [`Error::Timeout`] on an expired `deadline`.
pub fn parse_db(path: &Path, deadline: Instant) -> Result<(Vec<ParsedEvent>, Vec<Warning>)> {
    opencode::parse_db(path, Source::Zcode, deadline)
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use super::*;
    use crate::transcripts::PASS_DEADLINE;

    const BASIC_SQL: &str = include_str!("../../tests/fixtures/transcripts/zcode_basic.sql");

    #[test]
    fn golden_tolerates_fork_extras() {
        // Fixture carries ZCode-only columns (task_type, sequence) and
        // envelope extras (contextSnapshot, semantics, reasoningLevel).
        let (_tmp, path) = opencode::tests::db_from_sql(BASIC_SQL);
        let (events, warnings) = parse_db(&path, Instant::now() + PASS_DEADLINE).unwrap();
        assert!(warnings.is_empty(), "{warnings:?}");
        // user-text + assistant-text (reasoning skipped).
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].event.source, Source::Zcode);
        assert_eq!(events[0].event.session_id, "sess_1111");
        assert_eq!(events[0].event.project, "/home/u/demo");
        assert_eq!(events[0].event.text, "hello");
        assert_eq!(events[1].event.text, "hi there");
        assert_eq!(events[1].event.model.as_deref(), Some("GLM-5.2"));
    }
}
