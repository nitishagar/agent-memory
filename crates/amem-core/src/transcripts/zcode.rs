//! `ZCode` transcripts: `~/.zcode/cli/db/db.sqlite`.
//!
//! `ZCode`'s backend is an `OpenCode` fork: the canonical `session`,
//! `message`, and `part` tables parse through the shared
//! [`crate::transcripts::opencode`] core (named-column selects tolerate
//! `ZCode`'s extra columns). Sub-agent `transcript.jsonl` sidecars and
//! `rollout/model-io-*.jsonl` dumps are complementary, not sources of
//! truth, and are out of scope for the MVP parser.

use std::path::Path;

use crate::error::Result;
use crate::transcripts::{ParsedEvent, Source, Warning, opencode};

/// Parse one `ZCode` `db.sqlite`. See [`opencode::parse_db`].
///
/// # Errors
///
/// Returns [`Error::Db`] only when the file is not a database at all.
pub fn parse_db(path: &Path) -> Result<(Vec<ParsedEvent>, Vec<Warning>)> {
    opencode::parse_db(path, Source::Zcode)
}

#[cfg(test)]
mod tests {
    use super::*;

    const BASIC_SQL: &str = include_str!("../../tests/fixtures/transcripts/zcode_basic.sql");

    #[test]
    fn golden_tolerates_fork_extras() {
        // Fixture carries ZCode-only columns (task_type, sequence) and
        // envelope extras (contextSnapshot, semantics, reasoningLevel).
        let (_tmp, path) = opencode::tests::db_from_sql(BASIC_SQL);
        let (events, warnings) = parse_db(&path).unwrap();
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
