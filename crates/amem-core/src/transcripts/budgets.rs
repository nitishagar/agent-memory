//! Streaming budget enforcement tests: 100MB JSONL, binary blobs, torn
//! tails, oversize `SQLite` values, and the pass time-box. Slow by design
//! (multi-MB fixtures generated at test time — never committed).

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use crate::transcripts::{
        MAX_EVENT_TEXT_BYTES, PASS_DEADLINE, ReadStats, Warning, cursor, is_torn_tail,
        read_jsonl_bounded,
    };

    #[test]
    fn big_jsonl_streams_bounded() {
        // ~100MB of small lines: must complete with exact counts.
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("big.jsonl");
        let line = r#"{"type":"user","message":{"role":"user","content":"ping"},"uuid":"u","timestamp":"2026-10-30T00:00:00.000Z","sessionId":"s","cwd":"/p"}"#;
        let line_len = line.len() + 1;
        let target: usize = 100 * 1024 * 1024;
        let count = target / line_len + 1;
        let mut content = String::with_capacity(count * line_len);
        for _ in 0..count {
            content.push_str(line);
            content.push('\n');
        }
        std::fs::write(&path, &content).unwrap();
        assert!(path.metadata().unwrap().len() >= 100 * 1024 * 1024);

        let mut lines: u64 = 0;
        let stats: ReadStats = read_jsonl_bounded(&path, Instant::now() + PASS_DEADLINE, |l, _| {
            assert!(l.len() < crate::transcripts::MAX_LINE_BYTES);
            lines += 1;
            Ok(true)
        })
        .unwrap();
        assert_eq!(lines, count as u64);
        assert_eq!(stats.lines, count as u64);
        assert_eq!(stats.skipped, 0);
    }

    #[test]
    fn binary_blob_never_buffers_whole() {
        // 1MB with no newlines: one overlong line, skipped, no panic.
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("blob.jsonl");
        // Newline-free binary: byte 10 would split lines.
        let blob: Vec<u8> = (0..1_000_000u32)
            .map(|i| u8::try_from((i % 254) + 1).unwrap())
            .map(|b| if b == 10 { 11 } else { b })
            .collect();
        std::fs::write(&path, blob).unwrap();
        let mut seen = 0;
        let stats = read_jsonl_bounded(&path, Instant::now() + PASS_DEADLINE, |_, _| {
            seen += 1;
            Ok(true)
        })
        .unwrap();
        assert_eq!(seen, 0);
        assert_eq!(stats.skipped, 1);
        assert!(!is_torn_tail(false));
    }

    #[test]
    fn sqlite_big_value_truncates_with_marker() {
        // 2MB bubble text inside a ~200MB database: value truncates,
        // the rest of the file parses, the pass stays quick.
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("state.vscdb");
        {
            let conn = rusqlite::Connection::open(&path).unwrap();
            conn.execute_batch(
                "CREATE TABLE cursorDiskKV(key TEXT UNIQUE ON CONFLICT REPLACE, value BLOB);",
            )
            .unwrap();
            let big_text = "z".repeat(2 * 1024 * 1024);
            let bubble = serde_json::json!({
                "type": 1, "bubbleId": "big1", "text": big_text,
                "createdAt": "2026-10-30T00:00:00.000Z",
                "workspaceProjectDir": "/home/u/proj",
            });
            conn.execute(
                "INSERT INTO cursorDiskKV VALUES ('bubbleId:c1:big1', ?1)",
                rusqlite::params![bubble.to_string()],
            )
            .unwrap();
            // Pad the file past 200MB with filler blobs.
            let filler = vec![7u8; 2 * 1024 * 1024];
            for i in 0..100 {
                conn.execute(
                    "INSERT INTO cursorDiskKV VALUES (?1, ?2)",
                    rusqlite::params![format!("filler:{i}"), filler],
                )
                .unwrap();
            }
        }
        assert!(path.metadata().unwrap().len() >= 200 * 1024 * 1024);
        let start = Instant::now();
        let (events, warnings) = cursor::parse_db(&path, Instant::now() + PASS_DEADLINE).unwrap();
        assert!(start.elapsed() < std::time::Duration::from_secs(60));
        assert!(warnings.is_empty(), "{warnings:?}");
        assert_eq!(events.len(), 1);
        assert!(events[0].event.text.len() <= MAX_EVENT_TEXT_BYTES + 64);
        assert!(events[0].event.text.contains("[truncated 2097152 bytes]"));
    }

    #[test]
    fn adversarial_corpus_never_panics() {
        // Every JSONL parser meets the same hostile file: empty lines,
        // wrong JSON shapes, deep nesting, wrong types, garbage.
        let mut hostile = String::from("\nnull\n[]\n123\n\"str\"\n");
        hostile.push_str(&"{\"a\":".repeat(500));
        hostile.push_str("{\"type\":123,\"sessionId\":[],\"timestamp\":true}\n");
        hostile.push_str("{\"type\":\"user\"}\n"); // missing message
        hostile.push_str(&"q".repeat(crate::transcripts::MAX_LINE_BYTES));
        hostile.push('\n');
        for name in ["claude", "pi", "codex"] {
            let tmp = tempfile::tempdir().unwrap();
            let path = tmp.path().join(format!("hostile-{name}.jsonl"));
            std::fs::write(&path, &hostile).unwrap();
            let deadline = Instant::now() + PASS_DEADLINE;
            let (events, warnings, stats) = match name {
                "claude" => crate::transcripts::claude::parse_file(&path, deadline).unwrap(),
                "pi" => crate::transcripts::pi::parse_file(&path, deadline).unwrap(),
                _ => crate::transcripts::codex::parse_file(&path, deadline).unwrap(),
            };
            assert!(stats.lines >= 7, "{name}: {stats:?}");
            assert!(!warnings.is_empty(), "{name}: hostile lines must warn");
            drop(events);
        }
        // SQLite parsers meet wrong-typed values, NULLs, and bad JSON.
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("hostile.vscdb");
        {
            let conn = rusqlite::Connection::open(&path).unwrap();
            conn.execute_batch(
                "CREATE TABLE cursorDiskKV(key TEXT UNIQUE ON CONFLICT REPLACE, value BLOB);
                 INSERT INTO cursorDiskKV VALUES ('composerData:c', 42);
                 INSERT INTO cursorDiskKV VALUES ('bubbleId:c:b', NULL);
                 INSERT INTO cursorDiskKV VALUES ('bubbleId:c:b2', 'not json');
                 INSERT INTO cursorDiskKV VALUES ('bubbleId:c:b3', '{\"type\":\"nan\"}');",
            )
            .unwrap();
        }
        let (events, warnings) = cursor::parse_db(&path, Instant::now() + PASS_DEADLINE).unwrap();
        assert!(events.is_empty(), "{events:?}");
        assert!(!warnings.is_empty(), "{warnings:?}");
    }

    #[test]
    fn expired_deadline_aborts() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("d.jsonl");
        std::fs::write(&path, "{}\n").unwrap();
        let past = Instant::now()
            .checked_sub(std::time::Duration::from_secs(1))
            .unwrap();
        let err = read_jsonl_bounded(&path, past, |_, _| Ok(true)).unwrap_err();
        assert!(matches!(err, crate::error::Error::Timeout(_)));
        let _: Vec<Warning> = Vec::new();
    }
}
