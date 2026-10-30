//! Structured fuzz: transcript parsers + hook-input-adjacent parsers.
//! Seeded PRNG (xorshift inline, no new deps), corpus + mutations.
//! Env: `FUZZ_SECS` (default 2 locally, CI passes 60), `FUZZ_SEED`.
//! Invariants: no panic, no hang past deadline, errors are typed.

use std::io::Write;
use std::time::{Duration, Instant};

use amem_core::transcripts::{Source, claude, codex, cursor, opencode, pi, zcode};

/// Deterministic xorshift64 (no dependency weight for a test target).
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0 | 1;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    fn below(&mut self, n: usize) -> usize {
        usize::try_from(self.next() % (n as u64).max(1)).unwrap_or(0)
    }
    fn bytes(&mut self, len: usize) -> Vec<u8> {
        (0..len)
            .map(|_| u8::try_from(self.next() % 256).unwrap_or(0))
            .collect()
    }
}

/// Corpus seeds: shaped like real inputs, then mutated.
const SEEDS: &[&str] = &[
    r#"{"role":"user","content":"hello","ts":1700000000}"#,
    r#"{"type":"message","session":"abc","text":"x"}"#,
    r#"{"cmd":"cargo test -- --password s3cret","exit":0}"#,
    "not json at all",
    "",
    "\0\0\0",
    r#"{"a":{"b":{"c":[1,2,{"d":null}]}}}"#,
    "\u{1b}[0m\u{7f}controls\u{0}here",
];

fn mutate(rng: &mut Rng, seed: &[u8]) -> Vec<u8> {
    let mut v = seed.to_vec();
    match rng.below(6) {
        0 => {
            let n = rng.below(64);
            v.extend_from_slice(&rng.bytes(n)); // append junk
        }
        1 => {
            // flip random bytes
            for _ in 0..rng.below(8) {
                if !v.is_empty() {
                    let i = rng.below(v.len());
                    v[i] = u8::try_from(rng.next() % 256).unwrap_or(0);
                }
            }
        }
        2 => v.truncate(rng.below(v.len().saturating_add(1))), // truncate (torn tails)
        3 => {
            // splice two seeds
            let other = SEEDS[rng.below(SEEDS.len())].as_bytes();
            let at = rng.below(v.len().saturating_add(1));
            v.splice(at..at.min(v.len()), other.iter().copied());
        }
        4 => {
            let n = rng.below(300);
            v = rng.bytes(n); // pure noise
        }
        _ => {} // seed verbatim
    }
    if rng.below(3) == 0 {
        v.push(b'\n');
        let n = rng.below(200);
        v.extend_from_slice(&rng.bytes(n)); // second line
    }
    v
}

fn fuzz_secs() -> u64 {
    std::env::var("FUZZ_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2)
}

fn fuzz_seed() -> u64 {
    std::env::var("FUZZ_SEED")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0x5EED)
}

#[test]
fn jsonl_parsers_never_panic() {
    let budget = Duration::from_secs(fuzz_secs());
    let start = Instant::now();
    let mut rng = Rng(fuzz_seed());
    let tmp = tempfile::tempdir().unwrap();
    let redactor = amem_core::redact::Redactor::new();
    let mut iters = 0u64;
    while start.elapsed() < budget {
        iters += 1;
        let seed = SEEDS[rng.below(SEEDS.len())].as_bytes();
        let bytes = mutate(&mut rng, seed);
        let path = tmp.path().join(format!("f{rng}", rng = rng.next() % 16));
        std::fs::File::create(&path)
            .unwrap()
            .write_all(&bytes)
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        // All four JSONL parsers must return (events or warnings), never panic.
        let _ = claude::parse_file(&path, deadline);
        let _ = codex::parse_file(&path, deadline);
        let _ = pi::parse_file(&path, deadline);
        // SQLite parsers get the same bytes as garbage DBs: must error cleanly.
        let _ = cursor::parse_db(&path, deadline);
        let _ = opencode::parse_db(&path, Source::Opencode, deadline);
        let _ = zcode::parse_db(&path, deadline);
        // Hook-input-adjacent: redaction + text utils over arbitrary UTF-8 lossy text.
        let text = String::from_utf8_lossy(&bytes);
        let _ = redactor.redact(&text);
        let _ = amem_core::transcripts::truncate_text(&text, 64);
        let _ = amem_core::transcripts::parse_iso_ms(&text);
    }
    eprintln!("fuzz iters={iters} secs={}", start.elapsed().as_secs());
}
