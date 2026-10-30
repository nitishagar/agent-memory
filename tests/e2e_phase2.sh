#!/usr/bin/env bash
# Phase 2 end-to-end: fixture transcripts in fake agent dirs → ingest → search.
# Usage: tests/e2e_phase2.sh <path-to-amem>
# Baselines pinned here: 8 files / 24 events / 24 ingested; per-source recall below.
set -u
AMEM="${1:?usage: e2e_phase2.sh <amem-binary>}"
AMEM_BIN="$(cd "$(dirname "$AMEM")" && pwd)/$(basename "$AMEM")"
FIXDIR="$(cd "$(dirname "$0")/../crates/amem-core/tests/fixtures/transcripts" && pwd)"
PASS=0; FAIL=0
note() { echo "e2e2 [$1] $2"; }
pass() { PASS=$((PASS+1)); note PASS "$1"; }
fail() { FAIL=$((FAIL+1)); note FAIL "$1"; }

home="$(mktemp -d)"; store="$home/store"; fake="$home/fake"
mkdir -p "$fake/claude/slug1" "$fake/codex/2026/10/30" "$fake/pi/--home-u-proj--"
cp "$FIXDIR/claude_basic.jsonl" "$fake/claude/slug1/sess-aaaa.jsonl"
printf 'this is not json\n{"type":"user","oops":\n' > "$fake/claude/slug1/broken.jsonl"
cp "$FIXDIR/codex_basic.jsonl" "$fake/codex/2026/10/30/rollout-1.jsonl"
cp "$FIXDIR/codex_legacy.jsonl" "$fake/codex/2026/10/30/rollout-0.jsonl"
cp "$FIXDIR/pi_basic.jsonl" "$fake/pi/--home-u-proj--/2026-10-30T00-00-00-000Z_01936f10.jsonl"
python3 - "$FIXDIR/cursor_basic.sql" "$fake/cursor.vscdb" "$FIXDIR/opencode_basic.sql" "$fake/opencode.db" "$FIXDIR/zcode_basic.sql" "$fake/zcode.db" <<'PYEOF'
import sqlite3, sys
for sql_path, db_path in [(sys.argv[1], sys.argv[2]), (sys.argv[3], sys.argv[4]), (sys.argv[5], sys.argv[6])]:
    sql = open(sql_path).read()
    con = sqlite3.connect(db_path)
    con.executescript(sql)
    con.commit()
    con.close()
PYEOF

AMEM_STORE_DIR="$store" "$AMEM_BIN" ingest --once \
  --transcript-root "claude=$fake/claude" \
  --transcript-root "codex=$fake/codex" \
  --transcript-root "cursor=$fake/cursor.vscdb" \
  --transcript-root "opencode=$fake/opencode.db" \
  --transcript-root "pi=$fake/pi" \
  --transcript-root "zcode=$fake/zcode.db" \
  > "$home/stats.json" 2> "$home/warnings.txt"

stat() { python3 -c "import json; print(json.load(open('$home/stats.json'))['$1'])"; }
[ "$(stat transcript_files)" = "8" ] && pass "8 transcript files" || fail "files got [$(stat transcript_files)] want 8"
[ "$(stat transcript_events)" = "24" ] && pass "24 transcript events" || fail "events got [$(stat transcript_events)] want 24"
[ "$(stat transcript_ingested)" = "24" ] && pass "24 ingested" || fail "ingested got [$(stat transcript_ingested)] want 24"
if grep -q "transcript warn" "$home/warnings.txt"; then pass "malformed warned"; else fail "malformed file silent"; fi

recall() { # <stream> <keyword> <want> — count search hits
  local n
  n=$(AMEM_STORE_DIR="$store" "$AMEM_BIN" search "$2" --stream "$1" | wc -l)
  if [ "$n" = "$3" ]; then pass "$1 recall $2=$3"; else fail "$1 recall $2 got [$n] want $3"; fi
}
recall "transcript:claude" "Listing" 1
recall "transcript:codex" "Listed" 1
recall "transcript:pi" "hello" 1
recall "transcript:cursor" "retries" 1
recall "transcript:opencode" "hello" 1
recall "transcript:zcode" "there" 1

# Session/project identity intact per stream (pinned).
python3 - "$store/memory.db" > "$home/identity.txt" <<'PYEOF'
import json, sqlite3, sys
con = sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True)
rows = con.execute("select stream, payload from records where stream like 'transcript:%'").fetchall()
seen = {}
for stream, payload in rows:
    p = json.loads(payload)
    seen.setdefault(stream, set()).add((p["session"], p["project"]))
for stream in sorted(seen):
    for session, project in sorted(seen[stream]):
        print(f"{stream} {session} {project}")
PYEOF
for want in \
  "transcript:claude aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaaa /home/u/proj" \
  "transcript:codex 019f97ce-aaaa-7aaa-aaaa-aaaaaaaaaaaa /home/u/demo" \
  "transcript:codex 019f97ce-bbbb-7bbb-bbbb-bbbbbbbbbbbb /home/u/old" \
  "transcript:pi 01936f10-1111-7111-8111-111111111111 /home/u/proj" \
  "transcript:cursor aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee /home/u/proj" \
  "transcript:opencode ses_00000000000000000000000001 /home/u/proj" \
  "transcript:zcode sess_1111 /home/u/demo"; do
  if grep -qF "$want" "$home/identity.txt"; then pass "identity $want"; else fail "identity missing $want"; fi
done

# Re-ingest: exactly-once, no duplicates.
AMEM_STORE_DIR="$store" "$AMEM_BIN" ingest --once \
  --transcript-root "claude=$fake/claude" \
  --transcript-root "codex=$fake/codex" \
  --transcript-root "cursor=$fake/cursor.vscdb" \
  --transcript-root "opencode=$fake/opencode.db" \
  --transcript-root "pi=$fake/pi" \
  --transcript-root "zcode=$fake/zcode.db" \
  > "$home/stats2.json" 2>/dev/null
n2=$(python3 -c "import json; print(json.load(open('$home/stats2.json'))['transcript_ingested'])")
[ "$n2" = "0" ] && pass "re-ingest no-op" || fail "re-ingest got [$n2] want 0"

rm -rf "$home"
echo "e2e2: PASS=$PASS FAIL=$FAIL"
[ "$FAIL" -eq 0 ]
