#!/usr/bin/env bash
# Phase 3 end-to-end: raw JSON-RPC over stdio — seed → search → guarded
# write → conflicting write → context brief (recall >= 95% of seeded events).
# Usage: tests/e2e_phase3.sh <path-to-amem>
set -u
AMEM="${1:?usage: e2e_phase3.sh <amem-binary>}"
AMEM_BIN="$(cd "$(dirname "$AMEM")" && pwd)/$(basename "$AMEM")"
FIXDIR="$(cd "$(dirname "$0")/../crates/amem-core/tests/fixtures/transcripts" && pwd)"
PASS=0; FAIL=0
note() { echo "e2e3 [$1] $2"; }
pass() { PASS=$((PASS+1)); note PASS "$1"; }
fail() { FAIL=$((FAIL+1)); note FAIL "$1"; }

home="$(mktemp -d)"; store="$home/store"; fake="$home/fake"
mkdir -p "$fake/claude/slug1"
cp "$FIXDIR/claude_basic.jsonl" "$fake/claude/slug1/sess.jsonl"
export AMEM_STORE_DIR="$store"

id1=$("$AMEM_BIN" history start --shell bash --cmd "e2e3-build-widgets --release" --session e2e3)
"$AMEM_BIN" history end --id "$id1" --exit 0 >/dev/null
id2=$("$AMEM_BIN" history start --shell bash --cmd "e2e3-test-widgets --all" --session e2e3)
"$AMEM_BIN" history end --id "$id2" --exit 1 >/dev/null
"$AMEM_BIN" ingest --once \
  --transcript-root "claude=$fake/claude" \
  --transcript-root "codex=$fake/none" \
  --transcript-root "cursor=$fake/none" \
  --transcript-root "opencode=$fake/none" \
  --transcript-root "pi=$fake/none" \
  --transcript-root "zcode=$fake/none" 2>/dev/null | grep -q '"transcript_ingested":5' \
  && pass "seed ingested (2 shell + 5 claude)" || fail "seed ingest stats"

# mcp_session <stem> — one stdio run; reads <stem>.calls.json, writes <stem>.json.
mcp_session() {
  python3 - "$AMEM_BIN" "$store" "$1" <<'PYEOF'
import json, subprocess, sys
amem, store, stem = sys.argv[1], sys.argv[2], sys.argv[3]
out = stem + ".json"
calls = json.loads(open(stem + ".calls.json").read())
msgs = [
    {"jsonrpc": "2.0", "id": 1, "method": "initialize",
     "params": {"protocolVersion": "2025-03-26", "capabilities": {},
                "clientInfo": {"name": "e2e3", "version": "0"}}},
    {"jsonrpc": "2.0", "method": "notifications/initialized"},
]
msgs += [{"jsonrpc": "2.0", "id": c, "method": m, "params": p} for c, m, p in calls]
inp = "\n".join(json.dumps(m) for m in msgs) + "\n"
r = subprocess.run([amem, "mcp"], input=inp, capture_output=True, text=True,
                   timeout=30, env={"AMEM_STORE_DIR": store, "PATH": "/usr/bin:/bin"})
resps = {}
for line in r.stdout.splitlines():
    try:
        o = json.loads(line)
    except Exception:
        continue
    if o.get("id") in (2, 3, 4, 5, 6):
        resps[str(o["id"])] = o.get("result", {})
json.dump(resps, open(out, "w"))
PYEOF
}

echo '[[2,"tools/call",{"name":"mem_write","arguments":{"id":"e2e3note","title":"ship","text":"e2e3-ship-note","format":"json"}}]]' > "$home/a.calls.json"
mcp_session "$home/a"
sum=$(python3 -c "import json; print(json.load(open('$home/a.json'))['2']['content'][0]['text'])" | python3 -c "import json,sys; print(json.load(sys.stdin)['checksum'])")
[ -n "$sum" ] && pass "guarded write created note" || fail "note create"

cat > "$home/b.calls.json" <<EOF
[[2,"tools/call",{"name":"mem_search","arguments":{"query":"e2e3-build"}}],
 [3,"tools/call",{"name":"mem_write","arguments":{"id":"e2e3note","title":"ship","text":"e2e3-ship-note-v2","checksum":"$sum"}}],
 [4,"tools/call",{"name":"mem_write","arguments":{"id":"e2e3note","title":"ship","text":"e2e3-stale","checksum":"deadbeef"}}],
 [5,"tools/call",{"name":"mem_context","arguments":{}}]]
EOF
mcp_session "$home/b"

python3 - "$home/b.json" > "$home/b.txt" <<'PYEOF'
import json, sys
r = json.load(open(sys.argv[1]))
for i in ("2", "3", "4", "5"):
    content = r[i]["content"]
    print(f"--- {i} isError={r[i].get('isError', False)}")
    for b in content:
        print(b.get("text", ""))
PYEOF
grep -q "e2e3-build-widgets" "$home/b.txt" && pass "search hit" || fail "search miss"
grep -q "^updated notes#" "$home/b.txt" && pass "guarded write ok" || fail "guarded write"
if grep -q "CONFLICT" "$home/b.txt" && grep -q "retry:" "$home/b.txt"; then
  pass "conflicting write typed + guided"
else
  fail "conflict response"
fi

# Recall: brief must contain >=95% of seeded markers.
python3 - "$home/b.txt" <<'PYEOF'
import re, sys
brief = open(sys.argv[1]).read()
brief = brief.split("--- 5 ", 1)[1]
markers = ["e2e3-build-widgets", "e2e3-test-widgets", "e2e3-ship-note",
           "list files", "Listing."]
found = sum(1 for m in markers if m in brief)
recall = found / len(markers)
print(f"recall {found}/{len(markers)} = {recall:.0%}")
sys.exit(0 if recall >= 0.95 else 1)
PYEOF
[ $? = 0 ] && pass "context recall >= 95%" || fail "context recall"

echo "e2e3: PASS=$PASS FAIL=$FAIL"
[ "$FAIL" = 0 ]
