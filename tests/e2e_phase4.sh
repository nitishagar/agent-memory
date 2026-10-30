#!/usr/bin/env bash
# Phase 4 end-to-end: two stores + TLS server — divergent histories,
# conflicting note, kill -9 resume, wipe+recover, quarantine round-trip,
# byte-equal unions, 10k fresh-sync timing.
# Usage: tests/e2e_phase4.sh <path-to-amem>
set -u
AMEM="${1:?usage: e2e_phase4.sh <amem-binary>}"
AMEM_BIN="$(cd "$(dirname "$AMEM")" && pwd)/$(basename "$AMEM")"
PASS=0; FAIL=0
note() { echo "e2e4 [$1] $2"; }
pass() { PASS=$((PASS+1)); note PASS "$1"; }
fail() { FAIL=$((FAIL+1)); note FAIL "$1"; }

home="$(mktemp -d)"; srv="$home/srv"; sta="$home/sta"; stb="$home/stb"
"$AMEM_BIN" sync-server --dir "$srv" --init-cert >/dev/null
port=$(python3 -c "import socket; s=socket.socket(); s.bind(('127.0.0.1',0)); print(s.getsockname()[1])")
EP="https://127.0.0.1:$port/"
start_srv() {
  "$AMEM_BIN" sync-server --dir "$srv" --listen "127.0.0.1:$port" >/dev/null 2>&1 &
  srvpid=$!
  sleep 1
}
start_srv
trap 'kill $srvpid 2>/dev/null; wait 2>/dev/null' EXIT

seed_cmd() { # <store> <cmd> <session>
  local id
  id=$(AMEM_STORE_DIR="$1" "$AMEM_BIN" history start --shell bash --cmd "$2" --session "$3")
  AMEM_STORE_DIR="$1" "$AMEM_BIN" history end --id "$id" --exit 0 >/dev/null
}
seed_cmd "$sta" "e2e4-alpha-build" "w1"
seed_cmd "$sta" "e2e4-alpha-test" "w1"
seed_cmd "$stb" "e2e4-beta-deploy" "w2"
write_note() { # <store> <id> <text>
  printf '%s\n' \
    '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"e","version":"0"}}}' \
    '{"jsonrpc":"2.0","method":"notifications/initialized"}' \
    "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/call\",\"params\":{\"name\":\"mem_write\",\"arguments\":{\"id\":\"$2\",\"title\":\"t\",\"text\":\"$3\"}}}" \
    | AMEM_STORE_DIR="$1" timeout 20 "$AMEM_BIN" mcp >/dev/null
}
AMEM_STORE_DIR="$sta" "$AMEM_BIN" ingest --once --no-transcripts >/dev/null
AMEM_STORE_DIR="$stb" "$AMEM_BIN" ingest --once --no-transcripts >/dev/null
# Notes need memory.db (MCP opens the store), so they come after ingest.
write_note "$sta" "deploy" "ALPHA says freeze"
write_note "$stb" "deploy" "BETA says ship"

dump_union() { # <store> — sorted records dump across all devices
  python3 - "$1" <<'PYEOF'
import sqlite3, sys
con = sqlite3.connect(f"{sys.argv[1]}/memory.db")
rows = con.execute("SELECT device, stream, idx, hash, payload FROM records ORDER BY device, stream, idx").fetchall()
for r in rows:
    print("|".join(str(c) for c in r))
PYEOF
}

out=$(AMEM_STORE_DIR="$sta" "$AMEM_BIN" sync --endpoint "$EP" --trust-new 2>/dev/null)
echo "$out" | grep -q '"uploaded":3' && pass "A uploads 3" || fail "A sync: $out"
out=$(AMEM_STORE_DIR="$stb" "$AMEM_BIN" sync --endpoint "$EP" --trust-new 2>/dev/null)
echo "$out" | grep -q '"uploaded":2' && pass "B uploads 2" || fail "B sync: $out"
echo "$out" | grep -q '"branches":1' && pass "conflicting note branched" || fail "branch: $out"
out=$(AMEM_STORE_DIR="$sta" "$AMEM_BIN" sync --endpoint "$EP" 2>/dev/null)
echo "$out" | grep -q '"downloaded":2' && pass "A downloads 2" || fail "A sync2: $out"

dump_union "$sta" > "$home/ua.txt"
dump_union "$stb" > "$home/ub.txt"
if cmp -s "$home/ua.txt" "$home/ub.txt" && [ "$(wc -l < "$home/ua.txt")" = "5" ]; then
  pass "byte-equal 5-record unions"
else
  fail "union mismatch"; diff "$home/ua.txt" "$home/ub.txt" | head -5
fi
AMEM_STORE_DIR="$stb" "$AMEM_BIN" branches | grep -q '"reason":"id"' && pass "branch flagged reason=id" || fail "branches"
AMEM_STORE_DIR="$stb" "$AMEM_BIN" search "ALPHA says freeze" | grep -q "ALPHA" && pass "both notes survive" || fail "note loss"

# kill -9 mid-batch via a 10k spool flood, then resume.
python3 - "$sta/spool" <<'PYEOF'
import json, sys, time
spool = sys.argv[1]
now = time.time_ns()
for i in range(10_000):
    sid = f"{i:08x}" + "0" * 24
    start = {"v": 1, "op": "start", "id": sid, "shell": "bash",
             "cmd": f"flood-cmd-{i}", "cwd": "/tmp", "ts_ns": now + i, "session": "flood"}
    end = {"v": 1, "op": "end", "id": sid, "exit": 0, "dur_ns": 1, "ts_ns": now + i + 1}
    open(f"{spool}/spool-a-{i:05d}.jsonl", "w").write(json.dumps(start) + "\n")
    open(f"{spool}/spool-b-{i:05d}.jsonl", "w").write(json.dumps(end) + "\n")
PYEOF
AMEM_STORE_DIR="$sta" "$AMEM_BIN" ingest --once --no-transcripts 2>/dev/null | grep -q '"ingested":10000' \
  && pass "10k spool ingested" || fail "flood ingest"
t0=$(date +%s)
AMEM_STORE_DIR="$sta" "$AMEM_BIN" sync --endpoint "$EP" >/dev/null 2>&1 &
syncpid=$!
sleep 1
kill -9 $syncpid 2>/dev/null || true
wait $syncpid 2>/dev/null || true
out=$(AMEM_STORE_DIR="$sta" "$AMEM_BIN" sync --endpoint "$EP" 2>/dev/null)
t1=$(date +%s)
[ $((t1 - t0)) -le 120 ] && pass "10k fresh sync ≤120s ($((t1-t0))s)" || fail "10k too slow ($((t1-t0))s)"
dump_union "$sta" > "$home/ua2.txt"
[ "$(wc -l < "$home/ua2.txt")" = "10005" ] && pass "no dupes after kill -9 resume" || fail "dupe count: $(wc -l < "$home/ua2.txt")"
t0=$(date +%s)
out=$(AMEM_STORE_DIR="$stb" "$AMEM_BIN" sync --endpoint "$EP" 2>/dev/null)
t1=$(date +%s)
echo "$out" | grep -q '"downloaded":10000' && pass "B pulls 10k" || fail "B pull: $out"
[ $((t1 - t0)) -le 120 ] && pass "10k download ≤120s ($((t1-t0))s)" || fail "slow pull ($((t1-t0))s)"
# Delta sync after one new record.
seed_cmd "$stb" "e2e4-beta-one-more" "w2"
AMEM_STORE_DIR="$stb" "$AMEM_BIN" ingest --once --no-transcripts >/dev/null
t0=$(date +%s)
AMEM_STORE_DIR="$stb" "$AMEM_BIN" sync --endpoint "$EP" >/dev/null 2>&1
AMEM_STORE_DIR="$sta" "$AMEM_BIN" sync --endpoint "$EP" >/dev/null 2>&1
t1=$(date +%s)
[ $((t1 - t0)) -le 30 ] && pass "delta sync ≤30s ($((t1-t0))s)" || fail "slow delta ($((t1-t0))s)"
dump_union "$sta" > "$home/ua3.txt"
dump_union "$stb" > "$home/ub3.txt"
cmp -s "$home/ua3.txt" "$home/ub3.txt" && pass "byte-equal unions at scale" || fail "scale union mismatch"

# Server wipe + recover.
kill $srvpid 2>/dev/null; wait 2>/dev/null
rm -f "$srv/server.db" "$srv/server.db-wal" "$srv/server.db-shm"
start_srv
trap 'kill $srvpid 2>/dev/null; wait 2>/dev/null' EXIT
if AMEM_STORE_DIR="$sta" "$AMEM_BIN" sync --endpoint "$EP" 2>&1 | grep -q "divergence"; then
  pass "wipe refuses with divergence"
else
  fail "wipe not detected"
fi
out=$(AMEM_STORE_DIR="$sta" "$AMEM_BIN" sync --endpoint "$EP" --recover --confirm 2>/dev/null)
echo "$out" | grep -q '"uploaded":10003' && pass "recover re-pushes A" || fail "recover A: $out"
if AMEM_STORE_DIR="$stb" "$AMEM_BIN" sync --endpoint "$EP" 2>&1 | grep -q "divergence"; then
  pass "B still refuses until its recover"
else
  fail "B premature merge"
fi
out=$(AMEM_STORE_DIR="$stb" "$AMEM_BIN" sync --endpoint "$EP" --recover --confirm 2>/dev/null)
echo "$out" | grep -q '"uploaded":3' && pass "recover re-pushes B" || fail "recover B: $out"
AMEM_STORE_DIR="$sta" "$AMEM_BIN" sync --endpoint "$EP" >/dev/null 2>&1
dump_union "$sta" > "$home/ua4.txt"
dump_union "$stb" > "$home/ub4.txt"
cmp -s "$home/ua4.txt" "$home/ub4.txt" && pass "post-recover unions byte-equal" || fail "recover union"

# Quarantine round-trip + export (triple resolved via sqlite).
triple=$(python3 - "$sta" <<'PYEOF'
import sqlite3, sys
con = sqlite3.connect(f"{sys.argv[1]}/memory.db")
d, s, i = con.execute("SELECT device, stream, idx FROM records WHERE payload LIKE '%flood-cmd-0\"%'").fetchone()
print(f"{d}/{s}/{i}")
PYEOF
)
AMEM_STORE_DIR="$sta" "$AMEM_BIN" quarantine "$triple" --reason e2e4 >/dev/null
AMEM_STORE_DIR="$sta" "$AMEM_BIN" search "flood-cmd-0" | grep -q "flood" && fail "quarantined recalled" || pass "quarantine excluded from recall"
AMEM_STORE_DIR="$sta" "$AMEM_BIN" export --vault "$home/vault" | grep -q '"quarantined":1' && pass "quarantine exported flagged" || fail "quarantine export"
[ -f "$home/vault/quarantine-index.md" ] && pass "quarantine-index written" || fail "quarantine index"
AMEM_STORE_DIR="$sta" "$AMEM_BIN" unquarantine "$triple" | grep -q '"restored"' && pass "unquarantine restores" || fail "unquarantine"
AMEM_STORE_DIR="$sta" "$AMEM_BIN" search "flood-cmd-0" | grep -q "flood" && pass "restored record recalled" || fail "restore recall"

echo "e2e4: PASS=$PASS FAIL=$FAIL"
[ "$FAIL" = 0 ]
