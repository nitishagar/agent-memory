#!/usr/bin/env bash
# TLS refusal + TOFU pinning: init-cert, plaintext refusal, TLS-only client,
# unknown-server guidance, pin/mismatch behavior.
# Usage: tests/tls_refusal.sh <path-to-amem>
set -u
AMEM="${1:?usage: tls_refusal.sh <amem-binary>}"
AMEM_BIN="$(cd "$(dirname "$AMEM")" && pwd)/$(basename "$AMEM")"
PASS=0; FAIL=0
note() { echo "tls [$1] $2"; }
pass() { PASS=$((PASS+1)); note PASS "$1"; }
fail() { FAIL=$((FAIL+1)); note FAIL "$1"; }

home="$(mktemp -d)"; srv="$home/srv"; store="$home/store"
export AMEM_STORE_DIR="$store"

fp=$(AMEM_STORE_DIR="$home/empty" "$AMEM_BIN" sync-server --dir "$srv" --init-cert)
echo "$fp" | grep -q '"fingerprint":"[0-9a-f]\{64\}"' && pass "init-cert prints blake3 fingerprint" || fail "init-cert output: $fp"
[ -f "$srv/cert.pem" ] && [ -f "$srv/key.pem" ] && pass "cert+key written" || fail "cert files"
[ "$(stat -c %a "$srv/key.pem")" = "600" ] && pass "key.pem 0600" || fail "key perms"

port=$(python3 -c "import socket; s=socket.socket(); s.bind(('127.0.0.1',0)); print(s.getsockname()[1])")
"$AMEM_BIN" sync-server --dir "$srv" --listen "127.0.0.1:$port" >/dev/null 2>&1 &
srvpid=$!
trap 'kill $srvpid 2>/dev/null; wait 2>/dev/null' EXIT
sleep 1

# Plaintext HTTP to the TLS port: no HTTP reply.
python3 - "$port" <<'PYEOF'
import socket, sys
s = socket.create_connection(("127.0.0.1", int(sys.argv[1])), timeout=5)
s.sendall(b"GET /v1/status HTTP/1.1\r\nHost: x\r\n\r\n")
s.settimeout(3)
try:
    data = s.recv(64)
except Exception:
    data = b""
assert not data.startswith(b"HTTP/"), data[:32]
print("plaintext refused")
PYEOF
[ $? = 0 ] && pass "plaintext gets no HTTP reply" || fail "plaintext reply"

# http:// endpoint: refused before any network attempt.
if AMEM_STORE_DIR="$store" "$AMEM_BIN" sync --endpoint "http://127.0.0.1:$port/" 2>&1 | grep -q "TLS-only"; then
  pass "http endpoint refused (TLS-only)"
else
  fail "http endpoint"
fi

# Unknown server without --trust-new: guided, no network.
if AMEM_STORE_DIR="$store" "$AMEM_BIN" sync --endpoint "https://127.0.0.1:$port/" 2>&1 | grep -q "unknown sync server"; then
  pass "unknown server guided to --trust-new"
else
  fail "unknown server"
fi

# Seed one record so the TOFU sync has content.
mkdir -p "$store/spool"
id1=$(AMEM_STORE_DIR="$store" "$AMEM_BIN" history start --shell bash --cmd "tls-seed-cmd" --session t)
AMEM_STORE_DIR="$store" "$AMEM_BIN" history end --id "$id1" --exit 0 >/dev/null
AMEM_STORE_DIR="$store" "$AMEM_BIN" ingest --once --no-transcripts >/dev/null

# TOFU sync: pins (fingerprint on stderr) and uploads.
out=$(AMEM_STORE_DIR="$store" "$AMEM_BIN" sync --endpoint "https://127.0.0.1:$port/" --trust-new 2>"$home/stderr.txt")
echo "$out" | grep -q '"uploaded":1' && pass "TOFU sync uploads" || fail "TOFU sync: $out"
grep -q "TOFU: pinning" "$home/stderr.txt" && pass "TOFU pin announced" || fail "TOFU announce"
[ -f "$store/sync_pins.json" ] && pass "pin persisted" || fail "pin file"

# Pinned reconnect without --trust-new.
out=$(AMEM_STORE_DIR="$store" "$AMEM_BIN" sync --endpoint "https://127.0.0.1:$port/" 2>/dev/null)
echo "$out" | grep -q '"uploaded":0' && pass "pinned reconnect quiet" || fail "reconnect: $out"

# Tampered pin fails closed.
cp "$store/sync_pins.json" "$home/pins.bak"
python3 - "$store/sync_pins.json" <<'PYEOF'
import json, sys
p = sys.argv[1]
pins = json.load(open(p))
k = next(iter(pins))
pins[k] = pins[k].replace("A", "B", 1)
json.dump(pins, open(p, "w"))
PYEOF
if AMEM_STORE_DIR="$store" "$AMEM_BIN" sync --endpoint "https://127.0.0.1:$port/" 2>&1 | grep -qi "mismatch\|invalid\|pin"; then
  pass "tampered pin fails closed"
else
  fail "tampered pin"
fi

echo "tls: PASS=$PASS FAIL=$FAIL"
[ "$FAIL" = 0 ]
