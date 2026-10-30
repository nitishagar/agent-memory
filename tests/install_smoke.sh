#!/usr/bin/env bash
# Install smoke: fresh HOME — install -> record -> query -> uninstall,
# zero rc residue, backup restored.
# Usage: tests/install_smoke.sh <amem-binary>
set -u
AMEM="${1:?usage: install_smoke.sh <amem-binary>}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PASS=0; FAIL=0
pass() { PASS=$((PASS+1)); echo "smoke [PASS] $1"; }
fail() { FAIL=$((FAIL+1)); echo "smoke [FAIL] $1"; }

home="$(mktemp -d)"
export HOME="$home"
export SHELL=/bin/bash
mkdir -p "$home"
echo "# my rc" > "$home/.bashrc"

bash "$ROOT/installer/install.sh" --shell bash --from "$AMEM" --prefix "$home/.local/bin" >/dev/null 2>&1
[ -x "$home/.local/bin/amem" ] && pass "binary installed" || fail "binary missing"
grep -q 'amem init bash' "$home/.bashrc" && pass "hook added" || fail "hook missing"
[ -f "$home/.bashrc.amem-bak" ] && pass "rc backed up" || fail "no backup"

export PATH="$home/.local/bin:$PATH"
export AMEM_STORE_DIR="$home/store"
# record one command through the real hook path
out="$(bash -c 'eval "$(amem init bash)"; true' 2>&1)"
id_out="$(AMEM_STORE_DIR="$home/store" amem history start --shell bash --cmd "smoke-deploy-cmd" --session s1)"
AMEM_STORE_DIR="$home/store" amem history end --id "$id_out" --exit 0 >/dev/null
AMEM_STORE_DIR="$home/store" amem ingest --once --no-transcripts >/dev/null 2>&1
AMEM_STORE_DIR="$home/store" amem search "smoke-deploy-cmd" 2>/dev/null | grep -q "smoke-deploy-cmd" \
  && pass "record -> query round-trip" || fail "round-trip"

bash "$ROOT/installer/install.sh" --uninstall --shell bash --prefix "$home/.local/bin" >/dev/null 2>&1
[ ! -e "$home/.local/bin/amem" ] && pass "binary removed" || fail "binary remains"
cmp -s "$home/.bashrc" "$home/.bashrc.amem-bak" && pass "rc restored from backup" || fail "rc residue"
! grep -q "amem" "$home/.bashrc" && pass "zero rc residue" || fail "rc mentions amem"

echo "smoke: PASS=$PASS FAIL=$FAIL"
[ "$FAIL" = 0 ]
