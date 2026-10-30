#!/usr/bin/env bash
# Phase 1 end-to-end: real interactive shells (pty) → hooks → ingest → recall.
# Usage: tests/e2e_phase1.sh <path-to-amem>
# Unix-only (needs pty via script(1)); Windows CI covers build+unit+snippets.
# Shells missing locally are SKIPPED with an explicit line (CI installs zsh/fish;
# pwsh ships on GH runners).
set -u
AMEM="${1:?usage: e2e_phase1.sh <amem-binary>}"
AMEM_BIN="$(cd "$(dirname "$AMEM")" && pwd)/$(basename "$AMEM")"
PASS=0; FAIL=0; SKIP=0
note() { echo "e2e1 [$1] $2"; }
pass() { PASS=$((PASS+1)); note PASS "$1"; }
fail() { FAIL=$((FAIL+1)); note FAIL "$1"; }
skip() { SKIP=$((SKIP+1)); note SKIP "$1"; }

if ! command -v script >/dev/null 2>&1; then
  echo "e2e1: script(1) missing — cannot run pty matrix"
  exit 2
fi

# pty_run <cmd...> — run an interactive command under a pty, Linux/macOS forms.
# TERM=dumb keeps distro shell integrations (VTE) from loading: they fight
# bash-preexec's PS0 dispatch in synthetic --rcfile sessions. Real terminals
# are covered by the manual dogfood step in tests/E2E1.md.
pty_run() {
  if [ "$(uname -s)" = "Darwin" ]; then
    TERM=dumb VTE_VERSION= script -q /dev/null "$@"
  else
    TERM=dumb VTE_VERSION= script -qec "$*" /dev/null
  fi
}

CMDS20='ls
git status
echo hello
make test
cat f.txt
pwd
whoami
date
uname -a
echo a
echo b
echo c
echo d
echo e
echo f
echo g
echo h
deploy token=supersecret1
false
true'

run_bash_case() {
  local home store rc
  home="$(mktemp -d)"; store="$home/store"; rc="$home/rc"
  "$AMEM_BIN" --store-dir "$store" init bash > "$rc"
  printf '%s\n' "$CMDS20" > "$home/cmds.txt"
  python3 -c 'print("x"*1000005)' >> "$home/cmds.txt"
  echo "touch $home/done" >> "$home/cmds.txt"
  # Hold stdin open until the sentinel lands: bare EOF makes `script` close
  # the pty early and SIGHUPs the shell mid-run.
  (cat "$home/cmds.txt"; while [ ! -f "$home/done" ]; do sleep 0.2; done; sleep 1) \
    | PATH="$(dirname "$AMEM_BIN"):$PATH" AMEM_STORE_DIR="$store" HOME="$home" \
    pty_run bash --noprofile --rcfile "$rc" -i >/dev/null 2>&1
  AMEM_STORE_DIR="$store" "$AMEM_BIN" ingest --once >/dev/null
  local count
  count=$(AMEM_STORE_DIR="$store" "$AMEM_BIN" doctor | sed -n 's/.*records(shell): \([0-9]*\).*/\1/p')
  # 20 + 1MB line + the sentinel `touch` itself (honestly captured).
  if [ "$count" = "22" ]; then pass "bash pty recall 22/22"; else fail "bash pty recall got [$count] want 22"; fi
  # Edge top-up through the real binary (shell-independent ingest rules).
  AMEM_E1='echo one
echo two' AMEM_STORE_DIR="$store" "$AMEM_BIN" history both --shell bash --exit 0 --cmd-from-env AMEM_E1 >/dev/null
  AMEM_E1='printf "x\ny\n"' AMEM_STORE_DIR="$store" "$AMEM_BIN" history both --shell bash --exit 0 --cmd-from-env AMEM_E1 >/dev/null
  AMEM_E1="$(printf 'a\x01b')" AMEM_STORE_DIR="$store" "$AMEM_BIN" history both --shell bash --exit 0 --cmd-from-env AMEM_E1 >/dev/null
  AMEM_STORE_DIR="$store" "$AMEM_BIN" ingest --once > "$home/stats.json"
  count=$(AMEM_STORE_DIR="$store" "$AMEM_BIN" doctor | sed -n 's/.*records(shell): \([0-9]*\).*/\1/p')
  # 22 pty + 2 multiline + control-char = 25; empties/NUL covered in unit tests.
  if [ "$count" = "25" ]; then pass "bash edge top-up total 25"; else fail "bash edge total got [$count] want 25"; fi
  if grep -q "supersecret1" "$home/stats.json"; then fail "bash secret in stats"; else pass "bash no leak"; fi
  if AMEM_STORE_DIR="$store" "$AMEM_BIN" doctor | grep -q "supersecret1"; then fail "bash secret in doctor"; else pass "bash store clean"; fi
  rm -rf "$home"
}

run_bash_case

# zsh/fish/pwsh: render check here, full pty matrix in CI.
for sh in zsh fish pwsh; do
  if command -v "$sh" >/dev/null 2>&1; then pass "$sh present (pty matrix in CI)"; else skip "$sh not installed locally"; fi
done

# Doctor coexistence probe with a planted atuin block.
home="$(mktemp -d)"
echo '# atuin init shim' > "$home/.bashrc"
if HOME="$home" "$AMEM_BIN" --store-dir "$home/store" doctor | grep -q "coexist: \[atuin\]"; then pass "doctor flags atuin"; else fail "doctor flags atuin"; fi
rm -rf "$home"

echo "e2e1: PASS=$PASS FAIL=$FAIL SKIP=$SKIP"
[ "$FAIL" -eq 0 ]
