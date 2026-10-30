#!/usr/bin/env bash
# Docs smoke: every internal link resolves, demo.gif is a valid GIF,
# the generator reproduces it (same dimensions + frame count).
# Usage: tests/docs_smoke.sh <amem-binary>
set -u
AMEM="${1:?usage: docs_smoke.sh <amem-binary>}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PASS=0; FAIL=0
pass() { PASS=$((PASS+1)); echo "docs [PASS] $1"; }
fail() { FAIL=$((FAIL+1)); echo "docs [FAIL] $1"; }

# 0. served installer matches the canonical one
if cmp -s "$ROOT/installer/install.sh" "$ROOT/docs/install.sh"; then
  pass "docs/install.sh in sync"
else
  fail "docs/install.sh drifted from installer/install.sh"
fi

# 1. internal links resolve
missing=0
for page in "$ROOT"/docs/*.html; do
  while IFS= read -r href; do
    case "$href" in
      http*|https*|mailto*|'#'*|"") continue;;
    esac
    target="$ROOT/docs/$href"
    if [ ! -e "$target" ]; then
      echo "docs: broken link $href in $(basename "$page")"
      missing=1
    fi
  done < <(grep -oE '(href|src)="[^"]+"' "$page" | sed -E 's/^(href|src)="([^"]+)"$/\2/' | cut -d'#' -f1 | sort -u)
done
[ "$missing" = 0 ] && pass "all internal links resolve" || fail "broken links"

# 2. demo.gif is a valid GIF
gif="$ROOT/docs/assets/demo.gif"
if [ -f "$gif" ] && [ "$(head -c 6 "$gif")" = "GIF89a" ]; then
  pass "demo.gif present and valid"
else
  fail "demo.gif missing or invalid"
fi

# 3. generator reproduces (same geometry + frame count)
info() {
  python3 - "$1" <<'PYEOF'
import struct, sys
d = open(sys.argv[1], 'rb').read()
w, h = struct.unpack('<HH', d[6:10])
print(f"{w}x{h} frames={d.count(b'\x21\xf9\x04')}")
PYEOF
}
before="$(info "$gif")"
cp "$gif" /tmp/demo-before.gif
python3 "$ROOT/docs/demo/gen_gif.py" "$AMEM" >/dev/null 2>&1
after="$(info "$gif")"
[ "$before" = "$after" ] && pass "generator reproduces ($after)" || fail "gif drift: $before vs $after"

echo "docs: PASS=$PASS FAIL=$FAIL"
[ "$FAIL" = 0 ]
