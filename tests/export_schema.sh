#!/usr/bin/env bash
# Export schema check: seed shell + transcript + note, export, validate vault.
# Usage: tests/export_schema.sh <path-to-amem>
# Every frontmatter YAML-parses, every [[link]] resolves, 0 orphans.
set -u
AMEM="${1:?usage: export_schema.sh <amem-binary>}"
AMEM_BIN="$(cd "$(dirname "$AMEM")" && pwd)/$(basename "$AMEM")"
FIXDIR="$(cd "$(dirname "$0")/../crates/amem-core/tests/fixtures/transcripts" && pwd)"

# GNU timeout is absent on macOS; perl alarm+exec is the portable cap.
run_timed() {
  if [ "$(uname -s)" = "Darwin" ]; then
    perl -e 'alarm shift; exec @ARGV' "$@"
  else
    timeout "$@"
  fi
}

home="$(mktemp -d)"; store="$home/store"; fake="$home/fake"; vault="$home/vault"
mkdir -p "$fake/claude/slug1"
cp "$FIXDIR/claude_basic.jsonl" "$fake/claude/slug1/sess.jsonl"

export AMEM_STORE_DIR="$store"
id1=$("$AMEM_BIN" history start --shell bash --cmd "cargo build --release" --session demo)
"$AMEM_BIN" history end --id "$id1" --exit 0 >/dev/null
id2=$("$AMEM_BIN" history start --shell bash --cmd "export TOKEN=hunter2sekrit" --session demo)
"$AMEM_BIN" history end --id "$id2" --exit 0 >/dev/null
"$AMEM_BIN" ingest --once \
  --transcript-root "claude=$fake/claude" \
  --transcript-root "codex=$fake/none" \
  --transcript-root "cursor=$fake/none" \
  --transcript-root "opencode=$fake/none" \
  --transcript-root "pi=$fake/none" \
  --transcript-root "zcode=$fake/none" >/dev/null
printf '%s\n' \
  '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"ex","version":"0"}}}' \
  '{"jsonrpc":"2.0","method":"notifications/initialized"}' \
  '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"mem_write","arguments":{"id":"plan","title":"ship","text":"cut the release"}}}' \
  | run_timed 20 "$AMEM_BIN" mcp >/dev/null
out=$("$AMEM_BIN" export --vault "$vault")
echo "export_schema: $out"

python3 - "$vault" <<'PYEOF'
import json, re, sys
from pathlib import Path
vault = Path(sys.argv[1])
try:
    import yaml
    have_yaml = True
except ImportError:
    have_yaml = False
errors = []
files = sorted(vault.glob("*.md"))
assert files, "no markdown files exported"
stems = {p.stem for p in files}
links_from: dict[str, set[str]] = {}
RECORD_KEYS = {"id", "stream", "actor", "hlc", "kind", "checksum", "redacted"}
for p in files:
    text = p.read_text()
    m = re.match(r"---\n(.*?)\n---\n", text, re.S)
    if p.stem in ("day-index", "quarantine-index") or re.fullmatch(r"\d{4}-\d{2}-\d{2}", p.stem) or p.stem.startswith("session-"):
        if m:
            errors.append(f"{p.name}: index page must not have record frontmatter")
    else:
        if not m:
            errors.append(f"{p.name}: missing frontmatter")
            continue
        body = m.group(1)
        if have_yaml:
            try:
                fm = yaml.safe_load(body)
            except Exception as e:
                errors.append(f"{p.name}: YAML error {e}")
                continue
            missing = RECORD_KEYS - set(fm)
            if missing:
                errors.append(f"{p.name}: frontmatter missing {sorted(missing)}")
        else:
            keys = set()
            for line in body.splitlines():
                mm = re.fullmatch(r"([A-Za-z_]+): (.*)", line)
                if not mm:
                    errors.append(f"{p.name}: bad frontmatter line {line!r}")
                    continue
                keys.add(mm.group(1))
                v = mm.group(2)
                if v not in ("true", "false", "null") and not re.fullmatch(r"-?\d+", v):
                    try:
                        json.loads(v)
                    except Exception:
                        errors.append(f"{p.name}: bad scalar {line!r}")
            if not RECORD_KEYS <= keys:
                errors.append(f"{p.name}: frontmatter missing {sorted(RECORD_KEYS - keys)}")
    targets = set()
    # Structural links only: the nav line and index bullets. Record bodies
    # may quote [[text]] that is content, not a link (valid in Obsidian).
    for line in text.splitlines():
        if "[[day-index]]" in line or line.startswith("- [["):
            for link in re.findall(r"\[\[([^\]]+)\]\]", line):
                targets.add(link.split("|")[0])
    links_from[p.stem] = targets
    for t in targets:
        if t not in stems:
            errors.append(f"{p.stem}: dangling link [[{t}]]")
# Index pages exist and every record is linked from its day + session pages.
assert "day-index" in stems, "day-index.md missing"
day_pages = [s for s in stems if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s)]
assert day_pages, "no day pages"
sess_pages = [s for s in stems if s.startswith("session-")]
assert sess_pages, "no session pages"
records = stems - {"day-index", "quarantine-index"} - set(day_pages) - set(sess_pages)
assert records, "no record files"
linked = set()
for s in day_pages + sess_pages:
    linked |= links_from.get(s, set())
for r in records:
    if r not in linked:
        errors.append(f"{r}: orphan (no index links it)")
    for need in ("day-index",):
        if need not in links_from.get(r, set()):
            errors.append(f"{r}: missing [[{need}]] link")
    if not [t for t in links_from.get(r, set()) if t.startswith("session-")]:
        errors.append(f"{r}: missing [[session-...]] link")
# Capture-time redaction survived into the vault.
joined = "\n".join(p.read_text() for p in files)
if "hunter2sekrit" in joined:
    errors.append("raw secret leaked into vault")
if errors:
    print("export_schema FAIL:")
    for e in errors:
        print(f"  - {e}")
    sys.exit(1)
print(f"export_schema: PASS ({len(files)} files, {len(records)} records, 0 orphans)")
PYEOF
