#!/usr/bin/env bash
# amem installer: binary -> PREFIX, hook line -> shell rc (with backup).
# Usage:
#   install.sh [--shell bash|zsh|fish|powershell] [--from FILE|URL] [--prefix DIR]
#   install.sh --uninstall [--shell ...] [--prefix DIR]
# Env: AMEM_VERSION (default: latest release), GITHUB_REPO (default: nitishagar/agent-memory)
set -u
REPO="${GITHUB_REPO:-nitishagar/agent-memory}"
PREFIX="$HOME/.local/bin"
SHELL_FLAVOR=""
FROM=""
UNINSTALL=0

while [ $# -gt 0 ]; do
  case "$1" in
    --shell) SHELL_FLAVOR="$2"; shift 2;;
    --from) FROM="$2"; shift 2;;
    --prefix) PREFIX="$2"; shift 2;;
    --uninstall) UNINSTALL=1; shift;;
    -h|--help) sed -n '2,7p' "$0"; exit 0;;
    *) echo "unknown arg: $1" >&2; exit 1;;
  esac
done

detect_shell() {
  case "${SHELL:-}" in
    *zsh) echo zsh;; *fish) echo fish;;
    *) echo bash;;
  esac
}
[ -n "$SHELL_FLAVOR" ] || SHELL_FLAVOR="$(detect_shell)"
case "$SHELL_FLAVOR" in
  bash) RC="$HOME/.bashrc";;
  zsh) RC="$HOME/.zshrc";;
  fish) RC="$HOME/.config/fish/config.fish";;
  powershell) RC="";;
  *) echo "unsupported shell: $SHELL_FLAVOR" >&2; exit 1;;
esac

MARK_BEGIN="# >>> amem >>>"
MARK_END="# <<< amem <<<"
hook_line() {
  if [ "$SHELL_FLAVOR" = "fish" ]; then
    echo "amem init fish | source"
  else
    echo 'eval "$(amem init '"$SHELL_FLAVOR"')"'
  fi
}

if [ "$UNINSTALL" = 1 ]; then
  rm -f "$PREFIX/amem"
  if [ -n "$RC" ] && [ -f "$RC" ]; then
    if [ -f "$RC.amem-bak" ]; then
      cp "$RC.amem-bak" "$RC"
      echo "restored $RC from backup"
    else
      # No backup: strip only the marked block.
      awk -v b="$MARK_BEGIN" -v e="$MARK_END" '
        $0 == b { skip=1; next } $0 == e { skip=0; next } !skip { print }
      ' "$RC" > "$RC.tmp" && mv "$RC.tmp" "$RC"
      echo "removed amem block from $RC"
    fi
  fi
  [ "$SHELL_FLAVOR" = "powershell" ] && echo "remove the amem line from your \$PROFILE manually"
  echo "uninstalled (data left in place; see docs/uninstall.html)"
  exit 0
fi

# --- install ---
os="$(uname -s | tr '[:upper:]' '[:lower:]')"
arch="$(uname -m)"
case "$os-$arch" in
  linux-x86_64) target="x86_64-unknown-linux-gnu";;
  darwin-x86_64) target="x86_64-apple-darwin";;
  darwin-arm64) target="aarch64-apple-darwin";;
  linux-aarch64|linux-arm64) target="aarch64-unknown-linux-gnu";;
  *) echo "unsupported platform: $os-$arch" >&2; exit 1;;
esac

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT
if [ -n "$FROM" ] && [ -f "$FROM" ]; then
  cp "$FROM" "$tmpdir/amem"
elif [ -n "$FROM" ]; then
  url="$FROM"
  curl -fsSL -o "$tmpdir/amem" "$url" || { echo "download failed: $url" >&2; exit 1; }
else
  version="${AMEM_VERSION:-latest}"
  if [ "$version" = "latest" ]; then
    url="https://github.com/$REPO/releases/latest/download/amem-$target"
  else
    url="https://github.com/$REPO/releases/download/$version/amem-$target"
  fi
  curl -fsSL -o "$tmpdir/amem" "$url" || { echo "download failed: $url" >&2; exit 1; }
  if curl -fsSL -o "$tmpdir/amem.sha256" "$url.sha256" 2>/dev/null; then
    (cd "$tmpdir" && sha256sum -c amem.sha256 --status) || { echo "checksum mismatch" >&2; exit 1; }
  fi
fi

mkdir -p "$PREFIX"
install -m 0755 "$tmpdir/amem" "$PREFIX/amem"
echo "installed $PREFIX/amem"

if [ "$SHELL_FLAVOR" = "powershell" ]; then
  echo "add this to your \$PROFILE:"
  hook_line
  exit 0
fi
touch "$RC"
if grep -qF "$MARK_BEGIN" "$RC"; then
  echo "hook already present in $RC"
else
  cp "$RC" "$RC.amem-bak"
  {
    echo "$MARK_BEGIN"
    hook_line
    echo "$MARK_END"
  } >> "$RC"
  echo "hooked $RC (backup: $RC.amem-bak)"
fi
echo "restart your shell, then run: amem ingest --once"
