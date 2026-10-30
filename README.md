# agent-memory

Local-first coding-agent memory recorder: shell commands, git context, and agent
transcripts captured on your machine, queryable from any MCP agent
("what did I do and why"), with optional self-hosted team sync.

**Docs:** https://nitishagar.github.io/agent-memory/ (quickstart, per-agent
MCP snippets, self-host guide, [security model](docs/security.html), uninstall).

Status: MVP complete (Phases 0–5). Apache-2.0. No telemetry, no cloud fallback.

```sh
curl -fsSL https://nitishagar.github.io/agent-memory/install.sh | bash
# or: cargo build --release && ./target/release/amem --help
```

## Record

```sh
eval "$(amem init bash)"   # or zsh, fish, powershell
amem ingest --once        # spool + transcripts → store
amem search "cargo"       # substring recall, newest first (team union after sync)
```

## Agents (MCP over stdio)

Tools: `mem_search`, `mem_read`, `mem_list`, `mem_write`, `mem_delete`,
`mem_context`. Text-default, `format: "json"` on request. Mutations are
checksum-guarded (`mem_read` → `mem_write`/`mem_delete` with `checksum`);
stale guards refuse with typed errors + retry guidance, never silent
last-write-wins. Recorder streams are read-only; only `notes` mutate.

Claude Code (`~/.claude.json`, `mcpServers`):

```json
{ "agent-memory": { "command": "/path/to/amem", "args": ["mcp"] } }
```

## Team sync (self-hosted, TLS-only)

```sh
amem sync-server --dir /srv/amem --init-cert   # once; prints fingerprint
amem sync-server --dir /srv/amem --listen 0.0.0.0:8443 --public --confirm
amem sync --endpoint https://sync.internal:8443 --trust-new
```

Conflicts branch instead of overwriting (`amem branches`); corrupt data
quarantines (`amem quarantine`); wipes refuse until `--recover --confirm`.
Read the [security model](docs/security.html) before exposing the port:
MVP sync has no client auth — network access means full read/write.

## Inspect (Obsidian vault export)

```sh
amem export --vault ~/memory-vault
```

One Markdown file per record (`<date>-<slug>.md`, YAML frontmatter,
`[[day-index]]` / `[[session-<id>]]` links) plus day and session index
pages. Open the directory as an Obsidian vault. Export reads the redacted
store, so capture-time redaction applies.

## Verify

```sh
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo build -p amem-cli
for t in e2e_phase1 e2e_phase2 export_schema e2e_phase3 tls_refusal e2e_phase4 install_smoke docs_smoke; do
  ./tests/$t.sh ./target/debug/amem
done
```
