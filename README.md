# agent-memory

Local-first coding-agent memory recorder: shell commands, git context, and agent
transcripts captured on your machine, queryable from any MCP agent
("what did I do and why"), with optional self-hosted team sync.

Status: Phase 3 (MCP + export). Apache-2.0. No telemetry, no cloud fallback.

```sh
cargo build --release
./target/release/amem --help
```

## Record

```sh
eval "$(./target/release/amem init bash)"   # or zsh, fish, powershell
./target/release/amem ingest --once        # spool + transcripts → store
./target/release/amem search "cargo"       # substring recall, newest first
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

## Inspect (Obsidian vault export)

```sh
./target/release/amem export --vault ~/memory-vault
```

One Markdown file per record (`<date>-<slug>.md`, YAML frontmatter,
`[[day-index]]` / `[[session-<id>]]` links) plus day and session index
pages. Open the directory as an Obsidian vault. Export reads the redacted
store, so capture-time redaction applies.
