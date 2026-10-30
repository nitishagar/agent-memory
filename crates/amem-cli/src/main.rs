mod doctor;
mod history;
mod hooks;
mod paths;
mod sync_stub;

use std::path::PathBuf;

use amem_core::error::{Error, Result};
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "amem",
    version,
    about = "Local-first coding-agent memory recorder"
)]
struct Cli {
    /// Override the store directory (default: OS data dir + agent-memory).
    #[arg(long, global = true)]
    store_dir: Option<PathBuf>,
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Print the shell integration snippet for eval in your rc file.
    Init {
        /// Shell flavour: bash, zsh, fish, powershell.
        shell: String,
    },
    /// Capture subcommands (called by shell hooks; not for interactive use).
    History(HistoryArgs),
    /// Run one ingest pass: spool files → redacted records → store.
    Ingest {
        /// Process pending spool files once and exit.
        #[arg(long)]
        once: bool,
        /// TEST ONLY: abort after N commits (unclean death for recovery tests).
        #[arg(long, hide = true)]
        crash_after: Option<u64>,
    },
    /// Report store/hook/transcript health. Read-only, always exit 0.
    Doctor,
    /// Probe a sync endpoint (Phase 1: validation stub, no transport yet).
    Sync {
        /// Sync server endpoint URL.
        #[arg(long)]
        endpoint: String,
    },
    /// Print a random id (hook sessions, test fixtures).
    #[command(hide = true)]
    Uuid,
}

#[derive(Parser)]
struct HistoryArgs {
    #[command(subcommand)]
    op: HistoryOp,
}

#[derive(Subcommand)]
enum HistoryOp {
    /// Record a command start; prints the pairing id.
    Start {
        /// Shell flavour.
        #[arg(long)]
        shell: String,
        /// Command text (or pass --cmd-from-env / --cmd-from-stdin).
        #[arg(long)]
        cmd: Option<String>,
        /// Read command text from this env var (quoting-safe).
        #[arg(long)]
        cmd_from_env: Option<String>,
        /// Read command text from stdin (unbounded; argv/env hit `MAX_ARG_STRLEN`).
        #[arg(long)]
        cmd_from_stdin: bool,
        /// Working directory (default: current dir).
        #[arg(long)]
        cwd: Option<PathBuf>,
        /// Session id (default: `$AMEM_SESSION` or "default").
        #[arg(long)]
        session: Option<String>,
    },
    /// Record a command end paired by id.
    End {
        /// Pairing id from `history start`.
        #[arg(long)]
        id: String,
        /// Exit code.
        #[arg(long)]
        exit: i64,
        /// Duration in nanoseconds (optional).
        #[arg(long)]
        duration_ns: Option<i64>,
    },
    /// Record a completed command (start+end together).
    Both {
        /// Shell flavour.
        #[arg(long)]
        shell: String,
        /// Exit code.
        #[arg(long)]
        exit: i64,
        /// Duration in nanoseconds (optional).
        #[arg(long)]
        duration_ns: Option<i64>,
        /// Env var holding the command text.
        #[arg(long, default_value = "AMEM_COMMAND_LINE")]
        cmd_from_env: String,
        /// Read command text from stdin instead (unbounded).
        #[arg(long)]
        cmd_from_stdin: bool,
    },
}

fn actor() -> String {
    let user = std::env::var("USER")
        .or_else(|_| std::env::var("USERNAME"))
        .unwrap_or_else(|_| "unknown".to_string());
    let host = hostname::get()
        .ok()
        .and_then(|h| h.into_string().ok())
        .unwrap_or_else(|| "unknown-host".to_string());
    format!("{host}:{user}")
}

#[derive(serde::Deserialize, Default)]
struct RedactConfig {
    #[serde(default)]
    allow: Vec<String>,
    #[serde(default)]
    extra: Vec<String>,
}

/// Maximum `redact.json` size and custom-pattern count: the file is
/// store-writable, so a hostile config must not wedge ingest.
const MAX_REDACT_JSON_BYTES: u64 = 1024 * 1024;
const MAX_REDACT_EXTRA: usize = 1024;

fn redactor(store: &std::path::Path) -> Result<amem_core::redact::Redactor> {
    let path = store.join("redact.json");
    let cfg: RedactConfig = match std::fs::metadata(&path) {
        Ok(m) if m.len() > MAX_REDACT_JSON_BYTES => {
            eprintln!("amem: warn: {} oversize, using defaults", path.display());
            RedactConfig::default()
        }
        Ok(_) => match std::fs::read_to_string(&path) {
            Ok(text) => serde_json::from_str(&text)
                .map_err(|e| Error::InvalidArguments(format!("bad {}: {e}", path.display())))?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => RedactConfig::default(),
            Err(e) => return Err(Error::Io(e)),
        },
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => RedactConfig::default(),
        Err(e) => return Err(Error::Io(e)),
    };
    let mut r = amem_core::redact::Redactor::new().with_allow(cfg.allow);
    for pat in cfg.extra.iter().take(MAX_REDACT_EXTRA) {
        if let Err(e) = r.try_add_extra(pat) {
            eprintln!("amem: warn: skipping redaction pattern: {e}");
        }
    }
    if cfg.extra.len() > MAX_REDACT_EXTRA {
        eprintln!(
            "amem: warn: {} extra patterns over cap {MAX_REDACT_EXTRA}, rest ignored",
            cfg.extra.len()
        );
    }
    Ok(r)
}

fn run() -> Result<()> {
    // `serde` derive lives in amem-core; the CLI needs it for RedactConfig.
    let cli = Cli::parse();
    let store = paths::store_dir(cli.store_dir)?;
    match cli.command {
        Some(Commands::Init { shell }) => {
            print!("{}", hooks::snippet(&shell)?);
            Ok(())
        }
        Some(Commands::History(args)) => {
            let spool = paths::spool_dir(&store);
            match args.op {
                HistoryOp::Start {
                    shell,
                    cmd,
                    cmd_from_env,
                    cmd_from_stdin,
                    cwd,
                    session,
                } => history::start(
                    &spool,
                    &shell,
                    cmd,
                    cmd_from_env,
                    cmd_from_stdin,
                    cwd,
                    session,
                ),
                HistoryOp::End {
                    id,
                    exit,
                    duration_ns,
                } => history::end(&spool, &id, exit, duration_ns),
                HistoryOp::Both {
                    shell,
                    exit,
                    duration_ns,
                    cmd_from_env,
                    cmd_from_stdin,
                } => history::both(
                    &spool,
                    &shell,
                    exit,
                    duration_ns,
                    &cmd_from_env,
                    cmd_from_stdin,
                ),
            }
        }
        Some(Commands::Ingest { once, crash_after }) => {
            if !once {
                return Err(Error::InvalidArguments(
                    "ingest needs --once (daemon mode is not in Phase 1)".to_string(),
                ));
            }
            paths::ensure_writable(&store)?;
            let mut db = amem_core::store::Store::open(&store)?;
            let device = amem_core::store::device_id(&store)?;
            let redactor = redactor(&store)?;
            let stats = amem_core::ingest::ingest_once(
                &mut db,
                &paths::spool_dir(&store),
                &device,
                &actor(),
                &redactor,
                amem_core::ingest::IngestOptions {
                    crash_after: crash_after.unwrap_or(0),
                },
            )?;
            println!("{}", serde_json::to_string(&stats).unwrap_or_default());
            Ok(())
        }
        Some(Commands::Doctor) => {
            let spool_n =
                amem_core::spool::pending_spools(&paths::spool_dir(&store)).map_or(0, |v| v.len());
            // Strictly read-only: never create the store, the DB, or the
            // device id. Absent pieces report as "not initialized".
            let count = std::fs::read_to_string(store.join("device_id"))
                .ok()
                .map(|d| d.trim().to_string())
                .filter(|d| !d.is_empty())
                .and_then(|d| {
                    amem_core::store::Store::open_readonly(&store)
                        .ok()
                        .and_then(|db| db.count_stream(&d, "shell").ok())
                });
            print!("{}", doctor::report(&store, spool_n, count));
            Ok(())
        }
        Some(Commands::Sync { endpoint }) => {
            println!("{}", sync_stub::check(&endpoint)?);
            Ok(())
        }
        Some(Commands::Uuid) => {
            println!("{}", uuid::Uuid::new_v4().simple());
            Ok(())
        }
        None => {
            println!("amem {} — run `amem --help`", env!("CARGO_PKG_VERSION"));
            Ok(())
        }
    }
}

fn main() {
    match run() {
        Ok(()) => {}
        Err(e) => {
            eprintln!("amem: error [{}]: {e}", e.code());
            std::process::exit(1);
        }
    }
}
