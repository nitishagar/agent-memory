use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "amem",
    version,
    about = "Local-first coding-agent memory recorder"
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Print shell integration snippet (Phase 1 target).
    Init {
        /// Shell flavour: bash, zsh, fish, powershell.
        shell: String,
    },
}

fn main() {
    let cli = Cli::parse();
    match cli.command {
        Some(Commands::Init { shell }) => {
            println!("# amem: {shell} integration lands in Phase 1");
        }
        None => {
            println!("amem 0.1.0 — run `amem --help`");
        }
    }
}
