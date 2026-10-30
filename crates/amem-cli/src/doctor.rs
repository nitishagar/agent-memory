//! `amem doctor`: coexistence, capability, and store health report.
//!
//! Read-only except the store writability probe (temp file round-trip).
//! Exit 0 with warnings — doctor diagnoses, it never fails the shell.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};

/// Markers of other preexec/prompt integrations sharing our hook points.
const FOREIGN_HOOK_MARKERS: &[(&str, &str)] = &[
    ("atuin", "atuin"),
    ("oh-my-zsh", "oh-my-zsh"),
    ("prezto", "prezto"),
    ("bash-preexec(other)", "bash-preexec"),
];

/// rc files consulted per shell (Linux-first; macOS/Windows differ only in
/// the PowerShell profile path, handled below).
fn rc_files() -> Vec<(&'static str, PathBuf)> {
    let home = dirs::home_dir().unwrap_or_else(|| PathBuf::from("~"));
    let mut out = vec![
        ("bash", home.join(".bashrc")),
        ("zsh", home.join(".zshrc")),
        ("fish", home.join(".config/fish/config.fish")),
    ];
    #[cfg(target_os = "windows")]
    out.push((
        "powershell",
        home.join("Documents/PowerShell/Microsoft.PowerShell_profile.ps1"),
    ));
    #[cfg(not(target_os = "windows"))]
    out.push((
        "powershell",
        home.join(".config/powershell/Microsoft.PowerShell_profile.ps1"),
    ));
    out
}

/// Known agent transcript locations (existence probe only).
fn transcript_dirs() -> Vec<(&'static str, PathBuf)> {
    let home = dirs::home_dir().unwrap_or_else(|| PathBuf::from("~"));
    let data = dirs::data_dir().unwrap_or_else(|| home.join(".local/share"));
    let config = dirs::config_dir().unwrap_or_else(|| home.join(".config"));
    vec![
        ("claude", home.join(".claude/projects")),
        ("codex", home.join(".codex")),
        ("cursor", config.join("Cursor/User/workspaceStorage")),
        ("opencode", data.join("opencode")),
        ("pi", home.join(".pi/agent/sessions")),
        ("zcode", home.join(".zcode/cli/db")),
    ]
}

fn readable_dir(path: &Path) -> bool {
    std::fs::read_dir(path).is_ok()
}

/// Render the full report for `store_dir`. Never fails hard: every probe
/// degrades to a printed line.
#[must_use]
pub fn report(store_dir: &Path, spool_count: usize, record_count: Option<i64>) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "amem doctor");
    let _ = writeln!(out, "store: {}", store_dir.display());
    match record_count {
        Some(n) => {
            let _ = writeln!(out, "  records(shell): {n}");
        }
        None => {
            let _ = writeln!(out, "  records: store not initialized");
        }
    }
    let _ = writeln!(out, "  pending spool files: {spool_count}");
    let _ = writeln!(out, "shell rc files:");
    for (shell, rc) in rc_files() {
        let status = match std::fs::read_to_string(&rc) {
            Ok(text) => {
                let ours = text.contains("amem init");
                let mut foreign = Vec::new();
                for (name, marker) in FOREIGN_HOOK_MARKERS {
                    // Our own bash snippet embeds bash-preexec: not foreign.
                    let self_embed = *name == "bash-preexec(other)" && ours;
                    if !self_embed && text.contains(marker) {
                        foreign.push(*name);
                    }
                }
                format!(
                    "present (amem: {}, coexist: [{}])",
                    if ours { "yes" } else { "no" },
                    if foreign.is_empty() {
                        "none".to_string()
                    } else {
                        foreign.join(",")
                    }
                )
            }
            Err(_) => "missing".to_string(),
        };
        let _ = writeln!(out, "  {shell}: {} [{status}]", rc.display());
    }
    let _ = writeln!(out, "transcript sources:");
    for (name, dir) in transcript_dirs() {
        let state = if readable_dir(&dir) {
            "readable"
        } else {
            "unsupported on this machine (absent/unreadable)"
        };
        let _ = writeln!(out, "  {name}: {} [{state}]", dir.display());
    }
    out
}
