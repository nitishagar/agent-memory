//! Shell integration snippets (`amem init <shell>`).
//!
//! Shapes mirror atuin's hooks (preexec/precmd per shell); bash embeds the
//! vendored `bash-preexec.sh` (MIT, rcaloras: attribution header intact).
//! PowerShell deliberately uses a `prompt`-override (completed-command
//! capture via Get-History) instead of `PSReadLine` surgery: no module
//! dependency, testable anywhere pwsh runs. See PLAN.md Amendments if this
//! needs revisiting.

use amem_core::error::{Error, Result};

const BASH_PREEXEC: &str = include_str!("hooks/bash-preexec.sh");
const BASH_FUNCS: &str = include_str!("hooks/bash.sh");
const ZSH_SNIPPET: &str = include_str!("hooks/zsh.sh");
const FISH_SNIPPET: &str = include_str!("hooks/fish.sh");
const POWERSHELL_SNIPPET: &str = include_str!("hooks/powershell.ps1");

/// Render the integration snippet for one shell flavour.
///
/// # Errors
///
/// Returns [`Error::InvalidArguments`] for unknown shells.
pub fn snippet(shell: &str) -> Result<String> {
    match shell {
        "bash" => Ok(format!(
            "# >>> amem init (bash): preexec support + hooks >>>\n{BASH_PREEXEC}\n{BASH_FUNCS}"
        )),
        "zsh" => Ok(ZSH_SNIPPET.to_string()),
        "fish" => Ok(FISH_SNIPPET.to_string()),
        "powershell" => Ok(POWERSHELL_SNIPPET.to_string()),
        other => Err(Error::InvalidArguments(format!(
            "unknown shell '{other}': expected {}",
            supported().join(", ")
        ))),
    }
}

/// Shells with embedded snippets.
#[must_use]
pub const fn supported() -> &'static [&'static str] {
    &["bash", "zsh", "fish", "powershell"]
}
