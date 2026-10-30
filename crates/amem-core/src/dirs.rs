//! OS directory resolution: home, data, config.
//!
//! A dependency-free replacement for the `dirs` crate, whose `option-ext`
//! dependency is MPL-2.0 (outside the project's license allow-list).
//! Semantics mirror `dirs` 7 on each OS: env vars first, OS conventional
//! fallbacks below `$HOME`.

use std::path::PathBuf;

/// User home: `$HOME` (unix) / `%USERPROFILE%` (Windows). Empty values
/// count as missing.
#[must_use]
pub fn home_dir() -> Option<PathBuf> {
    #[cfg(windows)]
    let raw = std::env::var_os("USERPROFILE");
    #[cfg(not(windows))]
    let raw = std::env::var_os("HOME");
    raw.map(PathBuf::from).filter(|p| !p.as_os_str().is_empty())
}

/// Per-user data dir: `$XDG_DATA_HOME` or `~/.local/share` (Linux),
/// `~/Library/Application Support` (macOS), `%LOCALAPPDATA%` (Windows).
#[must_use]
pub fn data_dir() -> Option<PathBuf> {
    #[cfg(target_os = "macos")]
    {
        home_dir().map(|h| h.join("Library/Application Support"))
    }
    #[cfg(target_os = "windows")]
    {
        std::env::var_os("LOCALAPPDATA")
            .map(PathBuf::from)
            .filter(|p| !p.as_os_str().is_empty())
    }
    #[cfg(not(any(target_os = "macos", target_os = "windows")))]
    {
        if let Some(xdg) = std::env::var_os("XDG_DATA_HOME") {
            let p = PathBuf::from(xdg);
            if !p.as_os_str().is_empty() {
                return Some(p);
            }
        }
        home_dir().map(|h| h.join(".local/share"))
    }
}

/// Per-user config dir: `$XDG_CONFIG_HOME` or `~/.config` (Linux),
/// `~/Library/Application Support` (macOS), `%APPDATA%` (Windows).
#[must_use]
pub fn config_dir() -> Option<PathBuf> {
    #[cfg(target_os = "macos")]
    {
        home_dir().map(|h| h.join("Library/Application Support"))
    }
    #[cfg(target_os = "windows")]
    {
        std::env::var_os("APPDATA")
            .map(PathBuf::from)
            .filter(|p| !p.as_os_str().is_empty())
    }
    #[cfg(not(any(target_os = "macos", target_os = "windows")))]
    {
        if let Some(xdg) = std::env::var_os("XDG_CONFIG_HOME") {
            let p = PathBuf::from(xdg);
            if !p.as_os_str().is_empty() {
                return Some(p);
            }
        }
        home_dir().map(|h| h.join(".config"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dirs_agree_with_home() {
        // No env mutation (process-global): assert internal consistency.
        let home = home_dir().expect("test box has a home");
        assert!(home.is_absolute());
        for dir in [data_dir(), config_dir()].into_iter().flatten() {
            assert!(dir.is_absolute());
        }
        // data/config either live under home or came from XDG overrides.
        for dir in [data_dir(), config_dir()].into_iter().flatten() {
            let under_home = dir.starts_with(&home);
            let from_env = std::env::var_os("XDG_DATA_HOME").is_some()
                || std::env::var_os("XDG_CONFIG_HOME").is_some();
            assert!(under_home || from_env, "{}", dir.display());
        }
    }
}
