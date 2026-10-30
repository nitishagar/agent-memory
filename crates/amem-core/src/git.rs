//! Git context probe: associate captured events with their repository.
//!
//! Runs `git rev-parse` probes with a hard timeout. Never fails ingestion:
//! callers treat `Ok(None)` (not a repo) and `Err` (timeout/broken git) as
//! "unknown git context" and store the record anyway.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::time::Duration;

use crate::error::{Error, Result};

/// Hard timeout for the whole probe sequence.
const PROBE_TIMEOUT: Duration = Duration::from_secs(5);

/// Kill flag shared with the probe thread: on timeout the waiter sets it
/// and the in-flight `git` is killed instead of orphaned.
#[derive(Debug, Default)]
struct Killer {
    die: AtomicBool,
}

/// Repository identity for one captured event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitInfo {
    /// Repository identity: toplevel path, or the bare repo dir itself.
    pub repo_id: String,
    /// Current branch; `None` when detached, bare, or unborn.
    pub branch: Option<String>,
    /// HEAD commit sha; `None` for empty repos.
    pub commit: Option<String>,
    /// True when `cwd` is itself a bare repository.
    pub bare: bool,
    /// Outer worktree when inside a submodule.
    pub submodule_outer: Option<String>,
    /// True when no `origin` remote exists (never leaves the machine implicitly).
    pub local_only: bool,
}

/// Probe `cwd` for git context.
///
/// Returns `Ok(None)` when `cwd` is not inside any repository.
///
/// # Errors
///
/// Returns [`Error::Timeout`] if git does not answer within 5 s, or
/// [`Error::Io`] if git cannot be spawned.
pub fn probe(cwd: &Path) -> Result<Option<GitInfo>> {
    let cwd = cwd.to_path_buf();
    let killer = Arc::new(Killer::default());
    let (tx, rx) = mpsc::channel();
    std::thread::scope(|s| {
        let killer2 = Arc::clone(&killer);
        s.spawn(move || {
            let _ = tx.send(probe_inner(&cwd, &killer2));
        });
        if let Ok(info) = rx.recv_timeout(PROBE_TIMEOUT) {
            Ok(info)
        } else {
            killer.die.store(true, Ordering::SeqCst);
            Err(Error::Timeout(format!(
                "git probe exceeded {}s",
                PROBE_TIMEOUT.as_secs()
            )))
        }
    })
}

fn git(cwd: &Path, args: &[&str], killer: &Killer) -> Option<String> {
    let mut child = Command::new("git")
        .arg("-C")
        .arg(cwd)
        .args(args)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .ok()?;
    loop {
        if killer.die.load(Ordering::SeqCst) {
            let _ = child.kill();
            let _ = child.wait();
            return None;
        }
        match child.try_wait() {
            Ok(Some(status)) => {
                let mut stdout = Vec::new();
                if let Some(mut pipe) = child.stdout.take() {
                    use std::io::Read as _;
                    let _ = pipe.read_to_end(&mut stdout);
                }
                if !status.success() {
                    return None;
                }
                return Some(String::from_utf8_lossy(&stdout).trim().to_string());
            }
            Ok(None) => std::thread::sleep(Duration::from_millis(5)),
            Err(_) => return None,
        }
    }
}

fn probe_inner(cwd: &Path, killer: &Killer) -> Option<GitInfo> {
    // Bare repo check first: cwd itself may be the repo (no work tree).
    if git(cwd, &["rev-parse", "--is-bare-repository"], killer).as_deref() == Some("true") {
        let canonical = canonical(cwd);
        let local_only = git(cwd, &["remote", "get-url", "origin"], killer).is_none();
        return Some(GitInfo {
            repo_id: canonical,
            branch: None,
            commit: git(cwd, &["rev-parse", "HEAD"], killer),
            bare: true,
            submodule_outer: None,
            local_only,
        });
    }
    let toplevel = git(cwd, &["rev-parse", "--show-toplevel"], killer)?;
    let toplevel_path = PathBuf::from(&toplevel);
    let branch = git(cwd, &["rev-parse", "--abbrev-ref", "HEAD"], killer)
        .filter(|b| b != "HEAD" && !b.is_empty());
    let commit = git(cwd, &["rev-parse", "HEAD"], killer);
    let submodule_outer = git(
        cwd,
        &["rev-parse", "--show-superproject-working-tree"],
        killer,
    )
    .filter(|s| !s.is_empty());
    let local_only = git(cwd, &["remote", "get-url", "origin"], killer).is_none();
    let _ = &toplevel_path;
    Some(GitInfo {
        repo_id: toplevel,
        branch,
        commit,
        bare: false,
        submodule_outer,
        local_only,
    })
}

fn canonical(p: &Path) -> String {
    std::fs::canonicalize(p).map_or_else(
        |_| p.to_string_lossy().into_owned(),
        |c| c.to_string_lossy().into_owned(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;

    fn git_in(dir: &Path, args: &[&str]) {
        let st = Command::new("git")
            .arg("-C")
            .arg(dir)
            .args(args)
            .env("GIT_AUTHOR_NAME", "t")
            .env("GIT_AUTHOR_EMAIL", "t@t")
            .env("GIT_COMMITTER_NAME", "t")
            .env("GIT_COMMITTER_EMAIL", "t@t")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .status()
            .expect("git runs");
        assert!(st.success(), "git {args:?} failed");
    }

    fn commit_one(dir: &Path) {
        std::fs::write(dir.join("f.txt"), "x").unwrap();
        git_in(dir, &["add", "."]);
        git_in(dir, &["commit", "-qm", "one"]);
    }

    #[test]
    fn git_matrix() {
        let tmp = tempfile::tempdir().unwrap();

        // Plain repo with remote.
        let plain = tmp.path().join("plain");
        std::fs::create_dir(&plain).unwrap();
        git_in(&plain, &["init", "-qb", "main"]);
        git_in(
            &plain,
            &["remote", "add", "origin", "https://example.invalid/x"],
        );
        commit_one(&plain);
        let info = probe(&plain).unwrap().expect("repo detected");
        assert_eq!(info.branch.as_deref(), Some("main"));
        assert!(info.commit.as_ref().is_some_and(|c| c.len() == 40));
        assert!(!info.bare && !info.local_only && info.submodule_outer.is_none());

        // No remote → local-only.
        let nor = tmp.path().join("noremote");
        std::fs::create_dir(&nor).unwrap();
        git_in(&nor, &["init", "-qb", "main"]);
        commit_one(&nor);
        let info = probe(&nor).unwrap().unwrap();
        assert!(info.local_only);

        // Detached HEAD → branch None, commit Some.
        let sha = info.commit.clone().unwrap();
        git_in(&nor, &["checkout", "-q", &sha]);
        let info = probe(&nor).unwrap().unwrap();
        assert_eq!(info.branch, None);
        assert_eq!(info.commit.as_deref(), Some(sha.as_str()));

        // Bare repo.
        let bare = tmp.path().join("bare.git");
        git_in(tmp.path(), &["init", "-q", "--bare", "bare.git"]);
        let info = probe(&bare).unwrap().expect("bare detected");
        assert!(info.bare && info.branch.is_none());

        // Submodule → outer recorded.
        let outer = tmp.path().join("outer");
        std::fs::create_dir(&outer).unwrap();
        git_in(&outer, &["init", "-qb", "main"]);
        commit_one(&outer);
        git_in(
            &outer,
            &[
                "-c",
                "protocol.file.allow=always",
                "submodule",
                "add",
                "-q",
                "../noremote",
                "sub",
            ],
        );
        let info = probe(&outer.join("sub")).unwrap().unwrap();
        assert!(info.submodule_outer.is_some(), "outer: {info:?}");

        // Not a repo → None.
        let plain_dir = tmp.path().join("empty");
        std::fs::create_dir(&plain_dir).unwrap();
        assert_eq!(probe(&plain_dir).unwrap(), None);
    }
}
