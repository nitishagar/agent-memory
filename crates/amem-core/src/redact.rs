//! Secret redaction at capture time.
//!
//! Command text is scanned before durable storage. Matches are replaced with
//! a visible `[redacted:<rule>]` marker — never silently dropped. Users may
//! exempt exact strings (allowlist) or add their own patterns (deny extras).
//! Default rules are inspired by gitleaks' rule set (names and shapes only;
//! no gitleaks code or config is vendored).

use regex::Regex;

use crate::error::{Error, Result};

/// Marker format for a redacted span.
fn marker(rule: &str) -> String {
    format!("[redacted:{rule}]")
}

/// Default rules: (name, pattern). Order matters — specific token shapes
/// first, generic assignment last.
const DEFAULT_RULES: &[(&str, &str)] = &[
    ("aws-access-key", "AKIA[0-9A-Z]{16}"),
    ("github-token", "gh[pousr]_[A-Za-z0-9_]+"),
    ("slack-token", "xox[baprs]-[A-Za-z0-9-]+"),
    ("openai-key", "sk-[A-Za-z0-9]{8,}"),
    ("private-key", "-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----"),
    ("bearer", "(?i)\\bBearer\\s+[A-Za-z0-9\\-._~+/=]+"),
    (
        "secret-assign",
        "(?i)\\b(api[_-]?key|api[_-]?secret|secret|token|passwd|password|auth[_-]?token|access[_-]?token)\\s*([:=])\\s*\\S+",
    ),
    // Space-separated flags: `--token hunter2`. The dashes are required —
    // a bare word + word (`token tomorrow`) is prose, not a secret. Group 1
    // is the boundary char (no look-around in `regex`): it keeps the inner
    // `-token` of `--with-token` from matching and is re-emitted verbatim.
    (
        "secret-flag",
        "(?i)(^|[^A-Za-z0-9_-])(--?(?:api[_-]?key|api[_-]?secret|secret|token|passwd|password|auth[_-]?token|access[_-]?token))\\s+(\\S+)",
    ),
];

/// Redactor with compiled defaults plus user configuration.
#[derive(Debug)]
pub struct Redactor {
    defaults: Vec<(String, Regex)>,
    allow: Vec<String>,
    extra: Vec<(String, Regex)>,
}

impl Redactor {
    /// Build with default rules, no allowlist, no extras.
    ///
    /// # Panics
    ///
    /// Only if a built-in pattern fails to compile (a programming error —
    /// covered by `defaults_compile`).
    #[must_use]
    pub fn new() -> Self {
        let defaults = DEFAULT_RULES
            .iter()
            .map(|(name, pat)| {
                (
                    (*name).to_string(),
                    Regex::new(pat).expect("built-in redaction pattern"),
                )
            })
            .collect();
        Self {
            defaults,
            allow: Vec::new(),
            extra: Vec::new(),
        }
    }

    /// Exempt exact substrings from redaction (restored verbatim).
    #[must_use]
    pub fn with_allow(mut self, allow: Vec<String>) -> Self {
        self.allow = allow;
        self
    }

    /// Add user patterns (named `user-N` in markers).
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidArguments`] if any pattern is not valid regex.
    pub fn with_extra(mut self, patterns: &[String]) -> Result<Self> {
        for pat in patterns {
            self.try_add_extra(pat)?;
        }
        Ok(self)
    }

    /// Add one user pattern, naming it `user-N` for its position.
    ///
    /// Unlike [`Self::with_extra`], a bad pattern fails only itself, so
    /// callers can skip-and-warn instead of losing every custom rule.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidArguments`] if the pattern is not valid regex.
    pub fn try_add_extra(&mut self, pat: &str) -> Result<()> {
        let re =
            Regex::new(pat).map_err(|e| Error::InvalidArguments(format!("bad {pat:?}: {e}")))?;
        let n = self.extra.len();
        self.extra.push((format!("user-{n}"), re));
        Ok(())
    }

    /// Scan `text`, returning redacted text plus whether anything matched.
    ///
    /// Allowlisted substrings survive verbatim; everything else matching a
    /// rule becomes `[redacted:<rule>]`.
    #[must_use]
    pub fn redact(&self, text: &str) -> (String, bool) {
        // Shelter allowlisted spans behind placeholders first.
        let mut sheltered = text.to_string();
        let mut shelters: Vec<(String, String)> = Vec::new();
        for (i, needle) in self.allow.iter().enumerate() {
            if !needle.is_empty() && sheltered.contains(needle) {
                let token = format!("\u{0}AMEM-ALLOW-{i}\u{0}");
                sheltered = sheltered.replace(needle, &token);
                shelters.push((token, needle.clone()));
            }
        }
        let mut hit = false;
        let mut out = sheltered;
        for (name, re) in self.defaults.iter().chain(self.extra.iter()) {
            if re.is_match(&out) {
                hit = true;
                let m = marker(name);
                // Keep `key=`/`key:` prefix readable for assignment matches.
                if name == "secret-assign" {
                    out = re
                        .replace_all(&out, |caps: &regex::Captures| {
                            format!("{}{}{m}", &caps[1], &caps[2])
                        })
                        .into_owned();
                } else if name == "secret-flag" {
                    out = re
                        .replace_all(&out, |caps: &regex::Captures| {
                            format!("{}{} {m}", &caps[1], &caps[2])
                        })
                        .into_owned();
                } else if name == "bearer" {
                    out = re
                        .replace_all(&out, format!("Bearer {m}").as_str())
                        .into_owned();
                } else {
                    out = re.replace_all(&out, m.as_str()).into_owned();
                }
            }
        }
        for (token, original) in shelters {
            out = out.replace(&token, &original);
        }
        (out, hit)
    }
}

impl Default for Redactor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_compile() {
        let r = Redactor::new();
        assert_eq!(r.defaults.len(), DEFAULT_RULES.len());
    }

    /// Oracle corpus: (input, `must_contain_marker`, `must_not_contain_secret`).
    const CORPUS: &[(&str, &str, &[&str])] = &[
        (
            "export AWS_KEY=AKIAIOSFODNN7EXAMPLE",
            "aws-access-key",
            &["AKIAIOSFODNN7EXAMPLE"],
        ),
        (
            "gh auth login --with-token ghp_abcDEF1234567890xyz",
            "github-token",
            &["ghp_abcDEF1234567890xyz"],
        ),
        (
            "curl -H 'Authorization: Bearer eyJhbGciOiJIUzI1NiJ9' api",
            "bearer",
            &["eyJhbGciOiJIUzI1NiJ9"],
        ),
        (
            "sk-abcdefghijklmnopqrstuvwx curl test",
            "openai-key",
            &["sk-abcdefghijklmnopqrstuvwx"],
        ),
        (
            "cat id_rsa -----BEGIN RSA PRIVATE KEY-----",
            "private-key",
            &["PRIVATE KEY-----"],
        ),
        (
            "token=supersecretvalue123 deploy",
            "secret-assign",
            &["supersecretvalue123"],
        ),
        ("cli --token hunter2 run", "secret-flag", &["hunter2"]),
        (
            "curl --api-key [REDACTED] https://x",
            "secret-flag",
            &["[REDACTED]"],
        ),
        (
            "mysql --password hunter3 db",
            "secret-flag",
            &["[REDACTED]"],
        ),
        (
            "xoxb-1234-5678-abcdEFGH test",
            "slack-token",
            &["xoxb-1234-5678-abcdEFGH"],
        ),
    ];

    const CLEAN: &[&str] = &[
        "ls -la",
        "git status",
        "cargo test --workspace",
        "echo hello world",
        "make -j8 all",
        "grep -r password NOTES.md",
    ];

    #[test]
    fn corpus_no_leaks() {
        let r = Redactor::new();
        for (input, rule, secrets) in CORPUS {
            let (out, hit) = r.redact(input);
            assert!(hit, "no match for: {input}");
            assert!(
                out.contains(&marker(rule)),
                "missing marker {rule} in: {out}"
            );
            for s in *secrets {
                assert!(!out.contains(s), "leaked {s} in: {out}");
            }
        }
    }

    #[test]
    fn clean_commands_untouched() {
        let r = Redactor::new();
        for cmd in CLEAN {
            let (out, hit) = r.redact(cmd);
            assert!(!hit, "false positive on: {cmd}");
            assert_eq!(&out, cmd);
        }
    }

    #[test]
    fn allowlist_survives() {
        let r = Redactor::new().with_allow(vec!["token=public-demo".to_string()]);
        let (out, hit) = r.redact("token=public-demo and token=realsecret1");
        assert!(hit);
        assert!(out.contains("token=public-demo"), "allowlist broken: {out}");
        assert!(!out.contains("realsecret1"), "leak: {out}");
    }

    #[test]
    fn extra_patterns_and_bad_regex() {
        let r = Redactor::new()
            .with_extra(&["INTERNAL-[0-9]+".to_string()])
            .unwrap();
        let (out, hit) = r.redact("deploy INTERNAL-42 now");
        assert!(hit && out.contains("[redacted:user-0]"), "out: {out}");
        assert!(Redactor::new().with_extra(&["([".to_string()]).is_err());
    }
}
