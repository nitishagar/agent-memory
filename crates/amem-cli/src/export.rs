//! Obsidian vault export: one Markdown file per record plus day/session indexes.
//!
//! Schema: `<YYYY-MM-DD>-<slug>.md` with YAML frontmatter and
//! `[[day-index]]` / `[[session-<id>]]` links; every `[[link]]` resolves.
//! Export reads the redacted store, so it obeys capture-time redaction.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::Path;

use amem_core::error::{Error, Result};

/// Export outcome counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Stats {
    /// Record files written.
    pub files: usize,
    /// Day pages written.
    pub days: usize,
    /// Session pages written.
    pub sessions: usize,
    /// Quarantined files written (subset of `files`).
    pub quarantined: usize,
}

/// Export `store_dir` into `vault` (created; files overwritten).
///
/// # Errors
///
/// Returns [`Error::NotFound`] on an uninitialized store, [`Error::Db`]
/// on store failure, [`Error::Io`] on write failure.
#[allow(clippy::too_many_lines)]
pub fn export_vault(store_dir: &Path, vault: &Path) -> Result<Stats> {
    if !store_dir.join("memory.db").exists() {
        return Err(Error::NotFound(
            "store not initialized; nothing to export".to_string(),
        ));
    }
    let db = amem_core::store::Store::open_readonly(store_dir)?;
    let device = std::fs::read_to_string(store_dir.join("device_id"))
        .map(|d| d.trim().to_string())
        .unwrap_or_default();
    let records = db.list_all(&device)?;
    // Quarantine renders as pseudo-records (flagged, never silently dropped).
    let mut items: Vec<(amem_core::store::Record, String, bool)> = Vec::new();
    for rec in records {
        let ext = db
            .external_id(&device, &rec.stream, rec.idx)?
            .unwrap_or_else(|| format!("{}#{}", rec.stream, rec.idx));
        items.push((rec, ext, false));
    }
    // Quarantine matches the records scope (own device only): teammates'
    // synced records are absent from this export, so their quarantined
    // scraps must not appear either.
    for q in db.quarantine_list()? {
        if q.device != device {
            continue;
        }
        if let Some((rec, ext)) = quarantine_pseudo(&q) {
            items.push((rec, ext, true));
        }
    }
    std::fs::create_dir_all(vault)?;

    // Pre-pass: session page stems, disambiguating `safe()` collisions
    // with a hash suffix, so record files link exact pages.
    let mut session_names: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for (rec, _, _) in &items {
        let payload: serde_json::Value =
            serde_json::from_str(&rec.payload).unwrap_or(serde_json::Value::Null);
        session_names.insert(session_of(rec, &payload));
    }
    let mut taken: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut page_for: BTreeMap<String, String> = BTreeMap::new();
    for session in &session_names {
        let mut stem = format!("session-{}", safe(session));
        if !taken.insert(stem.clone()) {
            let hash = amem_core::store::hash_payload(session);
            stem = format!("session-{}-{}", safe(session), &hash[..8]);
            taken.insert(stem.clone());
        }
        page_for.insert(session.clone(), stem);
    }

    let mut days: BTreeMap<String, Vec<(String, String)>> = BTreeMap::new();
    let mut sessions: BTreeMap<String, Vec<(String, String)>> = BTreeMap::new();
    let mut quarantined: Vec<(String, String)> = Vec::new();
    for (rec, ext, is_q) in &items {
        let payload: serde_json::Value =
            serde_json::from_str(&rec.payload).unwrap_or(serde_json::Value::Null);
        let date = civil_date(rec.ts_ns);
        let slug = slug_for(rec, &payload);
        let stem = if *is_q {
            format!("{date}-q-{slug}")
        } else {
            format!("{date}-{slug}")
        };
        let session = session_of(rec, &payload);
        let title = title_for(rec, &payload);
        let body = render_record(
            rec,
            &payload,
            ext,
            &session,
            &page_for[&session],
            &title,
            *is_q,
        );
        std::fs::write(vault.join(format!("{stem}.md")), body)?;
        days.entry(date)
            .or_default()
            .push((stem.clone(), title.clone()));
        sessions
            .entry(session)
            .or_default()
            .push((stem.clone(), title.clone()));
        if *is_q {
            quarantined.push((stem, title));
        }
    }

    for (date, files) in &days {
        let mut page = format!("# {date}\n\n[[day-index]]\n\n## records\n");
        for (stem, title) in files {
            let _ = writeln!(page, "- [[{stem}]] — {}", link_text(title));
        }
        std::fs::write(vault.join(format!("{date}.md")), page)?;
    }
    let mut index = "# day index\n\n".to_string();
    for (date, files) in &days {
        let _ = writeln!(index, "- [[{date}]] ({} records)", files.len());
    }
    std::fs::write(vault.join("day-index.md"), index)?;

    for (session, files) in &sessions {
        let mut page = format!("# session {session}\n\n[[day-index]]\n\n## records\n");
        for (stem, title) in files {
            let _ = writeln!(page, "- [[{stem}]] — {}", link_text(title));
        }
        let stem = &page_for[session];
        std::fs::write(vault.join(format!("{stem}.md")), page)?;
    }
    if !quarantined.is_empty() {
        let mut page =
            "# quarantine\n\n[[day-index]]\n\nHeld out of recall until restored:\n".to_string();
        for (stem, title) in &quarantined {
            let _ = writeln!(page, "- [[{stem}]] — {}", link_text(title));
        }
        std::fs::write(vault.join("quarantine-index.md"), page)?;
    }
    Ok(Stats {
        files: items.len(),
        days: days.len(),
        sessions: sessions.len(),
        quarantined: quarantined.len(),
    })
}

/// A quarantined envelope as an exportable pseudo-record + its ext id.
fn quarantine_pseudo(
    q: &amem_core::store::Quarantined,
) -> Option<(amem_core::store::Record, String)> {
    let env = serde_json::from_str::<serde_json::Value>(&q.envelope).ok()?;
    let str_field = |key: &str| {
        env.get(key)
            .and_then(serde_json::Value::as_str)
            .unwrap_or("")
            .to_string()
    };
    let payload = env
        .get("payload")
        .cloned()
        .unwrap_or(serde_json::Value::Null);
    Some((
        amem_core::store::Record {
            device: q.device.clone(),
            stream: q.stream.clone(),
            idx: q.idx,
            ts_ns: env
                .get("ts_ns")
                .and_then(serde_json::Value::as_i64)
                .unwrap_or(q.ts_ns),
            actor: str_field("actor"),
            kind: str_field("kind"),
            payload: payload.to_string(),
            hash: str_field("hash"),
        },
        str_field("id"),
    ))
}

fn slug_for(rec: &amem_core::store::Record, payload: &serde_json::Value) -> String {
    if rec.stream == "shell" {
        return format!("sh-{}", rec.idx);
    }
    if rec.stream == "notes" {
        let id = payload
            .get("id")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("");
        if id.is_empty() {
            return format!("note-{}", rec.idx);
        }
        // `idx` disambiguates versions of one id (append-only history).
        return format!("note-{}-{}", safe(id), rec.idx);
    }
    let source = rec.stream.strip_prefix("transcript:").unwrap_or("tx");
    format!("tx-{}-{}", safe(source), rec.idx)
}

fn session_of(rec: &amem_core::store::Record, payload: &serde_json::Value) -> String {
    if rec.stream == "notes" {
        return "notes".to_string();
    }
    payload
        .get("session")
        .and_then(serde_json::Value::as_str)
        .filter(|s| !s.is_empty())
        .unwrap_or("default")
        .to_string()
}

fn title_for(rec: &amem_core::store::Record, payload: &serde_json::Value) -> String {
    let raw = if rec.stream == "shell" {
        payload
            .get("cmd")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("?")
    } else if rec.stream == "notes" {
        payload
            .get("title")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("note")
    } else {
        let source = payload
            .get("source")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("?");
        let role = payload
            .get("role")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("?");
        return format!("{source} {role} #{}", rec.idx);
    };
    raw.lines().next().unwrap_or("?").chars().take(80).collect()
}

fn str_field(payload: &serde_json::Value, key: &str) -> String {
    payload
        .get(key)
        .and_then(serde_json::Value::as_str)
        .unwrap_or("")
        .to_string()
}

/// One JSON-quoted YAML scalar (valid YAML for any string).
fn yml(s: &str) -> String {
    serde_json::to_string(s).unwrap_or_else(|_| "\"\"".to_string())
}

#[allow(clippy::too_many_arguments)]
fn render_record(
    rec: &amem_core::store::Record,
    payload: &serde_json::Value,
    ext: &str,
    session: &str,
    session_page: &str,
    title: &str,
    quarantined: bool,
) -> String {
    let redacted = payload
        .get("redacted")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let mut fm = format!(
        "---\nid: {}\nstream: {}\nactor: {}\nhlc: {}\nkind: {}\nchecksum: {}\nredacted: {}\nquarantined: {}\n",
        yml(ext),
        yml(&rec.stream),
        yml(&rec.actor),
        rec.ts_ns,
        yml(&rec.kind),
        yml(&rec.hash),
        redacted,
        quarantined,
    );
    let content = if rec.stream == "shell" {
        let exit_code = payload.get("exit").map_or_else(
            || "null".to_string(),
            |v| {
                v.as_i64()
                    .map_or_else(|| "null".to_string(), |e| e.to_string())
            },
        );
        let _ = writeln!(
            fm,
            "cmd: {}\nexit: {exit_code}\ncwd: {}\nrepo: {}\nbranch: {}\nsession: {}\nshell: {}",
            yml(&str_field(payload, "cmd")),
            yml(&str_field(payload, "cwd")),
            yml(&str_field(payload, "repo")),
            yml(&str_field(payload, "branch")),
            yml(session),
            yml(&str_field(payload, "shell")),
        );
        format!(
            "```\n$ {}\n```\n\nexit: {exit_code}, cwd: {}\n",
            str_field(payload, "cmd"),
            str_field(payload, "cwd"),
        )
    } else if rec.stream == "notes" {
        let _ = writeln!(fm, "title: {}", yml(&str_field(payload, "title")));
        str_field(payload, "text")
    } else {
        let _ = writeln!(
            fm,
            "source: {}\nsession: {}\nproject: {}\nrole: {}\ntool: {}\nmodel: {}",
            yml(&str_field(payload, "source")),
            yml(session),
            yml(&str_field(payload, "project")),
            yml(&str_field(payload, "role")),
            yml(&str_field(payload, "tool")),
            yml(&str_field(payload, "model")),
        );
        str_field(payload, "text")
    };
    format!("{fm}---\n\n# {title}\n\n[[day-index]] · [[{session_page}]]\n\n{content}\n")
}

/// Filename/link-safe token: alphanumerics plus `-_.`, else `-`, capped.
fn safe(s: &str) -> String {
    let out: String = s
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' {
                c
            } else {
                '-'
            }
        })
        .take(80)
        .collect();
    if out.is_empty() {
        "unknown".to_string()
    } else {
        out
    }
}

/// Unix-nanosecond timestamp to UTC civil date (Howard Hinnant's algorithm).
fn civil_date(ts_ns: i64) -> String {
    let days = ts_ns.div_euclid(1_000_000_000).div_euclid(86_400);
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let mut y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    if m <= 2 {
        y += 1;
    }
    format!("{y:04}-{m:02}-{d:02}")
}

/// Index bullet titles must not smuggle `[[...]]` into link position.
fn link_text(title: &str) -> String {
    title.chars().filter(|c| *c != '[' && *c != ']').collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn civil_date_known_days() {
        // 2026-10-30T00:00:00Z.
        assert_eq!(civil_date(1_793_318_400_000_000_000), "2026-10-30");
        assert_eq!(civil_date(0), "1970-01-01");
    }

    #[test]
    fn safe_replaces_hostile_chars() {
        assert_eq!(safe("a/b:c"), "a-b-c");
        assert_eq!(safe(""), "unknown");
    }
}
