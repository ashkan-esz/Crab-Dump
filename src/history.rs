//! Monthly JSONL history for database backup attempts.

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::SystemTime;

use anyhow::{Context, Result};
use chrono::{DateTime, Datelike, Utc};
use serde::{Deserialize, Serialize};

const HISTORY_FILE_SUFFIX: &str = ".jsonl";

/// One complete database backup attempt.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoryRecord {
    pub started_at: String,
    pub ended_at: String,
    pub database_index: usize,
    pub database_name: String,
    pub status: String,
    pub error: Option<String>,
    pub dump_bytes: u64,
    pub packaged_bytes: u64,
    pub chunk_count: usize,
    pub sha256: Option<String>,
    pub encrypted: bool,
    pub duration_secs: f64,
    pub upload_duration_secs: f64,
    pub upload_attempts: u64,
    pub upload_retries: u64,
}

/// Process-wide serialized history appender.
#[derive(Debug)]
pub struct HistoryStore {
    directory: PathBuf,
    retention_months: u32,
    lock: Mutex<()>,
}

impl HistoryStore {
    pub fn new(directory: impl Into<PathBuf>, retention_months: u32) -> Self {
        Self {
            directory: directory.into(),
            retention_months: retention_months.max(1),
            lock: Mutex::new(()),
        }
    }

    pub fn append(&self, record: &HistoryRecord) -> Result<()> {
        let _guard = self.lock.lock().unwrap_or_else(|e| e.into_inner());
        fs::create_dir_all(&self.directory)
            .with_context(|| format!("creating history directory {}", self.directory.display()))?;

        let start = parse_timestamp(&record.started_at)?;
        let path = self.directory.join(format!(
            "{:04}-{:02}{HISTORY_FILE_SUFFIX}",
            start.year(),
            start.month()
        ));
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .with_context(|| format!("opening history file {}", path.display()))?;
        serde_json::to_writer(&mut file, record).context("serializing history record")?;
        file.write_all(b"\n")
            .with_context(|| format!("appending newline to {}", path.display()))?;
        file.sync_data()
            .with_context(|| format!("syncing history file {}", path.display()))?;

        self.remove_old_files(start)?;
        Ok(())
    }

    pub fn directory_display(&self) -> String {
        self.directory.display().to_string()
    }

    fn remove_old_files(&self, current: DateTime<Utc>) -> Result<()> {
        let current_month = month_number(current.year(), current.month());
        let oldest = current_month - i64::from(self.retention_months - 1);
        for entry in fs::read_dir(&self.directory)
            .with_context(|| format!("scanning history directory {}", self.directory.display()))?
        {
            let entry = entry.context("reading history directory entry")?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            let Some(month) = parse_month_filename(name) else {
                continue;
            };
            if month < oldest {
                fs::remove_file(entry.path()).with_context(|| {
                    format!("removing old history file {}", entry.path().display())
                })?;
            }
        }
        Ok(())
    }
}

fn parse_timestamp(value: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .map(|timestamp| timestamp.with_timezone(&Utc))
        .with_context(|| format!("invalid history timestamp `{value}`"))
}

fn month_number(year: i32, month: u32) -> i64 {
    i64::from(year) * 12 + i64::from(month) - 1
}

fn parse_month_filename(name: &str) -> Option<i64> {
    let stem = name.strip_suffix(HISTORY_FILE_SUFFIX)?;
    let (year, month) = stem.split_once('-')?;
    let year = year.parse::<i32>().ok()?;
    let month = month.parse::<u32>().ok()?;
    (1..=12).contains(&month).then(|| month_number(year, month))
}

/// Redact credentials and configured secrets before an error enters history.
pub fn sanitize_error(error: &str, database_url: &str, bot_token: &str, chat_id: &str) -> String {
    let mut sanitized = error.replace(database_url, "[REDACTED_DATABASE_URL]");
    sanitized = sanitized.replace(bot_token, "[REDACTED_BOT_TOKEN]");
    sanitized = sanitized.replace(chat_id, "[REDACTED_CHAT_ID]");

    for scheme in ["postgresql://", "postgres://"] {
        let mut rest = sanitized.as_str();
        while let Some(start) = rest.find(scheme) {
            let absolute = sanitized.len() - rest.len() + start;
            let after_scheme = absolute + scheme.len();
            let Some(slash) = sanitized[after_scheme..].find('/') else {
                break;
            };
            let end = after_scheme + slash;
            if let Some(at) = sanitized[after_scheme..end].find('@') {
                let credentials_end = after_scheme + at;
                sanitized.replace_range(after_scheme..credentials_end, "[REDACTED_CREDENTIALS]");
                rest = &sanitized[credentials_end..];
            } else {
                rest = &sanitized[end..];
            }
        }
    }
    sanitized
}

pub fn timestamp(time: SystemTime) -> String {
    DateTime::<Utc>::from(time).to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    fn temp_dir(label: &str) -> PathBuf {
        let path =
            std::env::temp_dir().join(format!("crab-dump-history-{label}-{}", std::process::id()));
        fs::remove_dir_all(&path).ok();
        path
    }

    fn record(started_at: &str) -> HistoryRecord {
        HistoryRecord {
            started_at: started_at.into(),
            ended_at: started_at.into(),
            database_index: 0,
            database_name: "app".into(),
            status: "success".into(),
            error: None,
            dump_bytes: 10,
            packaged_bytes: 5,
            chunk_count: 1,
            sha256: Some("00".repeat(32)),
            encrypted: false,
            duration_secs: 1.0,
            upload_duration_secs: 0.5,
            upload_attempts: 1,
            upload_retries: 0,
        }
    }

    #[test]
    fn serialization_has_success_and_failure_fields() {
        let mut success = record("2026-08-01T00:00:00Z");
        let value = serde_json::to_value(&success).unwrap();
        for field in [
            "started_at",
            "ended_at",
            "database_index",
            "database_name",
            "status",
            "error",
            "dump_bytes",
            "packaged_bytes",
            "chunk_count",
            "sha256",
            "encrypted",
            "duration_secs",
            "upload_duration_secs",
            "upload_attempts",
            "upload_retries",
        ] {
            assert!(value.get(field).is_some(), "missing {field}");
        }
        success.status = "failure".into();
        success.error = Some("bad dump".into());
        assert_eq!(serde_json::to_value(success).unwrap()["status"], "failure");
    }

    #[test]
    fn appends_utc_month_and_one_record_per_line() {
        let dir = temp_dir("append");
        let store = HistoryStore::new(&dir, 12);
        store.append(&record("2026-08-31T23:59:59Z")).unwrap();
        store.append(&record("2026-09-01T00:00:00Z")).unwrap();
        assert!(dir.join("2026-08.jsonl").exists());
        assert!(dir.join("2026-09.jsonl").exists());
        let lines = fs::read_to_string(dir.join("2026-08.jsonl"))
            .unwrap()
            .lines()
            .count();
        assert_eq!(lines, 1);
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn concurrent_appends_are_valid_json_lines() {
        let dir = temp_dir("concurrent");
        let store = Arc::new(HistoryStore::new(&dir, 12));
        let mut threads = Vec::new();
        for i in 0..8 {
            let store = Arc::clone(&store);
            threads.push(thread::spawn(move || {
                for _ in 0..20 {
                    let mut item = record("2026-08-01T00:00:00Z");
                    item.database_index = i;
                    store.append(&item).unwrap();
                }
            }));
        }
        for thread in threads {
            thread.join().unwrap();
        }
        let content = fs::read_to_string(dir.join("2026-08.jsonl")).unwrap();
        assert_eq!(content.lines().count(), 160);
        for line in content.lines() {
            serde_json::from_str::<HistoryRecord>(line).unwrap();
        }
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn retention_removes_old_months_across_year_boundary() {
        let dir = temp_dir("retention");
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("2025-12.jsonl"), "{}\n").unwrap();
        fs::write(dir.join("2026-01.jsonl"), "{}\n").unwrap();
        fs::write(dir.join("2026-02.jsonl"), "{}\n").unwrap();
        fs::write(dir.join("notes.txt"), "keep\n").unwrap();
        let store = HistoryStore::new(&dir, 2);
        store.append(&record("2026-02-15T00:00:00Z")).unwrap();
        assert!(!dir.join("2025-12.jsonl").exists());
        assert!(dir.join("2026-01.jsonl").exists());
        assert!(dir.join("2026-02.jsonl").exists());
        assert!(dir.join("notes.txt").exists());
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn sanitizer_redacts_url_credentials_and_secrets() {
        let out = sanitize_error(
            "postgresql://alice:secret@db/app token=tok chat=chat",
            "unused",
            "tok",
            "chat",
        );
        assert!(!out.contains("secret"));
        assert!(!out.contains("tok"));
        assert!(!out.contains("chat"));
    }
}
