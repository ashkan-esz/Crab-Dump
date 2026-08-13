//! Monthly JSONL history for database backup attempts.

use std::fs::{self, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::SystemTime;

use anyhow::{Context, Result};
use chrono::{DateTime, Datelike, Utc};
use serde::{Deserialize, Serialize};

const HISTORY_FILE_SUFFIX: &str = ".jsonl";

#[derive(Debug)]
pub struct HistorySnapshot {
    pub month: String,
    pub path: PathBuf,
}

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

#[derive(Debug, Clone, Serialize)]
pub struct HistoryStats {
    pub attempts: usize,
    pub successes: usize,
    pub failures: usize,
    pub success_rate: f64,
    pub last_run: Option<String>,
    pub last_success: Option<String>,
    pub average_duration_secs: f64,
    pub average_dump_bytes: f64,
    pub average_packaged_bytes: f64,
    pub average_upload_retries: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct HistorySummary {
    pub database: String,
    pub stats: HistoryStats,
    pub records: Vec<HistoryRecord>,
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

    /// Copy the active monthly JSONL file while holding the append lock.
    ///
    /// The returned file is a private work-dir artifact owned by the caller;
    /// it is deliberately not retained in the history directory. A missing
    /// or empty active file returns `None`, so the scheduler never uploads a
    /// bogus empty history document.
    pub fn snapshot_active(&self, work_dir: &std::path::Path) -> Result<Option<HistorySnapshot>> {
        let _guard = self.lock.lock().unwrap_or_else(|e| e.into_inner());
        let now = Utc::now();
        let month = format!("{:04}-{:02}", now.year(), now.month());
        let source = self.directory.join(format!("{month}{HISTORY_FILE_SUFFIX}"));
        let metadata = match fs::metadata(&source) {
            Ok(metadata) if metadata.is_file() && metadata.len() > 0 => metadata,
            Ok(_) => return Ok(None),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("statting active history file {}", source.display()))
            }
        };

        fs::create_dir_all(work_dir)
            .with_context(|| format!("creating work directory {}", work_dir.display()))?;
        let unique = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let destination = work_dir.join(format!(".history-{month}-{unique}.jsonl"));
        let mut input = fs::File::open(&source)
            .with_context(|| format!("opening active history file {}", source.display()))?;
        let mut output = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&destination)
            .with_context(|| format!("creating history snapshot {}", destination.display()))?;
        if let Err(error) = io::copy(&mut input, &mut output).and_then(|copied| {
            if copied == metadata.len() {
                output.sync_all()
            } else {
                Err(io::Error::other("history snapshot length changed"))
            }
        }) {
            let _ = fs::remove_file(&destination);
            return Err(error)
                .with_context(|| format!("copying history snapshot {}", source.display()));
        }
        Ok(Some(HistorySnapshot {
            month,
            path: destination,
        }))
    }

    /// Read retained history for one database without loading the history
    /// corpus into memory. Malformed JSONL lines are ignored so one truncated
    /// record cannot hide the rest of the dashboard history.
    pub fn summary(&self, database_name: &str, limit: usize) -> Result<HistorySummary> {
        let mut all = Vec::new();
        let mut stats = HistoryStatsBuilder::default();
        let entries = match fs::read_dir(&self.directory) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(HistorySummary {
                    database: database_name.to_string(),
                    stats: stats.finish(),
                    records: Vec::new(),
                });
            }
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("scanning history directory {}", self.directory.display())
                });
            }
        };

        let mut paths = entries
            .filter_map(|entry| entry.ok().map(|entry| entry.path()))
            .filter(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .and_then(parse_month_filename)
                    .is_some()
            })
            .collect::<Vec<_>>();
        paths.sort();

        for path in paths {
            let file = fs::File::open(&path)
                .with_context(|| format!("opening history file {}", path.display()))?;
            for line in BufReader::new(file).lines() {
                let line =
                    line.with_context(|| format!("reading history file {}", path.display()))?;
                let Ok(record) = serde_json::from_str::<HistoryRecord>(&line) else {
                    continue;
                };
                if record.database_name != database_name {
                    continue;
                }
                stats.add(&record);
                all.push(record);
            }
        }

        all.sort_by(|a, b| b.started_at.cmp(&a.started_at));
        all.truncate(limit);
        Ok(HistorySummary {
            database: database_name.to_string(),
            stats: stats.finish(),
            records: all,
        })
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

#[derive(Default)]
struct HistoryStatsBuilder {
    attempts: usize,
    successes: usize,
    duration: f64,
    dump_bytes: f64,
    packaged_bytes: f64,
    upload_retries: f64,
    last_run: Option<String>,
    last_success: Option<String>,
}

impl HistoryStatsBuilder {
    fn add(&mut self, record: &HistoryRecord) {
        self.attempts += 1;
        if record.status == "success" {
            self.successes += 1;
            if self
                .last_success
                .as_ref()
                .is_none_or(|last| record.started_at > *last)
            {
                self.last_success = Some(record.started_at.clone());
            }
        }
        if self
            .last_run
            .as_ref()
            .is_none_or(|last| record.started_at > *last)
        {
            self.last_run = Some(record.started_at.clone());
        }
        self.duration += record.duration_secs;
        self.dump_bytes += record.dump_bytes as f64;
        self.packaged_bytes += record.packaged_bytes as f64;
        self.upload_retries += record.upload_retries as f64;
    }

    fn finish(self) -> HistoryStats {
        let attempts = self.attempts as f64;
        HistoryStats {
            attempts: self.attempts,
            successes: self.successes,
            failures: self.attempts.saturating_sub(self.successes),
            success_rate: if self.attempts == 0 {
                0.0
            } else {
                self.successes as f64 / self.attempts as f64 * 100.0
            },
            last_run: self.last_run,
            last_success: self.last_success,
            average_duration_secs: if attempts > 0.0 {
                self.duration / attempts
            } else {
                0.0
            },
            average_dump_bytes: if attempts > 0.0 {
                self.dump_bytes / attempts
            } else {
                0.0
            },
            average_packaged_bytes: if attempts > 0.0 {
                self.packaged_bytes / attempts
            } else {
                0.0
            },
            average_upload_retries: if attempts > 0.0 {
                self.upload_retries / attempts
            } else {
                0.0
            },
        }
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
pub fn sanitize_error(
    error: &str,
    database_url: &str,
    bot_token: &str,
    chat_ids: &[String],
) -> String {
    let mut sanitized = error.replace(database_url, "[REDACTED_DATABASE_URL]");
    sanitized = sanitized.replace(bot_token, "[REDACTED_BOT_TOKEN]");
    for chat_id in chat_ids {
        sanitized = sanitized.replace(chat_id, "[REDACTED_CHAT_ID]");
    }

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
            &["chat".to_string()],
        );
        assert!(!out.contains("secret"));
        assert!(!out.contains("tok"));
        assert!(!out.contains("chat"));
    }

    #[test]
    fn sanitizer_redacts_all_chat_ids() {
        let out = sanitize_error(
            "first=-100111 second=@backup-channel",
            "unused",
            "unused-token",
            &["-100111".into(), "@backup-channel".into()],
        );
        assert!(!out.contains("-100111"));
        assert!(!out.contains("@backup-channel"));
    }

    #[test]
    fn summary_is_bounded_newest_first_and_aggregates_all_records() {
        let dir = temp_dir("summary");
        let store = HistoryStore::new(&dir, 12);
        for ordinal in 1..=35 {
            let timestamp = if ordinal <= 28 {
                format!("2026-08-{ordinal:02}T00:00:00Z")
            } else {
                format!("2026-09-{:02}T00:00:00Z", ordinal - 28)
            };
            let mut item = record(&timestamp);
            item.status = if ordinal % 5 == 0 {
                "failure"
            } else {
                "success"
            }
            .into();
            item.duration_secs = ordinal as f64;
            item.dump_bytes = ordinal;
            item.packaged_bytes = ordinal * 2;
            item.upload_retries = (ordinal % 3) as u64;
            store.append(&item).unwrap();
        }

        let summary = store.summary("app", 30).unwrap();
        assert_eq!(summary.records.len(), 30);
        assert_eq!(summary.records[0].started_at, "2026-09-07T00:00:00Z");
        assert_eq!(summary.records[29].started_at, "2026-08-06T00:00:00Z");
        assert_eq!(summary.stats.attempts, 35);
        assert_eq!(summary.stats.successes, 28);
        assert_eq!(summary.stats.failures, 7);
        assert!((summary.stats.success_rate - 80.0).abs() < f64::EPSILON);
        assert_eq!(
            summary.stats.last_success.as_deref(),
            Some("2026-09-06T00:00:00Z")
        );
        assert!((summary.stats.average_duration_secs - 18.0).abs() < f64::EPSILON);
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn summary_skips_malformed_lines_and_handles_missing_databases() {
        let dir = temp_dir("summary-malformed");
        fs::create_dir_all(&dir).unwrap();
        let item = record("2026-08-01T00:00:00Z");
        fs::write(
            dir.join("2026-08.jsonl"),
            format!("not json\n{}\n", serde_json::to_string(&item).unwrap()),
        )
        .unwrap();
        let store = HistoryStore::new(&dir, 12);
        assert_eq!(store.summary("app", 30).unwrap().stats.attempts, 1);
        assert!(store.summary("missing", 30).unwrap().records.is_empty());
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn summary_of_empty_or_absent_history_is_zeroed() {
        let dir = temp_dir("summary-empty");
        let store = HistoryStore::new(&dir, 12);
        let summary = store.summary("app", 30).unwrap();
        assert_eq!(summary.stats.attempts, 0);
        assert_eq!(summary.stats.success_rate, 0.0);
        assert!(summary.stats.last_run.is_none());
        assert!(summary.records.is_empty());
    }

    #[test]
    fn snapshot_selects_active_month_and_is_not_empty() {
        let dir = temp_dir("snapshot");
        let work = temp_dir("snapshot-work");
        fs::create_dir_all(&dir).unwrap();
        let month = Utc::now().format("%Y-%m").to_string();
        fs::write(
            dir.join(format!("{month}.jsonl")),
            b"{\"database_name\":\"app\"}\n",
        )
        .unwrap();
        let store = HistoryStore::new(&dir, 12);
        let snapshot = store.snapshot_active(&work).unwrap().unwrap();
        assert_eq!(snapshot.month, month);
        assert_eq!(
            fs::read_to_string(snapshot.path).unwrap(),
            "{\"database_name\":\"app\"}\n"
        );
        fs::remove_dir_all(dir).ok();
        fs::remove_dir_all(work).ok();
    }

    #[test]
    fn snapshot_skips_missing_and_empty_active_history() {
        let dir = temp_dir("snapshot-empty");
        let work = temp_dir("snapshot-empty-work");
        fs::create_dir_all(&dir).unwrap();
        let month = Utc::now().format("%Y-%m").to_string();
        fs::write(dir.join(format!("{month}.jsonl")), b"").unwrap();
        let store = HistoryStore::new(&dir, 12);
        assert!(store.snapshot_active(&work).unwrap().is_none());
        fs::remove_dir_all(dir).ok();
        fs::remove_dir_all(work).ok();
    }
}
