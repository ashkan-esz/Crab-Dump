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
    /// Origin of the backup attempt. Older JSONL records omit this field and
    /// deserialize as scheduled attempts for backward compatibility.
    #[serde(default = "default_source")]
    pub source: String,
    /// Dashboard-selected receiver for manual backups. Older records and
    /// scheduled/one-shot attempts omit this field.
    #[serde(default)]
    pub recipient: Option<String>,
    pub status: String,
    pub error: Option<String>,
    pub dump_bytes: u64,
    pub packaged_bytes: u64,
    pub chunk_count: usize,
    pub sha256: Option<String>,
    /// Effective compression codec for this attempt. Older records omit this
    /// field and are shown as unknown rather than being misclassified.
    #[serde(default = "default_compression_type")]
    pub compression_type: String,
    /// Effective codec level. None means compression was disabled or the
    /// record predates compression metadata.
    #[serde(default)]
    pub compression_level: Option<i32>,
    pub encrypted: bool,
    pub duration_secs: f64,
    pub upload_duration_secs: f64,
    pub upload_attempts: u64,
    pub upload_retries: u64,
}

fn default_source() -> String {
    "scheduled".into()
}

fn default_compression_type() -> String {
    "unknown".into()
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
    pub page: usize,
    pub page_size: usize,
    pub total_records: usize,
    pub total_pages: usize,
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
    pub fn summary(
        &self,
        database_name: &str,
        page: usize,
        page_size: usize,
    ) -> Result<HistorySummary> {
        let mut all = Vec::new();
        let mut stats = HistoryStatsBuilder::default();
        let entries = match fs::read_dir(&self.directory) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(HistorySummary {
                    database: database_name.to_string(),
                    stats: stats.finish(),
                    records: Vec::new(),
                    page: 1,
                    page_size,
                    total_records: 0,
                    total_pages: 0,
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
        let total_records = all.len();
        let total_pages = total_records.div_ceil(page_size);
        let page = if total_pages == 0 {
            1
        } else {
            page.max(1).min(total_pages)
        };
        let start = (page - 1) * page_size;
        let end = (start + page_size).min(total_records);
        let records = all.into_iter().skip(start).take(end - start).collect();
        Ok(HistorySummary {
            database: database_name.to_string(),
            stats: stats.finish(),
            records,
            page,
            page_size,
            total_records,
            total_pages,
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
        if matches!(record.status.as_str(), "enabled" | "disabled") {
            return;
        }
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
            source: "scheduled".into(),
            recipient: None,
            status: "success".into(),
            error: None,
            dump_bytes: 10,
            packaged_bytes: 5,
            chunk_count: 1,
            sha256: Some("00".repeat(32)),
            compression_type: "zstd".into(),
            compression_level: Some(3),
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
            "source",
            "recipient",
            "status",
            "error",
            "dump_bytes",
            "packaged_bytes",
            "chunk_count",
            "sha256",
            "compression_type",
            "compression_level",
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
    fn old_records_without_source_deserialize_as_scheduled() {
        let mut value = serde_json::to_value(record("2026-08-01T00:00:00Z")).unwrap();
        value.as_object_mut().unwrap().remove("source");
        let parsed: HistoryRecord = serde_json::from_value(value).unwrap();
        assert_eq!(parsed.source, "scheduled");
        assert_eq!(parsed.recipient, None);
    }

    #[test]
    fn old_records_without_compression_metadata_deserialize_as_unknown() {
        let mut value = serde_json::to_value(record("2026-08-01T00:00:00Z")).unwrap();
        value.as_object_mut().unwrap().remove("compression_type");
        value.as_object_mut().unwrap().remove("compression_level");
        let parsed: HistoryRecord = serde_json::from_value(value).unwrap();
        assert_eq!(parsed.compression_type, "unknown");
        assert_eq!(parsed.compression_level, None);
    }

    #[test]
    fn manual_records_preserve_recipient() {
        let mut item = record("2026-08-01T00:00:00Z");
        item.source = "manual".into();
        item.recipient = Some("Alice".into());
        let parsed: HistoryRecord =
            serde_json::from_value(serde_json::to_value(item).unwrap()).unwrap();
        assert_eq!(parsed.source, "manual");
        assert_eq!(parsed.recipient.as_deref(), Some("Alice"));
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
            if ordinal == 1 {
                item.source = "manual".into();
            }
            item.status = if ordinal % 5 == 0 {
                "failure"
            } else {
                "success"
            }
            .into();
            item.duration_secs = ordinal as f64;
            item.dump_bytes = ordinal;
            item.packaged_bytes = ordinal * 2;
            item.upload_retries = ordinal % 3;
            store.append(&item).unwrap();
        }

        let summary = store.summary("app", 1, 30).unwrap();
        assert_eq!(summary.records.len(), 30);
        assert_eq!(summary.page, 1);
        assert_eq!(summary.page_size, 30);
        assert_eq!(summary.total_records, 35);
        assert_eq!(summary.total_pages, 2);
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

        let second_page = store.summary("app", 2, 20).unwrap();
        assert_eq!(second_page.page, 2);
        assert_eq!(second_page.page_size, 20);
        assert_eq!(second_page.total_records, 35);
        assert_eq!(second_page.total_pages, 2);
        assert_eq!(second_page.records.len(), 15);
        assert_eq!(
            second_page.records.first().unwrap().started_at,
            "2026-08-15T00:00:00Z"
        );
        assert_eq!(
            second_page.records.last().unwrap().started_at,
            "2026-08-01T00:00:00Z"
        );

        let clamped = store.summary("app", 99, 50).unwrap();
        assert_eq!(clamped.page, 1);
        assert_eq!(clamped.total_pages, 1);
        assert_eq!(clamped.records.len(), 35);
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
        assert_eq!(store.summary("app", 1, 30).unwrap().stats.attempts, 1);
        assert!(store.summary("missing", 1, 30).unwrap().records.is_empty());
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn action_records_are_retained_but_excluded_from_statistics() {
        let dir = temp_dir("actions");
        let store = HistoryStore::new(&dir, 12);
        let mut action = record("2026-08-02T00:00:00Z");
        action.status = "disabled".into();
        action.dump_bytes = 999;
        action.duration_secs = 99.0;
        action.upload_retries = 99;
        store.append(&action).unwrap();

        let backup = record("2026-08-01T00:00:00Z");
        store.append(&backup).unwrap();

        let summary = store.summary("app", 1, 30).unwrap();
        assert_eq!(summary.records.len(), 2);
        assert_eq!(summary.records[0].status, "disabled");
        assert_eq!(summary.stats.attempts, 1);
        assert_eq!(summary.stats.successes, 1);
        assert_eq!(summary.stats.failures, 0);
        assert_eq!(summary.stats.average_duration_secs, 1.0);
        assert_eq!(summary.stats.average_upload_retries, 0.0);
    }

    #[test]
    fn summary_of_empty_or_absent_history_is_zeroed() {
        let dir = temp_dir("summary-empty");
        let store = HistoryStore::new(&dir, 12);
        let summary = store.summary("app", 1, 30).unwrap();
        assert_eq!(summary.stats.attempts, 0);
        assert_eq!(summary.stats.success_rate, 0.0);
        assert!(summary.stats.last_run.is_none());
        assert!(summary.records.is_empty());
        assert_eq!(summary.page, 1);
        assert_eq!(summary.total_records, 0);
        assert_eq!(summary.total_pages, 0);
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
