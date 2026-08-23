//! Versioned export/import of dashboard-managed state.
//!
//! The snapshot is deliberately assembled from dashboard stores rather than
//! resolved configuration.  This keeps `.env` values, runtime paths, proxy
//! settings, schedules, and dashboard credentials out of the artifact.

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Datelike, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::compression_config::{CompressionConfigStore, CompressionSettings};
use crate::database_registry::{DashboardDatabase, DatabaseMutation, DatabaseRegistry};
use crate::database_state::DatabaseStateStore;
use crate::health_monitor::{HealthMonitor, Incident, ServiceDefinition, ServiceRuntime};
use crate::history::HistoryRecord;
use crate::routing::ProfileStore;
use crate::telegram_users::{TelegramUser, TelegramUserStore};

pub const FORMAT_VERSION: u32 = 1;
const MAX_SNAPSHOT_BYTES: usize = 25 * 1024 * 1024;
const MAX_HISTORY_RECORDS: usize = 100_000;

#[derive(Serialize)]
struct UsersFile<'a> {
    users: &'a [TelegramUser],
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExporterMetadata {
    pub application: String,
    pub generated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DatabaseSnapshot {
    pub entries: Vec<DashboardDatabase>,
    pub enabled: HashMap<String, bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompressionSnapshot {
    pub codec: String,
    pub level: Option<i32>,
    pub checksum: Option<bool>,
    pub overridden: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HealthSnapshot {
    pub services: Vec<ServiceDefinition>,
    pub incidents: Vec<Incident>,
    pub runtime: HashMap<String, ServiceRuntime>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MigrationSnapshot {
    pub format_version: u32,
    pub exporter: ExporterMetadata,
    pub databases: DatabaseSnapshot,
    pub telegram_users: Vec<TelegramUser>,
    pub routing_profiles: Value,
    pub compression: CompressionSnapshot,
    pub health: HealthSnapshot,
    pub database_audit: Vec<DatabaseMutation>,
    pub history: Vec<HistoryRecord>,
}

#[derive(Clone)]
pub struct MigrationContext {
    pub data_dir: PathBuf,
    pub history_dir: PathBuf,
    pub database_registry: Arc<DatabaseRegistry>,
    pub database_states: Arc<DatabaseStateStore>,
    pub telegram_users: Arc<TelegramUserStore>,
    pub routing_profiles: Arc<ProfileStore>,
    pub compression: Arc<CompressionConfigStore>,
    pub health: Arc<HealthMonitor>,
}

impl MigrationContext {
    pub fn export(&self) -> Result<Vec<u8>> {
        let (settings, overridden) = self.compression.snapshot();
        let (services, incidents, runtime) = self.health.snapshot();
        let snapshot = MigrationSnapshot {
            format_version: FORMAT_VERSION,
            exporter: ExporterMetadata {
                application: "crab-dump".into(),
                generated_at: Utc::now().to_rfc3339(),
            },
            databases: DatabaseSnapshot {
                entries: self.database_registry.dashboard_entries(),
                enabled: self.database_states.snapshot(),
            },
            telegram_users: self.telegram_users.list(),
            routing_profiles: self.routing_profiles.snapshot_json()?,
            compression: CompressionSnapshot {
                codec: settings.codec_name().into(),
                level: settings.level,
                checksum: settings.checksum,
                overridden,
            },
            health: HealthSnapshot {
                services,
                incidents,
                runtime,
            },
            database_audit: self.database_registry.audit_history()?,
            history: read_history(&self.history_dir)?,
        };
        let bytes =
            serde_json::to_vec_pretty(&snapshot).context("serializing migration snapshot")?;
        if bytes.len() > MAX_SNAPSHOT_BYTES {
            bail!(
                "migration snapshot exceeds the {} MiB limit",
                MAX_SNAPSHOT_BYTES / 1024 / 1024
            );
        }
        Ok(bytes)
    }

    pub fn validate_and_apply(&self, bytes: &[u8]) -> Result<()> {
        if bytes.len() > MAX_SNAPSHOT_BYTES {
            bail!(
                "migration snapshot exceeds the {} MiB limit",
                MAX_SNAPSHOT_BYTES / 1024 / 1024
            );
        }
        let snapshot: MigrationSnapshot =
            serde_json::from_slice(bytes).context("parsing migration snapshot")?;
        validate(&snapshot, self)?;
        let current = self.export()?;
        let backup = self.data_dir.join(format!(
            ".migration-backup-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        fs::create_dir_all(&backup).context("creating pre-import backup")?;
        for name in [
            "databases.json",
            "database-state.json",
            "telegram_users.toml",
            "routing_profiles.json",
            "compression-config.json",
            "health-services.json",
            "health-incidents.json",
            "health-runtime.json",
            "database-mutations.jsonl",
        ] {
            let source = self.data_dir.join(name);
            if source.exists() {
                fs::copy(&source, backup.join(name))
                    .with_context(|| format!("backing up {name} before import"))?;
            }
        }
        if let Err(error) = self.write_files(&snapshot) {
            let _ = self.write_bytes(&current);
            let _ = fs::remove_dir_all(&backup);
            return Err(error);
        }
        if let Err(error) = self.activate(&snapshot) {
            let _ = self.write_bytes(&current);
            if let Ok(previous) = serde_json::from_slice::<MigrationSnapshot>(&current) {
                let _ = self.activate(&previous);
            }
            let _ = fs::remove_dir_all(&backup);
            return Err(error).context("activating imported dashboard state");
        }
        let _ = fs::remove_dir_all(&backup);
        Ok(())
    }

    fn write_files(&self, snapshot: &MigrationSnapshot) -> Result<()> {
        fs::create_dir_all(&self.data_dir).context("creating migration data directory")?;
        let files = self.snapshot_files(snapshot)?;
        let stage = self.data_dir.join(".migration-stage");
        fs::create_dir_all(&stage).context("creating migration staging directory")?;
        for (name, content) in files {
            let path = stage.join(name);
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).context("creating migration staging path")?;
            }
            fs::write(&path, content).context("writing migration staging file")?;
        }
        for name in [
            "databases.json",
            "database-state.json",
            "telegram_users.toml",
            "routing_profiles.json",
            "compression-config.json",
            "health-services.json",
            "health-incidents.json",
            "health-runtime.json",
            "database-mutations.jsonl",
        ] {
            let staged = stage.join(name);
            let target = self.data_dir.join(name);
            if staged.exists() {
                fs::rename(&staged, &target).with_context(|| format!("installing {name}"))?;
            } else if target.exists() {
                fs::remove_file(&target).with_context(|| format!("removing {name}"))?;
            }
        }
        fs::remove_dir_all(stage).ok();
        self.replace_history(&snapshot.history)
    }

    fn write_bytes(&self, bytes: &[u8]) -> Result<()> {
        let old: MigrationSnapshot =
            serde_json::from_slice(bytes).context("reading rollback snapshot")?;
        self.write_files(&old)
    }

    fn snapshot_files(&self, snapshot: &MigrationSnapshot) -> Result<Vec<(String, Vec<u8>)>> {
        let mut files = Vec::new();
        files.push((
            "databases.json".into(),
            serde_json::to_vec_pretty(&serde_json::json!({
                "databases": snapshot.databases.entries
            }))?,
        ));
        files.push((
            "database-state.json".into(),
            serde_json::to_vec_pretty(&serde_json::json!({
                "databases": snapshot.databases.enabled
            }))?,
        ));
        files.push((
            "telegram_users.toml".into(),
            toml::to_string_pretty(&UsersFile {
                users: &snapshot.telegram_users,
            })?
            .into_bytes(),
        ));
        files.push((
            "routing_profiles.json".into(),
            serde_json::to_vec_pretty(&snapshot.routing_profiles)?,
        ));
        if snapshot.compression.overridden {
            files.push((
                "compression-config.json".into(),
                serde_json::to_vec_pretty(&serde_json::json!({
                    "codec": snapshot.compression.codec,
                    "level": snapshot.compression.level,
                    "checksum": snapshot.compression.checksum
                }))?,
            ));
        }
        files.push((
            "health-services.json".into(),
            serde_json::to_vec_pretty(&serde_json::json!({
                "services": snapshot.health.services
            }))?,
        ));
        files.push((
            "health-incidents.json".into(),
            serde_json::to_vec_pretty(&serde_json::json!({
                "incidents": snapshot.health.incidents
            }))?,
        ));
        files.push((
            "health-runtime.json".into(),
            serde_json::to_vec_pretty(&serde_json::json!({
                "runtimes": snapshot.health.runtime
            }))?,
        ));
        let mut audit = Vec::new();
        for record in &snapshot.database_audit {
            serde_json::to_writer(&mut audit, record)?;
            audit.push(b'\n');
        }
        files.push(("database-mutations.jsonl".into(), audit));
        Ok(files)
    }

    fn activate(&self, snapshot: &MigrationSnapshot) -> Result<()> {
        self.database_registry
            .replace_dashboard_entries(snapshot.databases.entries.clone())?;
        let names = self
            .database_registry
            .config_snapshot()
            .iter()
            .map(|db| db.display_name())
            .collect::<Vec<_>>();
        self.database_states
            .replace(snapshot.databases.enabled.clone(), &names)?;
        self.telegram_users
            .replace(snapshot.telegram_users.clone())?;
        self.routing_profiles
            .replace_json(snapshot.routing_profiles.clone())?;
        let settings = CompressionSettings::from_parts(
            (snapshot.compression.codec != "none").then_some(snapshot.compression.codec.as_str()),
            snapshot.compression.level,
            snapshot.compression.checksum,
        )?;
        if snapshot.compression.overridden {
            self.compression.replace(settings, true)?;
        } else {
            self.compression.clear_override()?;
        }
        self.health.replace_snapshot(
            snapshot.health.services.clone(),
            snapshot.health.incidents.clone(),
            snapshot.health.runtime.clone(),
        )?;
        Ok(())
    }

    fn replace_history(&self, records: &[HistoryRecord]) -> Result<()> {
        fs::create_dir_all(&self.history_dir).context("creating history directory")?;
        for entry in fs::read_dir(&self.history_dir).context("scanning history directory")? {
            let path = entry?.path();
            if path.extension().and_then(|ext| ext.to_str()) == Some("jsonl") {
                fs::remove_file(path).context("removing old history file")?;
            }
        }
        let mut grouped: HashMap<String, Vec<&HistoryRecord>> = HashMap::new();
        for record in records {
            let timestamp = DateTime::parse_from_rfc3339(&record.started_at)
                .context("invalid history timestamp")?
                .with_timezone(&Utc);
            grouped
                .entry(format!("{:04}-{:02}", timestamp.year(), timestamp.month()))
                .or_default()
                .push(record);
        }
        for (month, records) in grouped {
            let path = self.history_dir.join(format!("{month}.jsonl"));
            let mut output = Vec::new();
            for record in records {
                serde_json::to_writer(&mut output, record)?;
                output.push(b'\n');
            }
            fs::write(path, output).context("writing imported history")?;
        }
        Ok(())
    }
}

fn validate(snapshot: &MigrationSnapshot, context: &MigrationContext) -> Result<()> {
    if snapshot.format_version != FORMAT_VERSION {
        bail!(
            "unsupported migration format version {}",
            snapshot.format_version
        );
    }
    let mut ids = HashSet::new();
    let mut names = HashSet::new();
    for entry in &snapshot.databases.entries {
        if entry.id.trim().is_empty() || !ids.insert(entry.id.clone()) {
            bail!("duplicate or blank managed database ID");
        }
        let name = entry
            .name
            .as_deref()
            .map(str::to_string)
            .unwrap_or_else(|| {
                entry
                    .url
                    .split('?')
                    .next()
                    .unwrap_or(&entry.url)
                    .rsplit('/')
                    .next()
                    .unwrap_or("unknown-db")
                    .to_string()
            });
        if !names.insert(name.to_ascii_lowercase()) {
            bail!("duplicate managed database name");
        }
        if !(entry.url.starts_with("postgres://") || entry.url.starts_with("postgresql://")) {
            bail!("managed database URL must use postgres:// or postgresql://");
        }
    }
    if snapshot.databases.entries.len()
        + context
            .database_registry
            .snapshot()
            .iter()
            .filter(|db| matches!(db.source, crate::database_registry::DatabaseSource::Env))
            .count()
        > 10
    {
        bail!("imported databases exceed the configured resource limit");
    }
    let database_names = snapshot
        .databases
        .entries
        .iter()
        .map(|entry| {
            entry.name.clone().unwrap_or_else(|| {
                entry
                    .url
                    .split('?')
                    .next()
                    .unwrap_or(&entry.url)
                    .rsplit('/')
                    .next()
                    .unwrap_or("unknown-db")
                    .to_string()
            })
        })
        .collect::<HashSet<_>>();
    if snapshot
        .databases
        .enabled
        .keys()
        .any(|name| !database_names.contains(name))
    {
        bail!("database enablement references an unknown database");
    }
    let service_names = snapshot
        .health
        .services
        .iter()
        .map(|service| service.name.as_str())
        .collect::<HashSet<_>>();
    if snapshot.health.services.len() > 500 || snapshot.health.incidents.len() > 500 {
        bail!("health snapshot exceeds resource limits");
    }
    if snapshot
        .health
        .incidents
        .iter()
        .any(|incident| !service_names.contains(incident.service.as_str()))
        || snapshot
            .health
            .runtime
            .keys()
            .any(|name| !service_names.contains(name.as_str()))
    {
        bail!("health snapshot contains an invalid service reference");
    }
    if snapshot.history.len() > MAX_HISTORY_RECORDS {
        bail!("history snapshot exceeds resource limits");
    }
    let mut user_ids = HashSet::new();
    for user in &snapshot.telegram_users {
        if user.name.trim().is_empty()
            || user.chat_id.trim().is_empty()
            || !user_ids.insert(&user.chat_id)
        {
            bail!("Telegram user directory contains an invalid or duplicate chat ID");
        }
    }
    let routing_object = snapshot
        .routing_profiles
        .as_object()
        .context("routing profiles section must be an object")?;
    let profiles = routing_object
        .get("profiles")
        .and_then(Value::as_array)
        .context("routing profiles are missing")?;
    let mut profile_ids = HashSet::new();
    for profile in profiles {
        let id = profile
            .get("id")
            .and_then(Value::as_str)
            .context("routing profile ID is missing")?;
        if !profile_ids.insert(id) {
            bail!("duplicate routing profile ID");
        }
    }
    if let Some(active) = routing_object.get("active_id").and_then(Value::as_str) {
        if !profile_ids.contains(active) {
            bail!("active routing profile does not exist");
        }
    }
    CompressionSettings::from_parts(
        (snapshot.compression.codec != "none").then_some(snapshot.compression.codec.as_str()),
        snapshot.compression.level,
        snapshot.compression.checksum,
    )?;
    Ok(())
}

fn read_history(directory: &Path) -> Result<Vec<HistoryRecord>> {
    let mut records = Vec::new();
    let entries = match fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(records),
        Err(error) => return Err(error).context("reading history directory"),
    };
    for entry in entries {
        let path = entry?.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("jsonl") {
            continue;
        }
        for line in fs::read_to_string(&path)?.lines() {
            if let Ok(record) = serde_json::from_str(line) {
                records.push(record);
            }
        }
    }
    if records.len() > MAX_HISTORY_RECORDS {
        bail!("history exceeds resource limits");
    }
    Ok(records)
}
