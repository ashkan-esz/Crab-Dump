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
use crate::config::DatabaseConfig;
use crate::database_registry::{
    DashboardDatabase, DatabaseMutation, DatabaseRegistry, DatabaseSource, RuntimeDatabase,
};
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
        let (services, mut incidents, mut runtime) = self.health.snapshot();
        remove_unknown_health_references(&services, &mut incidents, &mut runtime);
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
        let mut snapshot: MigrationSnapshot =
            serde_json::from_slice(bytes).context("parsing migration snapshot")?;
        remove_unknown_health_references(
            &snapshot.health.services,
            &mut snapshot.health.incidents,
            &mut snapshot.health.runtime,
        );
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
        let name = imported_database_name(entry);
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
    validate_database_enablement_keys(
        &snapshot.databases.enabled,
        &snapshot.databases.entries,
        &context.database_registry.snapshot(),
    )?;
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

fn remove_unknown_health_references(
    services: &[ServiceDefinition],
    incidents: &mut Vec<Incident>,
    runtime: &mut HashMap<String, ServiceRuntime>,
) {
    let service_names = services
        .iter()
        .map(|service| service.name.as_str())
        .collect::<HashSet<_>>();
    incidents.retain(|incident| service_names.contains(incident.service.as_str()));
    runtime.retain(|name, _| service_names.contains(name.as_str()));
}

fn imported_database_name(entry: &DashboardDatabase) -> String {
    DatabaseConfig {
        url: entry.url.clone(),
        name: entry.name.clone(),
        pg_dump_extra_args: entry.pg_dump_extra_args.clone(),
    }
    .display_name()
}

fn validate_database_enablement_keys(
    enabled: &HashMap<String, bool>,
    imported_entries: &[DashboardDatabase],
    current_databases: &[RuntimeDatabase],
) -> Result<()> {
    let allowed_names = imported_entries
        .iter()
        .map(imported_database_name)
        .chain(
            current_databases
                .iter()
                .filter(|database| database.source == DatabaseSource::Env)
                .map(|database| database.config.display_name()),
        )
        .collect::<HashSet<_>>();
    if enabled.keys().any(|name| !allowed_names.contains(name)) {
        bail!("database enablement references an unknown database");
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn dashboard_entry(name: Option<&str>, url: &str) -> DashboardDatabase {
        DashboardDatabase {
            id: "dashboard-1".into(),
            url: url.into(),
            name: name.map(str::to_string),
            pg_dump_extra_args: None,
            created_at: "2026-01-01T00:00:00Z".into(),
            updated_at: "2026-01-01T00:00:00Z".into(),
        }
    }

    fn environment_database(name: Option<&str>, url: &str) -> RuntimeDatabase {
        RuntimeDatabase {
            id: None,
            source: DatabaseSource::Env,
            config: DatabaseConfig {
                url: url.into(),
                name: name.map(str::to_string),
                pg_dump_extra_args: None,
            },
            created_at: None,
            updated_at: None,
        }
    }

    #[test]
    fn enablement_allows_imported_and_environment_databases() {
        let imported = dashboard_entry(Some("dashboard"), "postgres://host/imported");
        let environment = environment_database(None, "postgres://host/environment");
        let enabled = HashMap::from([
            ("dashboard".to_string(), true),
            ("environment".to_string(), false),
        ]);

        assert!(validate_database_enablement_keys(&enabled, &[imported], &[environment],).is_ok());
    }

    #[test]
    fn enablement_rejects_unknown_databases() {
        let enabled = HashMap::from([("missing".to_string(), true)]);

        let error = validate_database_enablement_keys(
            &enabled,
            &[dashboard_entry(
                Some("dashboard"),
                "postgres://host/imported",
            )],
            &[environment_database(None, "postgres://host/environment")],
        )
        .expect_err("unknown enablement key must be rejected");

        assert!(error
            .to_string()
            .contains("database enablement references an unknown database"));
    }

    #[test]
    fn imported_enablement_uses_database_config_display_name() {
        let imported = dashboard_entry(None, "postgres://host/imported?sslmode=require");
        let enabled = HashMap::from([("imported".to_string(), true)]);

        assert!(validate_database_enablement_keys(&enabled, &[imported], &[]).is_ok());
    }

    #[test]
    fn export_health_cleanup_removes_stale_references() {
        let services = vec![ServiceDefinition {
            name: "current".into(),
            url: "https://example.test/health".into(),
            expected_status: 200,
            interval_secs: 60,
            retries: 1,
            failure_threshold: 1,
            version_header: "X-Version".into(),
            recipients: Vec::new(),
            use_active_routing_profile: false,
            enabled: true,
            created_at: "2026-01-01T00:00:00Z".into(),
            updated_at: "2026-01-01T00:00:00Z".into(),
        }];
        let mut incidents = vec![
            Incident {
                id: 1,
                service: "current".into(),
                event: "recovery".into(),
                timestamp: "2026-01-01T00:00:00Z".into(),
                reason: None,
                status_code: Some(200),
                version: None,
                last_up_version: None,
                consecutive_failures: 0,
                acknowledged: false,
            },
            Incident {
                id: 2,
                service: "deleted".into(),
                event: "outage".into(),
                timestamp: "2026-01-01T00:00:00Z".into(),
                reason: Some("gone".into()),
                status_code: None,
                version: None,
                last_up_version: None,
                consecutive_failures: 1,
                acknowledged: false,
            },
        ];
        let mut runtime = HashMap::from([
            (
                "current".into(),
                ServiceRuntime {
                    name: "current".into(),
                    status: Default::default(),
                    consecutive_failures: 0,
                    last_check: None,
                    last_success: None,
                    last_failure: None,
                    current_version: None,
                    last_observed_version: None,
                    last_up_version: None,
                    last_reason: None,
                    last_status_code: None,
                    latency_ms: None,
                    last_error: None,
                },
            ),
            (
                "deleted".into(),
                ServiceRuntime {
                    name: "deleted".into(),
                    status: Default::default(),
                    consecutive_failures: 1,
                    last_check: None,
                    last_success: None,
                    last_failure: None,
                    current_version: None,
                    last_observed_version: None,
                    last_up_version: None,
                    last_reason: None,
                    last_status_code: None,
                    latency_ms: None,
                    last_error: None,
                },
            ),
        ]);

        remove_unknown_health_references(&services, &mut incidents, &mut runtime);

        assert_eq!(incidents.len(), 1);
        assert_eq!(incidents[0].service, "current");
        assert_eq!(runtime.len(), 1);
        assert!(runtime.contains_key("current"));
    }
}
