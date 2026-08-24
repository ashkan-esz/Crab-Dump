//! Persistent registry for environment and dashboard-managed databases.

use anyhow::{bail, Context, Result};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use crate::config::{parse_pg_dump_extra_args, DatabaseConfig};

static ID_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum DatabaseSource {
    Env,
    Dashboard,
}

#[derive(Debug, Clone)]
pub struct RuntimeDatabase {
    pub id: Option<String>,
    pub source: DatabaseSource,
    pub config: DatabaseConfig,
    pub created_at: Option<String>,
    pub updated_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DashboardDatabase {
    pub id: String,
    pub url: String,
    pub name: Option<String>,
    pub pg_dump_extra_args: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct RegistryFile {
    #[serde(default)]
    databases: Vec<DashboardDatabase>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseMutation {
    pub actor: String,
    pub timestamp: String,
    pub action: String,
    pub database_id: Option<String>,
    pub database_name: String,
    pub source: DatabaseSource,
    pub result: String,
}

#[derive(Debug)]
pub struct DatabaseRegistry {
    path: PathBuf,
    audit_path: PathBuf,
    max_databases: usize,
    env: Vec<RuntimeDatabase>,
    dashboard: Mutex<Vec<DashboardDatabase>>,
    audit: Mutex<()>,
    snapshot: RwLock<Vec<RuntimeDatabase>>,
}

impl DatabaseRegistry {
    pub fn load(
        data_dir: impl Into<PathBuf>,
        env_databases: Vec<DatabaseConfig>,
        max_databases: usize,
    ) -> Result<Arc<Self>> {
        let data_dir = data_dir.into();
        let path = data_dir.join("databases.json");
        let dashboard = match fs::read_to_string(&path) {
            Ok(contents) => match serde_json::from_str::<RegistryFile>(&contents) {
                Ok(file) => file.databases,
                Err(error) => {
                    tracing::warn!(path = %path.display(), error = %error, "malformed dashboard database registry; starting with no dashboard entries");
                    Vec::new()
                }
            },
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("reading dashboard database registry {}", path.display())
                })
            }
        };
        let env = env_databases
            .into_iter()
            .map(|config| RuntimeDatabase {
                id: None,
                source: DatabaseSource::Env,
                config,
                created_at: None,
                updated_at: None,
            })
            .collect::<Vec<_>>();
        let registry = Arc::new(Self {
            path,
            audit_path: data_dir.join("database-mutations.jsonl"),
            max_databases,
            env,
            dashboard: Mutex::new(dashboard),
            audit: Mutex::new(()),
            snapshot: RwLock::new(Vec::new()),
        });
        registry.validate_all()?;
        registry.refresh_snapshot()?;
        Ok(registry)
    }

    pub fn snapshot(&self) -> Vec<RuntimeDatabase> {
        self.snapshot
            .read()
            .expect("database registry snapshot lock poisoned")
            .clone()
    }

    pub fn config_snapshot(&self) -> Vec<DatabaseConfig> {
        self.snapshot().into_iter().map(|db| db.config).collect()
    }

    pub fn dashboard_entries(&self) -> Vec<DashboardDatabase> {
        self.dashboard
            .lock()
            .expect("dashboard database registry lock poisoned")
            .clone()
    }

    pub fn replace_dashboard_entries(&self, entries: Vec<DashboardDatabase>) -> Result<()> {
        let mut candidate = self.env.clone();
        candidate.extend(entries.iter().map(runtime_from_entry));
        validate_runtime(&candidate, self.max_databases)?;
        let mut dashboard = self
            .dashboard
            .lock()
            .expect("dashboard database registry lock poisoned");
        self.persist_locked(&entries)?;
        *dashboard = entries;
        drop(dashboard);
        self.refresh_snapshot()
    }

    pub fn refresh_snapshot(&self) -> Result<()> {
        let mut all = self.env.clone();
        all.extend(
            self.dashboard_entries()
                .into_iter()
                .map(|entry| RuntimeDatabase {
                    id: Some(entry.id),
                    source: DatabaseSource::Dashboard,
                    config: DatabaseConfig {
                        url: entry.url,
                        name: entry.name,
                        pg_dump_extra_args: entry.pg_dump_extra_args,
                    },
                    created_at: Some(entry.created_at),
                    updated_at: Some(entry.updated_at),
                }),
        );
        validate_runtime(&all, self.max_databases)?;
        *self
            .snapshot
            .write()
            .expect("database registry snapshot lock poisoned") = all;
        Ok(())
    }

    pub fn add(
        &self,
        url: String,
        name: Option<String>,
        pg_dump_extra_args: Option<String>,
    ) -> Result<DashboardDatabase> {
        let mut entries = self
            .dashboard
            .lock()
            .expect("dashboard database registry lock poisoned");
        let mut candidate = entries
            .iter()
            .map(|entry| RuntimeDatabase {
                id: Some(entry.id.clone()),
                source: DatabaseSource::Dashboard,
                config: DatabaseConfig {
                    url: entry.url.clone(),
                    name: entry.name.clone(),
                    pg_dump_extra_args: entry.pg_dump_extra_args.clone(),
                },
                created_at: Some(entry.created_at.clone()),
                updated_at: Some(entry.updated_at.clone()),
            })
            .collect::<Vec<_>>();
        let now = Utc::now().to_rfc3339();
        let entry = DashboardDatabase {
            id: new_id(),
            url,
            name,
            pg_dump_extra_args,
            created_at: now.clone(),
            updated_at: now,
        };
        candidate.push(RuntimeDatabase {
            id: Some(entry.id.clone()),
            source: DatabaseSource::Dashboard,
            config: DatabaseConfig {
                url: entry.url.clone(),
                name: entry.name.clone(),
                pg_dump_extra_args: entry.pg_dump_extra_args.clone(),
            },
            created_at: Some(entry.created_at.clone()),
            updated_at: Some(entry.updated_at.clone()),
        });
        let mut all = self.env.clone();
        all.append(&mut candidate);
        validate_runtime(&all, self.max_databases)?;
        entries.push(entry.clone());
        self.persist_locked(&entries)?;
        drop(entries);
        self.refresh_snapshot()?;
        Ok(entry)
    }

    pub fn update(
        &self,
        id: &str,
        url: Option<String>,
        name: Option<String>,
        pg_dump_extra_args: Option<String>,
    ) -> Result<Option<DashboardDatabase>> {
        let mut entries = self
            .dashboard
            .lock()
            .expect("dashboard database registry lock poisoned");
        let Some(index) = entries.iter().position(|entry| entry.id == id) else {
            return Ok(None);
        };
        let mut candidate = entries[index].clone();
        if let Some(url) = url {
            candidate.url = url;
        }
        if name.is_some() {
            candidate.name = name;
        }
        candidate.pg_dump_extra_args = pg_dump_extra_args;
        candidate.updated_at = Utc::now().to_rfc3339();
        let mut all = self.env.clone();
        for (entry_index, entry) in entries.iter().enumerate() {
            let entry = if entry_index == index {
                &candidate
            } else {
                entry
            };
            all.push(runtime_from_entry(entry));
        }
        validate_runtime(&all, self.max_databases)?;
        entries[index] = candidate.clone();
        self.persist_locked(&entries)?;
        drop(entries);
        self.refresh_snapshot()?;
        Ok(Some(candidate))
    }

    pub fn delete(&self, id: &str) -> Result<Option<DashboardDatabase>> {
        let mut entries = self
            .dashboard
            .lock()
            .expect("dashboard database registry lock poisoned");
        let Some(index) = entries.iter().position(|entry| entry.id == id) else {
            return Ok(None);
        };
        let entry = entries.remove(index);
        self.persist_locked(&entries)?;
        drop(entries);
        self.refresh_snapshot()?;
        Ok(Some(entry))
    }

    pub fn append_audit(&self, mutation: &DatabaseMutation) -> Result<()> {
        let _guard = self.audit.lock().expect("database audit lock poisoned");
        if let Some(parent) = self.audit_path.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!("creating database audit directory {}", parent.display())
            })?;
        }
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.audit_path)
            .with_context(|| format!("opening database audit {}", self.audit_path.display()))?;
        set_owner_only(&self.audit_path)?;
        serde_json::to_writer(&mut file, mutation).context("serializing database mutation")?;
        use std::io::Write;
        file.write_all(b"\n")
            .context("appending database mutation")?;
        file.sync_data().context("syncing database mutation")?;
        Ok(())
    }

    pub fn audit_history(&self) -> Result<Vec<DatabaseMutation>> {
        let contents = match fs::read_to_string(&self.audit_path) {
            Ok(contents) => contents,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(error).context("reading database mutation history"),
        };
        Ok(contents
            .lines()
            .filter_map(|line| serde_json::from_str(line).ok())
            .collect())
    }

    fn validate_all(&self) -> Result<()> {
        let mut all = self.env.clone();
        all.extend(
            self.dashboard_entries()
                .into_iter()
                .map(|entry| runtime_from_entry(&entry)),
        );
        validate_runtime(&all, self.max_databases)
    }

    fn persist_locked(&self, entries: &[DashboardDatabase]) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!("creating database registry directory {}", parent.display())
            })?;
        }
        let tmp = self.path.with_extension("json.tmp");
        let bytes = serde_json::to_vec_pretty(&RegistryFile {
            databases: entries.to_vec(),
        })
        .context("serializing dashboard database registry")?;
        fs::write(&tmp, bytes)
            .with_context(|| format!("writing temporary database registry {}", tmp.display()))?;
        set_owner_only(&tmp)?;
        fs::rename(&tmp, &self.path).with_context(|| {
            format!(
                "atomically replacing database registry {}",
                self.path.display()
            )
        })?;
        Ok(())
    }
}

#[cfg(unix)]
fn set_owner_only(path: &std::path::Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let mut permissions = fs::metadata(path)
        .with_context(|| format!("reading permissions for {}", path.display()))?
        .permissions();
    permissions.set_mode(0o600);
    fs::set_permissions(path, permissions)
        .with_context(|| format!("restricting permissions for {}", path.display()))?;
    Ok(())
}

#[cfg(not(unix))]
fn set_owner_only(_path: &std::path::Path) -> Result<()> {
    Ok(())
}

fn runtime_from_entry(entry: &DashboardDatabase) -> RuntimeDatabase {
    RuntimeDatabase {
        id: Some(entry.id.clone()),
        source: DatabaseSource::Dashboard,
        config: DatabaseConfig {
            url: entry.url.clone(),
            name: entry.name.clone(),
            pg_dump_extra_args: entry.pg_dump_extra_args.clone(),
        },
        created_at: Some(entry.created_at.clone()),
        updated_at: Some(entry.updated_at.clone()),
    }
}

fn validate_runtime(databases: &[RuntimeDatabase], max: usize) -> Result<()> {
    if databases.is_empty() {
        bail!("at least one database is required");
    }
    if databases.len() > max {
        bail!(
            "too many databases: {} exceeds maximum {}",
            databases.len(),
            max
        );
    }
    let mut names = std::collections::HashSet::new();
    for database in databases {
        let name = database.config.display_name();
        if name.trim().is_empty()
            || !name
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || "-_.".contains(c))
        {
            bail!("invalid database name `{name}`");
        }
        if !names.insert(name.to_ascii_lowercase()) {
            bail!("duplicate database name `{name}`");
        }
        if !(database.config.url.starts_with("postgres://")
            || database.config.url.starts_with("postgresql://"))
        {
            bail!("database URL must use postgres:// or postgresql://");
        }
        if !database.config.url.contains('/') {
            bail!("database URL is missing a database name");
        }
        if let Some(extra_args) = database.config.pg_dump_extra_args.as_deref() {
            parse_pg_dump_extra_args(extra_args)
                .context("invalid PG_DUMP_EXTRA_ARGS for dashboard database")?;
        }
    }
    Ok(())
}

fn new_id() -> String {
    let count = ID_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!(
        "db-{}-{}",
        Utc::now().timestamp_nanos_opt().unwrap_or_default(),
        count
    )
}

pub fn redact_url(url: &str) -> String {
    let Some(scheme_end) = url.find("://") else {
        return "<redacted>".into();
    };
    let authority_start = scheme_end + 3;
    let Some(path_offset) = url[authority_start..].find('/') else {
        return url.to_string();
    };
    let authority_end = authority_start + path_offset;
    let authority = &url[authority_start..authority_end];
    let Some(at) = authority.rfind('@') else {
        return url.to_string();
    };
    let userinfo = &authority[..at];
    let user = userinfo.split(':').next().unwrap_or("");
    format!(
        "{}{}:***@{}{}",
        &url[..authority_start],
        user,
        &authority[at + 1..],
        &url[authority_end..]
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env_db(name: &str) -> DatabaseConfig {
        DatabaseConfig {
            url: format!("postgresql://user:secret@example/{name}"),
            name: Some(name.into()),
            pg_dump_extra_args: None,
        }
    }

    #[test]
    fn redacts_password_without_hiding_host() {
        assert_eq!(
            redact_url("postgresql://alice:secret@example/db"),
            "postgresql://alice:***@example/db"
        );
    }

    #[test]
    fn dashboard_entries_persist_reload_and_enforce_names_and_limit() {
        let root = std::env::temp_dir().join(format!(
            "crab-dump-registry-{}-{}",
            std::process::id(),
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let registry = DatabaseRegistry::load(&root, vec![env_db("env_db")], 2).unwrap();
        let entry = registry
            .add(
                "postgresql://dashboard:secret@example/app_db".into(),
                Some("app_db".into()),
                None,
            )
            .unwrap();
        assert!(root.join("databases.json").exists());
        assert_eq!(registry.config_snapshot().len(), 2);
        let reloaded =
            DatabaseRegistry::load(&root, vec![env_db("env_db")], 3).expect("reload registry");
        assert_eq!(reloaded.dashboard_entries()[0].id, entry.id);
        assert!(reloaded
            .add(
                "postgresql://dashboard:secret@example/other".into(),
                Some("other".into()),
                None,
            )
            .is_ok());
        assert!(reloaded
            .add(
                "postgresql://dashboard:secret@example/other".into(),
                Some("ENV_DB".into()),
                None,
            )
            .is_err());
        std::fs::remove_dir_all(root).ok();
    }
}
