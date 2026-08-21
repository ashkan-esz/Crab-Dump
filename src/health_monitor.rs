//! Dashboard-managed HTTP service health monitoring.

use anyhow::{Context, Result};
use chrono::Utc;
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::telegram;
use crate::telegram_users::TelegramUserStore;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_INCIDENTS: usize = 500;

pub type SharedClient = Arc<RwLock<Arc<Client>>>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ServiceDefinition {
    pub name: String,
    pub url: String,
    pub expected_status: u16,
    pub interval_secs: u64,
    pub retries: u32,
    pub failure_threshold: u32,
    pub version_header: String,
    #[serde(default)]
    pub recipients: Vec<String>,
    pub enabled: bool,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum ServiceStatus {
    #[default]
    Unknown,
    Up,
    Down,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceRuntime {
    pub name: String,
    pub status: ServiceStatus,
    pub consecutive_failures: u32,
    pub last_check: Option<String>,
    pub last_success: Option<String>,
    pub last_failure: Option<String>,
    pub current_version: Option<String>,
    pub last_observed_version: Option<String>,
    pub last_reason: Option<String>,
    pub last_status_code: Option<u16>,
    pub latency_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Incident {
    pub id: u64,
    pub service: String,
    pub event: String,
    pub timestamp: String,
    pub reason: Option<String>,
    pub status_code: Option<u16>,
    pub version: Option<String>,
    pub consecutive_failures: u32,
    pub acknowledged: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServiceInput {
    pub name: String,
    pub url: String,
    pub expected_status: Option<u16>,
    pub interval_secs: Option<u64>,
    pub retries: Option<u32>,
    pub failure_threshold: Option<u32>,
    pub version_header: Option<String>,
    pub recipients: Option<Vec<String>>,
    pub enabled: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct DefinitionsFile {
    #[serde(default)]
    services: Vec<ServiceDefinition>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct IncidentsFile {
    #[serde(default)]
    incidents: Vec<Incident>,
}

struct EventData {
    reason: Option<String>,
    status_code: Option<u16>,
    version: Option<String>,
    failures: u32,
}

#[derive(Debug)]
struct Store {
    definitions_path: PathBuf,
    incidents_path: PathBuf,
    definitions: Mutex<Vec<ServiceDefinition>>,
    incidents: Mutex<Vec<Incident>>,
    runtime: Mutex<HashMap<String, ServiceRuntime>>,
}

#[derive(Debug)]
pub struct HealthMonitor {
    store: Arc<Store>,
    client: SharedClient,
    bot_token: String,
    users: Arc<TelegramUserStore>,
    wake: Arc<(Mutex<bool>, Condvar)>,
}

impl HealthMonitor {
    pub fn load(
        data_dir: impl Into<PathBuf>,
        client: SharedClient,
        bot_token: String,
        users: Arc<TelegramUserStore>,
    ) -> Result<Arc<Self>> {
        let data_dir = data_dir.into();
        fs::create_dir_all(&data_dir).with_context(|| {
            format!(
                "creating health monitor data directory {}",
                data_dir.display()
            )
        })?;
        let definitions_path = data_dir.join("health-services.json");
        let incidents_path = data_dir.join("health-incidents.json");
        let definitions = read_json::<DefinitionsFile>(&definitions_path)?.services;
        validate_definitions(&definitions)?;
        let incidents = read_json::<IncidentsFile>(&incidents_path)?.incidents;
        Ok(Arc::new(Self {
            store: Arc::new(Store {
                definitions_path,
                incidents_path,
                definitions: Mutex::new(definitions),
                incidents: Mutex::new(incidents),
                runtime: Mutex::new(HashMap::new()),
            }),
            client,
            bot_token,
            users,
            wake: Arc::new((Mutex::new(false), Condvar::new())),
        }))
    }

    pub fn start(self: &Arc<Self>) {
        let monitor = Arc::clone(self);
        thread::spawn(move || monitor.run());
    }

    pub fn list(&self) -> Vec<ServiceDefinition> {
        self.store
            .definitions
            .lock()
            .expect("health definitions lock poisoned")
            .clone()
    }

    pub fn get(&self, name: &str) -> Option<ServiceDefinition> {
        self.list().into_iter().find(|service| service.name == name)
    }

    pub fn runtime(&self, name: &str) -> ServiceRuntime {
        self.store
            .runtime
            .lock()
            .expect("health runtime lock poisoned")
            .get(name)
            .cloned()
            .unwrap_or_else(|| ServiceRuntime {
                name: name.to_string(),
                status: ServiceStatus::Unknown,
                consecutive_failures: 0,
                last_check: None,
                last_success: None,
                last_failure: None,
                current_version: None,
                last_observed_version: None,
                last_reason: None,
                last_status_code: None,
                latency_ms: None,
            })
    }

    pub fn details(&self, name: &str) -> Option<(ServiceDefinition, ServiceRuntime)> {
        self.get(name).map(|service| {
            let runtime = self.runtime(&service.name);
            (service, runtime)
        })
    }

    pub fn incidents(&self, name: &str, page: usize, page_size: usize) -> (Vec<Incident>, usize) {
        let page = page.max(1);
        let page_size = page_size.clamp(1, 100);
        let incidents = self
            .store
            .incidents
            .lock()
            .expect("health incidents lock poisoned");
        let mut selected = incidents
            .iter()
            .filter(|incident| incident.service == name)
            .cloned()
            .collect::<Vec<_>>();
        selected.reverse();
        let total = selected.len();
        let start = (page - 1).saturating_mul(page_size);
        let records = selected.into_iter().skip(start).take(page_size).collect();
        (records, total)
    }

    pub fn acknowledge(&self, id: u64, acknowledged: bool) -> bool {
        let mut incidents = self
            .store
            .incidents
            .lock()
            .expect("health incidents lock poisoned");
        let Some(index) = incidents.iter().position(|incident| incident.id == id) else {
            return false;
        };
        let mut next = incidents.clone();
        next[index].acknowledged = acknowledged;
        if persist_json(
            &self.store.incidents_path,
            &IncidentsFile {
                incidents: next.clone(),
            },
        )
        .is_err()
        {
            return false;
        }
        *incidents = next;
        true
    }

    pub fn create(&self, input: ServiceInput) -> Result<ServiceDefinition> {
        let service = make_definition(input, None)?;
        let mut definitions = self
            .store
            .definitions
            .lock()
            .expect("health definitions lock poisoned");
        if definitions
            .iter()
            .any(|existing| existing.name == service.name)
        {
            anyhow::bail!("service name already exists");
        }
        let mut next = definitions.clone();
        next.push(service.clone());
        persist_json(
            &self.store.definitions_path,
            &DefinitionsFile {
                services: next.clone(),
            },
        )?;
        *definitions = next;
        drop(definitions);
        self.wake_now();
        Ok(service)
    }

    pub fn update(&self, name: &str, input: ServiceInput) -> Result<Option<ServiceDefinition>> {
        let mut service = make_definition(input, Some(name))?;
        let mut definitions = self
            .store
            .definitions
            .lock()
            .expect("health definitions lock poisoned");
        let Some(index) = definitions
            .iter()
            .position(|existing| existing.name == name)
        else {
            return Ok(None);
        };
        if service.name != name
            && definitions
                .iter()
                .any(|existing| existing.name == service.name)
        {
            anyhow::bail!("service name already exists");
        }
        service.created_at = definitions[index].created_at.clone();
        let mut next = definitions.clone();
        next[index] = service.clone();
        persist_json(
            &self.store.definitions_path,
            &DefinitionsFile {
                services: next.clone(),
            },
        )?;
        *definitions = next;
        drop(definitions);
        self.wake_now();
        Ok(Some(service))
    }

    pub fn delete(&self, name: &str) -> Result<bool> {
        let mut definitions = self
            .store
            .definitions
            .lock()
            .expect("health definitions lock poisoned");
        let old_len = definitions.len();
        let mut next = definitions.clone();
        next.retain(|service| service.name != name);
        if next.len() == old_len {
            return Ok(false);
        }
        persist_json(
            &self.store.definitions_path,
            &DefinitionsFile {
                services: next.clone(),
            },
        )?;
        *definitions = next;
        self.store
            .runtime
            .lock()
            .expect("health runtime lock poisoned")
            .remove(name);
        self.wake_now();
        Ok(true)
    }

    fn wake_now(&self) {
        let (lock, wake) = &*self.wake;
        *lock.lock().expect("health wake lock poisoned") = true;
        wake.notify_one();
    }

    fn run(self: Arc<Self>) {
        loop {
            let definitions = self
                .list()
                .into_iter()
                .filter(|service| service.enabled)
                .collect::<Vec<_>>();
            for service in definitions {
                if self.should_check(&service.name, service.interval_secs) {
                    self.check(&service);
                }
            }
            let (lock, wake) = &*self.wake;
            let mut changed = lock.lock().expect("health wake lock poisoned");
            if !*changed {
                let _ = wake
                    .wait_timeout(changed, Duration::from_secs(1))
                    .expect("health wake wait poisoned");
            } else {
                *changed = false;
            }
        }
    }

    fn should_check(&self, name: &str, interval_secs: u64) -> bool {
        let runtime = self
            .store
            .runtime
            .lock()
            .expect("health runtime lock poisoned");
        runtime
            .get(name)
            .and_then(|entry| entry.last_check.as_ref())
            .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
            .map(|last| {
                Utc::now()
                    .signed_duration_since(last.with_timezone(&Utc))
                    .num_seconds()
                    >= interval_secs as i64
            })
            .unwrap_or(true)
    }

    fn check(&self, service: &ServiceDefinition) {
        let started = std::time::Instant::now();
        let mut result = Err("request failed".to_string());
        let mut status_code = None;
        let mut version = None;
        for attempt in 0..=service.retries {
            let client = self
                .client
                .read()
                .expect("health client lock poisoned")
                .clone();
            match client.get(&service.url).timeout(REQUEST_TIMEOUT).send() {
                Ok(response) => {
                    let code = response.status().as_u16();
                    let observed = response
                        .headers()
                        .get(&service.version_header)
                        .and_then(|value| value.to_str().ok())
                        .map(str::to_string);
                    status_code = Some(code);
                    version = observed.clone();
                    if code == service.expected_status {
                        result = Ok(());
                        break;
                    }
                    result = Err(format!("unexpected HTTP status {code}"));
                }
                Err(error) => {
                    result = Err(if error.is_timeout() {
                        "request timed out".into()
                    } else {
                        "network request failed".into()
                    })
                }
            }
            if attempt < service.retries {
                thread::sleep(Duration::from_millis(100));
            }
        }
        self.record_check(service, result, status_code, version, started.elapsed());
    }

    fn record_check(
        &self,
        service: &ServiceDefinition,
        result: Result<(), String>,
        status_code: Option<u16>,
        version: Option<String>,
        elapsed: Duration,
    ) {
        let now = Utc::now().to_rfc3339();
        let mut runtime_map = self
            .store
            .runtime
            .lock()
            .expect("health runtime lock poisoned");
        let entry = runtime_map
            .entry(service.name.clone())
            .or_insert_with(|| ServiceRuntime {
                name: service.name.clone(),
                status: ServiceStatus::Unknown,
                consecutive_failures: 0,
                last_check: None,
                last_success: None,
                last_failure: None,
                current_version: None,
                last_observed_version: None,
                last_reason: None,
                last_status_code: None,
                latency_ms: None,
            });
        let previous = entry.status;
        entry.last_check = Some(now.clone());
        entry.last_status_code = status_code;
        entry.latency_ms = Some(elapsed.as_millis() as u64);
        entry.last_observed_version = version.clone();
        match result {
            Ok(()) => {
                entry.consecutive_failures = 0;
                entry.last_success = Some(now.clone());
                entry.last_reason = None;
                entry.current_version = version;
                entry.status = ServiceStatus::Up;
                if previous == ServiceStatus::Down {
                    self.event(
                        service,
                        "recovery",
                        &now,
                        EventData {
                            reason: None,
                            status_code,
                            version: entry.current_version.clone(),
                            failures: 0,
                        },
                    );
                }
            }
            Err(reason) => {
                entry.consecutive_failures = entry.consecutive_failures.saturating_add(1);
                entry.last_failure = Some(now.clone());
                entry.last_reason = Some(reason.clone());
                if entry.consecutive_failures >= service.failure_threshold {
                    entry.status = ServiceStatus::Down;
                    entry.current_version = None;
                    if previous != ServiceStatus::Down {
                        self.event(
                            service,
                            "outage",
                            &now,
                            EventData {
                                reason: Some(reason),
                                status_code,
                                version,
                                failures: entry.consecutive_failures,
                            },
                        );
                    }
                }
            }
        }
    }

    fn event(&self, service: &ServiceDefinition, event: &str, timestamp: &str, data: EventData) {
        let mut incidents = self
            .store
            .incidents
            .lock()
            .expect("health incidents lock poisoned");
        let id = incidents
            .iter()
            .map(|incident| incident.id)
            .max()
            .unwrap_or(0)
            .saturating_add(1);
        incidents.push(Incident {
            id,
            service: service.name.clone(),
            event: event.into(),
            timestamp: timestamp.into(),
            reason: data.reason,
            status_code: data.status_code,
            version: data.version,
            consecutive_failures: data.failures,
            acknowledged: false,
        });
        if incidents.len() > MAX_INCIDENTS {
            let excess = incidents.len() - MAX_INCIDENTS;
            incidents.drain(0..excess);
        }
        if let Err(error) = persist_json(
            &self.store.incidents_path,
            &IncidentsFile {
                incidents: incidents.clone(),
            },
        ) {
            tracing::warn!(error = %error, "health incident persistence failed");
        }
        let recipients = self
            .users
            .list()
            .into_iter()
            .filter(|user| user.enabled && service.recipients.iter().any(|id| id == &user.chat_id))
            .map(|user| user.chat_id)
            .collect::<Vec<_>>();
        let text = if event == "outage" {
            format!(
                "⚠️ <b>{}</b> is DOWN after {} failed checks.",
                service.name, data.failures
            )
        } else {
            format!("✅ <b>{}</b> recovered and is UP.", service.name)
        };
        for chat_id in recipients {
            let client = self
                .client
                .read()
                .expect("health client lock poisoned")
                .clone();
            if let Err(error) = telegram::send_message(&client, &self.bot_token, &chat_id, &text) {
                tracing::warn!(service = %service.name, error = %error, "health alert delivery failed");
            }
        }
    }
}

fn make_definition(input: ServiceInput, _existing_name: Option<&str>) -> Result<ServiceDefinition> {
    let name = input.name.trim().to_string();
    if name.is_empty() || name.len() > 120 {
        anyhow::bail!("service name must be 1..120 characters")
    }
    let url = input.url.trim().to_string();
    let parsed = reqwest::Url::parse(&url).context("service URL is invalid")?;
    if !matches!(parsed.scheme(), "http" | "https") {
        anyhow::bail!("service URL must use http or https")
    }
    let now = Utc::now().to_rfc3339();
    Ok(ServiceDefinition {
        name,
        url,
        expected_status: input.expected_status.unwrap_or(200),
        interval_secs: input.interval_secs.unwrap_or(60).max(1),
        retries: input.retries.unwrap_or(2).min(10),
        failure_threshold: input.failure_threshold.unwrap_or(3).max(1),
        version_header: input
            .version_header
            .filter(|v| !v.trim().is_empty())
            .unwrap_or_else(|| "X-Version".into()),
        recipients: input.recipients.unwrap_or_default(),
        enabled: input.enabled.unwrap_or(true),
        created_at: now.clone(),
        updated_at: now,
    })
}

fn validate_definitions(definitions: &[ServiceDefinition]) -> Result<()> {
    let mut names = std::collections::HashSet::new();
    for service in definitions {
        if !names.insert(service.name.clone()) {
            anyhow::bail!("duplicate service name")
        }
    }
    Ok(())
}

fn read_json<T: for<'de> Deserialize<'de> + Default>(path: &Path) -> Result<T> {
    match fs::read_to_string(path) {
        Ok(content) => serde_json::from_str(&content)
            .with_context(|| format!("parsing health monitor file {}", path.display())),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(T::default()),
        Err(error) => {
            Err(error).with_context(|| format!("reading health monitor file {}", path.display()))
        }
    }
}

fn persist_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent).with_context(|| format!("creating {}", parent.display()))?;
    let data = serde_json::to_vec_pretty(value).context("serializing health monitor state")?;
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let tmp = parent.join(format!(".health-monitor-{nonce}.tmp"));
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&tmp)
        .context("creating health monitor temporary file")?;
    file.write_all(&data)
        .context("writing health monitor temporary file")?;
    file.sync_all()
        .context("flushing health monitor temporary file")?;
    drop(file);
    fs::rename(&tmp, path).with_context(|| format!("atomically replacing {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input(name: &str) -> ServiceInput {
        ServiceInput {
            name: name.into(),
            url: "http://localhost:8080/health".into(),
            expected_status: None,
            interval_secs: None,
            retries: None,
            failure_threshold: None,
            version_header: None,
            recipients: None,
            enabled: None,
        }
    }

    #[test]
    fn defaults_and_duplicate_names() {
        let definition = make_definition(input("api"), None).unwrap();
        assert_eq!(definition.interval_secs, 60);
        assert_eq!(definition.retries, 2);
        assert_eq!(definition.failure_threshold, 3);
        assert_eq!(definition.version_header, "X-Version");
        assert!(validate_definitions(&[definition.clone(), definition]).is_err());
    }
}
