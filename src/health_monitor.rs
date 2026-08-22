//! Dashboard-managed HTTP service health monitoring.

use anyhow::{Context, Result};
use chrono::Utc;
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::collections::HashSet;
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
    #[serde(default)]
    pub use_active_routing_profile: bool,
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
    #[serde(default)]
    pub last_up_version: Option<String>,
    pub last_reason: Option<String>,
    pub last_status_code: Option<u16>,
    pub latency_ms: Option<u64>,
    #[serde(default)]
    pub last_error: Option<String>,
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
    #[serde(default)]
    pub last_up_version: Option<String>,
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
    #[serde(default)]
    pub use_active_routing_profile: bool,
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

#[derive(Debug, Serialize, Deserialize, Default)]
struct RuntimeFile {
    #[serde(default)]
    runtimes: HashMap<String, ServiceRuntime>,
}

struct EventData {
    reason: Option<String>,
    status_code: Option<u16>,
    version: Option<String>,
    last_up_version: Option<String>,
    failures: u32,
}

#[derive(Debug)]
struct Store {
    definitions_path: PathBuf,
    incidents_path: PathBuf,
    runtime_path: PathBuf,
    definitions: Mutex<Vec<ServiceDefinition>>,
    incidents: Mutex<Vec<Incident>>,
    runtime: Mutex<HashMap<String, ServiceRuntime>>,
    checking: Mutex<HashSet<String>>,
}

#[derive(Debug)]
pub struct HealthMonitor {
    store: Arc<Store>,
    client: SharedClient,
    direct_client: Arc<Client>,
    bot_token: String,
    users: Arc<TelegramUserStore>,
    wake: Arc<(Mutex<bool>, Condvar)>,
}

impl HealthMonitor {
    pub fn load(
        data_dir: impl Into<PathBuf>,
        client: SharedClient,
        direct_client: Arc<Client>,
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
        let runtime_path = data_dir.join("health-runtime.json");
        let definitions = read_json::<DefinitionsFile>(&definitions_path)?.services;
        validate_definitions(&definitions)?;
        let incidents = read_json::<IncidentsFile>(&incidents_path)?.incidents;
        let persisted_runtime: HashMap<String, ServiceRuntime> =
            read_json::<RuntimeFile>(&runtime_path)?
                .runtimes
                .into_iter()
                .filter(|(name, _)| definitions.iter().any(|service| service.name == *name))
                .collect();
        let mut runtime = runtime_from_incidents(&definitions, &incidents);
        runtime.extend(persisted_runtime);
        Ok(Arc::new(Self {
            store: Arc::new(Store {
                definitions_path,
                incidents_path,
                runtime_path,
                definitions: Mutex::new(definitions),
                incidents: Mutex::new(incidents),
                runtime: Mutex::new(runtime),
                checking: Mutex::new(HashSet::new()),
            }),
            client,
            direct_client,
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
                last_up_version: None,
                last_reason: None,
                last_status_code: None,
                latency_ms: None,
                last_error: None,
            })
    }

    pub fn check_now(&self, name: &str) -> Result<Option<(ServiceDefinition, ServiceRuntime)>> {
        let Some(service) = self.get(name) else {
            return Ok(None);
        };
        if !self.begin_check(&service.name) {
            anyhow::bail!("service check already in progress");
        }
        self.check(&service);
        Ok(self.details(name))
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
                if self.should_check(&service.name, service.interval_secs)
                    && self.begin_check(&service.name)
                {
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

    fn check_client(&self, service: &ServiceDefinition) -> Arc<Client> {
        if service.use_active_routing_profile {
            self.client
                .read()
                .expect("health client lock poisoned")
                .clone()
        } else {
            Arc::clone(&self.direct_client)
        }
    }

    fn check(&self, service: &ServiceDefinition) {
        let started = std::time::Instant::now();
        let mut result = Err("request failed".to_string());
        let mut status_code = None;
        let mut version = None;
        for attempt in 0..=service.retries {
            let client = self.check_client(service);
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
                    result = Err(format!(
                        "unexpected HTTP status {code}; expected HTTP {} ({} attempts)",
                        service.expected_status,
                        service.retries.saturating_add(1)
                    ));
                }
                Err(error) => {
                    let attempts = service.retries.saturating_add(1);
                    result = Err(if error.is_timeout() {
                        format!("request timed out ({attempts} attempts)")
                    } else if error.is_connect() {
                        format!("network request failed (connection error; {attempts} attempts)")
                    } else {
                        format!("network request failed (transport error; {attempts} attempts)")
                    })
                }
            }
            if attempt < service.retries {
                thread::sleep(Duration::from_millis(100));
            }
        }
        self.record_check(service, result, status_code, version, started.elapsed());
        self.end_check(&service.name);
    }

    fn begin_check(&self, name: &str) -> bool {
        self.store
            .checking
            .lock()
            .expect("health checking lock poisoned")
            .insert(name.to_string())
    }

    fn end_check(&self, name: &str) {
        self.store
            .checking
            .lock()
            .expect("health checking lock poisoned")
            .remove(name);
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
                last_up_version: None,
                last_reason: None,
                last_status_code: None,
                latency_ms: None,
                last_error: None,
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
                if entry.current_version.is_some() {
                    entry.last_up_version = entry.current_version.clone();
                }
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
                            last_up_version: entry.last_up_version.clone(),
                            failures: 0,
                        },
                    );
                }
            }
            Err(reason) => {
                entry.consecutive_failures = entry.consecutive_failures.saturating_add(1);
                entry.last_failure = Some(now.clone());
                entry.last_reason = Some(reason.clone());
                entry.last_error = Some(reason.clone());
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
                                last_up_version: entry.last_up_version.clone(),
                                failures: entry.consecutive_failures,
                            },
                        );
                    }
                }
            }
        }
        if let Err(error) = persist_json(
            &self.store.runtime_path,
            &RuntimeFile {
                runtimes: runtime_map.clone(),
            },
        ) {
            tracing::warn!(error = %error, "health runtime persistence failed");
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
            last_up_version: data.last_up_version,
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
        use_active_routing_profile: input.use_active_routing_profile,
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

fn runtime_from_incidents(
    definitions: &[ServiceDefinition],
    incidents: &[Incident],
) -> HashMap<String, ServiceRuntime> {
    definitions
        .iter()
        .filter_map(|service| {
            let incident = incidents.iter().rev().find(|incident| {
                incident.service == service.name
                    && matches!(incident.event.as_str(), "outage" | "recovery")
            })?;
            let last_error = incidents
                .iter()
                .rev()
                .find(|incident| incident.service == service.name && incident.event == "outage");
            Some((
                service.name.clone(),
                runtime_from_incident(incident, last_error),
            ))
        })
        .collect()
}

fn runtime_from_incident(incident: &Incident, last_error: Option<&Incident>) -> ServiceRuntime {
    let is_outage = incident.event == "outage";
    ServiceRuntime {
        name: incident.service.clone(),
        status: if is_outage {
            ServiceStatus::Down
        } else {
            ServiceStatus::Up
        },
        consecutive_failures: incident.consecutive_failures,
        last_check: Some(incident.timestamp.clone()),
        last_success: (!is_outage).then(|| incident.timestamp.clone()),
        last_failure: is_outage.then(|| incident.timestamp.clone()),
        current_version: (!is_outage).then(|| incident.version.clone()).flatten(),
        last_observed_version: incident.version.clone(),
        last_up_version: incident
            .last_up_version
            .clone()
            .or_else(|| incident.version.clone()),
        last_reason: if is_outage {
            incident.reason.clone()
        } else {
            None
        },
        last_status_code: incident.status_code,
        latency_ms: None,
        last_error: last_error.and_then(|incident| incident.reason.clone()),
    }
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
    use std::fs;
    use std::sync::{Arc, RwLock};

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
            use_active_routing_profile: false,
            enabled: None,
        }
    }

    fn test_service(name: &str) -> ServiceDefinition {
        let mut service = make_definition(input(name), None).unwrap();
        service.failure_threshold = 1;
        service
    }

    fn test_monitor(
        service: &ServiceDefinition,
        incidents: &[Incident],
    ) -> (Arc<HealthMonitor>, std::path::PathBuf) {
        let data_dir = std::env::temp_dir().join(format!(
            "crab-dump-health-monitor-test-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir(&data_dir).unwrap();
        persist_json(
            &data_dir.join("health-services.json"),
            &DefinitionsFile {
                services: vec![service.clone()],
            },
        )
        .unwrap();
        persist_json(
            &data_dir.join("health-incidents.json"),
            &IncidentsFile {
                incidents: incidents.to_vec(),
            },
        )
        .unwrap();
        let users =
            Arc::new(TelegramUserStore::load(data_dir.join("telegram-users.toml")).unwrap());
        let client = Arc::new(RwLock::new(Arc::new(reqwest::blocking::Client::new())));
        (
            HealthMonitor::load(
                data_dir.clone(),
                client,
                Arc::new(reqwest::blocking::Client::new()),
                "test-token".into(),
                users,
            )
            .unwrap(),
            data_dir,
        )
    }

    fn incident(event: &str, service: &str) -> Incident {
        Incident {
            id: 1,
            service: service.into(),
            event: event.into(),
            timestamp: "2026-08-21T00:00:00+00:00".into(),
            reason: Some("connection refused".into()),
            status_code: Some(503),
            version: Some("v1".into()),
            last_up_version: None,
            consecutive_failures: 2,
            acknowledged: false,
        }
    }

    fn record_failure(monitor: &HealthMonitor, service: &ServiceDefinition) {
        monitor.record_check(
            service,
            Err("connection refused".into()),
            Some(503),
            Some("v1".into()),
            Duration::from_millis(1),
        );
    }

    fn record_success(monitor: &HealthMonitor, service: &ServiceDefinition) {
        monitor.record_check(
            service,
            Ok(()),
            Some(200),
            Some("v2".into()),
            Duration::from_millis(1),
        );
    }

    #[test]
    fn defaults_and_duplicate_names() {
        let definition = make_definition(input("api"), None).unwrap();
        assert_eq!(definition.interval_secs, 60);
        assert_eq!(definition.retries, 2);
        assert_eq!(definition.failure_threshold, 3);
        assert_eq!(definition.version_header, "X-Version");
        assert!(!definition.use_active_routing_profile);
        assert!(validate_definitions(&[definition.clone(), definition]).is_err());
    }

    #[test]
    fn legacy_definitions_default_to_direct_health_checks() {
        let file: DefinitionsFile = serde_json::from_str(
            r#"{
                "services": [{
                    "name": "api",
                    "url": "https://example.test/health",
                    "expected_status": 200,
                    "interval_secs": 60,
                    "retries": 2,
                    "failure_threshold": 3,
                    "version_header": "X-Version",
                    "enabled": true,
                    "created_at": "2026-08-21T00:00:00Z",
                    "updated_at": "2026-08-21T00:00:00Z"
                }]
            }"#,
        )
        .unwrap();

        assert!(!file.services[0].use_active_routing_profile);
    }

    #[test]
    fn explicit_routing_mode_survives_definition_creation() {
        let mut service_input = input("api");
        service_input.use_active_routing_profile = true;

        let definition = make_definition(service_input, None).unwrap();

        assert!(definition.use_active_routing_profile);
        let encoded = serde_json::to_value(&definition).unwrap();
        assert_eq!(encoded["use_active_routing_profile"], true);
    }

    #[test]
    fn health_checks_select_direct_or_routed_client_per_service() {
        let service = test_service("direct");
        let (monitor, data_dir) = test_monitor(&service, &[]);
        let routed_client = reqwest::blocking::Client::builder()
            .proxy(reqwest::Proxy::all("http://127.0.0.1:9").unwrap())
            .build()
            .unwrap();
        *monitor.client.write().unwrap() = Arc::new(routed_client);

        let direct_client = monitor.check_client(&service);
        assert!(Arc::ptr_eq(&direct_client, &monitor.direct_client));

        let mut routed_service = service;
        routed_service.use_active_routing_profile = true;
        let routed_client = monitor.check_client(&routed_service);
        let shared_client = monitor.client.read().unwrap().clone();
        assert!(Arc::ptr_eq(&routed_client, &shared_client));

        fs::remove_dir_all(data_dir).unwrap();
    }

    #[test]
    fn outage_and_recovery_events_are_emitted_once_per_transition() {
        let service = test_service("api");
        let (monitor, data_dir) = test_monitor(&service, &[]);

        record_failure(&monitor, &service);
        record_failure(&monitor, &service);
        let (incidents, _) = monitor.incidents("api", 1, 100);
        assert_eq!(
            incidents
                .iter()
                .filter(|incident| incident.event == "outage")
                .count(),
            1
        );

        record_success(&monitor, &service);
        record_success(&monitor, &service);
        let (incidents, _) = monitor.incidents("api", 1, 100);
        assert_eq!(
            incidents
                .iter()
                .filter(|incident| incident.event == "recovery")
                .count(),
            1
        );

        fs::remove_dir_all(data_dir).unwrap();
    }

    #[test]
    fn loading_outage_state_suppresses_outage_and_recovers_on_success() {
        let service = test_service("api");
        let outage = incident("outage", "api");
        let (monitor, data_dir) = test_monitor(&service, std::slice::from_ref(&outage));

        let runtime = monitor.runtime("api");
        assert_eq!(runtime.status, ServiceStatus::Down);
        assert_eq!(runtime.consecutive_failures, outage.consecutive_failures);
        assert_eq!(runtime.last_reason, outage.reason);
        assert_eq!(runtime.last_status_code, outage.status_code);
        assert_eq!(runtime.last_observed_version, outage.version);
        assert_eq!(
            runtime.last_up_version,
            outage.last_up_version.or(outage.version)
        );

        record_failure(&monitor, &service);
        let (incidents, _) = monitor.incidents("api", 1, 100);
        assert_eq!(incidents.len(), 1);

        record_success(&monitor, &service);
        let (incidents, _) = monitor.incidents("api", 1, 100);
        assert_eq!(
            incidents
                .iter()
                .filter(|incident| incident.event == "recovery")
                .count(),
            1
        );

        fs::remove_dir_all(data_dir).unwrap();
    }

    #[test]
    fn loading_recovery_state_starts_service_up() {
        let service = test_service("api");
        let mut recovery = incident("recovery", "api");
        recovery.reason = None;
        recovery.status_code = Some(200);
        recovery.version = Some("v2".into());
        recovery.consecutive_failures = 0;
        let (monitor, data_dir) = test_monitor(&service, &[recovery]);

        let runtime = monitor.runtime("api");
        assert_eq!(runtime.status, ServiceStatus::Up);
        assert_eq!(runtime.consecutive_failures, 0);
        assert_eq!(runtime.current_version.as_deref(), Some("v2"));
        assert_eq!(runtime.last_up_version.as_deref(), Some("v2"));
        assert_eq!(
            runtime.last_success.as_deref(),
            Some("2026-08-21T00:00:00+00:00")
        );

        fs::remove_dir_all(data_dir).unwrap();
    }

    #[test]
    fn last_error_survives_recovery_and_restart() {
        let service = test_service("api");
        let (monitor, data_dir) = test_monitor(&service, &[]);

        record_failure(&monitor, &service);
        let failed = monitor.runtime("api");
        assert_eq!(failed.last_error.as_deref(), Some("connection refused"));
        assert_eq!(failed.last_failure.as_deref(), failed.last_check.as_deref());

        record_success(&monitor, &service);
        let recovered = monitor.runtime("api");
        assert_eq!(recovered.status, ServiceStatus::Up);
        assert_eq!(recovered.last_error.as_deref(), Some("connection refused"));

        let users =
            Arc::new(TelegramUserStore::load(data_dir.join("telegram-users-reload.toml")).unwrap());
        let client = Arc::new(RwLock::new(Arc::new(reqwest::blocking::Client::new())));
        let reloaded = HealthMonitor::load(
            data_dir.clone(),
            client,
            Arc::new(reqwest::blocking::Client::new()),
            "test-token".into(),
            users,
        )
        .unwrap();
        assert_eq!(
            reloaded.runtime("api").last_error.as_deref(),
            Some("connection refused")
        );

        fs::remove_dir_all(data_dir).unwrap();
    }

    #[test]
    fn last_up_version_survives_outage_and_failed_version_changes() {
        let service = test_service("api");
        let (monitor, data_dir) = test_monitor(&service, &[]);

        record_success(&monitor, &service);
        record_failure(&monitor, &service);

        let runtime = monitor.runtime("api");
        assert_eq!(runtime.status, ServiceStatus::Down);
        assert_eq!(runtime.current_version, None);
        assert_eq!(runtime.last_up_version.as_deref(), Some("v2"));
        assert_eq!(runtime.last_observed_version.as_deref(), Some("v1"));

        let (incidents, _) = monitor.incidents("api", 1, 100);
        let outage = incidents
            .iter()
            .find(|incident| incident.event == "outage")
            .expect("outage incident");
        assert_eq!(outage.version.as_deref(), Some("v1"));
        assert_eq!(outage.last_up_version.as_deref(), Some("v2"));

        let users =
            Arc::new(TelegramUserStore::load(data_dir.join("telegram-users-reload.toml")).unwrap());
        let client = Arc::new(RwLock::new(Arc::new(reqwest::blocking::Client::new())));
        let reloaded = HealthMonitor::load(
            data_dir.clone(),
            client,
            Arc::new(reqwest::blocking::Client::new()),
            "test-token".into(),
            users,
        )
        .unwrap();
        assert_eq!(
            reloaded.runtime("api").last_up_version.as_deref(),
            Some("v2")
        );

        fs::remove_dir_all(data_dir).unwrap();
    }

    #[test]
    fn legacy_runtime_and_incident_json_load_without_last_up_version() {
        let service = test_service("api");
        let data_dir = std::env::temp_dir().join(format!(
            "crab-dump-health-monitor-legacy-test-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir(&data_dir).unwrap();
        persist_json(
            &data_dir.join("health-services.json"),
            &DefinitionsFile {
                services: vec![service.clone()],
            },
        )
        .unwrap();
        fs::write(
            data_dir.join("health-incidents.json"),
            r#"{"incidents":[{"id":1,"service":"api","event":"outage","timestamp":"2026-08-21T00:00:00+00:00","reason":"down","status_code":503,"version":"v1","consecutive_failures":1,"acknowledged":false}]}"#,
        )
        .unwrap();
        fs::write(
            data_dir.join("health-runtime.json"),
            r#"{"runtimes":{"api":{"name":"api","status":"down","consecutive_failures":1,"last_check":"2026-08-21T00:00:00+00:00","last_success":null,"last_failure":"2026-08-21T00:00:00+00:00","current_version":null,"last_observed_version":"v1","last_reason":"down","last_status_code":503,"latency_ms":1,"last_error":null}}}"#,
        )
        .unwrap();
        let users =
            Arc::new(TelegramUserStore::load(data_dir.join("telegram-users.toml")).unwrap());
        let client = Arc::new(RwLock::new(Arc::new(reqwest::blocking::Client::new())));
        let monitor = HealthMonitor::load(
            data_dir.clone(),
            client,
            Arc::new(reqwest::blocking::Client::new()),
            "test-token".into(),
            users,
        )
        .unwrap();
        assert_eq!(monitor.runtime("api").last_up_version, None);
        assert_eq!(monitor.incidents("api", 1, 100).0[0].last_up_version, None);
        fs::remove_dir_all(data_dir).unwrap();
    }
}
