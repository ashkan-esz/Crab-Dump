//! Dashboard-managed routing share profiles and selectable local routing cores.
//!
//! Secrets live only in the restricted profile file and the generated config
//! file. Public summaries and all errors intentionally omit URLs and
//! credentials.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub const DEFAULT_SING_BOX_PATH: &str = "/usr/local/bin/sing-box";
pub const DEFAULT_SHOES_PATH: &str = "/usr/local/bin/shoes";
const SING_BOX_CONFIG_FILE: &str = "sing-box.json";
const SHOES_CONFIG_FILE: &str = "shoes.yaml";
const PROFILES_FILE: &str = "routing_profiles.json";

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum RoutingBackend {
    #[default]
    SingBox,
    Shoes,
}

impl RoutingBackend {
    pub fn parse(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "" | "sing-box" | "sing_box" | "singbox" => Ok(Self::SingBox),
            "shoes" => Ok(Self::Shoes),
            _ => anyhow::bail!("routing core must be `sing-box` or `shoes`"),
        }
    }

    pub const ALL: [Self; 2] = [Self::SingBox, Self::Shoes];

    pub fn as_str(self) -> &'static str {
        self.label()
    }

    fn config_file(self) -> &'static str {
        match self {
            Self::SingBox => SING_BOX_CONFIG_FILE,
            Self::Shoes => SHOES_CONFIG_FILE,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::SingBox => "sing-box",
            Self::Shoes => "shoes",
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ProfileKind {
    Vmess,
    Vless,
    Shadowsocks,
    Trojan,
}

#[derive(Clone, Deserialize, Serialize)]
struct StoredProfile {
    id: String,
    name: String,
    url: String,
    kind: ProfileKind,
    #[serde(default)]
    compatible_cores: Vec<RoutingBackend>,
    created_at: u64,
    updated_at: u64,
}

#[derive(Clone, Default, Deserialize, Serialize)]
struct ProfileFile {
    #[serde(default)]
    selected_core: RoutingBackend,
    profiles: Vec<StoredProfile>,
    active_id: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ProfileSummary {
    pub id: String,
    pub name: String,
    pub kind: ProfileKind,
    pub compatible_cores: Vec<RoutingBackend>,
    pub active: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct RoutingStatus {
    pub selected_core: RoutingBackend,
    pub running_core: Option<RoutingBackend>,
    pub active_profile: Option<String>,
    pub available_cores: Vec<RoutingBackend>,
}

#[derive(Clone, Eq, PartialEq)]
pub struct ParsedProfile {
    pub kind: ProfileKind,
    pub name: String,
    pub address: String,
    pub port: u16,
    pub uuid: String,
    pub password: Option<String>,
    pub method: Option<String>,
    pub alter_id: u16,
    pub security: String,
    pub transport: String,
    pub host: Option<String>,
    pub path: Option<String>,
    pub tls: bool,
    pub sni: Option<String>,
    pub reality_public_key: Option<String>,
    pub reality_short_id: Option<String>,
}

impl std::fmt::Debug for ParsedProfile {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ParsedProfile")
            .field("kind", &self.kind)
            .field("name", &self.name)
            .field("address", &"[REDACTED]")
            .field("port", &self.port)
            .field("uuid", &"[REDACTED]")
            .field("password", &self.password.as_ref().map(|_| "[REDACTED]"))
            .field("method", &self.method)
            .field("alter_id", &self.alter_id)
            .field("security", &self.security)
            .field("transport", &self.transport)
            .field("host", &self.host.as_ref().map(|_| "[REDACTED]"))
            .field("path", &self.path.as_ref().map(|_| "[REDACTED]"))
            .field("tls", &self.tls)
            .field("sni", &self.sni.as_ref().map(|_| "[REDACTED]"))
            .finish()
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct ProfileInput {
    pub name: String,
    pub url: String,
}

pub struct ProfileStore {
    path: PathBuf,
    state: Mutex<ProfileFile>,
}

impl std::fmt::Debug for ProfileStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProfileStore")
            .field("path", &self.path)
            .field("profile_count", &self.list().len())
            .field("active_id", &self.active_id())
            .finish()
    }
}

impl ProfileStore {
    pub fn load(data_dir: impl Into<PathBuf>) -> Result<Self> {
        let path = data_dir.into().join(PROFILES_FILE);
        let state = match fs::read_to_string(&path) {
            Ok(contents) => {
                let mut state: ProfileFile =
                    serde_json::from_str(&contents).context("reading routing profiles")?;
                for profile in &mut state.profiles {
                    if profile.compatible_cores.is_empty() {
                        profile.compatible_cores = derive_compatible_cores(&profile.url)
                            .unwrap_or_else(|_| vec![RoutingBackend::SingBox]);
                    }
                }
                state
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => ProfileFile::default(),
            Err(error) => return Err(error).context("opening routing profiles"),
        };
        Ok(Self {
            path,
            state: Mutex::new(state),
        })
    }

    pub fn list(&self) -> Vec<ProfileSummary> {
        self.list_for_available(&RoutingBackend::ALL)
    }

    pub fn list_for_available(&self, available: &[RoutingBackend]) -> Vec<ProfileSummary> {
        let state = self.state.lock().expect("profile store lock poisoned");
        let selected_available = available.contains(&state.selected_core);
        state
            .profiles
            .iter()
            .map(|profile| ProfileSummary {
                id: profile.id.clone(),
                name: profile.name.clone(),
                kind: profile.kind,
                compatible_cores: profile
                    .compatible_cores
                    .iter()
                    .copied()
                    .filter(|core| available.contains(core))
                    .collect(),
                active: selected_available
                    && state.active_id.as_deref() == Some(profile.id.as_str()),
            })
            .collect()
    }

    pub fn active_url(&self) -> Option<String> {
        let state = self.state.lock().expect("profile store lock poisoned");
        let id = state.active_id.as_deref()?;
        state
            .profiles
            .iter()
            .find(|profile| profile.id == id)
            .map(|profile| profile.url.clone())
    }

    pub fn get_url(&self, id: &str) -> Option<String> {
        self.state
            .lock()
            .expect("profile store lock poisoned")
            .profiles
            .iter()
            .find(|profile| profile.id == id)
            .map(|profile| profile.url.clone())
    }

    #[cfg(test)]
    pub fn create(&self, input: ProfileInput) -> Result<ProfileSummary> {
        let compatible = derive_compatible_cores(&input.url)?;
        self.create_with_compatibility(input, compatible)
    }

    pub fn create_with_compatibility(
        &self,
        input: ProfileInput,
        compatible_cores: Vec<RoutingBackend>,
    ) -> Result<ProfileSummary> {
        if compatible_cores.is_empty() {
            anyhow::bail!("routing profile is incompatible with every routing core");
        }
        let parsed = parse_share_url(&input.url)?;
        let name = clean_name(&input.name)?;
        let mut state = self.state.lock().expect("profile store lock poisoned");
        let previous = state.clone();
        let id = unique_id(&state.profiles);
        let now = epoch();
        state.profiles.push(StoredProfile {
            id: id.clone(),
            name,
            url: input.url,
            kind: parsed.kind,
            compatible_cores: compatible_cores.clone(),
            created_at: now,
            updated_at: now,
        });
        if let Err(error) = self.persist(&state) {
            *state = previous;
            return Err(error);
        }
        Ok(ProfileSummary {
            id,
            name: state
                .profiles
                .last()
                .expect("profile was pushed")
                .name
                .clone(),
            kind: parsed.kind,
            compatible_cores,
            active: false,
        })
    }

    pub fn update_with_compatibility(
        &self,
        id: &str,
        input: ProfileInput,
        compatible_cores: Vec<RoutingBackend>,
    ) -> Result<Option<ProfileSummary>> {
        if compatible_cores.is_empty() {
            anyhow::bail!("routing profile is incompatible with every routing core");
        }
        let parsed = parse_share_url(&input.url)?;
        let name = clean_name(&input.name)?;
        let mut state = self.state.lock().expect("profile store lock poisoned");
        let previous = state.clone();
        let Some(profile) = state.profiles.iter_mut().find(|profile| profile.id == id) else {
            return Ok(None);
        };
        profile.name = name;
        profile.url = input.url;
        profile.kind = parsed.kind;
        profile.compatible_cores = compatible_cores;
        profile.updated_at = epoch();
        let summary = ProfileSummary {
            id: profile.id.clone(),
            name: profile.name.clone(),
            kind: profile.kind,
            compatible_cores: profile.compatible_cores.clone(),
            active: state.active_id.as_deref() == Some(id),
        };
        if let Err(error) = self.persist(&state) {
            *state = previous;
            return Err(error);
        }
        Ok(Some(summary))
    }

    pub fn delete(&self, id: &str) -> Result<bool> {
        let mut state = self.state.lock().expect("profile store lock poisoned");
        let previous = state.clone();
        let old_len = state.profiles.len();
        state.profiles.retain(|profile| profile.id != id);
        if state.active_id.as_deref() == Some(id) {
            state.active_id = None;
        }
        if state.profiles.len() == old_len {
            return Ok(false);
        }
        if let Err(error) = self.persist(&state) {
            *state = previous;
            return Err(error);
        }
        Ok(true)
    }

    pub fn set_active(&self, id: &str) -> Result<bool> {
        let mut state = self.state.lock().expect("profile store lock poisoned");
        if !state.profiles.iter().any(|profile| profile.id == id) {
            return Ok(false);
        }
        let previous = state.clone();
        state.active_id = Some(id.to_string());
        if let Err(error) = self.persist(&state) {
            *state = previous;
            return Err(error);
        }
        Ok(true)
    }

    pub fn clear_active(&self) -> Result<bool> {
        let mut state = self.state.lock().expect("profile store lock poisoned");
        if state.active_id.is_none() {
            return Ok(false);
        }
        let previous = state.clone();
        state.active_id = None;
        if let Err(error) = self.persist(&state) {
            *state = previous;
            return Err(error);
        }
        Ok(true)
    }

    pub fn active_id(&self) -> Option<String> {
        self.state
            .lock()
            .expect("profile store lock poisoned")
            .active_id
            .clone()
    }

    pub fn selected_core(&self) -> RoutingBackend {
        self.state
            .lock()
            .expect("profile store lock poisoned")
            .selected_core
    }

    pub fn set_selected_core(&self, core: RoutingBackend) -> Result<()> {
        let mut state = self.state.lock().expect("profile store lock poisoned");
        if state.selected_core == core {
            return Ok(());
        }
        let previous = state.clone();
        state.selected_core = core;
        if let Err(error) = self.persist(&state) {
            *state = previous;
            return Err(error);
        }
        Ok(())
    }

    pub fn routing_status(
        &self,
        running_core: Option<RoutingBackend>,
        available: &[RoutingBackend],
    ) -> RoutingStatus {
        let state = self.state.lock().expect("profile store lock poisoned");
        RoutingStatus {
            selected_core: state.selected_core,
            running_core,
            active_profile: available
                .contains(&state.selected_core)
                .then(|| state.active_id.clone())
                .flatten(),
            available_cores: available.to_vec(),
        }
    }

    pub fn compatible_cores(&self, id: &str) -> Option<Vec<RoutingBackend>> {
        self.state
            .lock()
            .expect("profile store lock poisoned")
            .profiles
            .iter()
            .find(|profile| profile.id == id)
            .map(|profile| profile.compatible_cores.clone())
    }

    fn persist(&self, state: &ProfileFile) -> Result<()> {
        let parent = self.path.parent().unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent).context("creating profile data directory")?;
        let tmp = self.path.with_extension("json.tmp");
        let bytes = serde_json::to_vec_pretty(state).context("serializing routing profiles")?;
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&tmp)
            .context("writing temporary routing profiles")?;
        restrict(&file)?;
        file.write_all(&bytes).context("writing routing profiles")?;
        file.sync_all().context("flushing routing profiles")?;
        drop(file);
        fs::rename(&tmp, &self.path).context("atomically replacing routing profiles")?;
        restrict_path(&self.path)?;
        Ok(())
    }
}

#[derive(Debug)]
struct ActiveCore {
    backend: RoutingBackend,
    child: Child,
    config_path: PathBuf,
    proxy: String,
}

#[derive(Debug)]
pub struct RouteManager {
    sing_box_path: PathBuf,
    shoes_path: PathBuf,
    work_dir: PathBuf,
    active: Mutex<Option<ActiveCore>>,
    pending_previous: Mutex<Option<ActiveCore>>,
}

pub struct TemporaryRoute {
    core: Option<ActiveCore>,
    proxy: String,
}

impl TemporaryRoute {
    pub fn proxy(&self) -> &str {
        &self.proxy
    }
}

impl Drop for TemporaryRoute {
    fn drop(&mut self) {
        if let Some(core) = self.core.take() {
            cleanup_core(core);
        }
    }
}

impl RouteManager {
    pub fn with_paths(
        work_dir: impl Into<PathBuf>,
        sing_box_path: impl Into<PathBuf>,
        shoes_path: impl Into<PathBuf>,
    ) -> Self {
        Self {
            sing_box_path: sing_box_path.into(),
            shoes_path: shoes_path.into(),
            work_dir: work_dir.into(),
            active: Mutex::new(None),
            pending_previous: Mutex::new(None),
        }
    }

    #[cfg(test)]
    pub fn with_backend(
        work_dir: impl Into<PathBuf>,
        executable_path: impl Into<PathBuf>,
        backend: RoutingBackend,
    ) -> Self {
        let path = executable_path.into();
        match backend {
            RoutingBackend::SingBox => Self::with_paths(work_dir, path, DEFAULT_SHOES_PATH),
            RoutingBackend::Shoes => Self::with_paths(work_dir, DEFAULT_SING_BOX_PATH, path),
        }
    }

    fn executable_path(&self, backend: RoutingBackend) -> &Path {
        match backend {
            RoutingBackend::SingBox => &self.sing_box_path,
            RoutingBackend::Shoes => &self.shoes_path,
        }
    }

    pub fn running_core(&self) -> Option<RoutingBackend> {
        self.active
            .lock()
            .expect("route manager lock poisoned")
            .as_ref()
            .map(|core| core.backend)
    }

    pub fn available_cores(&self) -> Vec<RoutingBackend> {
        RoutingBackend::ALL
            .into_iter()
            .filter(|backend| is_executable(self.executable_path(*backend)))
            .collect()
    }

    pub fn is_available(&self, backend: RoutingBackend) -> bool {
        is_executable(self.executable_path(backend))
    }

    pub fn active_proxy(&self) -> Option<String> {
        self.active
            .lock()
            .expect("route manager lock poisoned")
            .as_ref()
            .map(|core| core.proxy.clone())
    }

    pub fn is_healthy(&self) -> bool {
        let mut active = self.active.lock().expect("route manager lock poisoned");
        let Some(core) = active.as_mut() else {
            return true;
        };

        if matches!(core.child.try_wait(), Ok(Some(_)) | Err(_)) {
            return false;
        }

        let Some(port) = core
            .proxy
            .strip_prefix("socks5h://127.0.0.1:")
            .and_then(|port| port.parse::<u16>().ok())
        else {
            return false;
        };
        let Ok(address) = format!("127.0.0.1:{port}").parse::<std::net::SocketAddr>() else {
            return false;
        };
        TcpStream::connect_timeout(&address, Duration::from_millis(250)).is_ok()
    }

    pub fn test(&self, url: &str) -> Result<ParsedProfile> {
        parse_share_url(url)
    }

    /// Start an isolated core for a connectivity check.
    ///
    /// This deliberately does not touch `active` or `pending_previous`, so a
    /// check can never replace, stop, or otherwise mutate the active route.
    pub fn start_temporary(
        &self,
        url: &str,
        backend: RoutingBackend,
    ) -> Result<(ParsedProfile, TemporaryRoute)> {
        if !self.is_available(backend) {
            anyhow::bail!("routing core {} is unavailable", backend.label());
        }
        let profile = parse_share_url(url)?;
        let port = free_port()?;
        let config = generate_backend_config(backend, &profile, port)?;
        fs::create_dir_all(&self.work_dir)
            .with_context(|| format!("creating {} work directory", backend.label()))?;
        let config_path = self.work_dir.join(format!(
            "routing-check-{}-{port}.{}",
            std::process::id(),
            if backend == RoutingBackend::SingBox {
                "json"
            } else {
                "yaml"
            }
        ));
        if let Err(error) = write_backend_config(backend, &config_path, &config) {
            let _ = fs::remove_file(&config_path);
            return Err(error);
        }

        let mut command = Command::new(self.executable_path(backend));
        if backend == RoutingBackend::SingBox {
            command.args(["run", "-c"]);
        } else {
            command.arg("--no-reload");
        }
        let mut child = match command
            .arg(&config_path)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .context("starting temporary routing core")
        {
            Ok(child) => child,
            Err(error) => {
                let _ = fs::remove_file(&config_path);
                return Err(error);
            }
        };
        if !wait_for_listener(port) {
            let _ = child.kill();
            let _ = child.wait();
            let _ = child.stderr.take();
            let _ = fs::remove_file(&config_path);
            anyhow::bail!("temporary routing core did not open its local listener");
        }

        Ok((
            profile,
            TemporaryRoute {
                core: Some(ActiveCore {
                    backend,
                    child,
                    config_path,
                    proxy: format!("socks5h://127.0.0.1:{port}"),
                }),
                proxy: format!("socks5h://127.0.0.1:{port}"),
            },
        ))
    }

    pub fn apply(&self, url: &str, backend: RoutingBackend) -> Result<String> {
        if !self.is_available(backend) {
            anyhow::bail!("routing core {} is unavailable", backend.label());
        }
        let profile = parse_share_url(url)?;
        let port = free_port()?;
        let config = generate_backend_config(backend, &profile, port)?;
        fs::create_dir_all(&self.work_dir)
            .with_context(|| format!("creating {} work directory", backend.label()))?;
        let config_path = self.work_dir.join(backend.config_file());
        let tmp_path = self.work_dir.join(format!("{}.tmp", backend.config_file()));
        if let Err(error) = write_backend_config(backend, &tmp_path, &config) {
            let _ = fs::remove_file(&tmp_path);
            return Err(error);
        }
        if let Err(error) = fs::rename(&tmp_path, &config_path) {
            let _ = fs::remove_file(&tmp_path);
            return Err(error).context("installing managed routing config");
        }
        if let Err(error) = restrict_path(&config_path) {
            let _ = fs::remove_file(&config_path);
            return Err(error);
        }

        let mut command = Command::new(self.executable_path(backend));
        if backend == RoutingBackend::SingBox {
            command.args(["run", "-c"]);
        } else {
            command.arg("--no-reload");
        }
        let mut child = match command
            .arg(&config_path)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .context("starting managed routing core")
        {
            Ok(child) => child,
            Err(error) => {
                let _ = fs::remove_file(&config_path);
                return Err(error);
            }
        };
        if !wait_for_listener(port) {
            let _ = child.kill();
            let _ = child.wait();
            let diagnostic = child
                .stderr
                .take()
                .and_then(|mut stderr| {
                    let mut output = String::new();
                    stderr.read_to_string(&mut output).ok()?;
                    Some(output)
                })
                .map(|output| sanitize_core_diagnostic(&output))
                .filter(|output| !output.is_empty())
                .unwrap_or_else(|| "no diagnostic was emitted".to_string());
            let _ = fs::remove_file(&config_path);
            anyhow::bail!(
                "{} did not open its local listener: {diagnostic}",
                backend.label()
            );
        }

        let proxy = format!("socks5h://127.0.0.1:{port}");
        let mut active = self.active.lock().expect("route manager lock poisoned");
        let previous = active.replace(ActiveCore {
            backend,
            child,
            config_path,
            proxy: proxy.clone(),
        });
        drop(active);
        *self
            .pending_previous
            .lock()
            .expect("route manager lock poisoned") = previous;
        Ok(proxy)
    }

    pub fn commit(&self) {
        if let Some(previous) = self
            .pending_previous
            .lock()
            .expect("route manager lock poisoned")
            .take()
        {
            cleanup_core(previous);
        }
    }

    pub fn rollback(&self) {
        let previous = self
            .pending_previous
            .lock()
            .expect("route manager lock poisoned")
            .take();
        let failed = self
            .active
            .lock()
            .expect("route manager lock poisoned")
            .take();
        if let Some(failed) = failed {
            cleanup_core(failed);
        }
        if let Some(previous) = previous {
            *self.active.lock().expect("route manager lock poisoned") = Some(previous);
        }
    }

    pub fn stop(&self) {
        let mut active = self.active.lock().expect("route manager lock poisoned");
        if let Some(core) = active.take() {
            cleanup_core(core);
        }
        if let Some(previous) = self
            .pending_previous
            .lock()
            .expect("route manager lock poisoned")
            .take()
        {
            cleanup_core(previous);
        }
    }
}

fn generate_backend_config(
    backend: RoutingBackend,
    profile: &ParsedProfile,
    port: u16,
) -> Result<serde_json::Value> {
    match backend {
        RoutingBackend::SingBox => generate_config(profile, port),
        RoutingBackend::Shoes => generate_shoes_config(profile, port),
    }
}

fn derive_compatible_cores(url: &str) -> Result<Vec<RoutingBackend>> {
    let profile = parse_share_url(url)?;
    Ok(RoutingBackend::ALL
        .into_iter()
        .filter(|backend| generate_backend_config(*backend, &profile, 1).is_ok())
        .collect())
}

fn is_executable(path: &Path) -> bool {
    let Ok(metadata) = fs::metadata(path) else {
        return false;
    };
    if !metadata.is_file() {
        return false;
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        metadata.permissions().mode() & 0o111 != 0
    }
    #[cfg(not(unix))]
    {
        true
    }
}

fn write_backend_config(
    backend: RoutingBackend,
    path: &Path,
    config: &serde_json::Value,
) -> Result<()> {
    let bytes = match backend {
        RoutingBackend::SingBox => {
            serde_json::to_vec_pretty(config).context("serializing sing-box config")?
        }
        RoutingBackend::Shoes => serde_yaml::to_string(config)
            .context("serializing shoes config")?
            .into_bytes(),
    };
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)
        .with_context(|| format!("writing temporary {} config", backend.label()))?;
    restrict(&file)?;
    file.write_all(&bytes)
        .with_context(|| format!("writing temporary {} config", backend.label()))?;
    file.sync_all()
        .with_context(|| format!("flushing temporary {} config", backend.label()))?;
    Ok(())
}

impl Drop for RouteManager {
    fn drop(&mut self) {
        self.stop();
    }
}

fn sanitize_core_diagnostic(output: &str) -> String {
    let lower = output.to_ascii_lowercase();
    if [
        "password",
        "uuid",
        "server",
        "url",
        "token",
        "secret",
        "public_key",
    ]
    .iter()
    .any(|term| lower.contains(term))
    {
        "invalid managed routing configuration".to_string()
    } else {
        output
            .lines()
            .map(str::trim)
            .find(|line| !line.is_empty())
            .unwrap_or_default()
            .to_string()
    }
}

fn cleanup_core(mut core: ActiveCore) {
    let _ = core.child.kill();
    let _ = core.child.wait();
    let _ = fs::remove_file(core.config_path);
}

pub fn parse_share_url(input: &str) -> Result<ParsedProfile> {
    if input.starts_with("vmess://") {
        parse_vmess(input)
    } else if input.starts_with("vless://") {
        parse_vless(input)
    } else if input.starts_with("ss://") {
        parse_shadowsocks(input)
    } else if input.starts_with("trojan://") {
        parse_trojan(input)
    } else {
        anyhow::bail!("unsupported routing profile format")
    }
}

fn parse_vmess(input: &str) -> Result<ParsedProfile> {
    let encoded = input
        .strip_prefix("vmess://")
        .ok_or_else(|| anyhow::anyhow!("invalid routing profile"))?;
    let bytes = base64_decode(encoded)?;
    let value: serde_json::Value =
        serde_json::from_slice(&bytes).map_err(|_| anyhow::anyhow!("invalid VMess profile"))?;
    let string = |key: &str| value.get(key).and_then(|v| v.as_str()).unwrap_or("");
    let address = string("add").trim().to_string();
    let uuid = string("id").trim().to_string();
    let port = string("port")
        .parse()
        .or_else(|_| value.get("port").and_then(|v| v.as_u64()).ok_or(()))
        .map_err(|_| anyhow::anyhow!("invalid VMess profile"))?;
    let port = u16::try_from(port).map_err(|_| anyhow::anyhow!("invalid VMess profile"))?;
    if address.is_empty() || uuid.is_empty() || port == 0 {
        anyhow::bail!("VMess profile is missing required fields");
    }
    validate_uuid(&uuid)?;
    let transport = match string("net") {
        "" => "tcp",
        other => other,
    };
    if !matches!(transport, "tcp" | "ws" | "grpc" | "http") {
        anyhow::bail!("unsupported VMess transport");
    }
    Ok(ParsedProfile {
        kind: ProfileKind::Vmess,
        name: string("ps").to_string(),
        address,
        port,
        uuid,
        password: None,
        method: None,
        alter_id: string("aid").parse().unwrap_or(0),
        security: if string("scy").is_empty() {
            "auto"
        } else {
            string("scy")
        }
        .into(),
        transport: transport.into(),
        host: nonempty(string("host")),
        path: nonempty(string("path")),
        tls: matches!(string("tls"), "tls" | "1"),
        sni: nonempty(string("sni")),
        reality_public_key: None,
        reality_short_id: None,
    })
}

fn parse_vless(input: &str) -> Result<ParsedProfile> {
    let (authority, query_name) = input
        .strip_prefix("vless://")
        .and_then(|value| value.split_once('#').or(Some((value, ""))))
        .ok_or_else(|| anyhow::anyhow!("invalid VLESS profile"))?;
    let (authority, query) = authority.split_once('?').unwrap_or((authority, ""));
    let (uuid, host_port) = authority
        .split_once('@')
        .ok_or_else(|| anyhow::anyhow!("invalid VLESS profile"))?;
    let (address, port) = host_port
        .rsplit_once(':')
        .ok_or_else(|| anyhow::anyhow!("invalid VLESS profile"))?;
    let port: u16 = port
        .parse()
        .map_err(|_| anyhow::anyhow!("invalid VLESS profile"))?;
    if uuid.is_empty() || address.is_empty() || port == 0 {
        anyhow::bail!("VLESS profile is missing required fields");
    }
    validate_uuid(uuid)?;
    let params = query
        .split('&')
        .filter_map(|pair| pair.split_once('='))
        .collect::<std::collections::HashMap<_, _>>();
    let transport = params.get("type").copied().unwrap_or("tcp");
    if !matches!(transport, "tcp" | "ws" | "grpc" | "http") {
        anyhow::bail!("unsupported VLESS transport");
    }
    let security = params.get("security").copied().unwrap_or("none");
    if !matches!(security, "none" | "tls" | "reality") {
        anyhow::bail!("unsupported VLESS security");
    }
    Ok(ParsedProfile {
        kind: ProfileKind::Vless,
        name: percent_decode(query_name),
        address: address.to_string(),
        port,
        uuid: uuid.to_string(),
        password: None,
        method: None,
        alter_id: 0,
        security: security.to_string(),
        transport: transport.to_string(),
        host: params.get("host").map(|v| percent_decode(v)),
        path: params.get("path").map(|v| percent_decode(v)),
        tls: security != "none",
        sni: params.get("sni").map(|v| percent_decode(v)),
        reality_public_key: params
            .get("pbk")
            .or_else(|| params.get("publicKey"))
            .map(|v| percent_decode(v)),
        reality_short_id: params
            .get("sid")
            .or_else(|| params.get("shortId"))
            .map(|v| percent_decode(v)),
    })
}

fn parse_shadowsocks(input: &str) -> Result<ParsedProfile> {
    let value = input
        .strip_prefix("ss://")
        .ok_or_else(|| anyhow::anyhow!("invalid Shadowsocks profile"))?;
    let (value, name) = value.split_once('#').unwrap_or((value, ""));
    let (authority, _) = value.split_once('?').unwrap_or((value, ""));
    let (encoded_user, host_port) = authority
        .rsplit_once('@')
        .ok_or_else(|| anyhow::anyhow!("invalid Shadowsocks profile"))?;
    let user = String::from_utf8(base64_decode(&percent_decode(encoded_user))?)
        .map_err(|_| anyhow::anyhow!("invalid Shadowsocks profile"))?;
    let (method, password) = user
        .split_once(':')
        .ok_or_else(|| anyhow::anyhow!("invalid Shadowsocks profile"))?;
    let (address, port) = host_port
        .trim_end_matches('/')
        .rsplit_once(':')
        .ok_or_else(|| anyhow::anyhow!("invalid Shadowsocks profile"))?;
    let port: u16 = port
        .parse()
        .map_err(|_| anyhow::anyhow!("invalid Shadowsocks profile"))?;
    if method.is_empty() || password.is_empty() || address.is_empty() || port == 0 {
        anyhow::bail!("Shadowsocks profile is missing required fields");
    }
    Ok(ParsedProfile {
        kind: ProfileKind::Shadowsocks,
        name: percent_decode(name),
        address: percent_decode(address),
        port,
        uuid: String::new(),
        alter_id: 0,
        password: Some(percent_decode(password)),
        method: Some(percent_decode(method)),
        security: String::new(),
        transport: String::new(),
        host: None,
        path: None,
        tls: false,
        sni: None,
        reality_public_key: None,
        reality_short_id: None,
    })
}

fn parse_trojan(input: &str) -> Result<ParsedProfile> {
    let value = input
        .strip_prefix("trojan://")
        .ok_or_else(|| anyhow::anyhow!("invalid Trojan profile"))?;
    let (value, name) = value.split_once('#').unwrap_or((value, ""));
    let (authority, query) = value.split_once('?').unwrap_or((value, ""));
    let (password, host_port) = authority
        .split_once('@')
        .ok_or_else(|| anyhow::anyhow!("invalid Trojan profile"))?;
    let (address, port) = host_port
        .rsplit_once(':')
        .ok_or_else(|| anyhow::anyhow!("invalid Trojan profile"))?;
    let port: u16 = port
        .parse()
        .map_err(|_| anyhow::anyhow!("invalid Trojan profile"))?;
    let params = query
        .split('&')
        .filter_map(|pair| pair.split_once('='))
        .collect::<std::collections::HashMap<_, _>>();
    let security = params.get("security").copied().unwrap_or("tls");
    if security != "tls" {
        anyhow::bail!("unsupported Trojan security");
    }
    if password.is_empty() || address.is_empty() || port == 0 {
        anyhow::bail!("Trojan profile is missing required fields");
    }
    Ok(ParsedProfile {
        kind: ProfileKind::Trojan,
        name: percent_decode(name),
        address: percent_decode(address),
        port,
        uuid: String::new(),
        alter_id: 0,
        password: Some(percent_decode(password)),
        method: None,
        security: security.to_string(),
        transport: "tcp".to_string(),
        host: None,
        path: None,
        tls: true,
        sni: params.get("sni").map(|value| percent_decode(value)),
        reality_public_key: None,
        reality_short_id: None,
    })
}

fn generate_config(profile: &ParsedProfile, port: u16) -> Result<serde_json::Value> {
    let outbound_type = match profile.kind {
        ProfileKind::Vmess => "vmess",
        ProfileKind::Vless => "vless",
        ProfileKind::Shadowsocks => "shadowsocks",
        ProfileKind::Trojan => "trojan",
    };
    let mut outbound = match profile.kind {
        ProfileKind::Shadowsocks => serde_json::json!({
            "type": outbound_type,
            "tag": "proxy",
            "server": profile.address,
            "server_port": profile.port,
            "method": profile.method,
            "password": profile.password,
        }),
        ProfileKind::Trojan => serde_json::json!({
            "type": outbound_type,
            "tag": "proxy",
            "server": profile.address,
            "server_port": profile.port,
            "password": profile.password,
        }),
        ProfileKind::Vmess => serde_json::json!({
            "type": outbound_type,
            "tag": "proxy",
            "server": profile.address,
            "server_port": profile.port,
            "uuid": profile.uuid,
            "security": profile.security
        }),
        ProfileKind::Vless => serde_json::json!({
            "type": outbound_type,
            "tag": "proxy",
            "server": profile.address,
            "server_port": profile.port,
            "uuid": profile.uuid
        }),
    };
    if matches!(profile.kind, ProfileKind::Vmess | ProfileKind::Vless) && profile.transport != "tcp"
    {
        let mut transport = serde_json::json!({"type": profile.transport});
        if let Some(path) = profile.path.as_ref() {
            transport["path"] = serde_json::json!(path);
        }
        if let Some(host) = profile.host.as_ref() {
            transport["headers"] = serde_json::json!({"Host": host});
        }
        outbound["transport"] = transport;
    }
    if profile.kind == ProfileKind::Vmess {
        outbound["alter_id"] = serde_json::json!(profile.alter_id);
    }
    if profile.tls {
        let mut tls = serde_json::json!({"enabled": true});
        if let Some(sni) = profile.sni.as_ref() {
            tls["server_name"] = serde_json::json!(sni);
        }
        outbound["tls"] = tls;
    }
    Ok(serde_json::json!({
        "log": {"disabled": true},
        "inbounds": [{
            "type": "mixed",
            "tag": "local",
            "listen": "127.0.0.1",
            "listen_port": port
        }],
        "outbounds": [outbound, {
            "type": "direct",
            "tag": "direct"
        }]
    }))
}

fn generate_shoes_config(profile: &ParsedProfile, port: u16) -> Result<serde_json::Value> {
    if matches!(profile.transport.as_str(), "grpc" | "http") {
        anyhow::bail!(
            "shoes backend does not support {} transport for this routing profile",
            profile.transport
        );
    }

    let base = match profile.kind {
        ProfileKind::Shadowsocks => serde_json::json!({
            "type": "shadowsocks",
            "cipher": profile.method,
            "password": profile.password,
        }),
        ProfileKind::Trojan => serde_json::json!({
            "type": "trojan",
            "password": profile.password,
        }),
        ProfileKind::Vmess => serde_json::json!({
            "type": "vmess",
            "cipher": shoes_vmess_cipher(&profile.security)?,
            "user_id": profile.uuid,
            "udp_enabled": true,
        }),
        ProfileKind::Vless => serde_json::json!({
            "type": "vless",
            "user_id": profile.uuid,
            "udp_enabled": true,
        }),
    };

    let mut transport = base;
    if profile.transport == "ws" {
        let mut websocket = serde_json::json!({
            "type": "websocket",
            "protocol": transport,
        });
        if let Some(path) = profile.path.as_ref() {
            websocket["matching_path"] = serde_json::json!(path);
        }
        if let Some(host) = profile.host.as_ref() {
            websocket["matching_headers"] = serde_json::json!({"Host": host});
        }
        transport = websocket;
    }

    if profile.security == "reality" {
        let public_key = profile
            .reality_public_key
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("Reality profile is missing its public key"))?;
        let sni = profile
            .sni
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("Reality profile is missing its SNI"))?;
        transport = serde_json::json!({
            "type": "reality",
            "public_key": public_key,
            "short_id": profile.reality_short_id.as_deref().unwrap_or(""),
            "sni_hostname": sni,
            "vision": profile.kind == ProfileKind::Vless
                && profile.transport == "tcp",
            "protocol": transport,
        });
    } else if profile.tls {
        transport = serde_json::json!({
            "type": "tls",
            "verify": true,
            "sni_hostname": profile.sni.as_deref().unwrap_or(&profile.address),
            "vision": profile.kind == ProfileKind::Vless && profile.transport == "tcp",
            "protocol": transport,
        });
    }

    Ok(serde_json::json!([{
        "address": format!("127.0.0.1:{port}"),
        "protocol": {"type": "socks", "udp_enabled": true},
        "rules": [{
            "masks": "0.0.0.0/0",
            "action": "allow",
            "client_chain": {
                "address": format!("{}:{}", profile.address, profile.port),
                "protocol": transport,
            }
        }]
    }]))
}

fn shoes_vmess_cipher(security: &str) -> Result<&'static str> {
    match security {
        "" | "auto" => Ok("chacha20-poly1305"),
        "aes-128-gcm" => Ok("aes-128-gcm"),
        "chacha20-poly1305" => Ok("chacha20-poly1305"),
        "none" => Ok("none"),
        _ => anyhow::bail!("shoes backend does not support VMess cipher `{security}`"),
    }
}

fn clean_name(name: &str) -> Result<String> {
    let name = name.trim();
    if name.is_empty() || name.len() > 100 {
        anyhow::bail!("profile name is invalid");
    }
    Ok(name.to_string())
}

fn validate_uuid(value: &str) -> Result<()> {
    let valid = value.len() == 36
        && value.chars().enumerate().all(|(index, character)| {
            matches!(index, 8 | 13 | 18 | 23)
                .then_some(character == '-')
                .unwrap_or_else(|| character.is_ascii_hexdigit())
        });
    if !valid {
        anyhow::bail!("routing profile has an invalid identifier");
    }
    Ok(())
}

fn base64_decode(value: &str) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    let alphabet = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut buffer = 0u32;
    let mut bits = 0u8;
    for byte in value.bytes().filter(|byte| !byte.is_ascii_whitespace()) {
        if byte == b'=' {
            break;
        }
        let byte = match byte {
            b'-' => b'+',
            b'_' => b'/',
            other => other,
        };
        let Some(value) = alphabet.iter().position(|candidate| *candidate == byte) else {
            anyhow::bail!("invalid VMess profile");
        };
        buffer = (buffer << 6) | value as u32;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            bytes.push((buffer >> bits) as u8);
        }
    }
    Ok(bytes)
}

fn percent_decode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let bytes = value.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(byte) = u8::from_str_radix(&value[i + 1..i + 3], 16) {
                out.push(byte as char);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    out
}

fn nonempty(value: &str) -> Option<String> {
    (!value.trim().is_empty()).then(|| value.to_string())
}

fn unique_id(profiles: &[StoredProfile]) -> String {
    let mut id = epoch().to_string();
    let mut suffix = 0;
    while profiles.iter().any(|profile| profile.id == id) {
        suffix += 1;
        id = format!("{}-{suffix}", epoch());
    }
    id
}

fn epoch() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn free_port() -> Result<u16> {
    let listener =
        std::net::TcpListener::bind(("127.0.0.1", 0)).context("allocating route port")?;
    Ok(listener.local_addr().context("reading route port")?.port())
}

fn wait_for_listener(port: u16) -> bool {
    for _ in 0..40 {
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    false
}

fn restrict(file: &File) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(fs::Permissions::from_mode(0o600))
            .context("restricting secret file permissions")?;
    }
    Ok(())
}

fn restrict_path(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))
            .context("restricting secret file permissions")?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn route_manager_without_active_core_is_healthy() {
        let manager = RouteManager::with_backend(
            "/tmp/crab-routing-health",
            "/bin/false",
            RoutingBackend::SingBox,
        );
        assert!(manager.is_healthy());
    }

    fn executable_fixture(root: &Path, name: &str) -> PathBuf {
        let path = root.join(name);
        fs::write(&path, b"#!/bin/sh\n").unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();
        path
    }

    #[test]
    fn detects_zero_one_and_two_available_routing_cores() {
        let root = std::env::temp_dir().join(format!("crab-routing-capabilities-{}", epoch()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        let missing_sing_box = root.join("missing-sing-box");
        let missing_shoes = root.join("missing-shoes");

        let none = RouteManager::with_paths(&root, &missing_sing_box, &missing_shoes);
        assert!(none.available_cores().is_empty());

        let sing_box = executable_fixture(&root, "sing-box");
        let one = RouteManager::with_paths(&root, &sing_box, &missing_shoes);
        assert_eq!(one.available_cores(), vec![RoutingBackend::SingBox]);

        let shoes = executable_fixture(&root, "shoes");
        let two = RouteManager::with_paths(&root, &sing_box, &shoes);
        assert_eq!(
            two.available_cores(),
            vec![RoutingBackend::SingBox, RoutingBackend::Shoes]
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn unavailable_core_is_rejected_without_starting_a_process() {
        let root = std::env::temp_dir().join(format!("crab-routing-unavailable-{}", epoch()));
        let _ = fs::remove_dir_all(&root);
        let manager = RouteManager::with_paths(
            &root,
            root.join("missing-sing-box"),
            root.join("missing-shoes"),
        );
        let error = manager
            .apply(
                "vless://11111111-1111-1111-1111-111111111111@example.com:443?security=tls",
                RoutingBackend::SingBox,
            )
            .unwrap_err()
            .to_string();
        assert!(error.contains("sing-box") && error.contains("unavailable"));
        assert!(manager.running_core().is_none());
    }

    #[test]
    fn parses_vless_without_exposing_secret() {
        let profile = parse_share_url(
            "vless://11111111-1111-1111-1111-111111111111@example.com:443?type=ws&security=tls&path=%2Ftg&sni=example.com#demo",
        )
        .unwrap();
        assert_eq!(profile.kind, ProfileKind::Vless);
        assert_eq!(profile.transport, "ws");
        assert_eq!(profile.path.as_deref(), Some("/tg"));
        assert!(!format!("{profile:?}").contains("11111111-1111"));
    }

    #[test]
    fn rejects_unsupported_and_incomplete_profiles() {
        assert!(parse_share_url("trojan://secret@example.com").is_err());
        assert!(parse_share_url("vless://uuid@example.com").is_err());
        assert!(parse_share_url("vmess://not-base64").is_err());
    }

    #[test]
    fn parses_shadowsocks_sip002_without_exposing_secret() {
        let profile =
            parse_share_url("ss://YWVzLTI1Ni1nY206c2VjcmV0@example.com:8388#office").unwrap();
        assert_eq!(profile.kind, ProfileKind::Shadowsocks);
        assert_eq!(profile.method.as_deref(), Some("aes-256-gcm"));
        assert_eq!(profile.password.as_deref(), Some("secret"));
        assert_eq!(profile.name, "office");
        assert!(!format!("{profile:?}").contains("secret"));
    }

    #[test]
    fn parses_trojan_with_tls_sni() {
        let profile =
            parse_share_url("trojan://secret@example.com:443?security=tls&sni=cdn.example#edge")
                .unwrap();
        assert_eq!(profile.kind, ProfileKind::Trojan);
        assert_eq!(profile.password.as_deref(), Some("secret"));
        assert_eq!(profile.sni.as_deref(), Some("cdn.example"));
        assert!(profile.tls);
    }

    #[test]
    fn generates_protocol_specific_sing_box_outbounds() {
        let shadowsocks =
            parse_share_url("ss://YWVzLTI1Ni1nY206c2VjcmV0@example.com:8388").unwrap();
        let shadowsocks_config = generate_config(&shadowsocks, 12345).unwrap();
        let shadowsocks_outbound = &shadowsocks_config["outbounds"][0];
        assert_eq!(shadowsocks_outbound["type"], "shadowsocks");
        assert_eq!(shadowsocks_outbound["method"], "aes-256-gcm");
        assert_eq!(shadowsocks_outbound["password"], "secret");
        assert!(shadowsocks_outbound.get("uuid").is_none());
        assert!(shadowsocks_outbound.get("transport").is_none());

        let trojan = parse_share_url("trojan://secret@example.com:443").unwrap();
        let trojan_config = generate_config(&trojan, 12345).unwrap();
        let trojan_outbound = &trojan_config["outbounds"][0];
        assert_eq!(trojan_outbound["type"], "trojan");
        assert_eq!(trojan_outbound["password"], "secret");
        assert!(trojan_outbound.get("transport").is_none());
        assert_eq!(trojan_outbound["tls"]["enabled"], true);
    }

    #[test]
    fn generates_minimal_config_for_all_supported_profiles_and_transports() {
        let profile = |kind: ProfileKind, transport: &str| ParsedProfile {
            kind,
            name: "test".into(),
            address: "proxy.example".into(),
            port: 443,
            uuid: "11111111-1111-1111-1111-111111111111".into(),
            password: matches!(kind, ProfileKind::Shadowsocks | ProfileKind::Trojan)
                .then(|| "secret".into()),
            method: (kind == ProfileKind::Shadowsocks).then(|| "aes-256-gcm".into()),
            alter_id: 0,
            security: if matches!(kind, ProfileKind::Vmess | ProfileKind::Vless) {
                "tls".into()
            } else {
                String::new()
            },
            transport: transport.into(),
            host: None,
            path: None,
            tls: matches!(
                kind,
                ProfileKind::Vmess | ProfileKind::Vless | ProfileKind::Trojan
            ),
            sni: None,
            reality_public_key: None,
            reality_short_id: None,
        };

        for kind in [ProfileKind::Vmess, ProfileKind::Vless] {
            for transport in ["tcp", "ws", "grpc", "http"] {
                let config = generate_config(&profile(kind, transport), 12345).unwrap();
                assert_eq!(config.as_object().unwrap().len(), 3);
                assert!(config.get("dns").is_none());
                assert!(config.get("route").is_none());
                assert_eq!(config["inbounds"].as_array().unwrap().len(), 1);
                assert_eq!(config["outbounds"].as_array().unwrap().len(), 2);
                if transport == "tcp" {
                    assert!(config["outbounds"][0].get("transport").is_none());
                } else {
                    assert_eq!(config["outbounds"][0]["transport"]["type"], transport);
                }
            }
        }

        for kind in [ProfileKind::Shadowsocks, ProfileKind::Trojan] {
            let config = generate_config(&profile(kind, "tcp"), 12345).unwrap();
            assert_eq!(config.as_object().unwrap().len(), 3);
            assert_eq!(
                config["outbounds"][0]["type"],
                match kind {
                    ProfileKind::Shadowsocks => "shadowsocks",
                    ProfileKind::Trojan => "trojan",
                    _ => unreachable!(),
                }
            );
        }
    }

    #[test]
    fn generates_shoes_yaml_shape_for_tcp_and_websocket_profiles() {
        let tcp = parse_share_url(
            "vless://11111111-1111-1111-1111-111111111111@example.com:443?security=tls&sni=example.com",
        )
        .unwrap();
        let tcp_config = generate_shoes_config(&tcp, 12345).unwrap();
        assert_eq!(tcp_config[0]["address"], "127.0.0.1:12345");
        assert_eq!(tcp_config[0]["protocol"]["type"], "socks");
        assert_eq!(
            tcp_config[0]["rules"][0]["client_chain"]["protocol"]["type"],
            "tls"
        );
        assert_eq!(
            tcp_config[0]["rules"][0]["client_chain"]["protocol"]["protocol"]["type"],
            "vless"
        );

        let ws = parse_share_url(
            "vless://11111111-1111-1111-1111-111111111111@example.com:443?type=ws&security=tls&path=%2Ftg&host=cdn.example&sni=cdn.example",
        )
        .unwrap();
        let ws_config = generate_shoes_config(&ws, 12345).unwrap();
        let protocol = &ws_config[0]["rules"][0]["client_chain"]["protocol"];
        assert_eq!(protocol["type"], "tls");
        assert_eq!(protocol["protocol"]["type"], "websocket");
        assert_eq!(protocol["protocol"]["matching_path"], "/tg");
        assert_eq!(
            protocol["protocol"]["matching_headers"]["Host"],
            "cdn.example"
        );
    }

    #[test]
    fn shoes_rejects_transports_without_a_documented_mapping() {
        let profile = parse_share_url(
            "vless://11111111-1111-1111-1111-111111111111@example.com:443?type=grpc&security=tls",
        )
        .unwrap();
        assert!(generate_shoes_config(&profile, 12345).is_err());
    }

    #[test]
    fn shoes_supports_reality_client_wrapping() {
        let profile = parse_share_url(
            "vless://11111111-1111-1111-1111-111111111111@example.com:443?security=reality&sni=www.example.com&pbk=public-key&sid=0123",
        )
        .unwrap();
        let config = generate_shoes_config(&profile, 12345).unwrap();
        let reality = &config[0]["rules"][0]["client_chain"]["protocol"];
        assert_eq!(reality["type"], "reality");
        assert_eq!(reality["public_key"], "public-key");
        assert_eq!(reality["short_id"], "0123");
        assert_eq!(reality["protocol"]["type"], "vless");
    }

    #[test]
    fn parses_routing_backend_selection() {
        assert_eq!(
            RoutingBackend::parse("sing-box").unwrap(),
            RoutingBackend::SingBox
        );
        assert_eq!(
            RoutingBackend::parse("shoes").unwrap(),
            RoutingBackend::Shoes
        );
        assert!(RoutingBackend::parse("unknown").is_err());
    }

    #[test]
    fn sanitizes_routing_core_diagnostics() {
        assert_eq!(
            sanitize_core_diagnostic("password=secret"),
            "invalid managed routing configuration"
        );
        assert_eq!(
            sanitize_core_diagnostic("unknown field transport"),
            "unknown field transport"
        );
    }

    #[test]
    fn persists_atomically_with_restrictive_permissions() {
        let root = std::env::temp_dir().join(format!("crab-routing-{}", epoch()));
        let _ = fs::remove_dir_all(&root);
        let store = ProfileStore::load(&root).unwrap();
        store
            .create(ProfileInput {
                name: "demo".into(),
                url: "vless://11111111-1111-1111-1111-111111111111@example.com:443?security=tls"
                    .into(),
            })
            .unwrap();
        let mode = fs::metadata(root.join(PROFILES_FILE))
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600);
        assert!(!store.list()[0].name.is_empty());
        let raw = fs::read_to_string(root.join(PROFILES_FILE)).unwrap();
        assert!(raw.contains("11111111"));
        assert!(!root.join("routing_profiles.json.tmp").exists());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn selected_core_defaults_and_persists() {
        let root = std::env::temp_dir().join(format!("crab-routing-core-{}", epoch()));
        let _ = fs::remove_dir_all(&root);
        let store = ProfileStore::load(&root).unwrap();
        assert_eq!(store.selected_core(), RoutingBackend::SingBox);
        store.set_selected_core(RoutingBackend::Shoes).unwrap();
        assert_eq!(
            ProfileStore::load(&root).unwrap().selected_core(),
            RoutingBackend::Shoes
        );
        let raw = fs::read_to_string(root.join(PROFILES_FILE)).unwrap();
        assert!(raw.contains("\"selected_core\": \"shoes\""));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn persisted_active_profile_is_inactive_when_selected_core_is_missing() {
        let root = std::env::temp_dir().join(format!("crab-routing-missing-core-{}", epoch()));
        let _ = fs::remove_dir_all(&root);
        let store = ProfileStore::load(&root).unwrap();
        let profile = store
            .create(ProfileInput {
                name: "demo".into(),
                url: "vless://11111111-1111-1111-1111-111111111111@example.com:443?security=tls"
                    .into(),
            })
            .unwrap();
        store.set_active(&profile.id).unwrap();

        let status = store.routing_status(None, &[]);
        assert_eq!(status.active_profile, None);
        assert!(status.available_cores.is_empty());
        assert!(!store.list_for_available(&[])[0].active);
        assert_eq!(store.active_id(), Some(profile.id));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn compatibility_matrix_rejects_shoes_only_transports() {
        let tcp =
            "vless://11111111-1111-1111-1111-111111111111@example.com:443?type=tcp&security=tls";
        let grpc =
            "vless://11111111-1111-1111-1111-111111111111@example.com:443?type=grpc&security=tls";
        assert_eq!(
            derive_compatible_cores(tcp).unwrap(),
            vec![RoutingBackend::SingBox, RoutingBackend::Shoes]
        );
        assert_eq!(
            derive_compatible_cores(grpc).unwrap(),
            vec![RoutingBackend::SingBox]
        );
    }

    #[test]
    fn legacy_profiles_derive_compatibility_and_default_core() {
        let root = std::env::temp_dir().join(format!("crab-routing-legacy-{}", epoch()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        fs::write(
            root.join(PROFILES_FILE),
            r#"{"profiles":[{"id":"1","name":"grpc","url":"vless://11111111-1111-1111-1111-111111111111@example.com:443?type=grpc&security=tls","kind":"vless","created_at":1,"updated_at":1}],"active_id":null}"#,
        )
        .unwrap();
        let store = ProfileStore::load(&root).unwrap();
        assert_eq!(store.selected_core(), RoutingBackend::SingBox);
        assert_eq!(
            store.list()[0].compatible_cores,
            vec![RoutingBackend::SingBox]
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn clear_active_preserves_profiles_and_persists_disabled_state() {
        let root = std::env::temp_dir().join(format!("crab-routing-clear-{}", epoch()));
        let _ = fs::remove_dir_all(&root);
        let store = ProfileStore::load(&root).unwrap();
        let created = store
            .create(ProfileInput {
                name: "demo".into(),
                url: "vless://11111111-1111-1111-1111-111111111111@example.com:443?security=tls"
                    .into(),
            })
            .unwrap();

        assert!(store.set_active(&created.id).unwrap());
        assert!(store.clear_active().unwrap());
        assert!(!store.clear_active().unwrap());
        assert_eq!(store.active_id(), None);
        assert_eq!(store.list().len(), 1);

        let reloaded = ProfileStore::load(&root).unwrap();
        assert_eq!(reloaded.active_id(), None);
        assert_eq!(reloaded.list().len(), 1);
        let _ = fs::remove_dir_all(root);
    }
}
