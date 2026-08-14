//! Dashboard-managed routing share profiles and the bundled sing-box core.
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
const CONFIG_FILE: &str = "sing-box.json";
const PROFILES_FILE: &str = "routing_profiles.json";

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
    created_at: u64,
    updated_at: u64,
}

#[derive(Clone, Default, Deserialize, Serialize)]
struct ProfileFile {
    profiles: Vec<StoredProfile>,
    active_id: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ProfileSummary {
    pub id: String,
    pub name: String,
    pub kind: ProfileKind,
    pub active: bool,
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
            Ok(contents) => serde_json::from_str(&contents).context("reading routing profiles")?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => ProfileFile::default(),
            Err(error) => return Err(error).context("opening routing profiles"),
        };
        Ok(Self {
            path,
            state: Mutex::new(state),
        })
    }

    pub fn list(&self) -> Vec<ProfileSummary> {
        let state = self.state.lock().expect("profile store lock poisoned");
        state
            .profiles
            .iter()
            .map(|profile| ProfileSummary {
                id: profile.id.clone(),
                name: profile.name.clone(),
                kind: profile.kind,
                active: state.active_id.as_deref() == Some(profile.id.as_str()),
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

    pub fn create(&self, input: ProfileInput) -> Result<ProfileSummary> {
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
            active: false,
        })
    }

    pub fn update(&self, id: &str, input: ProfileInput) -> Result<Option<ProfileSummary>> {
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
        profile.updated_at = epoch();
        let summary = ProfileSummary {
            id: profile.id.clone(),
            name: profile.name.clone(),
            kind: profile.kind,
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
    child: Child,
    config_path: PathBuf,
    proxy: String,
}

#[derive(Debug)]
pub struct RouteManager {
    sing_box_path: PathBuf,
    work_dir: PathBuf,
    active: Mutex<Option<ActiveCore>>,
    pending_previous: Mutex<Option<ActiveCore>>,
}

impl RouteManager {
    pub fn new(work_dir: impl Into<PathBuf>, sing_box_path: impl Into<PathBuf>) -> Self {
        Self {
            sing_box_path: sing_box_path.into(),
            work_dir: work_dir.into(),
            active: Mutex::new(None),
            pending_previous: Mutex::new(None),
        }
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

    pub fn apply(&self, url: &str) -> Result<String> {
        let profile = parse_share_url(url)?;
        let port = free_port()?;
        let config = generate_config(&profile, port)?;
        fs::create_dir_all(&self.work_dir).context("creating sing-box work directory")?;
        let config_path = self.work_dir.join(CONFIG_FILE);
        let tmp_path = self.work_dir.join("sing-box.json.tmp");
        let bytes = serde_json::to_vec_pretty(&config).context("serializing sing-box config")?;
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&tmp_path)
            .context("writing sing-box config")?;
        restrict(&file)?;
        file.write_all(&bytes).context("writing sing-box config")?;
        file.sync_all().context("flushing sing-box config")?;
        drop(file);
        fs::rename(&tmp_path, &config_path).context("installing sing-box config")?;
        restrict_path(&config_path)?;

        let mut child = Command::new(&self.sing_box_path)
            .args(["run", "-c"])
            .arg(&config_path)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .context("starting managed routing core")?;
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
            anyhow::bail!("managed routing core did not open its local listener: {diagnostic}");
        }

        let proxy = format!("socks5h://127.0.0.1:{port}");
        let mut active = self.active.lock().expect("route manager lock poisoned");
        let previous = active.replace(ActiveCore {
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

impl Drop for RouteManager {
    fn drop(&mut self) {
        self.stop();
    }
}

fn sanitize_core_diagnostic(output: &str) -> String {
    let line = output
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .unwrap_or_default();
    let lower = line.to_ascii_lowercase();
    if ["password", "uuid", "server", "url", "token", "secret"]
        .iter()
        .any(|term| lower.contains(term))
    {
        "invalid managed routing configuration".to_string()
    } else {
        line.to_string()
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
        let manager = RouteManager::new("/tmp/crab-routing-health", "/bin/false");
        assert!(manager.is_healthy());
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
