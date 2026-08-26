//! Persistent Telegram backup manifests and the streaming restore pipeline.
#![allow(dead_code)]

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Component, Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, LazyLock, Mutex,
};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::telegram::{self, UploadReceipt};

const MANIFEST_FILE: &str = "backup-manifests.json";
const REQUEST_FILE: &str = "restore-requests.json";
static MANIFEST_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ManifestPart {
    pub index: usize,
    pub filename: String,
    pub bytes: u64,
    pub chat_id: String,
    pub message_id: i64,
    pub file_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BackupManifest {
    pub backup_id: String,
    pub database_name: String,
    pub timestamp: String,
    pub filename: String,
    pub codec: String,
    pub encrypted: bool,
    pub packaged_bytes: u64,
    pub sha256: String,
    pub part_count: usize,
    pub parts: Vec<ManifestPart>,
    pub upload_complete: bool,
}

impl BackupManifest {
    pub fn restorable(&self) -> bool {
        self.upload_complete
            && self.part_count > 0
            && safe_path_component(&self.backup_id)
            && safe_path_component(&self.filename)
            && valid_sha256(&self.sha256)
            && self.parts.len() >= self.part_count
            && (0..self.part_count).all(|index| self.valid_part(index).is_some())
            && (0..self.part_count)
                .filter_map(|index| self.valid_part(index))
                .map(|part| part.bytes)
                .sum::<u64>()
                == self.packaged_bytes
    }

    fn valid_part(&self, index: usize) -> Option<&ManifestPart> {
        self.parts.iter().find(|part| {
            part.index == index
                && !part.file_id.is_empty()
                && part.message_id > 0
                && safe_path_component(&part.filename)
                && part.filename == self.expected_part_name(index)
        })
    }

    fn expected_part_name(&self, index: usize) -> String {
        if self.part_count == 1 {
            self.filename.clone()
        } else {
            format!("{}.part{index:04}", self.filename)
        }
    }
}

fn safe_path_component(value: &str) -> bool {
    let mut components = Path::new(value).components();
    matches!(components.next(), Some(Component::Normal(_))) && components.next().is_none()
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

#[derive(Debug, Clone)]
pub struct ManifestStore {
    path: PathBuf,
    lock: Arc<Mutex<()>>,
}

impl ManifestStore {
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            path: data_dir.into().join(MANIFEST_FILE),
            lock: Arc::new(Mutex::new(())),
        }
    }

    pub fn list(&self) -> Result<Vec<BackupManifest>> {
        let _global_guard = MANIFEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let _guard = self.lock.lock().unwrap_or_else(|e| e.into_inner());
        self.read_locked()
    }

    pub fn restorable(&self) -> Result<Vec<BackupManifest>> {
        let mut manifests = self
            .list()?
            .into_iter()
            .filter(|item| item.restorable())
            .collect::<Vec<_>>();
        manifests.sort_by(|left, right| right.timestamp.cmp(&left.timestamp));
        Ok(manifests)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn begin(
        &self,
        backup_id: impl Into<String>,
        database_name: impl Into<String>,
        timestamp: impl Into<String>,
        filename: impl Into<String>,
        codec: impl Into<String>,
        encrypted: bool,
        packaged_bytes: u64,
        sha256: impl Into<String>,
        part_count: usize,
    ) -> Result<()> {
        let _global_guard = MANIFEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let _guard = self.lock.lock().unwrap_or_else(|e| e.into_inner());
        let mut manifests = self.read_locked()?;
        let backup_id = backup_id.into();
        manifests.retain(|item| item.backup_id != backup_id);
        manifests.push(BackupManifest {
            backup_id,
            database_name: database_name.into(),
            timestamp: timestamp.into(),
            filename: filename.into(),
            codec: codec.into(),
            encrypted,
            packaged_bytes,
            sha256: sha256.into(),
            part_count,
            parts: Vec::new(),
            upload_complete: false,
        });
        self.write_locked(&manifests)
    }

    pub fn record_part(&self, backup_id: &str, part: ManifestPart) -> Result<()> {
        let _global_guard = MANIFEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let _guard = self.lock.lock().unwrap_or_else(|e| e.into_inner());
        let mut manifests = self.read_locked()?;
        let manifest = manifests
            .iter_mut()
            .find(|item| item.backup_id == backup_id)
            .with_context(|| format!("backup manifest not found: {backup_id}"))?;
        manifest
            .parts
            .retain(|item| !(item.index == part.index && item.chat_id == part.chat_id));
        manifest.parts.push(part);
        manifest
            .parts
            .sort_by_key(|item| (item.index, item.chat_id.clone()));
        self.write_locked(&manifests)
    }

    pub fn complete(&self, backup_id: &str) -> Result<BackupManifest> {
        let _global_guard = MANIFEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let _guard = self.lock.lock().unwrap_or_else(|e| e.into_inner());
        let mut manifests = self.read_locked()?;
        let manifest = manifests
            .iter_mut()
            .find(|item| item.backup_id == backup_id)
            .with_context(|| format!("backup manifest not found: {backup_id}"))?;
        let expected = manifest.part_count;
        let valid = manifest.parts.len() >= expected
            && (0..expected).all(|index| {
                manifest.parts.iter().any(|part| {
                    part.index == index && !part.file_id.is_empty() && part.message_id > 0
                })
            });
        if !valid {
            bail!("backup manifest is incomplete; refusing to mark it restorable");
        }
        manifest.upload_complete = true;
        if !manifest.restorable() {
            manifest.upload_complete = false;
            bail!("backup manifest failed integrity checks; refusing to mark it restorable");
        }
        let result = manifest.clone();
        self.write_locked(&manifests)?;
        Ok(result)
    }

    pub fn find_restorable(&self, backup_id: &str) -> Result<BackupManifest> {
        self.restorable()?
            .into_iter()
            .find(|item| item.backup_id == backup_id)
            .with_context(|| format!("restorable backup not found: {backup_id}"))
    }

    fn read_locked(&self) -> Result<Vec<BackupManifest>> {
        let bytes = match fs::read(&self.path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("reading backup manifests {}", self.path.display()))
            }
        };
        serde_json::from_slice(&bytes)
            .with_context(|| format!("parsing backup manifests {}", self.path.display()))
    }

    fn write_locked(&self, manifests: &[BackupManifest]) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("creating manifest directory {}", parent.display()))?;
        }
        let temp = self.path.with_extension("json.tmp");
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temp)
            .with_context(|| format!("creating manifest temp file {}", temp.display()))?;
        restrict_file_permissions(&temp)?;
        serde_json::to_writer_pretty(&mut file, manifests)
            .context("serializing backup manifests")?;
        file.write_all(b"\n")
            .context("terminating backup manifests")?;
        file.sync_all().context("syncing backup manifests")?;
        fs::rename(&temp, &self.path).with_context(|| {
            format!(
                "installing backup manifests {} -> {}",
                temp.display(),
                self.path.display()
            )
        })
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum RestoreMode {
    Safe,
    Clean,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum RestoreStatus {
    Queued,
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

fn default_restore_requested_by() -> String {
    "unknown".to_string()
}

fn default_restore_mode() -> RestoreMode {
    RestoreMode::Safe
}

fn default_restore_status() -> RestoreStatus {
    RestoreStatus::Queued
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RestoreRequest {
    #[serde(default)]
    pub request_id: String,
    #[serde(default)]
    pub backup_id: String,
    #[serde(default)]
    pub database_name: String,
    #[serde(default = "default_restore_requested_by")]
    pub requested_by: String,
    #[serde(default = "default_restore_mode")]
    pub mode: RestoreMode,
    #[serde(default = "default_restore_status")]
    pub status: RestoreStatus,
    #[serde(default)]
    pub audit: Vec<String>,
    #[serde(default)]
    pub error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct RestoreController {
    path: PathBuf,
    lock: Arc<Mutex<()>>,
    cancellations: Arc<Mutex<HashMap<String, Arc<AtomicBool>>>>,
}

impl RestoreController {
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            path: data_dir.into().join(REQUEST_FILE),
            lock: Arc::new(Mutex::new(())),
            cancellations: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn list(&self) -> Result<Vec<RestoreRequest>> {
        let _guard = self.lock.lock().unwrap_or_else(|e| e.into_inner());
        self.read_locked()
    }

    pub fn recover_stale_running(&self) -> Result<usize> {
        let _guard = self.lock.lock().unwrap_or_else(|e| e.into_inner());
        let mut requests = self.read_locked()?;
        let mut recovered = 0;
        for request in &mut requests {
            if request.status == RestoreStatus::Running {
                request.status = RestoreStatus::Failed;
                request.error = Some("restore interrupted by process restart".into());
                request
                    .audit
                    .push("marked failed during startup recovery".into());
                recovered += 1;
            }
        }
        self.cancellations
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
        if recovered > 0 {
            self.write_locked(&requests)?;
        }
        Ok(recovered)
    }

    pub fn queue(&self, request: RestoreRequest) -> Result<()> {
        let _guard = self.lock.lock().unwrap_or_else(|e| e.into_inner());
        let mut requests = self.read_locked()?;
        if requests.iter().any(|item| {
            item.database_name == request.database_name
                && matches!(item.status, RestoreStatus::Queued | RestoreStatus::Running)
        }) {
            bail!("a restore is already queued or running for this database");
        }
        requests.push(request);
        self.write_locked(&requests)
    }

    pub fn approve(&self, request_id: &str, administrator: bool) -> Result<RestoreRequest> {
        self.approve_as(
            request_id,
            if administrator {
                "administrator"
            } else {
                "operator"
            },
            administrator,
        )
    }

    pub fn approve_as(
        &self,
        request_id: &str,
        actor: &str,
        administrator: bool,
    ) -> Result<RestoreRequest> {
        let _guard = self.lock.lock().unwrap_or_else(|e| e.into_inner());
        let mut requests = self.read_locked()?;
        let request = requests
            .iter_mut()
            .find(|item| item.request_id == request_id)
            .with_context(|| format!("restore request not found: {request_id}"))?;
        if request.mode == RestoreMode::Clean && !administrator {
            bail!("clean restore requires administrator approval");
        }
        if request.status != RestoreStatus::Queued {
            bail!("restore request is not queued");
        }
        request.status = RestoreStatus::Running;
        request.audit.push(format!("approved by {actor}"));
        let result = request.clone();
        self.write_locked(&requests)?;
        self.cancellations
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(request_id.to_string(), Arc::new(AtomicBool::new(false)));
        Ok(result)
    }

    pub fn set_mode(&self, request_id: &str, mode: RestoreMode) -> Result<()> {
        let _guard = self.lock.lock().unwrap_or_else(|e| e.into_inner());
        let mut requests = self.read_locked()?;
        let request = requests
            .iter_mut()
            .find(|item| item.request_id == request_id)
            .with_context(|| format!("restore request not found: {request_id}"))?;
        if request.status != RestoreStatus::Queued {
            bail!("restore request is not queued");
        }
        request.mode = mode;
        request.audit.push(format!(
            "restore mode set to {}",
            match mode {
                RestoreMode::Safe => "safe",
                RestoreMode::Clean => "clean",
            }
        ));
        self.write_locked(&requests)
    }

    pub fn finish(
        &self,
        request_id: &str,
        status: RestoreStatus,
        error: Option<String>,
    ) -> Result<()> {
        let _guard = self.lock.lock().unwrap_or_else(|e| e.into_inner());
        let mut requests = self.read_locked()?;
        let request = requests
            .iter_mut()
            .find(|item| item.request_id == request_id)
            .with_context(|| format!("restore request not found: {request_id}"))?;
        if !matches!(
            status,
            RestoreStatus::Succeeded | RestoreStatus::Failed | RestoreStatus::Cancelled
        ) {
            bail!("restore completion must use a terminal status");
        }
        if request.status != RestoreStatus::Running {
            bail!("restore request is not running");
        }
        request.status = status;
        request.error = error;
        request.audit.push(format!(
            "restore finished with {}",
            match request.status {
                RestoreStatus::Succeeded => "succeeded",
                RestoreStatus::Failed => "failed",
                RestoreStatus::Cancelled => "cancelled",
                RestoreStatus::Queued => "queued",
                RestoreStatus::Running => "running",
            }
        ));
        let result = self.write_locked(&requests);
        if result.is_ok() {
            self.cancellations
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(request_id);
        }
        result
    }

    pub fn cancel(&self, request_id: &str, actor: &str) -> Result<RestoreRequest> {
        let _guard = self.lock.lock().unwrap_or_else(|e| e.into_inner());
        let mut requests = self.read_locked()?;
        let request = requests
            .iter_mut()
            .find(|item| item.request_id == request_id)
            .with_context(|| format!("restore request not found: {request_id}"))?;
        match request.status {
            RestoreStatus::Queued => {}
            RestoreStatus::Running => {
                if let Some(token) = self
                    .cancellations
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .get(request_id)
                {
                    token.store(true, Ordering::SeqCst);
                }
            }
            _ => bail!("only queued or running restore requests can be cancelled"),
        }
        request.status = RestoreStatus::Cancelled;
        request.audit.push(format!("cancelled by {actor}"));
        let result = request.clone();
        self.write_locked(&requests)?;
        self.cancellations
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(request_id);
        Ok(result)
    }

    pub fn cancellation_token(&self, request_id: &str) -> Option<Arc<AtomicBool>> {
        self.cancellations
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(request_id)
            .cloned()
    }

    pub fn is_cancelled(&self, request_id: &str) -> bool {
        self.list()
            .ok()
            .and_then(|requests| {
                requests
                    .into_iter()
                    .find(|request| request.request_id == request_id)
            })
            .is_some_and(|request| request.status == RestoreStatus::Cancelled)
    }

    fn read_locked(&self) -> Result<Vec<RestoreRequest>> {
        match fs::read(&self.path) {
            Ok(bytes) if bytes.iter().all(u8::is_ascii_whitespace) => Ok(Vec::new()),
            Ok(bytes) => parse_restore_requests(&bytes).with_context(|| {
                format!("parsing persisted restore requests {}", self.path.display())
            }),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(Vec::new()),
            Err(error) => Err(error).with_context(|| {
                format!("reading persisted restore requests {}", self.path.display())
            }),
        }
    }

    fn write_locked(&self, requests: &[RestoreRequest]) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("creating restore directory {}", parent.display()))?;
        }
        let temp = self.path.with_extension("json.tmp");
        fs::write(
            &temp,
            serde_json::to_vec_pretty(requests).context("serializing restore requests")?,
        )
        .with_context(|| format!("writing restore requests {}", temp.display()))?;
        restrict_file_permissions(&temp)?;
        fs::rename(&temp, &self.path)
            .with_context(|| format!("installing restore requests {}", self.path.display()))
    }
}

fn parse_restore_requests(bytes: &[u8]) -> Result<Vec<RestoreRequest>> {
    let value: serde_json::Value = serde_json::from_slice(bytes).context("invalid JSON")?;
    match value {
        serde_json::Value::Null => Ok(Vec::new()),
        serde_json::Value::Array(_) => {
            serde_json::from_value(value).context("restore request list must be an array")
        }
        serde_json::Value::Object(mut object) => {
            let requests = object
                .remove("requests")
                .or_else(|| object.remove("restore_requests"))
                .context("restore request state must contain a requests array")?;
            serde_json::from_value(requests).context("restore request list must be an array")
        }
        _ => bail!("restore request state must be an array"),
    }
}

fn restrict_file_permissions(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))
            .with_context(|| format!("restricting permissions on {}", path.display()))?;
    }
    Ok(())
}

pub fn reassemble_and_verify(
    parts: &[PathBuf],
    output: &Path,
    expected_sha256: &str,
    expected_bytes: u64,
) -> Result<()> {
    let mut output_file = File::create(output)
        .with_context(|| format!("creating reassembled archive {}", output.display()))?;
    let mut hasher = Sha256::new();
    let mut total = 0u64;
    let mut buffer = [0u8; 64 * 1024];
    for path in parts {
        let mut input = File::open(path)
            .with_context(|| format!("opening downloaded part {}", path.display()))?;
        loop {
            let read = input
                .read(&mut buffer)
                .with_context(|| format!("reading downloaded part {}", path.display()))?;
            if read == 0 {
                break;
            }
            output_file
                .write_all(&buffer[..read])
                .context("writing reassembled archive")?;
            hasher.update(&buffer[..read]);
            total = total.saturating_add(read as u64);
        }
    }
    output_file
        .sync_all()
        .context("syncing reassembled archive")?;
    let actual = hex::encode(hasher.finalize());
    if total != expected_bytes || actual != expected_sha256 {
        bail!(
            "reassembled backup verification failed (bytes {total}/{expected_bytes}, sha256 {actual}/{expected_sha256})"
        );
    }
    Ok(())
}

pub fn restore_archive(
    archive: &Path,
    output_dir: &Path,
    database_url: &str,
    mode: RestoreMode,
    encrypted: bool,
    identity_file: Option<&Path>,
    passphrase: Option<&str>,
) -> Result<()> {
    validate_restore_credentials(encrypted, identity_file, passphrase)?;
    fs::create_dir_all(output_dir)
        .with_context(|| format!("creating restore work directory {}", output_dir.display()))?;
    let decrypted = output_dir.join("restore.decrypted");
    let decoded = output_dir.join("restore.dump");
    let mut input: Box<dyn Read> = if encrypted {
        let bytes = File::open(archive).context("opening encrypted archive")?;
        let decryptor = age::Decryptor::new(bytes).context("parsing age archive")?;
        if let Some(path) = identity_file {
            let text = fs::read_to_string(path).context("reading age identity file")?;
            let identities = parse_age_identities(&text)?;
            Box::new(
                decryptor
                    .decrypt(
                        identities
                            .iter()
                            .map(|identity| identity as &dyn age::Identity),
                    )
                    .context("decrypting age archive")?,
            )
        } else {
            let identity = age::scrypt::Identity::new(age::secrecy::SecretString::from(
                passphrase.unwrap_or_default().to_string(),
            ));
            Box::new(
                decryptor
                    .decrypt(std::iter::once(&identity as &dyn age::Identity))
                    .context("decrypting age archive")?,
            )
        }
    } else {
        Box::new(File::open(archive).context("opening restore archive")?)
    };
    let mut decrypted_file =
        File::create(&decrypted).context("creating decrypted restore stream")?;
    io::copy(&mut input, &mut decrypted_file).context("streaming restore archive")?;
    decrypted_file
        .sync_all()
        .context("syncing decrypted restore stream")?;
    let mut decoded_file = File::create(&decoded).context("creating decoded restore dump")?;
    if archive
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.contains(".zst"))
    {
        let mut zstd_reader = zstd::stream::read::Decoder::new(
            File::open(&decrypted).context("opening zstd stream")?,
        )
        .context("initializing zstd decoder")?;
        io::copy(&mut zstd_reader, &mut decoded_file)
            .context("decompressing zstd restore stream")?;
    } else {
        io::copy(
            &mut File::open(&decrypted).context("opening restore stream")?,
            &mut decoded_file,
        )
        .context("streaming restore dump")?;
    }
    decoded_file
        .sync_all()
        .context("syncing decoded restore dump")?;

    let args = pg_restore_args(database_url, mode, &decoded);
    let status = Command::new("pg_restore")
        .args(&args)
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .context("spawning pg_restore")?;
    if !status.success() {
        bail!("pg_restore exited with status {status}");
    }
    Ok(())
}

fn validate_restore_credentials(
    encrypted: bool,
    identity_file: Option<&Path>,
    passphrase: Option<&str>,
) -> Result<()> {
    if !encrypted {
        return Ok(());
    }
    if let Some(path) = identity_file {
        if !path.is_file() {
            bail!("configured AGE_IDENTITY_FILE is unavailable");
        }
        return Ok(());
    }
    if passphrase.is_some_and(|value| !value.is_empty()) {
        return Ok(());
    }
    bail!("encrypted backup requires AGE_IDENTITY_FILE or AGE_PASSPHRASE");
}

fn parse_age_identities(text: &str) -> Result<Vec<age::x25519::Identity>> {
    let identities = text
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(|line| {
            line.parse::<age::x25519::Identity>()
                .map_err(|error| anyhow::anyhow!("parsing age identity: {error}"))
        })
        .collect::<Result<Vec<_>>>()?;
    if identities.is_empty() {
        bail!("age identity file contains no identities");
    }
    Ok(identities)
}

pub fn pg_restore_args(database_url: &str, mode: RestoreMode, dump: &Path) -> Vec<String> {
    let mut args = vec![
        "--dbname".to_string(),
        database_url.to_string(),
        "--no-owner".into(),
    ];
    if mode == RestoreMode::Clean {
        args.extend(["--clean".into(), "--if-exists".into()]);
    }
    args.push(dump.display().to_string());
    args
}

/// Download a manifest's ordered parts through the caller's shared routed
/// client and execute the restore. The work directory is swept on every exit
/// unless `keep_failed` is set.
#[allow(clippy::too_many_arguments)]
pub fn download_and_restore(
    client: &reqwest::blocking::Client,
    bot_token: &str,
    manifest: &BackupManifest,
    work_dir: &Path,
    database_url: &str,
    mode: RestoreMode,
    identity_file: Option<&Path>,
    passphrase: Option<&str>,
    keep_failed: bool,
    cancellation: Option<&AtomicBool>,
) -> Result<()> {
    if !manifest.restorable() {
        bail!("backup manifest is incomplete or failed verification");
    }
    validate_restore_credentials(manifest.encrypted, identity_file, passphrase)?;
    let root = work_dir.join(format!("restore-{}", manifest.backup_id));
    fs::create_dir_all(&root)
        .with_context(|| format!("creating restore directory {}", root.display()))?;
    let result = (|| -> Result<()> {
        let mut downloaded = Vec::with_capacity(manifest.part_count);
        for index in 0..manifest.part_count {
            if cancellation.is_some_and(|token| token.load(Ordering::SeqCst)) {
                bail!("restore cancelled");
            }
            let part = manifest
                .parts
                .iter()
                .find(|part| part.index == index)
                .context("manifest has no part for expected index")?;
            let destination = root.join(format!("part-{index:04}"));
            telegram::download_file(client, bot_token, &part.file_id, &destination)?;
            downloaded.push(destination);
        }
        if cancellation.is_some_and(|token| token.load(Ordering::SeqCst)) {
            bail!("restore cancelled");
        }
        let archive = root.join(&manifest.filename);
        reassemble_and_verify(
            &downloaded,
            &archive,
            &manifest.sha256,
            manifest.packaged_bytes,
        )?;
        restore_archive(
            &archive,
            &root,
            database_url,
            mode,
            manifest.encrypted,
            identity_file,
            passphrase,
        )
    })();
    if result.is_ok() || !keep_failed {
        let _ = fs::remove_dir_all(&root);
    }
    result
}

/// Remove restore workspaces left behind by an interrupted process.
///
/// Restore directories are namespaced with the `restore-` prefix, so this
/// sweep cannot touch ordinary backup chunks or unrelated work files.
pub fn cleanup_stale_workspaces(work_dir: &Path, keep_failed: bool) -> Result<usize> {
    if keep_failed {
        return Ok(0);
    }
    let entries = match fs::read_dir(work_dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(0),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("reading restore work directory {}", work_dir.display()))
        }
    };
    let mut removed = 0;
    for entry in entries {
        let entry = entry.with_context(|| {
            format!(
                "reading restore work directory entry {}",
                work_dir.display()
            )
        })?;
        let name = entry.file_name();
        if !name.to_string_lossy().starts_with("restore-") || !entry.file_type()?.is_dir() {
            continue;
        }
        fs::remove_dir_all(entry.path()).with_context(|| {
            format!(
                "removing stale restore workspace {}",
                entry.path().display()
            )
        })?;
        removed += 1;
    }
    Ok(removed)
}

pub fn new_request_id() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("restore-{nanos:x}")
}

pub fn manifest_part(
    index: usize,
    path: &Path,
    chat_id: &str,
    receipt: &UploadReceipt,
) -> Result<ManifestPart> {
    Ok(ManifestPart {
        index,
        filename: path
            .file_name()
            .and_then(|name| name.to_str())
            .context("uploaded part has no valid filename")?
            .to_string(),
        bytes: fs::metadata(path)
            .with_context(|| format!("reading uploaded part {}", path.display()))?
            .len(),
        chat_id: chat_id.to_string(),
        message_id: receipt.message_id,
        file_id: receipt.file_id.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(label: &str) -> PathBuf {
        let path =
            std::env::temp_dir().join(format!("crab-restore-{label}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn incomplete_manifest_is_not_restorable() {
        let dir = temp_dir("manifest");
        let store = ManifestStore::new(&dir);
        store
            .begin("id", "db", "ts", "db.dump.zst", "zstd", false, 3, "abc", 2)
            .unwrap();
        store
            .record_part(
                "id",
                ManifestPart {
                    index: 0,
                    filename: "db.dump.zst.part0000".into(),
                    bytes: 3,
                    chat_id: "1".into(),
                    message_id: 2,
                    file_id: "file".into(),
                },
            )
            .unwrap();
        assert!(store.restorable().unwrap().is_empty());
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn restorable_manifest_requires_valid_hash_metadata() {
        let mut manifest = BackupManifest {
            backup_id: "backup".into(),
            database_name: "db".into(),
            timestamp: "ts".into(),
            filename: "db.dump.zst".into(),
            codec: "zstd".into(),
            encrypted: false,
            packaged_bytes: 3,
            sha256: "not-a-hash".into(),
            part_count: 1,
            parts: vec![ManifestPart {
                index: 0,
                filename: "db.dump.zst".into(),
                bytes: 3,
                chat_id: "1".into(),
                message_id: 1,
                file_id: "file".into(),
            }],
            upload_complete: true,
        };
        assert!(!manifest.restorable());
        manifest.sha256 = "a".repeat(64);
        assert!(manifest.restorable());
    }

    #[test]
    fn legacy_restore_request_defaults_missing_fields() {
        let request: RestoreRequest = serde_json::from_str(
            r#"{"request_id":"request-1","backup_id":"backup-1","database_name":"db"}"#,
        )
        .unwrap();
        assert_eq!(request.requested_by, "unknown");
        assert_eq!(request.mode, RestoreMode::Safe);
        assert_eq!(request.status, RestoreStatus::Queued);
        assert!(request.audit.is_empty());
        assert_eq!(request.error, None);
    }

    #[test]
    fn empty_restore_request_state_is_an_empty_queue() {
        let dir = temp_dir("empty-request-state");
        fs::write(dir.join(REQUEST_FILE), b" \n\t").unwrap();

        let controller = RestoreController::new(&dir);

        assert!(controller.list().unwrap().is_empty());
    }

    #[test]
    fn restore_request_state_accepts_legacy_wrappers_and_null() {
        let wrapped = br#"{"requests":[]}"#;
        let legacy = br#"{"restore_requests":[]}"#;
        assert!(parse_restore_requests(wrapped).unwrap().is_empty());
        assert!(parse_restore_requests(legacy).unwrap().is_empty());
        assert!(parse_restore_requests(b"null").unwrap().is_empty());
    }

    #[test]
    fn completion_does_not_persist_incomplete_integrity_metadata() {
        let dir = temp_dir("completion-integrity");
        let store = ManifestStore::new(&dir);
        store
            .begin(
                "backup",
                "db",
                "ts",
                "db.dump.zst",
                "zstd",
                false,
                3,
                "not-a-hash",
                1,
            )
            .unwrap();
        store
            .record_part(
                "backup",
                ManifestPart {
                    index: 0,
                    filename: "db.dump.zst".into(),
                    bytes: 3,
                    chat_id: "1".into(),
                    message_id: 1,
                    file_id: "file".into(),
                },
            )
            .unwrap();
        assert!(store.complete("backup").is_err());
        let saved = store.list().unwrap().pop().unwrap();
        assert!(!saved.upload_complete);
        assert!(!saved.restorable());
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn reassembly_is_ordered_and_verified() {
        let dir = temp_dir("reassemble");
        let first = dir.join("part0000");
        let second = dir.join("part0001");
        fs::write(&first, b"abc").unwrap();
        fs::write(&second, b"def").unwrap();
        let hash = hex::encode(Sha256::digest(b"abcdef"));
        let output = dir.join("archive");
        reassemble_and_verify(&[first, second], &output, &hash, 6).unwrap();
        assert_eq!(fs::read(output).unwrap(), b"abcdef");
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn restore_modes_have_expected_pg_restore_flags() {
        let dump = Path::new("/tmp/restore.dump");
        assert_eq!(
            pg_restore_args("postgresql://user:secret@host/db", RestoreMode::Safe, dump),
            vec![
                "--dbname",
                "postgresql://user:secret@host/db",
                "--no-owner",
                "/tmp/restore.dump"
            ]
        );
        let clean = pg_restore_args("postgresql://user:secret@host/db", RestoreMode::Clean, dump);
        assert!(clean.contains(&"--clean".into()));
        assert!(clean.contains(&"--if-exists".into()));
    }

    #[test]
    fn encrypted_restore_rejects_missing_credentials_before_execution() {
        let dir = temp_dir("credentials");
        let error = restore_archive(
            &dir.join("missing.dump.zst.age"),
            &dir,
            "postgresql://host/db",
            RestoreMode::Safe,
            true,
            None,
            None,
        )
        .unwrap_err();
        assert!(error.to_string().contains("AGE_IDENTITY_FILE"));
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn age_identity_files_allow_comments_blanks_and_multiple_identities() {
        const IDENTITY: &str =
            "AGE-SECRET-KEY-1GQ9778VQXMMJVE8SK7J6VT8UJ4HDQAJUVSFCWCM02D8GEWQ72PVQ2Y5J33";
        let identities =
            parse_age_identities(&format!("\n# generated\n{IDENTITY}\n\n{IDENTITY}\n")).unwrap();
        assert_eq!(identities.len(), 2);
        assert!(parse_age_identities("# only comments\n\n").is_err());
    }

    #[test]
    fn controller_rejects_duplicate_targets_and_non_admin_clean_approval() {
        let dir = temp_dir("controller");
        let controller = RestoreController::new(&dir);
        let request = RestoreRequest {
            request_id: "request-1".into(),
            backup_id: "backup-1".into(),
            database_name: "db".into(),
            requested_by: "chat".into(),
            mode: RestoreMode::Clean,
            status: RestoreStatus::Queued,
            audit: Vec::new(),
            error: None,
        };
        controller.queue(request.clone()).unwrap();
        assert!(controller.queue(request).is_err());
        assert!(controller
            .finish("request-1", RestoreStatus::Succeeded, None)
            .is_err());
        controller
            .set_mode("request-1", RestoreMode::Clean)
            .unwrap();
        assert!(controller
            .approve_as("request-1", "operator", false)
            .is_err());
        let approved = controller.approve_as("request-1", "admin", true).unwrap();
        assert_eq!(approved.status, RestoreStatus::Running);
        assert!(approved
            .audit
            .iter()
            .any(|entry| entry == "approved by admin"));
        controller
            .finish("request-1", RestoreStatus::Succeeded, None)
            .unwrap();
        let finished = controller.list().unwrap().pop().unwrap();
        assert!(finished
            .audit
            .iter()
            .any(|entry| entry == "restore mode set to clean"));
        assert!(finished
            .audit
            .iter()
            .any(|entry| entry == "restore finished with succeeded"));
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn startup_recovery_marks_running_requests_failed() {
        let dir = temp_dir("startup-recovery");
        let controller = RestoreController::new(&dir);
        controller
            .queue(RestoreRequest {
                request_id: "request-1".into(),
                backup_id: "backup-1".into(),
                database_name: "db".into(),
                requested_by: "chat".into(),
                mode: RestoreMode::Safe,
                status: RestoreStatus::Queued,
                audit: Vec::new(),
                error: None,
            })
            .unwrap();
        controller
            .approve_as("request-1", "operator", false)
            .unwrap();

        assert_eq!(controller.recover_stale_running().unwrap(), 1);
        let request = controller.list().unwrap().pop().unwrap();
        assert_eq!(request.status, RestoreStatus::Failed);
        assert_eq!(
            request.error.as_deref(),
            Some("restore interrupted by process restart")
        );
        assert!(request
            .audit
            .iter()
            .any(|entry| entry == "marked failed during startup recovery"));

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn queued_and_running_requests_can_be_cancelled() {
        let dir = temp_dir("cancel");
        let controller = RestoreController::new(&dir);
        controller
            .queue(RestoreRequest {
                request_id: "request-1".into(),
                backup_id: "backup-1".into(),
                database_name: "db".into(),
                requested_by: "chat".into(),
                mode: RestoreMode::Safe,
                status: RestoreStatus::Queued,
                audit: Vec::new(),
                error: None,
            })
            .unwrap();
        let cancelled = controller.cancel("request-1", "operator").unwrap();
        assert_eq!(cancelled.status, RestoreStatus::Cancelled);
        assert!(cancelled
            .audit
            .iter()
            .any(|entry| entry == "cancelled by operator"));

        controller
            .queue(RestoreRequest {
                request_id: "request-2".into(),
                backup_id: "backup-2".into(),
                database_name: "db".into(),
                requested_by: "chat".into(),
                mode: RestoreMode::Safe,
                status: RestoreStatus::Queued,
                audit: Vec::new(),
                error: None,
            })
            .unwrap();
        controller
            .approve_as("request-2", "operator", false)
            .unwrap();
        let cancelled = controller.cancel("request-2", "operator").unwrap();
        assert_eq!(cancelled.status, RestoreStatus::Cancelled);
        assert!(controller.cancellation_token("request-2").is_none());

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn independent_store_handles_serialize_manifest_writes() {
        let dir = temp_dir("manifest-concurrency");
        let first = ManifestStore::new(&dir);
        let second = ManifestStore::new(&dir);
        let first_thread = std::thread::spawn(move || {
            first
                .begin(
                    "backup-1",
                    "db-1",
                    "ts",
                    "db-1.dump.zst",
                    "zstd",
                    false,
                    1,
                    "a",
                    1,
                )
                .unwrap();
        });
        let second_thread = std::thread::spawn(move || {
            second
                .begin(
                    "backup-2",
                    "db-2",
                    "ts",
                    "db-2.dump.zst",
                    "zstd",
                    false,
                    1,
                    "b",
                    1,
                )
                .unwrap();
        });
        first_thread.join().unwrap();
        second_thread.join().unwrap();
        assert_eq!(ManifestStore::new(&dir).list().unwrap().len(), 2);
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn stale_workspace_cleanup_only_removes_restore_directories() {
        let dir = temp_dir("cleanup");
        fs::create_dir_all(dir.join("restore-old")).unwrap();
        fs::create_dir_all(dir.join("chunks")).unwrap();
        fs::write(dir.join("restore-file"), b"keep").unwrap();

        assert_eq!(cleanup_stale_workspaces(&dir, false).unwrap(), 1);
        assert!(!dir.join("restore-old").exists());
        assert!(dir.join("chunks").exists());
        assert!(dir.join("restore-file").exists());
        assert_eq!(cleanup_stale_workspaces(&dir, true).unwrap(), 0);

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn path_escaping_manifest_is_not_restorable() {
        let manifest = BackupManifest {
            backup_id: "../outside".into(),
            database_name: "db".into(),
            timestamp: "ts".into(),
            filename: "db.dump.zst".into(),
            codec: "zstd".into(),
            encrypted: false,
            packaged_bytes: 1,
            sha256: "00".into(),
            part_count: 1,
            parts: vec![ManifestPart {
                index: 0,
                filename: "db.dump.zst".into(),
                bytes: 1,
                chat_id: "1".into(),
                message_id: 1,
                file_id: "file".into(),
            }],
            upload_complete: true,
        };
        assert!(!manifest.restorable());
    }
}
