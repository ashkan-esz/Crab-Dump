//! Environment-based and file-based configuration.
//!
//! Config is loaded in this priority order (later overrides earlier):
//!   1. Hardcoded defaults
//!   2. `config.toml` (if present)
//!   3. Environment variables

use std::env;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail, Result};
use serde::Deserialize;

/// Minimum, default, and absolute maximum chunk size (Telegram cloud Bot API
/// caps `sendDocument` at 50 MiB; we leave a safety margin).
pub const DEFAULT_CHUNK_SIZE_MB: u64 = 49;
const MAX_CHUNK_SIZE_MB: u64 = 49;
const MIN_CHUNK_SIZE_MB: u64 = 1;

/// Default config file name searched in the current directory.
pub const DEFAULT_CONFIG_FILE: &str = "config.toml";

#[derive(Debug, Clone, Deserialize, Default)]
struct RawConfig {
    database_url: Option<String>,
    pg_dump_extra_args: Option<String>,
    tg_bot_token: Option<String>,
    tg_chat_id: Option<String>,
    age_recipient: Option<String>,
    chunk_size_mb: Option<u64>,
    work_dir: Option<String>,
    socks_proxy: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Config {
    /// Postgres connection string, e.g. `postgresql://user:pass@host:5432/db`.
    pub database_url: String,
    /// Optional extra args passed verbatim to `pg_dump`
    /// (e.g. `--format=custom --no-owner`).
    pub pg_dump_extra_args: Option<String>,
    /// Telegram bot token from @BotFather.
    pub tg_bot_token: String,
    /// Target chat: numeric id (e.g. `123456789`) or `@channelusername`.
    pub tg_chat_id: String,
    /// age X25519 recipient public key (`age1...`). Optional — if `None`,
    /// the dump is uploaded compressed but NOT encrypted.
    pub age_recipient: Option<String>,
    /// Max size of each uploaded part in MiB.
    pub chunk_size_mb: u64,
    /// Directory used for temp chunks; defaults to the OS temp dir.
    pub work_dir: PathBuf,
    /// Optional SOCKS5 proxy URL, e.g. `socks5://127.0.0.1:1080` or
    /// `socks5h://host:port` (the `h` variant resolves DNS through the proxy).
    pub socks_proxy: Option<String>,
}

impl Config {
    /// Chunk size in bytes.
    pub fn chunk_size_bytes(&self) -> u64 {
        self.chunk_size_mb * 1024 * 1024
    }

    /// Load configuration from `config.toml` and environment variables.
    ///
    /// Env vars take precedence over file values. Use `config_path` to specify
    /// a non-default config file, or `None` to search for `config.toml` in the
    /// current directory.
    pub fn from_env() -> Result<Self> {
        Self::with_config_path(None)
    }

    /// Like `from_env` but accepts an explicit config file path.
    pub fn with_config_path(config_path: Option<&Path>) -> Result<Self> {
        let raw = load_config_path(config_path);
        raw.try_into()
    }
}

impl TryFrom<RawConfig> for Config {
    type Error = anyhow::Error;

    fn try_from(raw: RawConfig) -> Result<Self> {
        let database_url = raw.database_url.ok_or_else(|| {
            anyhow!("DATABASE_URL is required (set in config.toml or environment)")
        })?;
        let tg_bot_token = raw.tg_bot_token.ok_or_else(|| {
            anyhow!("TG_BOT_TOKEN is required (set in config.toml or environment)")
        })?;
        let tg_chat_id = raw.tg_chat_id.ok_or_else(|| {
            anyhow!("TG_CHAT_ID is required (set in config.toml or environment)")
        })?;

        let age_recipient = match raw.age_recipient {
            Some(s) => {
                if !s.starts_with("age1") {
                    bail!(
                        "AGE_RECIPIENT looks invalid: expected an X25519 recipient \
                         starting with `age1`, got `{}`",
                        &s[..s.len().min(12)]
                    );
                }
                Some(s)
            }
            None => None,
        };

        let chunk_size_mb = match raw.chunk_size_mb {
            Some(n) => {
                if !(MIN_CHUNK_SIZE_MB..=MAX_CHUNK_SIZE_MB).contains(&n) {
                    bail!(
                        "CHUNK_SIZE_MB must be between {MIN_CHUNK_SIZE_MB} and \
                         {MAX_CHUNK_SIZE_MB} (MiB), got {n}"
                    );
                }
                n
            }
            None => DEFAULT_CHUNK_SIZE_MB,
        };

        let work_dir = match raw.work_dir {
            Some(p) => PathBuf::from(p),
            None => env::temp_dir(),
        };

        let socks_proxy = match raw.socks_proxy {
            Some(s) => {
                if !(s.starts_with("socks5://") || s.starts_with("socks5h://")) {
                    bail!(
                        "SOCKS_PROXY must start with `socks5://` or `socks5h://`, got `{s}`"
                    );
                }
                Some(s)
            }
            None => None,
        };

        Ok(Self {
            database_url,
            pg_dump_extra_args: raw.pg_dump_extra_args,
            tg_bot_token,
            tg_chat_id,
            age_recipient,
            chunk_size_mb,
            work_dir,
            socks_proxy,
        })
    }
}

/// Load and merge config: defaults < file < env.
fn load_config_path(path: Option<&Path>) -> RawConfig {
    let get_env = |k: &str| -> Option<String> {
        env::var(k).ok().filter(|v| !v.trim().is_empty())
    };

    let default_cfg = RawConfig {
        database_url: get_env("DATABASE_URL"),
        pg_dump_extra_args: get_env("PG_DUMP_EXTRA_ARGS"),
        tg_bot_token: get_env("TG_BOT_TOKEN"),
        tg_chat_id: get_env("TG_CHAT_ID"),
        age_recipient: get_env("AGE_RECIPIENT"),
        chunk_size_mb: get_env("CHUNK_SIZE_MB").and_then(|v| v.parse().ok()),
        work_dir: get_env("WORK_DIR"),
        socks_proxy: get_env("SOCKS_PROXY"),
    };

    let config_path = match path {
        Some(p) => p.to_path_buf(),
        None => {
            let cwd = match env::current_dir() {
                Ok(c) => c,
                Err(_) => return default_cfg,
            };
            cwd.join(DEFAULT_CONFIG_FILE)
        }
    };

    if !config_path.exists() {
        tracing::debug!(config = %config_path.display(), "config file not found, using defaults");
        return default_cfg;
    }

    let content = match std::fs::read_to_string(&config_path) {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(error = %e, config = %config_path.display(), "failed to read config file");
            return default_cfg;
        }
    };

    let file_cfg: RawConfig = match toml::from_str(&content) {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(error = %e, config = %config_path.display(), "failed to parse config file, skipping");
            return default_cfg;
        }
    };

    // Merge: file overrides default, env overrides both.
    RawConfig {
        database_url: file_cfg.database_url.or(default_cfg.database_url),
        pg_dump_extra_args: file_cfg.pg_dump_extra_args.or(default_cfg.pg_dump_extra_args),
        tg_bot_token: file_cfg.tg_bot_token.or(default_cfg.tg_bot_token),
        tg_chat_id: file_cfg.tg_chat_id.or(default_cfg.tg_chat_id),
        age_recipient: file_cfg.age_recipient.or(default_cfg.age_recipient),
        chunk_size_mb: file_cfg.chunk_size_mb.or(default_cfg.chunk_size_mb),
        work_dir: file_cfg.work_dir.or(default_cfg.work_dir),
        socks_proxy: file_cfg.socks_proxy.or(default_cfg.socks_proxy),
    }
}

/// Heuristic check that the `pg_dump` binary is on PATH.
pub fn pg_dump_available() -> bool {
    std::process::Command::new("pg_dump")
        .arg("--version")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok()
}
