//! Environment-based configuration.

use std::env;
use std::path::PathBuf;

use anyhow::{anyhow, bail, Context, Result};

/// Minimum, default, and absolute maximum chunk size (Telegram cloud Bot API
/// caps `sendDocument` at 50 MiB; we leave a safety margin).
pub const DEFAULT_CHUNK_SIZE_MB: u64 = 49;
const MAX_CHUNK_SIZE_MB: u64 = 49;
const MIN_CHUNK_SIZE_MB: u64 = 1;

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

    pub fn from_env() -> Result<Self> {
        let get = |k: &str| -> Option<String> {
            env::var(k).ok().filter(|v| !v.trim().is_empty())
        };

        let database_url = get("DATABASE_URL")
            .ok_or_else(|| anyhow!("DATABASE_URL is required"))?;
        let tg_bot_token = get("TG_BOT_TOKEN")
            .ok_or_else(|| anyhow!("TG_BOT_TOKEN is required"))?;
        let tg_chat_id = get("TG_CHAT_ID")
            .ok_or_else(|| anyhow!("TG_CHAT_ID is required"))?;
        let age_recipient = match get("AGE_RECIPIENT") {
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

        let chunk_size_mb = match get("CHUNK_SIZE_MB") {
            Some(v) => {
                let n: u64 = v.parse().with_context(|| {
                    format!("CHUNK_SIZE_MB must be a number, got `{v}`")
                })?;
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

        let work_dir = match get("WORK_DIR") {
            Some(p) => PathBuf::from(p),
            None => env::temp_dir(),
        };

        let socks_proxy = match get("SOCKS_PROXY") {
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
            pg_dump_extra_args: get("PG_DUMP_EXTRA_ARGS"),
            tg_bot_token,
            tg_chat_id,
            age_recipient,
            chunk_size_mb,
            work_dir,
            socks_proxy,
        })
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
