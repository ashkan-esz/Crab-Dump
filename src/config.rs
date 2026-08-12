//! Environment-based and file-based configuration.
//!
//! Configuration loading follows this priority chain (later overrides earlier):
//!   1. Hardcoded defaults
//!   2. `config.toml` (if present)
//!   3. Environment variables (highest priority)
//!
//! Multi-database support is resolved in this order:
//!   1. TOML `[[databases]]` arrays from config.toml
//!   2. Indexed environment variables (`DATABASE_URL_0`, `DATABASE_URL_1`, …)
//!   3. Single legacy `DATABASE_URL` (backward-compatibility fallback)
//!
//! Shared settings (`TG_BOT_TOKEN`, `AGE_RECIPIENT`, …) apply uniformly across
//! all databases. Per-database settings (`PG_DUMP_EXTRA_ARGS_N`) override the
//! shared defaults for their respective server.

use std::env;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use serde::Deserialize;

use crate::cron::Cron;

// ===========================================================================
// Constants & defaults
// ===========================================================================

/// Minimum, default, and absolute maximum chunk size. Telegram cloud Bot API
/// caps `sendDocument` at 50 MiB; we reserve a 1 MiB safety margin.
pub const DEFAULT_CHUNK_SIZE_MB: u64 = 49;
const MAX_CHUNK_SIZE_MB: u64 = 49;
const MIN_CHUNK_SIZE_MB: u64 = 1;

/// Maximum number of configured databases. Prevents resource exhaustion
/// when operators accidentally configure hundreds of databases.
const DEFAULT_MAX_DATABASES: usize = 10;

/// How many database backups run at the same time by default.
///
/// Each in-flight pipeline holds a `pg_dump` process, a zstd compressor and a
/// full compressed dump in `work_dir`, so peak disk and CPU scale with this
/// number — not with the total database count.
pub const DEFAULT_MAX_PARALLEL_DATABASES: usize = 4;

/// Shortest accepted `BACKUP_INTERVAL`. A dump cycle costs minutes of CPU and
/// disk; anything under a minute would stack cycles on top of each other.
const MIN_BACKUP_INTERVAL_SECS: u64 = 60;

/// Default config file name searched in the current directory.
pub const DEFAULT_CONFIG_FILE: &str = "config.toml";

// ===========================================================================
// Backup schedule
// ===========================================================================

/// When to run backup cycles, parsed from `BACKUP_INTERVAL`.
///
/// Both forms are accepted in the same variable — see [`parse_schedule`].
#[derive(Debug, Clone)]
pub enum Schedule {
    /// Repeat every fixed duration, measured from the start of each cycle.
    /// `6h` means "six hours apart", drifting with however long a cycle takes
    /// to start.
    Every(Duration),
    /// Fire at wall-clock times matching a crontab expression, so `0 */4 * * *`
    /// lands on 00:00, 04:00, 08:00 … regardless of when the process started.
    ///
    /// Boxed to keep [`Schedule`] (and so `SharedConfig`) small — the duration
    /// variant is one `u64` pair next to `Cron`'s six masks.
    Cron(Box<Cron>),
}

// ===========================================================================
// Shared configuration (applies uniformly across all databases)
// ===========================================================================

/// Shared settings loaded once and inherited by every database backup.
#[derive(Debug, Clone)]
pub struct SharedConfig {
    /// Telegram bot token from @BotFather.
    pub tg_bot_token: String,
    /// Target chat for uploaded chunks (numeric ID or `@channelusername`).
    pub tg_chat_id: String,
    /// age X25519 recipient public key (`age1…`). `None` → compressed but
    /// NOT encrypted.
    pub age_recipient: Option<String>,
    /// Maximum chunk size in MiB (Telegram limit: 50 MiB).
    pub chunk_size_mb: u64,
    /// Directory for temporary chunk files; falls back to OS temp dir.
    pub work_dir: PathBuf,
    /// Port for the HTTP status dashboard.
    pub api_port: u16,
    /// Optional SOCKS5 proxy URL (`socks5://` or `socks5h://`).
    pub socks_proxy: Option<String>,
    /// How many database backups may run at the same time (≥ 1). Databases
    /// beyond this many wait for a free slot.
    pub max_parallel_databases: usize,
    /// Keep the temporary chunk files of a failed backup for debugging.
    /// Default `false`: chunks are removed as soon as they are uploaded, and a
    /// failure sweeps whatever is left behind.
    pub keep_failed_dumps: bool,
    /// When to repeat the whole backup cycle. `None` (the default) runs once
    /// and exits, which is what an external cron or systemd timer wants.
    /// `Some(_)` keeps the process alive and backs up on that schedule.
    pub backup_schedule: Option<Schedule>,
}

impl SharedConfig {
    /// Chunk size expressed in bytes.
    pub fn chunk_size_bytes(&self) -> u64 {
        self.chunk_size_mb * 1024 * 1024
    }
}

// ===========================================================================
// Per-database configuration
// ===========================================================================

/// Configuration for a single PostgreSQL database backup.
#[derive(Debug, Clone)]
pub struct DatabaseConfig {
    /// PostgreSQL connection string, e.g. `postgresql://user:pass@host:5432/db`.
    pub url: String,
    /// Human-readable name for logs/manifests. Auto-extracted from the URL
    /// path (last segment before `?`) when `None`.
    pub name: Option<String>,
    /// Extra arguments passed to `pg_dump` for this database only.
    /// Falls back to the shared `PG_DUMP_EXTRA_ARGS` when `None`.
    pub pg_dump_extra_args: Option<String>,
}

impl DatabaseConfig {
    /// Produce a display-friendly name for this database.
    ///
    /// Prefers an explicit `name` override. When omitted, extracts the
    /// database name from the URL by taking the last path segment before
    /// any query string (`?`). For example:
    ///
    /// - `postgresql://host:5432/my_app_db` → `"my_app_db"`
    /// - `postgresql://host:5432/analytics?sslmode=require` → `"analytics"`
    pub fn display_name(&self) -> String {
        match &self.name {
            Some(n) if !n.is_empty() => n.clone(),
            _ => {
                // Strip query string, then grab the last path segment.
                let path = self.url.split('?').next().unwrap_or(&self.url);
                path.rsplit('/').next().unwrap_or("unknown-db").to_string()
            }
        }
    }
}

// ===========================================================================
// Configuration entry-point
// ===========================================================================

/// Namespace for configuration resolution. Carries no state — see
/// [`SharedConfig`] and [`DatabaseConfig`] for the resolved values.
pub struct Config;

impl Config {
    /// Primary entry-point: resolve all database configurations.
    ///
    /// Tries three sources in descending priority:
    /// 1. TOML `[[databases]]` arrays from `config.toml`
    /// 2. Indexed environment variables (`DATABASE_URL_0`, `_1`, …)
    /// 3. Single legacy `DATABASE_URL` + `PG_DUMP_EXTRA_ARGS`
    ///
    /// Returns `(shared_settings, vec_of_per_database_configs)`.
    ///
    /// See ADR-0001 for the full design rationale.
    pub fn resolve_databases() -> Result<(SharedConfig, Vec<DatabaseConfig>)> {
        // ── Step 1: Parse config.toml once, merged with the environment ─────
        // Every step below reads from this single view, so a setting works
        // identically whether it came from the file or from an env var.
        let raw = merge_raw_with_env(load_config_raw(), get_env);
        let shared = build_shared_config(&raw)?;

        // Shared pg_dump args, from either source — the default each database
        // inherits unless it declares its own.
        let shared_extra = raw
            .pg_dump_extra_args
            .clone()
            .filter(|v| !v.trim().is_empty());

        // ── Step 2: Load TOML [[databases]] arrays ──────────────────────────
        let toml_dbs = load_toml_databases(&raw, shared_extra.as_deref())?;

        // ── Step 3: Scan indexed environment variables ──────────────────────
        let indexed_dbs = scan_indexed_databases(shared_extra.as_deref(), get_env);

        // ── Step 4: Pick source and merge ───────────────────────────────────
        let databases = pick_database_source(toml_dbs, indexed_dbs, &raw, shared_extra.as_deref())?;

        // ── Step 5: Enforce database count cap ──────────────────────────────
        let max_databases = env::var("CRAB_MAX_DATABASES")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(DEFAULT_MAX_DATABASES);

        if databases.len() > max_databases {
            bail!(
                "Too many databases: {} exceeds the maximum of {} \
                 (raise with `CRAB_MAX_DATABASES`)",
                databases.len(),
                max_databases,
            );
        }

        // ── Step 6: Resolve per-database extra args ─────────────────────────
        // Every database inherits `shared_extra` unless it set its own override.
        let final_dbs: Vec<DatabaseConfig> = databases
            .into_iter()
            .map(|db| {
                let effective_extra =
                    match (db.pg_dump_extra_args.as_deref(), shared_extra.as_deref()) {
                        (Some(ov), _) if !ov.trim().is_empty() => Some(ov.to_string()),
                        (_, Some(sh)) => Some(sh.to_string()),
                        _ => None,
                    };
                DatabaseConfig {
                    url: db.url,
                    name: db.name,
                    pg_dump_extra_args: effective_extra,
                }
            })
            .collect();

        // ── Step 7: Validate ────────────────────────────────────────────────
        validate_databases(&final_dbs)?;

        Ok((shared, final_dbs))
    }
}

/// Pick which of the three configuration sources wins, in descending
/// priority: TOML `[[databases]]`, indexed env vars, single `database_url`.
///
/// Split out of [`Config::resolve_databases`] so the priority order is
/// testable: `resolve_databases` itself reads the process-global environment
/// and cannot be exercised from parallel tests.
fn pick_database_source(
    toml_dbs: Vec<DatabaseConfig>,
    indexed_dbs: Vec<DatabaseConfig>,
    raw: &RawConfigFile,
    shared_extra: Option<&str>,
) -> Result<Vec<DatabaseConfig>> {
    match (toml_dbs.is_empty(), indexed_dbs.is_empty()) {
        // TOML wins — more expressive, supports extra args inline.
        (false, _) => Ok(toml_dbs),
        (_, false) => Ok(indexed_dbs),
        // Neither source → fall back to the single `database_url`, which
        // may come from config.toml or from `DATABASE_URL`.
        (true, true) => match single_db_fallback(raw, shared_extra) {
            Some(db) => Ok(vec![db]),
            None => bail!(
                "No databases configured. Set `DATABASE_URL` for a single database,\n\
                 use `DATABASE_URL_0` / `DATABASE_URL_1` for indexed config, or\n\
                 add `[[databases]]` sections to `{}`.",
                DEFAULT_CONFIG_FILE,
            ),
        },
    }
}

/// Build the single-database fallback from the merged config.
///
/// Reads `database_url` from the merged view, so `config.toml` and
/// `DATABASE_URL` work identically. Returns `None` when neither set it.
fn single_db_fallback(raw: &RawConfigFile, shared_extra: Option<&str>) -> Option<DatabaseConfig> {
    let url = raw
        .database_url
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty())?;
    Some(DatabaseConfig {
        url: url.to_string(),
        name: None,
        pg_dump_extra_args: shared_extra.map(str::to_string),
    })
}

/// Validate a fully-resolved database list: URLs, name charset, and uniqueness
/// of display names.
///
/// Display names must be unique: they key the chunk-file prefix and the
/// dashboard, so a collision means two dumps interleaving into the same
/// `.partNNNN` files.
fn validate_databases(dbs: &[DatabaseConfig]) -> Result<()> {
    let mut seen_names: std::collections::HashMap<String, usize> = Default::default();
    for (idx, db) in dbs.iter().enumerate() {
        validate_database_url(idx, &db.url)?;
        if let Some(ref nm) = db.name {
            if !nm
                .chars()
                .all(|c| c.is_alphanumeric() || c == '-' || c == '_')
            {
                bail!(
                    "Database {}: invalid name '{}' (only letters, digits, `-`, `_)`",
                    idx,
                    nm,
                );
            }
        }
        let display = db.display_name();
        if let Some(first) = seen_names.insert(display.clone(), idx) {
            bail!(
                "Databases {first} and {idx} both resolve to the display name \
                 '{display}'. Names key the chunk filenames and the dashboard, \
                 so they must be unique — set `DB_NAME_{idx}` (indexed env) or \
                 `name` (config.toml [[databases]]) to distinguish them.",
            );
        }
    }
    Ok(())
}

// ===========================================================================
// TOML file helpers
// ===========================================================================

/// Parsed representation of `config.toml`. Fields are optional because the
/// file is merged with environment variables (which fill gaps).
#[derive(Debug, Clone, Deserialize, Default)]
struct RawConfigFile {
    database_url: Option<String>,
    pg_dump_extra_args: Option<String>,
    tg_bot_token: Option<String>,
    tg_chat_id: Option<String>,
    age_recipient: Option<String>,
    chunk_size_mb: Option<u64>,
    work_dir: Option<String>,
    api_port: Option<u16>,
    socks_proxy: Option<String>,
    max_parallel_databases: Option<usize>,
    keep_failed_dumps: Option<bool>,
    /// Interval (`"6h"`, `"90m"`, `"3600"`) or crontab expression
    /// (`"0 */4 * * *"`), parsed by [`parse_schedule`]. Kept as a string so the
    /// file and the environment accept exactly the same spellings.
    backup_interval: Option<String>,
    // Populated only when [[databases]] exists in the TOML file.
    databases: Option<Vec<TomlDatabase>>,
}

/// A single `[[databases]]` entry from the TOML file.
#[derive(Debug, Clone, Deserialize)]
struct TomlDatabase {
    url: Option<String>,
    name: Option<String>,
    #[serde(rename = "pg_dump_extra_args")]
    pg_dump_extra_args: Option<String>,
}

/// Load the TOML config file from disk, returning the default on any error.
fn load_config_raw() -> RawConfigFile {
    let cwd = match env::current_dir() {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(error = %e, "cannot determine current directory, skipping config file");
            return RawConfigFile::default();
        }
    };
    let config_path = cwd.join(DEFAULT_CONFIG_FILE);

    let content = match std::fs::read_to_string(&config_path) {
        Ok(c) => c,
        Err(e) => {
            tracing::debug!(
                error = %e,
                config = %config_path.display(),
                "config file not found or unreadable",
            );
            return RawConfigFile::default();
        }
    };

    match toml::from_str(&content) {
        Ok(parsed) => parsed,
        Err(e) => {
            tracing::warn!(
                error = %e,
                config = %config_path.display(),
                "failed to parse TOML, ignoring file contents",
            );
            RawConfigFile::default()
        }
    }
}

/// Helper: read an environment variable, returning `None` for missing or blank.
fn get_env(k: &str) -> Option<String> {
    env::var(k).ok().filter(|v| !v.trim().is_empty())
}

/// Build shared (non-database-specific) configuration from an already-merged
/// raw config.
///
/// The caller merges file and environment values (defaults < file < env), so
/// this function only validates and applies fallbacks.
fn build_shared_config(raw: &RawConfigFile) -> Result<SharedConfig> {
    let tg_bot_token = raw
        .tg_bot_token
        .clone()
        .ok_or_else(|| anyhow!("TG_BOT_TOKEN is required (set in config.toml or environment)"))?;

    let tg_chat_id = raw
        .tg_chat_id
        .clone()
        .ok_or_else(|| anyhow!("TG_CHAT_ID is required (set in config.toml or environment)"))?;

    let age_recipient = match raw.age_recipient.clone() {
        Some(s) => {
            if !s.starts_with("age1") {
                // Truncate by characters, not bytes — a byte slice panics when
                // the cut lands inside a multi-byte character.
                bail!(
                    "AGE_RECIPIENT looks invalid: expected an X25519 recipient \
                     starting with `age1`, got `{}`",
                    s.chars().take(12).collect::<String>(),
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
                    "CHUNK_SIZE_MB must be between {} and {} (MiB), got {n}",
                    MIN_CHUNK_SIZE_MB,
                    MAX_CHUNK_SIZE_MB,
                );
            }
            n
        }
        None => DEFAULT_CHUNK_SIZE_MB,
    };

    let work_dir = match raw.work_dir.clone() {
        Some(p) => PathBuf::from(p),
        None => env::temp_dir(),
    };

    let socks_proxy = match raw.socks_proxy.clone() {
        Some(s) => {
            if !(s.starts_with("socks5://") || s.starts_with("socks5h://")) {
                bail!("SOCKS_PROXY must start with `socks5://` or `socks5h://`, got `{s}`");
            }
            Some(s)
        }
        None => None,
    };

    // 0 would mean "back up nothing"; reject it instead of silently hanging or
    // silently running everything at once.
    let max_parallel_databases = match raw.max_parallel_databases {
        Some(0) => bail!("MAX_PARALLEL_DATABASES must be at least 1"),
        Some(n) => n,
        None => DEFAULT_MAX_PARALLEL_DATABASES,
    };

    // Unset (or explicitly blank/`0`) keeps the historical one-shot behaviour,
    // so an existing cron/systemd deployment is unaffected by the upgrade.
    let backup_schedule = match raw.backup_interval.as_deref().map(str::trim) {
        None | Some("") | Some("0") => None,
        Some(s) => Some(parse_schedule(s)?),
    };

    Ok(SharedConfig {
        tg_bot_token,
        tg_chat_id,
        age_recipient,
        chunk_size_mb,
        work_dir,
        api_port: raw.api_port.unwrap_or(8080),
        socks_proxy,
        max_parallel_databases,
        keep_failed_dumps: raw.keep_failed_dumps.unwrap_or(false),
        backup_schedule,
    })
}

/// Parse a `BACKUP_INTERVAL` value into a [`Schedule`].
///
/// Whitespace decides the form: a crontab expression is five space-separated
/// fields, and every duration spelling is a single token. That means neither
/// form can be mistaken for the other, and an operator does not have to set a
/// second variable to say which one they meant.
fn parse_schedule(s: &str) -> Result<Schedule> {
    let s = s.trim();
    if s.split_whitespace().count() > 1 {
        return Ok(Schedule::Cron(Box::new(Cron::parse(s).with_context(
            || format!("BACKUP_INTERVAL `{s}` is not a valid cron expression"),
        )?)));
    }
    Ok(Schedule::Every(parse_duration(s)?))
}

/// Parse a backup interval: a bare number of seconds, or a number with a
/// `s`/`m`/`h`/`d` suffix (`"30m"`, `"6h"`, `"1d"`).
///
/// Rejects anything below [`MIN_BACKUP_INTERVAL_SECS`]: a cycle that starts
/// before the previous one finished would pile up `pg_dump` processes and fill
/// `work_dir`, and the scheduler deliberately does not run cycles in parallel.
fn parse_duration(s: &str) -> Result<Duration> {
    let s = s.trim();
    // Split the trailing unit off the digits; no suffix means seconds.
    let (digits, multiplier) = match s.chars().last() {
        Some('s') | Some('S') => (&s[..s.len() - 1], 1),
        Some('m') | Some('M') => (&s[..s.len() - 1], 60),
        Some('h') | Some('H') => (&s[..s.len() - 1], 3600),
        Some('d') | Some('D') => (&s[..s.len() - 1], 86400),
        _ => (s, 1),
    };

    let value: u64 = digits.trim().parse().map_err(|_| {
        anyhow!(
            "BACKUP_INTERVAL must be a number of seconds or a number with an \
             `s`/`m`/`h`/`d` suffix (e.g. `6h`, `90m`, `3600`), got `{s}`"
        )
    })?;

    let secs = value
        .checked_mul(multiplier)
        .ok_or_else(|| anyhow!("BACKUP_INTERVAL `{s}` overflows — use a smaller value"))?;

    if secs < MIN_BACKUP_INTERVAL_SECS {
        bail!(
            "BACKUP_INTERVAL must be at least {MIN_BACKUP_INTERVAL_SECS}s \
             (got `{s}` = {secs}s); a backup cycle takes far longer than that",
        );
    }

    Ok(Duration::from_secs(secs))
}

/// Merge a raw TOML-parsed config with environment variables.
///
/// Environment values shadow file values for every scalar field — the order
/// the module doc, the README and `config.toml.example` all promise. The TOML
/// `databases` array is never merged with env vars (it is inherently
/// file-bound).
fn merge_raw_with_env(raw: RawConfigFile, env: impl Fn(&str) -> Option<String>) -> RawConfigFile {
    RawConfigFile {
        database_url: env("DATABASE_URL").or(raw.database_url),
        pg_dump_extra_args: env("PG_DUMP_EXTRA_ARGS").or(raw.pg_dump_extra_args),
        tg_bot_token: env("TG_BOT_TOKEN").or(raw.tg_bot_token),
        tg_chat_id: env("TG_CHAT_ID").or(raw.tg_chat_id),
        age_recipient: env("AGE_RECIPIENT").or(raw.age_recipient),
        chunk_size_mb: env("CHUNK_SIZE_MB")
            .and_then(|v| v.parse().ok())
            .or(raw.chunk_size_mb),
        work_dir: env("WORK_DIR").or(raw.work_dir),
        api_port: env("API_PORT")
            .and_then(|v| v.parse().ok())
            .or(raw.api_port),
        socks_proxy: env("SOCKS_PROXY").or(raw.socks_proxy),
        max_parallel_databases: env("MAX_PARALLEL_DATABASES")
            .and_then(|v| v.parse().ok())
            .or(raw.max_parallel_databases),
        keep_failed_dumps: env("KEEP_FAILED_DUMPS")
            .map(|v| parse_bool(&v))
            .or(raw.keep_failed_dumps),
        backup_interval: env("BACKUP_INTERVAL").or(raw.backup_interval),
        databases: raw.databases, // TOML-only; never merged with env.
    }
}

/// Parse a boolean env var. Anything other than an explicit off value counts as
/// on, so `KEEP_FAILED_DUMPS=yes` does not silently mean "delete my evidence".
fn parse_bool(v: &str) -> bool {
    !matches!(
        v.trim().to_ascii_lowercase().as_str(),
        "" | "0" | "false" | "no" | "off"
    )
}

/// Extract database configurations from TOML `[[databases]]` entries.
///
/// `shared_extra` is the already-merged shared `pg_dump_extra_args` (file or
/// environment); an entry inherits it unless it sets its own.
/// Returns an empty vec when no `[[databases]]` section exists.
fn load_toml_databases(
    raw: &RawConfigFile,
    shared_extra: Option<&str>,
) -> Result<Vec<DatabaseConfig>> {
    let entries = raw.databases.clone().unwrap_or_default();
    if entries.is_empty() {
        return Ok(Vec::new());
    }

    let mut databases = Vec::with_capacity(entries.len());

    for (idx, entry) in entries.iter().enumerate() {
        let url = entry
            .url
            .as_ref()
            .ok_or_else(|| anyhow!("TOML database #{}: missing required `url` field", idx))?;

        // Blank counts as absent, so the entry still inherits `shared_extra`
        // rather than silently dumping with no extra args at all.
        let effective_extra = entry
            .pg_dump_extra_args
            .clone()
            .filter(|v| !v.trim().is_empty())
            .or_else(|| shared_extra.map(str::to_string));

        databases.push(DatabaseConfig {
            url: url.clone(),
            name: entry.name.clone(),
            pg_dump_extra_args: effective_extra,
        });
    }

    Ok(databases)
}

// ===========================================================================
// Indexed environment-variable loading
// ===========================================================================

/// Scan `DATABASE_URL_N` / `DB_NAME_N` / `PG_DUMP_EXTRA_ARGS_N` indices
/// starting from 0 and stopping at the first gap (unseen index).
///
/// This allows operators to declare databases purely through environment:
///
/// ```text
/// DATABASE_URL_0=postgresql://...
/// DATABASE_URL_1=postgresql://...
/// PG_DUMP_EXTRA_ARGS_0=--exclude-table=logs
/// ```
///
/// Index-specific `PG_DUMP_EXTRA_ARGS_N` shadows `shared_extra`, the merged
/// shared default supplied by the caller. `lookup` is the variable source —
/// [`get_env`] in production, a fixture map in tests, since the real
/// environment is process-global and unusable from parallel tests.
fn scan_indexed_databases(
    shared_extra: Option<&str>,
    lookup: impl Fn(&str) -> Option<String>,
) -> Vec<DatabaseConfig> {
    let mut databases = Vec::new();
    let mut i = 0usize;

    // Walk upward until we hit a gap — that determines the total count.
    loop {
        let url_key = format!("DATABASE_URL_{i}");
        match lookup(&url_key) {
            Some(url) => {
                let name = lookup(&format!("DB_NAME_{i}"));
                // Index-specific extra args override the shared default.
                let extra_args = lookup(&format!("PG_DUMP_EXTRA_ARGS_{i}"))
                    .or_else(|| shared_extra.map(str::to_string));

                databases.push(DatabaseConfig {
                    url,
                    name,
                    pg_dump_extra_args: extra_args,
                });
                i += 1;
            }
            None => break, // Gap found or missing — stop scanning.
        }
    }

    // A gap silently truncates the list, so `DATABASE_URL_0` + `DATABASE_URL_2`
    // would back up one database without a word. Peek a few indices past the
    // stop point and say so.
    for probe in i + 1..i + 5 {
        if lookup(&format!("DATABASE_URL_{probe}")).is_some() {
            tracing::warn!(
                gap = i,
                found = probe,
                "DATABASE_URL_{i} is unset, so the indexed scan stopped there — \
                 DATABASE_URL_{probe} and any later index are ignored. Renumber \
                 the indices to be contiguous from 0.",
            );
            break;
        }
    }

    databases
}

// ===========================================================================
// Validation helpers
// ===========================================================================

/// Perform basic sanity checks on a database URL before attempting to connect.
fn validate_database_url(index: usize, url: &str) -> Result<()> {
    // Scheme check.
    if !(url.starts_with("postgresql://") || url.starts_with("postgres://")) {
        bail!(
            "Database {}: URL must use `postgresql://` or `postgres://` scheme \
             (got `{}`)",
            index,
            url,
        );
    }

    // Must contain a path segment for the database name.
    if let Some(colon_slash) = url.find("://") {
        let rest = &url[colon_slash + 3..];
        if !rest.contains('/') {
            bail!(
                "Database {}: URL is missing a database name \
                 (expected `postgresql://host:port/dbname`)",
                index,
            );
        }
    }

    Ok(())
}

// ===========================================================================
// Utility: pg_dump binary availability
// ===========================================================================

/// Heuristic check that the `pg_dump` binary is available on PATH.
pub fn pg_dump_available() -> bool {
    std::process::Command::new("pg_dump")
        .arg("--version")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok()
}

// ===========================================================================
// Unit tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // -- Display name --

    #[test]
    fn display_name_prefers_explicit_name() {
        let db = DatabaseConfig {
            url: "postgresql://u:p@h:5432/old_name".into(),
            name: Some("override".into()),
            pg_dump_extra_args: None,
        };
        assert_eq!(db.display_name(), "override");
    }

    #[test]
    fn display_name_extracts_from_url() {
        let db = DatabaseConfig {
            url: "postgresql://u:p@h:5432/my_production_db".into(),
            name: None,
            pg_dump_extra_args: None,
        };
        assert_eq!(db.display_name(), "my_production_db");
    }

    #[test]
    fn display_name_strips_query_string() {
        let db = DatabaseConfig {
            url: "postgresql://h:5432/analytics?sslmode=require".into(),
            name: None,
            pg_dump_extra_args: None,
        };
        assert_eq!(db.display_name(), "analytics");
    }

    #[test]
    fn display_name_handles_trailing_slash() {
        let db = DatabaseConfig {
            url: "postgresql://h:5432/mydb/".into(),
            name: None,
            pg_dump_extra_args: None,
        };
        assert_eq!(db.display_name(), "");
        // Empty name from trailing slash is acceptable; users should set explicit name.
    }

    // -- URL validation --

    #[test]
    fn validate_url_accepts_postgresql_scheme() {
        assert!(validate_database_url(0, "postgresql://host:5432/db").is_ok());
    }

    #[test]
    fn validate_url_accepts_postgres_scheme() {
        assert!(validate_database_url(0, "postgres://host:5432/db").is_ok());
    }

    #[test]
    fn validate_url_rejects_mysql_scheme() {
        let err = validate_database_url(0, "mysql://host:3306/db").unwrap_err();
        assert!(err.to_string().contains("scheme"));
    }

    #[test]
    fn validate_url_rejects_no_database_name() {
        let err = validate_database_url(0, "postgresql://host:5432").unwrap_err();
        assert!(err.to_string().contains("missing"));
    }

    #[test]
    fn validate_url_rejects_empty_string() {
        let err = validate_database_url(0, "").unwrap_err();
        assert!(err.to_string().contains("scheme"));
    }

    // -- Shared config helpers --

    #[test]
    fn chunk_size_bytes_computes_correctly() {
        let cfg = SharedConfig {
            tg_bot_token: "t".into(),
            tg_chat_id: "c".into(),
            age_recipient: None,
            chunk_size_mb: 49,
            work_dir: std::env::temp_dir(),
            api_port: 8080,
            socks_proxy: None,
            max_parallel_databases: DEFAULT_MAX_PARALLEL_DATABASES,
            keep_failed_dumps: false,
            backup_schedule: None,
        };
        assert_eq!(cfg.chunk_size_bytes(), 49 * 1024 * 1024);
    }

    // -- Parallelism limit --

    fn shared_raw() -> RawConfigFile {
        RawConfigFile {
            tg_bot_token: Some("t".into()),
            tg_chat_id: Some("c".into()),
            ..Default::default()
        }
    }

    #[test]
    fn max_parallel_defaults_and_reads_config() {
        assert_eq!(
            build_shared_config(&shared_raw())
                .unwrap()
                .max_parallel_databases,
            DEFAULT_MAX_PARALLEL_DATABASES,
        );
        let raw = RawConfigFile {
            max_parallel_databases: Some(2),
            ..shared_raw()
        };
        assert_eq!(build_shared_config(&raw).unwrap().max_parallel_databases, 2);
    }

    /// Zero would stall the run instead of limiting it — reject at startup.
    #[test]
    fn max_parallel_zero_is_rejected() {
        let raw = RawConfigFile {
            max_parallel_databases: Some(0),
            ..shared_raw()
        };
        let err = build_shared_config(&raw).unwrap_err().to_string();
        assert!(err.contains("at least 1"), "unexpected error: {err}");
    }

    #[test]
    fn max_parallel_env_shadows_file() {
        let raw = RawConfigFile {
            max_parallel_databases: Some(2),
            ..Default::default()
        };
        let env = |k: &str| (k == "MAX_PARALLEL_DATABASES").then(|| "7".to_string());
        assert_eq!(merge_raw_with_env(raw, env).max_parallel_databases, Some(7));
    }

    // -- Keeping failed dumps --

    /// Deleting dumps is the default; keeping them is opt-in, and the opt-in
    /// must not read `KEEP_FAILED_DUMPS=0` as "keep".
    #[test]
    fn keep_failed_dumps_defaults_off_and_parses_env() {
        assert!(
            !build_shared_config(&shared_raw())
                .unwrap()
                .keep_failed_dumps
        );
        for on in ["1", "true", "TRUE", "yes", "on"] {
            assert!(parse_bool(on), "`{on}` must enable keeping");
        }
        for off in ["0", "false", "no", "off", " "] {
            assert!(!parse_bool(off), "`{off}` must not enable keeping");
        }
    }

    #[test]
    fn keep_failed_dumps_env_shadows_file() {
        let raw = RawConfigFile {
            keep_failed_dumps: Some(true),
            ..shared_raw()
        };
        let env = |k: &str| (k == "KEEP_FAILED_DUMPS").then(|| "0".to_string());
        let merged = merge_raw_with_env(raw, env);
        assert!(!build_shared_config(&merged).unwrap().keep_failed_dumps);
    }

    // -- Backup schedule --

    /// Read a schedule's duration, or fail the test if it is a cron expression.
    fn every(s: &Schedule) -> Duration {
        match s {
            Schedule::Every(d) => *d,
            Schedule::Cron(c) => panic!("expected an interval, got cron `{c}`"),
        }
    }

    /// Unset means one-shot (the historical behaviour every cron deployment
    /// relies on); the suffixes are the whole point of the string form.
    #[test]
    fn backup_interval_defaults_off_and_parses_units() {
        assert!(build_shared_config(&shared_raw())
            .unwrap()
            .backup_schedule
            .is_none());

        for (input, secs) in [
            ("3600", 3600),
            ("120s", 120),
            ("90m", 5400),
            ("6h", 21600),
            ("1d", 86400),
            (" 2H ", 7200),
        ] {
            assert_eq!(
                every(&parse_schedule(input).unwrap()),
                Duration::from_secs(secs),
                "`{input}` must parse as {secs}s",
            );
        }
    }

    /// An explicit `0` (or blank) is the documented way to turn the schedule
    /// back off without deleting the variable.
    #[test]
    fn backup_interval_zero_means_one_shot() {
        for off in ["0", "", "   "] {
            let raw = RawConfigFile {
                backup_interval: Some(off.into()),
                ..shared_raw()
            };
            assert!(
                build_shared_config(&raw).unwrap().backup_schedule.is_none(),
                "`{off}` must mean one-shot",
            );
        }
    }

    /// A sub-minute interval would start the next cycle before the previous one
    /// finished, so it is a startup error rather than a runtime pile-up.
    #[test]
    fn backup_interval_rejects_too_short_and_garbage() {
        for bad in ["30", "59s", "0m"] {
            let err = parse_duration(bad).unwrap_err().to_string();
            assert!(err.contains("at least"), "`{bad}`: unexpected error: {err}");
        }
        for bad in ["", "soon", "6hh", "-1h", "1.5h"] {
            let err = parse_duration(bad).unwrap_err().to_string();
            assert!(err.contains("must be a number"), "`{bad}`: {err}");
        }
    }

    /// Multi-token values are crontab expressions; single tokens are durations.
    /// The split has to be unambiguous, since one variable carries both.
    #[test]
    fn multi_field_values_parse_as_cron_expressions() {
        for expr in [
            "0 */4 * * *",
            "30 3 * * *",
            "0 9 * * mon-fri",
            "  * * * * *  ",
        ] {
            match parse_schedule(expr).unwrap() {
                Schedule::Cron(c) => assert_eq!(c.to_string(), expr.trim()),
                Schedule::Every(d) => panic!("`{expr}` must be cron, got {d:?}"),
            }
        }
    }

    /// A cron typo must name both the variable and the reason, since a
    /// scheduler that never fires looks identical to one that is idle.
    #[test]
    fn invalid_cron_expression_is_rejected_at_startup() {
        let raw = RawConfigFile {
            backup_interval: Some("0 99 * * *".into()),
            ..shared_raw()
        };
        let msg = format!("{:#}", build_shared_config(&raw).unwrap_err());
        assert!(msg.contains("BACKUP_INTERVAL"), "unexpected error: {msg}");
        assert!(msg.contains("hour"), "must name the bad field: {msg}");
    }

    #[test]
    fn backup_interval_env_shadows_file() {
        let raw = RawConfigFile {
            backup_interval: Some("6h".into()),
            ..shared_raw()
        };
        let env = |k: &str| (k == "BACKUP_INTERVAL").then(|| "30m".to_string());
        let merged = merge_raw_with_env(raw, env);
        assert_eq!(
            every(
                &build_shared_config(&merged)
                    .unwrap()
                    .backup_schedule
                    .unwrap()
            ),
            Duration::from_secs(1800),
        );
    }

    // -- Single-database fallback (D1, D4) --

    #[test]
    fn fallback_reads_database_url_from_merged_config() {
        // A config.toml-only deployment: `database_url` present, no env vars.
        let raw = RawConfigFile {
            database_url: Some("postgresql://host:5432/app".into()),
            ..Default::default()
        };
        let db = single_db_fallback(&raw, None).expect("file database_url must resolve");
        assert_eq!(db.url, "postgresql://host:5432/app");
    }

    #[test]
    fn fallback_inherits_shared_extra_args() {
        let raw = RawConfigFile {
            database_url: Some("postgresql://host:5432/app".into()),
            ..Default::default()
        };
        let db = single_db_fallback(&raw, Some("--exclude-table=logs")).unwrap();
        assert_eq!(
            db.pg_dump_extra_args.as_deref(),
            Some("--exclude-table=logs")
        );
    }

    #[test]
    fn fallback_ignores_blank_database_url() {
        let raw = RawConfigFile {
            database_url: Some("   ".into()),
            ..Default::default()
        };
        assert!(single_db_fallback(&raw, None).is_none());
    }

    /// The module doc, README and `config.toml.example` all promise the
    /// environment shadows `config.toml`. D1 routed `database_url` through
    /// this merge, so the fallback inherits whatever order it uses.
    #[test]
    fn env_shadows_file_for_every_scalar() {
        let raw = RawConfigFile {
            database_url: Some("postgresql://file:5432/app".into()),
            tg_chat_id: Some("file-chat".into()),
            chunk_size_mb: Some(10),
            ..Default::default()
        };
        let env = |k: &str| match k {
            "DATABASE_URL" => Some("postgresql://env:5432/app".to_string()),
            "CHUNK_SIZE_MB" => Some("20".to_string()),
            _ => None,
        };
        let merged = merge_raw_with_env(raw, env);

        assert_eq!(
            merged.database_url.as_deref(),
            Some("postgresql://env:5432/app")
        );
        assert_eq!(merged.chunk_size_mb, Some(20));
        // Unset in the environment — the file value survives.
        assert_eq!(merged.tg_chat_id.as_deref(), Some("file-chat"));
    }

    // -- Shared pg_dump_extra_args inheritance (D4) --

    /// `config.toml.example` promises a `[[databases]]` entry inherits the
    /// top-level `pg_dump_extra_args` when it omits its own. Blank counts as
    /// omitted, otherwise a stray `""` silently drops the operator's filters.
    #[test]
    fn toml_entries_inherit_shared_extra_args() {
        let raw = RawConfigFile {
            databases: Some(vec![
                TomlDatabase {
                    url: Some("postgresql://host:5432/app".into()),
                    name: None,
                    pg_dump_extra_args: None,
                },
                TomlDatabase {
                    url: Some("postgresql://host:5432/analytics".into()),
                    name: None,
                    pg_dump_extra_args: Some("   ".into()),
                },
                TomlDatabase {
                    url: Some("postgresql://host:5432/logs".into()),
                    name: None,
                    pg_dump_extra_args: Some("--schema-only".into()),
                },
            ]),
            ..Default::default()
        };
        let dbs = load_toml_databases(&raw, Some("--exclude-table=sessions")).unwrap();

        assert_eq!(
            dbs[0].pg_dump_extra_args.as_deref(),
            Some("--exclude-table=sessions")
        );
        assert_eq!(
            dbs[1].pg_dump_extra_args.as_deref(),
            Some("--exclude-table=sessions")
        );
        // An entry that declares its own args keeps them.
        assert_eq!(dbs[2].pg_dump_extra_args.as_deref(), Some("--schema-only"));
    }

    // -- Duplicate display names (D3) --
    fn db(url: &str, name: Option<&str>) -> DatabaseConfig {
        DatabaseConfig {
            url: url.into(),
            name: name.map(str::to_string),
            pg_dump_extra_args: None,
        }
    }

    #[test]
    fn validate_rejects_duplicate_display_names() {
        // Same database name on two different hosts — the collision that made
        // both dumps interleave into one set of .partNNNN files.
        let dbs = vec![
            db("postgresql://host-a:5432/alpha", None),
            db("postgresql://host-b:5432/alpha", None),
        ];
        let err = validate_databases(&dbs).unwrap_err().to_string();
        assert!(err.contains("display name"), "unexpected error: {err}");
        assert!(err.contains("DB_NAME_1"), "error must name the fix: {err}");
    }

    #[test]
    fn validate_accepts_duplicate_names_disambiguated_by_override() {
        let dbs = vec![
            db("postgresql://host-a:5432/alpha", Some("alpha-a")),
            db("postgresql://host-b:5432/alpha", Some("alpha-b")),
        ];
        assert!(validate_databases(&dbs).is_ok());
    }

    // -- Indexed scan (I3) --

    #[test]
    fn indexed_scan_stops_at_first_gap() {
        // 0 and 2 set, 1 missing: the scan takes only index 0.
        let env = |k: &str| match k {
            "DATABASE_URL_0" => Some("postgresql://host:5432/a".to_string()),
            "DATABASE_URL_2" => Some("postgresql://host:5432/c".to_string()),
            _ => None,
        };
        let dbs = scan_indexed_databases(None, env);
        assert_eq!(dbs.len(), 1);
        assert_eq!(dbs[0].url, "postgresql://host:5432/a");
    }

    #[test]
    fn indexed_scan_per_index_args_shadow_shared() {
        let env = |k: &str| match k {
            "DATABASE_URL_0" => Some("postgresql://host:5432/a".to_string()),
            "DATABASE_URL_1" => Some("postgresql://host:5432/b".to_string()),
            "PG_DUMP_EXTRA_ARGS_1" => Some("--schema-only".to_string()),
            _ => None,
        };
        let dbs = scan_indexed_databases(Some("--exclude-table=logs"), env);
        assert_eq!(dbs.len(), 2);
        assert_eq!(
            dbs[0].pg_dump_extra_args.as_deref(),
            Some("--exclude-table=logs")
        );
        assert_eq!(dbs[1].pg_dump_extra_args.as_deref(), Some("--schema-only"));
    }

    // -- Source priority (I10) --

    /// TOML `[[databases]]` outranks the indexed env scan, which outranks the
    /// single `database_url`. Priority is documented in three places and
    /// enforced in exactly one — this pins that one.
    #[test]
    fn toml_databases_outrank_indexed_env() {
        let raw = RawConfigFile {
            database_url: Some("postgresql://host:5432/single".into()),
            ..Default::default()
        };
        let dbs = pick_database_source(
            vec![db("postgresql://host:5432/from-toml", None)],
            vec![db("postgresql://host:5432/from-env", None)],
            &raw,
            None,
        )
        .unwrap();
        assert_eq!(dbs.len(), 1);
        assert_eq!(dbs[0].url, "postgresql://host:5432/from-toml");
    }

    #[test]
    fn indexed_env_outranks_single_database_url() {
        let raw = RawConfigFile {
            database_url: Some("postgresql://host:5432/single".into()),
            ..Default::default()
        };
        let dbs = pick_database_source(
            vec![],
            vec![db("postgresql://host:5432/from-env", None)],
            &raw,
            None,
        )
        .unwrap();
        assert_eq!(dbs.len(), 1);
        assert_eq!(dbs[0].url, "postgresql://host:5432/from-env");
    }

    #[test]
    fn single_database_url_used_when_no_other_source() {
        let raw = RawConfigFile {
            database_url: Some("postgresql://host:5432/single".into()),
            ..Default::default()
        };
        let dbs = pick_database_source(vec![], vec![], &raw, None).unwrap();
        assert_eq!(dbs.len(), 1);
        assert_eq!(dbs[0].url, "postgresql://host:5432/single");
    }

    /// No source at all is a startup error naming all three ways to fix it.
    #[test]
    fn no_source_at_all_errors_with_all_three_remedies() {
        let err = pick_database_source(vec![], vec![], &RawConfigFile::default(), None)
            .unwrap_err()
            .to_string();
        assert!(err.contains("DATABASE_URL"), "must name the env var: {err}");
        assert!(err.contains("DATABASE_URL_0"), "must name indexed: {err}");
        assert!(err.contains("[[databases]]"), "must name TOML: {err}");
    }

    // -- Non-ASCII recipient (D2) --

    #[test]
    fn non_ascii_age_recipient_errors_without_panic() {
        let raw = RawConfigFile {
            tg_bot_token: Some("t".into()),
            tg_chat_id: Some("c".into()),
            // Byte index 12 lands inside a multi-byte char — the old byte-slice
            // excerpt panicked here instead of reporting the bad value.
            age_recipient: Some("a密码密码密码".into()),
            ..Default::default()
        };
        let err = build_shared_config(&raw).unwrap_err().to_string();
        assert!(err.contains("age1"), "unexpected error: {err}");
    }
}
