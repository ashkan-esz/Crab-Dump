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

use anyhow::{anyhow, bail, Result};
use serde::Deserialize;

// ===========================================================================
// Constants & defaults
// ===========================================================================

/// Minimum, default, and absolute maximum chunk size. Telegram cloud Bot API
/// caps `sendDocument` at 50 MiB; we reserve a 1 MiB safety margin.
pub const DEFAULT_CHUNK_SIZE_MB: u64 = 49;
const MAX_CHUNK_SIZE_MB: u64 = 49;
const MIN_CHUNK_SIZE_MB: u64 = 1;

/// Maximum number of concurrent database backups. Prevents resource exhaustion
/// when operators accidentally configure hundreds of databases.
const DEFAULT_MAX_DATABASES: usize = 10;

/// Default config file name searched in the current directory.
pub const DEFAULT_CONFIG_FILE: &str = "config.toml";

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
        let raw = merge_raw_with_env(load_config_raw());
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
        let databases: Vec<DatabaseConfig> = match (toml_dbs.is_empty(), indexed_dbs.is_empty()) {
            // TOML wins — more expressive, supports extra args inline.
            (false, _) => toml_dbs,
            (_, false) => indexed_dbs,
            // Neither source → fall back to the single `database_url`, which
            // may come from config.toml or from `DATABASE_URL`.
            (true, true) => match single_db_fallback(&raw, shared_extra.as_deref()) {
                Some(db) => vec![db],
                None => bail!(
                    "No databases configured. Set `DATABASE_URL` for a single database,\n\
                     use `DATABASE_URL_0` / `DATABASE_URL_1` for indexed config, or\n\
                     add `[[databases]]` sections to `{}`.",
                    DEFAULT_CONFIG_FILE,
                ),
            },
        };

        if databases.is_empty() {
            bail!("No databases configured after resolution.");
        }

        // ── Step 5: Enforce concurrency cap ─────────────────────────────────
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
                        (Some(ov), _) if !ov.is_empty() => Some(ov.to_string()),
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

/// Build the single-database fallback from the merged config.
///
/// Reads `database_url` from the merged view, so `config.toml` and
/// `DATABASE_URL` work identically. Returns `None` when neither set it.
fn single_db_fallback(raw: &RawConfigFile, shared_extra: Option<&str>) -> Option<DatabaseConfig> {
    let url = raw.database_url.as_deref().map(str::trim).filter(|v| !v.is_empty())?;
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

    Ok(SharedConfig {
        tg_bot_token,
        tg_chat_id,
        age_recipient,
        chunk_size_mb,
        work_dir,
        api_port: raw.api_port.unwrap_or(8080),
        socks_proxy,
    })
}

/// Merge a raw TOML-parsed config with environment variables.
///
/// Environment values shadow file values for every scalar field. The TOML
/// `databases` array is never merged with env vars (it is inherently file-bound).
fn merge_raw_with_env(raw: RawConfigFile) -> RawConfigFile {
    RawConfigFile {
        database_url: raw.database_url.or(get_env("DATABASE_URL")),
        pg_dump_extra_args: raw
            .pg_dump_extra_args
            .or_else(|| get_env("PG_DUMP_EXTRA_ARGS")),
        tg_bot_token: raw.tg_bot_token.or_else(|| get_env("TG_BOT_TOKEN")),
        tg_chat_id: raw.tg_chat_id.or_else(|| get_env("TG_CHAT_ID")),
        age_recipient: raw.age_recipient.or_else(|| get_env("AGE_RECIPIENT")),
        chunk_size_mb: raw
            .chunk_size_mb
            .or_else(|| get_env("CHUNK_SIZE_MB").and_then(|v| v.parse().ok())),
        work_dir: raw.work_dir.or_else(|| get_env("WORK_DIR")),
        api_port: raw
            .api_port
            .or_else(|| get_env("API_PORT").and_then(|v| v.parse().ok())),
        socks_proxy: raw.socks_proxy.or_else(|| get_env("SOCKS_PROXY")),
        databases: raw.databases, // TOML-only; never merged with env.
    }
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

        let effective_extra = entry
            .pg_dump_extra_args
            .clone()
            .filter(|v| !v.is_empty())
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
        };
        assert_eq!(cfg.chunk_size_bytes(), 49 * 1024 * 1024);
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
        assert_eq!(db.pg_dump_extra_args.as_deref(), Some("--exclude-table=logs"));
    }

    #[test]
    fn fallback_ignores_blank_database_url() {
        let raw = RawConfigFile {
            database_url: Some("   ".into()),
            ..Default::default()
        };
        assert!(single_db_fallback(&raw, None).is_none());
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
        assert_eq!(dbs[0].pg_dump_extra_args.as_deref(), Some("--exclude-table=logs"));
        assert_eq!(dbs[1].pg_dump_extra_args.as_deref(), Some("--schema-only"));
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
