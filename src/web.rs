//! HTTP status dashboard server.
//!
//! Serves `index.html` as a static page and provides API endpoints:
//! - `GET /api/config` — returns server metadata plus independent backup and
//!   history-upload schedule state
//! - `GET /api/status/service` — Telegram API connection status
//! - `GET /api/status/process` — aggregated PostgreSQL dump status (max across DBs)
//! - `GET /api/status/database/{name}` — per-database dump status
//! - `GET /api/status/databases` — all database statuses as a JSON array
//!
//! The dashboard polls these endpoints every 4 seconds and updates the UI.

use actix_web::{dev::ServiceRequest, web, App, Error, HttpResponse, HttpServer, Responder};
use actix_web_httpauth::{
    extractors::basic::{BasicAuth, Config as BasicConfig},
    middleware::HttpAuthentication,
};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Condvar, LazyLock, Mutex, RwLock};
use std::time::SystemTime;

use crate::database_state::DatabaseStateStore;
use crate::history::HistoryStore;
use crate::telegram_users::{TelegramUser, TelegramUserStore};

// ===========================================================================
// Global status atoms (Telegram service — single value)
// ===========================================================================

/// Atomic status values (0=UP, 1=DEGRADED, 2=DOWN).
static TELEGRAM_STATUS: AtomicU8 = AtomicU8::new(0);

/// Seconds since UNIX epoch when the server first received a request.
/// Set lazily on the first `api_config` call to avoid static initialization
/// with non-const expressions (which Rust forbids).
static START_EPOCH_SECS: AtomicU64 = AtomicU64::new(0);

/// Configured cap on concurrent database backups, published to the dashboard.
/// Set once at startup by [`set_max_parallel_databases`]; 0 means "not yet
/// reported", which the dashboard renders as unknown.
static MAX_PARALLEL_DATABASES: AtomicU64 = AtomicU64::new(0);

/// Number of configured Telegram destinations, published to the dashboard.
static TELEGRAM_CHAT_COUNT: AtomicU64 = AtomicU64::new(0);

/// Whether a backup cycle is executing right now. Distinguishes "working" from
/// "sleeping until the next slot", which the per-database cards alone cannot
/// say — every card reads "queued" both before the first cycle and between two.
static CYCLE_RUNNING: AtomicBool = AtomicBool::new(false);

/// Seconds since UNIX epoch of the next scheduled backup cycle, or 0 when
/// there is none (one-shot mode, or a cycle already in progress).
static BACKUP_NEXT_RUN_EPOCH_SECS: AtomicU64 = AtomicU64::new(0);

/// Seconds since UNIX epoch of the next scheduled history upload, or 0 when
/// history uploads are disabled.
static HISTORY_NEXT_RUN_EPOCH_SECS: AtomicU64 = AtomicU64::new(0);

/// Human-readable schedule, e.g. `every 6h` or `cron 0 */4 * * *`. Empty means
/// one-shot: run once and exit.
static SCHEDULE_LABEL: LazyLock<RwLock<String>> = LazyLock::new(|| RwLock::new(String::new()));

/// Human-readable history-upload schedule, or empty when disabled.
static HISTORY_SCHEDULE_LABEL: LazyLock<RwLock<String>> =
    LazyLock::new(|| RwLock::new(String::new()));

// ===========================================================================
// Per-database dump statuses (HashMap keyed by display name)
// ===========================================================================

/// Snapshot of one database's backup pipeline, as shown on the dashboard.
#[derive(Clone)]
struct DbStatus {
    /// Severity code: 0=UP, 1=DEGRADED, 2=DOWN.
    code: u8,
    /// Pipeline stage: "queued", "dump", "package", "upload" or "done".
    /// The three middle values are the timeline nodes drawn by the dashboard.
    stage: &'static str,
    /// Short description of what this stage is currently doing.
    detail: String,
    /// Bytes uploaded to Telegram so far in this run.
    bytes_done: u64,
    /// Total bytes to upload — 0 until the packaging stage finishes and the
    /// final (compressed, optionally encrypted) size is known.
    bytes_total: u64,
    /// Upload throughput in bytes/second; 0 when nothing is in flight.
    speed_bps: f64,
    /// Uncompressed bytes read from `pg_dump` stdout this run — the logical
    /// size of the database as dumped. Grows live during the dump stage and
    /// is kept for the rest of the run.
    dump_bytes: u64,
    /// RFC 3339 timestamp of the last update to this entry.
    updated: String,
}

/// Per-database dump statuses keyed by display name.
///
/// Global aggregation (`get_aggregated_dump_status`) is the **maximum**
/// severity value across all tracked databases. A single DOWN makes
/// the aggregate DOWN. Empty set → UP.
static DUMP_STATUSES: LazyLock<RwLock<HashMap<String, DbStatus>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));
static DATABASE_STATES: LazyLock<RwLock<Option<std::sync::Arc<DatabaseStateStore>>>> =
    LazyLock::new(|| RwLock::new(None));

/// Process-wide handoff between dashboard requests and the blocking scheduler.
///
/// Pending and active names are kept together so a manual request cannot be
/// accepted twice, and scheduled execution can use the same active-run guard.
#[derive(Debug, Default)]
struct ManualBackupState {
    pending: VecDeque<String>,
    pending_set: HashSet<String>,
    active: HashSet<String>,
}

#[derive(Debug, Default)]
pub struct ManualBackupController {
    state: Mutex<ManualBackupState>,
    wake: Condvar,
}

impl ManualBackupController {
    pub fn request(&self, name: &str) -> bool {
        let mut state = self.state.lock().expect("manual backup lock poisoned");
        if state.active.contains(name) || !state.pending_set.insert(name.to_string()) {
            return false;
        }
        state.pending.push_back(name.to_string());
        self.wake.notify_one();
        true
    }

    pub fn take_pending(&self) -> Vec<String> {
        let mut state = self.state.lock().expect("manual backup lock poisoned");
        let mut names = Vec::with_capacity(state.pending.len());
        while let Some(name) = state.pending.pop_front() {
            state.pending_set.remove(&name);
            state.active.insert(name.clone());
            names.push(name);
        }
        names
    }

    pub fn claim_scheduled(&self, name: &str) -> bool {
        let mut state = self.state.lock().expect("manual backup lock poisoned");
        if state.active.contains(name) || state.pending_set.contains(name) {
            return false;
        }
        state.active.insert(name.to_string());
        true
    }

    pub fn finish(&self, name: &str) {
        self.state
            .lock()
            .expect("manual backup lock poisoned")
            .active
            .remove(name);
    }

    pub fn wait_for_wake(&self, timeout: std::time::Duration) -> bool {
        let state = self.state.lock().expect("manual backup lock poisoned");
        if !state.pending.is_empty() {
            return true;
        }
        let (state, _) = self
            .wake
            .wait_timeout(state, timeout)
            .expect("manual backup lock poisoned");
        !state.pending.is_empty()
    }

    #[cfg(test)]
    fn is_active(&self, name: &str) -> bool {
        self.state
            .lock()
            .expect("manual backup lock poisoned")
            .active
            .contains(name)
    }
}

static MANUAL_BACKUPS: LazyLock<Arc<ManualBackupController>> =
    LazyLock::new(|| Arc::new(ManualBackupController::default()));

pub fn manual_backup_controller() -> Arc<ManualBackupController> {
    Arc::clone(&MANUAL_BACKUPS)
}

// ===========================================================================
// Public API — Telegram service status
// ===========================================================================

/// Set the Telegram service status. 0=UP, 1=DEGRADED, 2=DOWN.
pub fn set_telegram_status(state: u8) {
    TELEGRAM_STATUS.store(state, Ordering::SeqCst);
}

/// Read the current Telegram service status.
pub fn get_telegram_status() -> u8 {
    TELEGRAM_STATUS.load(Ordering::SeqCst)
}

// ===========================================================================
// Public API — Dump process status (per-database + aggregated)
// ===========================================================================

/// Record a database as configured but not yet started.
///
/// Called once per database at startup so the dashboard lists every
/// configured database, not just the ones that already began running.
pub fn set_database_state_store(store: std::sync::Arc<DatabaseStateStore>) {
    *DATABASE_STATES
        .write()
        .expect("database state lock poisoned") = Some(store);
}

pub fn database_state_store() -> std::sync::Arc<DatabaseStateStore> {
    DATABASE_STATES
        .read()
        .expect("database state lock poisoned")
        .as_ref()
        .expect("database state store not initialized")
        .clone()
}

pub fn register_database(db_name: &str, enabled: bool) {
    if enabled {
        set_db_status(db_name, 0, "queued", "Queued — waiting to start");
    } else {
        set_db_status(db_name, 0, "disabled", "DISABLED — backup skipped");
    }
}

/// Set the pipeline stage and status for a specific database.
///
/// States:
/// - `0` = UP (queued or completed)
/// - `1` = DEGRADED (running)
/// - `2` = DOWN (error / failure)
///
/// `stage` is one of [`STAGES`], `"queued"` or `"done"`; `detail` is the
/// human-readable line the dashboard shows as the current/next step.
///
/// Upload counters are reset — call [`set_db_transfer`] afterwards to
/// publish upload size and speed for the new stage. The dump size is kept
/// across stages so the card can keep showing it while packaging and
/// uploading; entering `"queued"` or `"dump"` marks a new run and clears it.
///
/// The global dump status (used by `/api/status/process`) is computed as
/// the maximum severity across all tracked databases.
pub fn set_db_status(db_name: &str, code: u8, stage: &'static str, detail: impl Into<String>) {
    let mut statuses = DUMP_STATUSES.write().expect("dump status lock poisoned");
    let dump_bytes = match statuses.get(db_name) {
        Some(prev) if stage != "queued" && stage != "dump" => prev.dump_bytes,
        _ => 0,
    };
    statuses.insert(
        db_name.to_string(),
        DbStatus {
            code,
            stage,
            detail: detail.into(),
            bytes_done: 0,
            bytes_total: 0,
            speed_bps: 0.0,
            dump_bytes,
            updated: chrono::Utc::now().to_rfc3339(),
        },
    );
}

/// Publish the uncompressed size dumped so far for a database.
///
/// Called periodically while draining `pg_dump` stdout, then once more with
/// the exact total. Does nothing for an unknown database.
pub fn set_db_dump_bytes(db_name: &str, dump_bytes: u64) {
    let mut statuses = DUMP_STATUSES.write().expect("dump status lock poisoned");
    if let Some(entry) = statuses.get_mut(db_name) {
        entry.dump_bytes = dump_bytes;
        entry.updated = chrono::Utc::now().to_rfc3339();
    }
}

/// Publish upload progress for a database: bytes sent so far, the total to
/// send, and current throughput in bytes/second.
///
/// Only updates the transfer counters — stage, code and detail are left as
/// the last [`set_db_status`] call set them. Does nothing for an unknown
/// database.
pub fn set_db_transfer(db_name: &str, bytes_done: u64, bytes_total: u64, speed_bps: f64) {
    let mut statuses = DUMP_STATUSES.write().expect("dump status lock poisoned");
    if let Some(entry) = statuses.get_mut(db_name) {
        entry.bytes_done = bytes_done;
        entry.bytes_total = bytes_total;
        entry.speed_bps = speed_bps;
        entry.updated = chrono::Utc::now().to_rfc3339();
    }
}

/// Mark a database as failed, preserving the stage it failed on so the
/// dashboard timeline can point at the step that actually broke.
///
/// Byte counters are kept (they show how far the upload got); throughput is
/// zeroed because nothing is in flight any more.
pub fn fail_db(db_name: &str, detail: impl Into<String>) {
    let mut statuses = DUMP_STATUSES.write().expect("dump status lock poisoned");
    let entry = statuses.entry(db_name.to_string()).or_insert(DbStatus {
        code: 2,
        stage: "dump",
        detail: String::new(),
        bytes_done: 0,
        bytes_total: 0,
        speed_bps: 0.0,
        dump_bytes: 0,
        updated: String::new(),
    });
    entry.code = 2;
    entry.detail = detail.into();
    entry.speed_bps = 0.0;
    entry.updated = chrono::Utc::now().to_rfc3339();
}

/// Get the dump status code for a specific database.
///
/// Returns `None` if no backup has been recorded for this database yet.
fn get_dump_status(db_name: &str) -> Option<u8> {
    let statuses = DUMP_STATUSES.read().expect("dump status lock poisoned");
    statuses.get(db_name).map(|s| s.code)
}

fn known_database(db_name: &str) -> bool {
    DUMP_STATUSES
        .read()
        .expect("dump status lock poisoned")
        .contains_key(db_name)
}

/// Compute the aggregated dump status across all tracked databases.
///
/// Rules:
/// - No tracked databases → `0` (UP — nothing to report).
/// - Otherwise → highest severity value among all databases.
///   A single DOWN makes the aggregate DOWN.
pub fn get_aggregated_dump_status() -> u8 {
    let statuses = DUMP_STATUSES.read().expect("dump status lock poisoned");
    statuses.values().map(|s| s.code).max().unwrap_or(0)
}

/// Publish the configured concurrency limit so `/api/config` can report it.
pub fn set_max_parallel_databases(limit: usize) {
    MAX_PARALLEL_DATABASES.store(limit as u64, Ordering::SeqCst);
}

/// Publish the number of configured Telegram destinations so `/api/config` can
/// report it without exposing any destination values.
pub fn set_telegram_chat_count(count: usize) {
    TELEGRAM_CHAT_COUNT.store(count as u64, Ordering::SeqCst);
}

/// Publish the schedule the process is running on, as the dashboard should
/// show it — `"every 6h"`, `"cron 0 */4 * * *"`, or empty for one-shot.
pub fn set_schedule_label(label: impl Into<String>) {
    *SCHEDULE_LABEL.write().expect("schedule lock poisoned") = label.into();
}

/// Mark a backup cycle as started (`true`) or finished (`false`).
///
/// Starting a cycle clears the countdown: the next firing time is only known
/// once the cycle has finished, and a stale target would tick past zero and sit
/// there for the whole run.
pub fn set_cycle_running(running: bool) {
    CYCLE_RUNNING.store(running, Ordering::SeqCst);
    if running {
        BACKUP_NEXT_RUN_EPOCH_SECS.store(0, Ordering::SeqCst);
    }
}

/// Publish when the next backup cycle starts, as seconds from now.
pub fn set_next_backup_run_in(wait: std::time::Duration) {
    BACKUP_NEXT_RUN_EPOCH_SECS.store(
        now_epoch_secs().saturating_add(wait.as_secs()),
        Ordering::SeqCst,
    );
}

/// Publish when the next history upload starts, as seconds from now.
pub fn set_next_history_run_in(wait: std::time::Duration) {
    HISTORY_NEXT_RUN_EPOCH_SECS.store(
        now_epoch_secs().saturating_add(wait.as_secs()),
        Ordering::SeqCst,
    );
}

/// Publish the configured history-upload schedule for the dashboard.
pub fn set_history_schedule_label(label: impl Into<String>) {
    *HISTORY_SCHEDULE_LABEL
        .write()
        .expect("history schedule lock poisoned") = label.into();
}

/// Current wall-clock time as whole seconds since the UNIX epoch.
fn now_epoch_secs() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("system time before UNIX epoch")
        .as_secs()
}

/// Response shape for the `/api/config` endpoint (includes uptime and server details).
#[derive(Serialize, Deserialize)]
pub struct ConfigResponse {
    /// Port the dashboard is listening on.
    pub port: u16,
    /// Server uptime in seconds.
    pub uptime_seconds: u64,
    /// Hostname the server is running on.
    pub hostname: String,
    /// How many database backups may run at the same time.
    pub max_parallel_databases: u64,
    /// Number of configured Telegram destinations.
    pub telegram_chat_count: u64,
    /// Backward-compatible alias for the configured backup schedule.
    pub schedule: String,
    /// The configured backup schedule, or empty in one-shot mode.
    pub backup_schedule: String,
    /// The configured history-upload schedule, or empty when disabled.
    pub history_schedule: String,
    /// `"running"` while a backup cycle is in flight, `"waiting"` between
    /// scheduled cycles, `"idle"` when there is no backup schedule.
    pub phase: &'static str,
    /// Backward-compatible alias for the backup countdown.
    pub next_run_secs: Option<u64>,
    /// Seconds until the next backup cycle.
    pub backup_next_run_secs: Option<u64>,
    /// Seconds until the next history upload.
    pub history_next_run_secs: Option<u64>,
}

/// GET /api/config — returns the current dashboard port.
///
/// Exposes server metadata (port, uptime, hostname) so the dashboard can
/// display a rich overview panel alongside the live status cards.
async fn api_config(cfg: web::Data<u16>) -> impl Responder {
    // Lazily store the start time on the first request. `compare_exchange`
    // makes a concurrent first request settle on a single start value.
    let now_secs = now_epoch_secs();
    let start = match START_EPOCH_SECS.compare_exchange(
        0,
        now_secs,
        Ordering::Relaxed,
        Ordering::Relaxed,
    ) {
        Ok(_) => now_secs,         // this request set the start time
        Err(existing) => existing, // another request already set it
    };

    let hostname = hostname();
    let uptime_seconds = now_secs.saturating_sub(start);
    let backup_schedule = SCHEDULE_LABEL
        .read()
        .expect("schedule lock poisoned")
        .clone();
    let history_schedule = HISTORY_SCHEDULE_LABEL
        .read()
        .expect("history schedule lock poisoned")
        .clone();
    let running = CYCLE_RUNNING.load(Ordering::SeqCst);
    let backup_next_run = BACKUP_NEXT_RUN_EPOCH_SECS.load(Ordering::SeqCst);
    let history_next_run = HISTORY_NEXT_RUN_EPOCH_SECS.load(Ordering::SeqCst);
    let backup_next_run_secs =
        (backup_next_run > 0).then(|| backup_next_run.saturating_sub(now_secs));
    let history_next_run_secs =
        (history_next_run > 0).then(|| history_next_run.saturating_sub(now_secs));
    HttpResponse::Ok().json(ConfigResponse {
        port: **cfg,
        uptime_seconds,
        hostname,
        max_parallel_databases: MAX_PARALLEL_DATABASES.load(Ordering::SeqCst),
        telegram_chat_count: TELEGRAM_CHAT_COUNT.load(Ordering::SeqCst),
        phase: match (running, backup_schedule.is_empty()) {
            (true, _) => "running",
            (false, false) => "waiting",
            // No schedule and no cycle running: a one-shot run that has either
            // not reached its cycle yet or already finished it.
            (false, true) => "idle",
        },
        schedule: backup_schedule.clone(),
        backup_schedule,
        history_schedule,
        // A target in the past reads as "due now" rather than a negative
        // countdown — the cycle is about to start.
        next_run_secs: backup_next_run_secs,
        backup_next_run_secs,
        history_next_run_secs,
    })
}

#[derive(Serialize)]
struct StatusResponse {
    state: &'static str,
    message: String,
    timestamp: String,
}

/// Map a severity code to its human-readable label.
fn state_label(code: u8) -> &'static str {
    match code {
        0 => "UP",
        1 => "DEGRADED",
        _ => "DOWN",
    }
}

/// Map atomic status to human-readable label and message.
fn status_entry(code: u8, message: &str) -> StatusResponse {
    StatusResponse {
        state: state_label(code),
        message: message.to_string(),
        timestamp: chrono::Utc::now().to_rfc3339(),
    }
}

/// GET /api/status/service — returns Telegram API connection status.
///
/// Returns a JSON object with:
/// - `state`: one of "UP", "DEGRADED", "DOWN"
/// - `message`: human-readable description
/// - `timestamp`: ISO 8601 timestamp of the status update
///
/// The dashboard polls this every 2 seconds to keep the UI fresh.
async fn api_service_status() -> impl Responder {
    let code = get_telegram_status();
    let msg = match code {
        0 => "Telegram API is reachable and responding normally".to_string(),
        1 => "Telegram API is slow or rate-limited".to_string(),
        _ => "Unable to reach Telegram API".to_string(),
    };
    HttpResponse::Ok().json(status_entry(code, &msg))
}

/// GET /api/status/process — returns aggregated dump process status.
///
/// Aggregates per-database statuses by taking the maximum severity.
/// Uses the same `state`/`message`/`timestamp` structure as the legacy single-db response.
async fn api_process_status() -> impl Responder {
    let code = get_aggregated_dump_status();
    let msg = match code {
        0 => "No active dump process or dump completed successfully".to_string(),
        1 => "Dump process is running or waiting for next backup".to_string(),
        _ => "Dump process failed or is in error state".to_string(),
    };
    HttpResponse::Ok().json(status_entry(code, &msg))
}

/// GET /api/status/database/{name} — returns per-database dump status.
///
/// Returns a JSON object with:
/// - `state`: one of "UP", "DEGRADED", "DOWN"
/// - `message`: human-readable description
/// - `timestamp`: ISO 8601 timestamp of the status update
///
/// Returns 404 if the named database has no recorded status.
async fn api_db_status(path: web::Path<String>) -> impl Responder {
    let db_name = path.into_inner();
    match get_dump_status(&db_name) {
        Some(code) => {
            let msg = match code {
                0 => format!("Database '{db_name}' backup completed successfully"),
                1 => format!("Database '{db_name}' backup is running"),
                _ => format!("Database '{db_name}' backup encountered an error"),
            };
            HttpResponse::Ok().json(status_entry(code, &msg))
        }
        None => HttpResponse::NotFound().json(serde_json::json!({
            "error": format!("Unknown database: {db_name}")
        })),
    }
}

/// One database entry in the `/api/status/databases` response.
#[derive(Serialize)]
struct DatabaseResponse {
    /// Display name of the database.
    name: String,
    enabled: bool,
    /// Severity label: "UP", "DEGRADED" or "DOWN".
    state: &'static str,
    /// Current pipeline stage — drives the dashboard timeline.
    stage: &'static str,
    /// Current/next step description shown under the name.
    detail: String,
    /// Bytes uploaded so far in this run.
    bytes_done: u64,
    /// Total bytes to upload; 0 while still unknown.
    bytes_total: u64,
    /// Upload throughput in bytes/second; 0 when idle.
    speed_bps: f64,
    /// Uncompressed bytes read from `pg_dump` this run; 0 while unknown.
    dump_bytes: u64,
    /// RFC 3339 timestamp of the last status update.
    timestamp: String,
}

/// GET /api/status/databases — returns status for all tracked databases.
///
/// Returns a JSON array sorted alphabetically by database name.
/// When no databases are tracked, returns an empty array.
async fn api_databases_list() -> impl Responder {
    let statuses = DUMP_STATUSES.read().expect("dump status lock poisoned");

    let mut entries: Vec<_> = statuses
        .iter()
        .map(|(name, s)| DatabaseResponse {
            name: name.clone(),
            enabled: database_state_store().is_enabled(name),
            state: state_label(s.code),
            stage: s.stage,
            detail: s.detail.clone(),
            bytes_done: s.bytes_done,
            bytes_total: s.bytes_total,
            speed_bps: s.speed_bps,
            dump_bytes: s.dump_bytes,
            timestamp: s.updated.clone(),
        })
        .collect();

    // Sort by database name (alphabetical) for stable dashboard ordering.
    entries.sort_by_key(|e| e.name.to_lowercase());
    HttpResponse::Ok().json(entries)
}

async fn api_database_action(
    path: web::Path<(String, String)>,
    history: web::Data<std::sync::Arc<HistoryStore>>,
) -> impl Responder {
    let (name, action) = path.into_inner();
    if action == "backup" {
        if !known_database(&name) {
            return HttpResponse::NotFound()
                .json(serde_json::json!({"error": format!("Unknown database: {name}")}));
        }
        if !database_state_store().is_enabled(&name) {
            return HttpResponse::Conflict()
                .json(serde_json::json!({"error": "Database is disabled"}));
        }
        if !manual_backup_controller().request(&name) {
            return HttpResponse::Conflict().json(
                serde_json::json!({"error": "Database backup is already running or queued"}),
            );
        }
        set_db_status(
            &name,
            1,
            "queued",
            "Manual backup queued — waiting to start",
        );
        return HttpResponse::Accepted()
            .json(serde_json::json!({"name": name, "status": "queued"}));
    }
    if !matches!(action.as_str(), "enable" | "disable") {
        return HttpResponse::NotFound()
            .json(serde_json::json!({"error": "Unknown database action"}));
    }
    let store = database_state_store();
    if !DUMP_STATUSES
        .read()
        .expect("dump status lock poisoned")
        .contains_key(&name)
    {
        return HttpResponse::NotFound()
            .json(serde_json::json!({"error": format!("Unknown database: {name}")}));
    }
    let enabled = action == "enable";
    if let Err(error) = store.set_enabled(&name, enabled) {
        tracing::warn!(database = %name, error = %error, "failed to persist database state");
        return HttpResponse::InternalServerError()
            .json(serde_json::json!({"error": "database state unavailable"}));
    }
    if enabled {
        set_db_status(&name, 0, "queued", "Queued — waiting to start");
    } else {
        set_db_status(&name, 0, "disabled", "DISABLED — backup skipped");
    }
    let now = crate::history::timestamp(std::time::SystemTime::now());
    let record = crate::history::HistoryRecord {
        started_at: now.clone(),
        ended_at: now,
        database_index: 0,
        database_name: name.clone(),
        source: "scheduled".into(),
        status: action,
        error: None,
        dump_bytes: 0,
        packaged_bytes: 0,
        chunk_count: 0,
        sha256: None,
        encrypted: false,
        duration_secs: 0.0,
        upload_duration_secs: 0.0,
        upload_attempts: 0,
        upload_retries: 0,
    };
    if let Err(error) = history.append(&record) {
        tracing::warn!(database = %name, error = %error, "failed to append database state history");
    }
    HttpResponse::Ok().json(serde_json::json!({"name": name, "enabled": enabled}))
}

/// GET /api/history/{database_name} — retained attempts and aggregate stats.
async fn api_history(
    path: web::Path<String>,
    history: web::Data<std::sync::Arc<HistoryStore>>,
) -> impl Responder {
    let database_name = path.into_inner();
    match history.summary(&database_name, 30) {
        Ok(summary) => HttpResponse::Ok().json(summary),
        Err(error) => {
            tracing::warn!(database = %database_name, error = %error, "failed to read database history");
            HttpResponse::InternalServerError().json(serde_json::json!({
                "error": "history is temporarily unavailable"
            }))
        }
    }
}

#[derive(Clone)]
pub struct DashboardAuth {
    username: String,
    password: String,
}

async fn dashboard_auth(
    req: ServiceRequest,
    credentials: BasicAuth,
) -> Result<ServiceRequest, (Error, ServiceRequest)> {
    let expected = req
        .app_data::<web::Data<DashboardAuth>>()
        .expect("dashboard auth state not initialized");
    if credentials.user_id() == expected.username
        && credentials
            .password()
            .is_some_and(|password| password == expected.password)
    {
        Ok(req)
    } else {
        let config = BasicConfig::default().realm("crab-dump");
        Err((
            actix_web_httpauth::extractors::AuthenticationError::from(config).into(),
            req,
        ))
    }
}

#[derive(Deserialize)]
struct TelegramUserPayload {
    name: String,
    chat_id: String,
    enabled: bool,
}

async fn api_telegram_users(store: web::Data<Arc<TelegramUserStore>>) -> impl Responder {
    HttpResponse::Ok().json(store.list())
}

async fn create_telegram_user(
    store: web::Data<Arc<TelegramUserStore>>,
    payload: web::Json<TelegramUserPayload>,
) -> impl Responder {
    let user = TelegramUser {
        name: payload.name.clone(),
        chat_id: payload.chat_id.clone(),
        enabled: payload.enabled,
    };
    match store.create(user.clone()) {
        Ok(()) => HttpResponse::Created().json(user),
        Err(error) if error.to_string().contains("already exists") => {
            HttpResponse::Conflict().json(serde_json::json!({"error": "chat ID already exists"}))
        }
        Err(error) if error.to_string().contains("must not be blank") => {
            HttpResponse::BadRequest().json(serde_json::json!({"error": error.to_string()}))
        }
        Err(error) => {
            tracing::warn!(error = %error, "failed to persist Telegram user");
            HttpResponse::InternalServerError()
                .json(serde_json::json!({"error": "Telegram user directory unavailable"}))
        }
    }
}

async fn update_telegram_user(
    path: web::Path<String>,
    store: web::Data<Arc<TelegramUserStore>>,
    payload: web::Json<TelegramUserPayload>,
) -> impl Responder {
    let chat_id = path.into_inner();
    let user = TelegramUser {
        name: payload.name.clone(),
        chat_id: payload.chat_id.clone(),
        enabled: payload.enabled,
    };
    match store.update(&chat_id, user.clone()) {
        Ok(true) => HttpResponse::Ok().json(user),
        Ok(false) => {
            HttpResponse::NotFound().json(serde_json::json!({"error": "Telegram user not found"}))
        }
        Err(error) if error.to_string().contains("already exists") => {
            HttpResponse::Conflict().json(serde_json::json!({"error": "chat ID already exists"}))
        }
        Err(error) if error.to_string().contains("must not be blank") => {
            HttpResponse::BadRequest().json(serde_json::json!({"error": error.to_string()}))
        }
        Err(error) => {
            tracing::warn!(error = %error, "failed to persist Telegram user");
            HttpResponse::InternalServerError()
                .json(serde_json::json!({"error": "Telegram user directory unavailable"}))
        }
    }
}

async fn delete_telegram_user(
    path: web::Path<String>,
    store: web::Data<Arc<TelegramUserStore>>,
) -> impl Responder {
    match store.delete(&path.into_inner()) {
        Ok(true) => HttpResponse::NoContent().finish(),
        Ok(false) => {
            HttpResponse::NotFound().json(serde_json::json!({"error": "Telegram user not found"}))
        }
        Err(error) => {
            tracing::warn!(error = %error, "failed to persist Telegram user deletion");
            HttpResponse::InternalServerError()
                .json(serde_json::json!({"error": "Telegram user directory unavailable"}))
        }
    }
}
/// Resolve the local hostname; fall back to "unknown" on failure.
fn hostname() -> String {
    std::fs::read_to_string("/etc/hostname")
        .map(|s| s.trim().to_string())
        .or_else(|_| std::env::var("HOSTNAME").map(|s| s.trim().to_string()))
        .unwrap_or_else(|_| "unknown".to_string())
}

/// Serve `index.html` embedded at compile time.
async fn serve_dashboard() -> impl Responder {
    HttpResponse::Ok()
        .content_type("text/html; charset=utf-8")
        .body(include_str!("../index.html"))
}

async fn serve_users() -> impl Responder {
    HttpResponse::Ok()
        .content_type("text/html; charset=utf-8")
        .body(include_str!("../users.html"))
}

/// Start the HTTP server and block until it stops.
///
/// Serves:
/// - `/` — the dashboard HTML
/// - `/index.html` — same dashboard HTML
/// - `/api/config` — server metadata plus schedule state (`schedule`, `phase`,
///   `next_run_secs`)
/// - `/api/status/service` — returns Telegram API connection status
/// - `/api/status/process` — returns aggregated PostgreSQL dump status
/// - `/api/status/database/{name}` — returns per-database dump status
/// - `/api/status/databases` — returns all tracked database statuses as an array
///
/// All status endpoints return JSON with `state`, `message`, and `timestamp` fields.
pub async fn start_server(
    host: &str,
    port: u16,
    history: std::sync::Arc<HistoryStore>,
    username: String,
    password: String,
    telegram_users: Arc<TelegramUserStore>,
) -> std::io::Result<()> {
    // Share the port via actix-web `Data` so every handler can read it.
    let port_data = web::Data::new(port);
    let auth_data = web::Data::new(DashboardAuth { username, password });
    let users_data = web::Data::new(telegram_users);

    HttpServer::new(move || {
        let auth = HttpAuthentication::basic(dashboard_auth);
        App::new()
            .app_data(port_data.clone())
            .app_data(web::Data::new(history.clone()))
            .app_data(auth_data.clone())
            .app_data(users_data.clone())
            .route("/api/config", web::get().to(api_config))
            .route("/api/status/service", web::get().to(api_service_status))
            .route("/api/status/process", web::get().to(api_process_status))
            .route("/api/status/database/{name}", web::get().to(api_db_status))
            .route("/api/status/databases", web::get().to(api_databases_list))
            .route(
                "/api/status/database/{name}/{action}",
                web::post().to(api_database_action),
            )
            .route("/api/history/{database_name}", web::get().to(api_history))
            .route("/api/telegram-users", web::get().to(api_telegram_users))
            .route("/api/telegram-users", web::post().to(create_telegram_user))
            .route(
                "/api/telegram-users/{chat_id}",
                web::put().to(update_telegram_user),
            )
            .route(
                "/api/telegram-users/{chat_id}",
                web::delete().to(delete_telegram_user),
            )
            .route("/api/info", web::get().to(api_config))
            .route("/", web::get().to(serve_dashboard))
            .route("/index.html", web::get().to(serve_dashboard))
            .route("/users", web::get().to(serve_users))
            .wrap(auth)
    })
    .bind((host, port))?
    // Dashboard runs on a background thread; actix's own SIGINT handler would
    // swallow Ctrl+C and leave the main thread hanging forever.
    .disable_signals()
    .run()
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::http::header;
    use actix_web::test as aw_test;
    use std::path::PathBuf;

    /// Read one entry's fields. Tests use distinct database names because
    /// `DUMP_STATUSES` is process-global and tests run in parallel.
    fn snapshot(name: &str) -> (u8, &'static str, u64, u64, f64) {
        let statuses = DUMP_STATUSES.read().expect("dump status lock poisoned");
        let s = statuses.get(name).expect("database not tracked");
        (s.code, s.stage, s.bytes_done, s.bytes_total, s.speed_bps)
    }

    #[test]
    fn failure_keeps_stage_and_bytes_but_zeroes_speed() {
        set_db_status("test-fail", 1, "upload", "uploading");
        set_db_transfer("test-fail", 40, 100, 20.0);
        fail_db("test-fail", "connection reset");

        // The stage it died on drives the red node in the dashboard timeline,
        // and the byte counts show how far the upload got.
        assert_eq!(snapshot("test-fail"), (2, "upload", 40, 100, 0.0));
    }

    #[test]
    fn advancing_stage_resets_transfer_counters() {
        set_db_status("test-reset", 1, "upload", "uploading");
        set_db_transfer("test-reset", 40, 100, 20.0);
        set_db_status("test-reset", 1, "dump", "dumping");

        assert_eq!(snapshot("test-reset"), (1, "dump", 0, 0, 0.0));
    }

    /// Read the dump size for one entry.
    fn dump_bytes(name: &str) -> u64 {
        let statuses = DUMP_STATUSES.read().expect("dump status lock poisoned");
        statuses.get(name).expect("database not tracked").dump_bytes
    }

    #[test]
    fn dump_size_survives_later_stages_and_resets_on_next_run() {
        set_db_status("test-size", 1, "dump", "dumping");
        set_db_dump_bytes("test-size", 4096);

        // Packaging and uploading keep showing the size dumped this run.
        set_db_status("test-size", 1, "package", "packaging");
        assert_eq!(dump_bytes("test-size"), 4096);
        set_db_status("test-size", 1, "upload", "uploading");
        assert_eq!(dump_bytes("test-size"), 4096);
        set_db_status("test-size", 0, "done", "done");
        assert_eq!(dump_bytes("test-size"), 4096);

        // A new run starts counting from zero again.
        set_db_status("test-size", 1, "dump", "dumping");
        assert_eq!(dump_bytes("test-size"), 0);
    }

    /// A countdown left over from the previous cycle would tick to zero and sit
    /// at "due now" for the whole run, so starting a cycle must clear it.
    #[test]
    fn starting_a_cycle_clears_the_countdown() {
        set_next_backup_run_in(std::time::Duration::from_secs(600));
        assert!(BACKUP_NEXT_RUN_EPOCH_SECS.load(Ordering::SeqCst) > now_epoch_secs());

        set_cycle_running(true);
        assert_eq!(BACKUP_NEXT_RUN_EPOCH_SECS.load(Ordering::SeqCst), 0);
        set_cycle_running(false);
    }

    #[test]
    fn backup_and_history_countdowns_are_independent() {
        set_next_backup_run_in(std::time::Duration::from_secs(600));
        set_next_history_run_in(std::time::Duration::from_secs(1200));
        assert!(BACKUP_NEXT_RUN_EPOCH_SECS.load(Ordering::SeqCst) > 0);
        assert!(HISTORY_NEXT_RUN_EPOCH_SECS.load(Ordering::SeqCst) > 0);

        set_cycle_running(true);
        assert_eq!(BACKUP_NEXT_RUN_EPOCH_SECS.load(Ordering::SeqCst), 0);
        assert!(HISTORY_NEXT_RUN_EPOCH_SECS.load(Ordering::SeqCst) > 0);
        set_cycle_running(false);
    }

    #[test]
    fn transfer_update_for_unknown_database_is_ignored() {
        set_db_transfer("test-absent", 1, 2, 3.0);
        let statuses = DUMP_STATUSES.read().expect("dump status lock poisoned");
        assert!(statuses.get("test-absent").is_none());
    }

    #[test]
    fn manual_controller_rejects_duplicate_and_active_names() {
        let controller = ManualBackupController::default();
        assert!(controller.request("app"));
        assert!(!controller.request("app"));
        assert_eq!(controller.take_pending(), vec!["app"]);
        assert!(controller.is_active("app"));
        assert!(!controller.request("app"));
        controller.finish("app");
        assert!(controller.request("app"));
    }

    fn users_test_store() -> Arc<TelegramUserStore> {
        let path = std::env::temp_dir().join(format!(
            "crab-dashboard-users-{}-{}.toml",
            std::process::id(),
            now_epoch_secs()
        ));
        let _ = std::fs::remove_file(&path);
        Arc::new(TelegramUserStore::load(PathBuf::from(path)).unwrap())
    }

    #[actix_web::test]
    async fn telegram_users_require_auth_and_support_crud() {
        let store = users_test_store();
        let app = aw_test::init_service(
            App::new()
                .app_data(web::Data::new(DashboardAuth {
                    username: "admin".into(),
                    password: "secret".into(),
                }))
                .app_data(web::Data::new(store))
                .route("/api/telegram-users", web::get().to(api_telegram_users))
                .route("/api/telegram-users", web::post().to(create_telegram_user))
                .route(
                    "/api/telegram-users/{chat_id}",
                    web::put().to(update_telegram_user),
                )
                .route(
                    "/api/telegram-users/{chat_id}",
                    web::delete().to(delete_telegram_user),
                )
                .wrap(HttpAuthentication::basic(dashboard_auth)),
        )
        .await;

        let missing = aw_test::TestRequest::get()
            .uri("/api/telegram-users")
            .to_request();
        assert_eq!(aw_test::call_service(&app, missing).await.status(), 401);

        let invalid = aw_test::TestRequest::get()
            .uri("/api/telegram-users")
            .insert_header((header::AUTHORIZATION, "Basic d3Jvbmc6Y3JlZA=="))
            .to_request();
        assert_eq!(aw_test::call_service(&app, invalid).await.status(), 401);

        let create = aw_test::TestRequest::post()
            .uri("/api/telegram-users")
            .insert_header((header::AUTHORIZATION, "Basic YWRtaW46c2VjcmV0"))
            .set_json(serde_json::json!({
                "name": "Alice",
                "chat_id": "-1",
                "enabled": true
            }))
            .to_request();
        assert_eq!(aw_test::call_service(&app, create).await.status(), 201);

        let update = aw_test::TestRequest::put()
            .uri("/api/telegram-users/-1")
            .insert_header((header::AUTHORIZATION, "Basic YWRtaW46c2VjcmV0"))
            .set_json(serde_json::json!({
                "name": "Alice updated",
                "chat_id": "-1",
                "enabled": false
            }))
            .to_request();
        assert_eq!(aw_test::call_service(&app, update).await.status(), 200);

        let delete = aw_test::TestRequest::delete()
            .uri("/api/telegram-users/-1")
            .insert_header((header::AUTHORIZATION, "Basic YWRtaW46c2VjcmV0"))
            .to_request();
        assert_eq!(aw_test::call_service(&app, delete).await.status(), 204);

        let missing_delete = aw_test::TestRequest::delete()
            .uri("/api/telegram-users/-1")
            .insert_header((header::AUTHORIZATION, "Basic YWRtaW46c2VjcmV0"))
            .to_request();
        assert_eq!(
            aw_test::call_service(&app, missing_delete).await.status(),
            404
        );
    }
}
