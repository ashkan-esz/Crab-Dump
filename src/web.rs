//! HTTP status dashboard server.
//!
//! Serves `index.html` as a static page and provides API endpoints:
//! - `GET /api/config` — returns server metadata plus independent backup and
//!   history-upload schedule state
//! - `GET /api/status/service` — Telegram API connection status
//! - `GET /api/status/process` — aggregated PostgreSQL dump status (max across DBs)
//! - `GET /api/status/database/{name}` — per-database dump status
//! - `GET /api/status/databases` — all database statuses as a JSON array
//! - `GET /api/status/resources` — CPU, memory, and WORK_DIR disk usage
//!
//! The dashboard polls these endpoints every 4 seconds and updates the UI.

use actix_web::{
    body::BoxBody,
    cookie::{Cookie, SameSite},
    dev::{ServiceRequest, ServiceResponse},
    http::{header, Method},
    middleware::{from_fn, Next},
    web, App, Error, HttpMessage, HttpRequest, HttpResponse, HttpServer, Responder,
};
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::mpsc::Sender;
use std::sync::{Arc, Condvar, LazyLock, Mutex, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::database_state::DatabaseStateStore;
use crate::history::HistoryStore;
use crate::resource_usage::ResourceCollector;
use crate::routing::{ProfileInput, ProfileKind, ProfileStore, RouteManager, RoutingBackend};
use crate::telegram;
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
/// Whether the long-lived scheduler is available to execute dashboard requests.
static MANUAL_BACKUP_AVAILABLE: AtomicBool = AtomicBool::new(false);
/// Serializes backup execution with runtime route mutations. Replacing the
/// Telegram client while a backup is uploading would split one backup across
/// two routes, so both operations must own this gate for their full duration.
static BACKUP_ROUTE_GATE: AtomicBool = AtomicBool::new(false);

/// Exclusive ownership of the backup/route operation slot.
pub struct BackupRouteGuard;

impl Drop for BackupRouteGuard {
    fn drop(&mut self) {
        BACKUP_ROUTE_GATE.store(false, Ordering::SeqCst);
    }
}

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
    /// One-based current chunk number, or 0 when no chunk is active.
    current_chunk: usize,
    /// Bytes streamed for the current chunk.
    current_chunk_done: u64,
    /// Total bytes in the current chunk.
    current_chunk_total: u64,
    /// Total number of chunks in the packaged upload.
    chunk_count: usize,
    /// Uncompressed bytes read from `pg_dump` stdout this run — the logical
    /// size of the database as dumped. Grows live during the dump stage and
    /// is kept for the rest of the run.
    dump_bytes: u64,
    /// RFC 3339 timestamp of the last update to this entry.
    updated: String,
}

#[derive(Clone, Copy, Default)]
pub struct ChunkProgress {
    pub number: usize,
    pub count: usize,
    pub done: u64,
    pub total: u64,
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
    pending: VecDeque<ManualBackupRequest>,
    pending_set: HashSet<String>,
    active: HashMap<String, Arc<AtomicBool>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManualBackupRequest {
    pub database_name: String,
    pub chat_id: String,
    pub recipient_name: String,
    pub no_encryption: bool,
}

#[derive(Debug, Default)]
pub struct ManualBackupController {
    state: Mutex<ManualBackupState>,
    wake: Condvar,
}

impl ManualBackupController {
    pub fn request(&self, request: ManualBackupRequest) -> bool {
        let mut state = self.state.lock().expect("manual backup lock poisoned");
        if state.active.contains_key(&request.database_name)
            || !state.pending_set.insert(request.database_name.clone())
        {
            return false;
        }
        state.pending.push_back(request);
        self.wake.notify_one();
        true
    }

    pub fn take_pending(&self) -> Vec<ManualBackupRequest> {
        let mut state = self.state.lock().expect("manual backup lock poisoned");
        let mut names = Vec::with_capacity(state.pending.len());
        while let Some(request) = state.pending.pop_front() {
            state.pending_set.remove(&request.database_name);
            state.active.insert(
                request.database_name.clone(),
                Arc::new(AtomicBool::new(false)),
            );
            names.push(request);
        }
        names
    }

    pub fn claim_scheduled(&self, name: &str) -> bool {
        let mut state = self.state.lock().expect("manual backup lock poisoned");
        if state.active.contains_key(name) || state.pending_set.contains(name) {
            return false;
        }
        state
            .active
            .insert(name.to_string(), Arc::new(AtomicBool::new(false)));
        true
    }

    pub fn cancellation_token(&self, name: &str) -> Option<Arc<AtomicBool>> {
        self.state
            .lock()
            .expect("manual backup lock poisoned")
            .active
            .get(name)
            .cloned()
    }

    pub fn can_cancel(&self, name: &str) -> bool {
        let state = self.state.lock().expect("manual backup lock poisoned");
        state.pending_set.contains(name) || state.active.contains_key(name)
    }

    pub fn cancel(&self, name: &str) -> CancelResult {
        let mut state = self.state.lock().expect("manual backup lock poisoned");
        if state.pending_set.remove(name) {
            state
                .pending
                .retain(|request| request.database_name != name);
            return CancelResult::Queued;
        }
        let Some(token) = state.active.get(name) else {
            return CancelResult::NotFound;
        };
        if token.swap(true, Ordering::SeqCst) {
            CancelResult::AlreadyCancelled
        } else {
            self.wake.notify_all();
            CancelResult::Active
        }
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
            .contains_key(name)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelResult {
    Queued,
    Active,
    AlreadyCancelled,
    NotFound,
}

#[derive(Debug)]
pub struct CancellationError;

impl std::fmt::Display for CancellationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("backup cancelled")
    }
}

impl std::error::Error for CancellationError {}

static MANUAL_BACKUPS: LazyLock<Arc<ManualBackupController>> =
    LazyLock::new(|| Arc::new(ManualBackupController::default()));

pub fn manual_backup_controller() -> Arc<ManualBackupController> {
    Arc::clone(&MANUAL_BACKUPS)
}

pub fn set_manual_backup_available(available: bool) {
    MANUAL_BACKUP_AVAILABLE.store(available, Ordering::SeqCst);
}

pub fn manual_backup_available() -> bool {
    MANUAL_BACKUP_AVAILABLE.load(Ordering::SeqCst)
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
            current_chunk: 0,
            current_chunk_done: 0,
            current_chunk_total: 0,
            chunk_count: 0,
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
    set_db_transfer_with_chunk(
        db_name,
        bytes_done,
        bytes_total,
        speed_bps,
        ChunkProgress::default(),
    );
}

/// Publish upload progress including the currently streaming chunk.
pub fn set_db_transfer_with_chunk(
    db_name: &str,
    bytes_done: u64,
    bytes_total: u64,
    speed_bps: f64,
    chunk: ChunkProgress,
) {
    let mut statuses = DUMP_STATUSES.write().expect("dump status lock poisoned");
    if let Some(entry) = statuses.get_mut(db_name) {
        entry.bytes_done = bytes_done;
        entry.bytes_total = bytes_total;
        entry.speed_bps = speed_bps;
        entry.current_chunk = chunk.number;
        entry.chunk_count = chunk.count;
        entry.current_chunk_done = chunk.done;
        entry.current_chunk_total = chunk.total;
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
        current_chunk: 0,
        current_chunk_done: 0,
        current_chunk_total: 0,
        chunk_count: 0,
        dump_bytes: 0,
        updated: String::new(),
    });
    entry.code = 2;
    entry.detail = detail.into();
    entry.speed_bps = 0.0;
    entry.current_chunk_done = 0;
    entry.current_chunk_total = 0;
    entry.updated = chrono::Utc::now().to_rfc3339();
}

pub fn cancel_db(db_name: &str) {
    let mut statuses = DUMP_STATUSES.write().expect("dump status lock poisoned");
    let entry = statuses.entry(db_name.to_string()).or_insert(DbStatus {
        code: 0,
        stage: "cancelled",
        detail: String::new(),
        bytes_done: 0,
        bytes_total: 0,
        speed_bps: 0.0,
        current_chunk: 0,
        current_chunk_done: 0,
        current_chunk_total: 0,
        chunk_count: 0,
        dump_bytes: 0,
        updated: String::new(),
    });
    entry.code = 0;
    entry.stage = "cancelled";
    entry.detail = "CANCELLED — backup stopped by operator".into();
    entry.speed_bps = 0.0;
    entry.current_chunk_done = 0;
    entry.current_chunk_total = 0;
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

/// Acquire exclusive ownership for a backup run.
pub fn acquire_backup_route_gate() -> BackupRouteGuard {
    while BACKUP_ROUTE_GATE
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_err()
    {
        std::thread::yield_now();
    }
    BackupRouteGuard
}

/// Try to acquire exclusive ownership for a runtime route mutation.
///
/// A non-blocking result lets the dashboard explain that the user should wait
/// for the active backup instead of leaving the request hanging.
pub fn try_acquire_route_gate() -> Option<BackupRouteGuard> {
    BACKUP_ROUTE_GATE
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .ok()
        .map(|_| BackupRouteGuard)
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
    pub manual_backup_available: bool,
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
        manual_backup_available: manual_backup_available(),
    })
}

#[derive(Serialize)]
struct StatusResponse {
    state: &'static str,
    message: String,
    timestamp: String,
}

#[derive(Serialize)]
struct ServiceStatusResponse {
    state: &'static str,
    message: String,
    timestamp: String,
    test_disabled: bool,
    test_disabled_reason: Option<&'static str>,
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
    let test_disabled_reason = telegram::test_disabled_reason();
    HttpResponse::Ok().json(ServiceStatusResponse {
        state: state_label(code),
        message: msg,
        timestamp: chrono::Utc::now().to_rfc3339(),
        test_disabled: test_disabled_reason.is_some(),
        test_disabled_reason,
    })
}

async fn test_telegram_api(
    client: web::Data<Arc<RwLock<Arc<Client>>>>,
    bot_token: web::Data<String>,
) -> impl Responder {
    let client = client
        .read()
        .expect("Telegram client lock poisoned")
        .clone();
    let token = bot_token.get_ref().clone();
    match tokio::task::spawn_blocking(move || telegram::try_test_api(&client, &token)).await {
        Ok(Ok(Ok(()))) => HttpResponse::Ok().json(serde_json::json!({
            "ok": true,
            "message": "Telegram API test succeeded"
        })),
        Ok(Err(reason)) => HttpResponse::Conflict().json(serde_json::json!({
            "ok": false,
            "error": reason,
            "disabled": true,
            "disabled_reason": reason
        })),
        Ok(Ok(Err(error))) => HttpResponse::BadGateway().json(serde_json::json!({
            "ok": false,
            "error": format!("Telegram API test failed: {error}")
        })),
        _ => HttpResponse::BadGateway().json(serde_json::json!({
            "ok": false,
            "error": "Telegram API test failed"
        })),
    }
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
    /// Whether a queued or active backup can currently be cancelled.
    cancellable: bool,
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
    /// One-based current chunk number, or 0 when no chunk is active.
    current_chunk: usize,
    /// Bytes streamed for the current chunk.
    current_chunk_done: u64,
    /// Total bytes in the current chunk.
    current_chunk_total: u64,
    /// Total number of chunks in the packaged upload.
    chunk_count: usize,
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
            cancellable: manual_backup_controller().can_cancel(name),
            state: state_label(s.code),
            stage: s.stage,
            detail: s.detail.clone(),
            bytes_done: s.bytes_done,
            bytes_total: s.bytes_total,
            speed_bps: s.speed_bps,
            current_chunk: s.current_chunk,
            current_chunk_done: s.current_chunk_done,
            current_chunk_total: s.current_chunk_total,
            chunk_count: s.chunk_count,
            dump_bytes: s.dump_bytes,
            timestamp: s.updated.clone(),
        })
        .collect();

    // Sort by database name (alphabetical) for stable dashboard ordering.
    entries.sort_by_key(|e| e.name.to_lowercase());
    HttpResponse::Ok().json(entries)
}

/// GET /api/status/resources — returns CPU, memory, and WORK_DIR disk usage.
async fn api_resource_status(collector: web::Data<ResourceCollector>) -> impl Responder {
    HttpResponse::Ok().json(collector.collect())
}

fn audit_action(req: &HttpRequest, action: &str, target: &str, result: &str) {
    let actor = req
        .extensions()
        .get::<AuthenticatedUser>()
        .map(|user| user.username.clone())
        .unwrap_or_else(|| "unknown".into());
    let source = req
        .connection_info()
        .realip_remote_addr()
        .unwrap_or("unknown")
        .to_string();
    tracing::info!(actor, source, action, target, result, "dashboard action");
}

async fn api_database_action(
    req: HttpRequest,
    path: web::Path<(String, String)>,
    history: web::Data<std::sync::Arc<HistoryStore>>,
    users: web::Data<Arc<TelegramUserStore>>,
    payload: Option<web::Json<ManualBackupPayload>>,
) -> impl Responder {
    let (name, action) = path.into_inner();
    if action == "cancel" {
        if !known_database(&name) {
            audit_action(&req, "cancel", &name, "not_found");
            return HttpResponse::NotFound()
                .json(serde_json::json!({"error": format!("Unknown database: {name}")}));
        }
        let result = manual_backup_controller().cancel(&name);
        match result {
            CancelResult::Queued | CancelResult::Active => {
                if matches!(result, CancelResult::Queued) {
                    cancel_db(&name);
                }
                let now = crate::history::timestamp(std::time::SystemTime::now());
                let record = crate::history::HistoryRecord {
                    started_at: now.clone(),
                    ended_at: now,
                    database_index: 0,
                    database_name: name.clone(),
                    source: "scheduled".into(),
                    recipient: None,
                    status: "cancelled".into(),
                    error: Some("cancelled by dashboard operator".into()),
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
                if matches!(result, CancelResult::Queued) {
                    if let Err(error) = history.append(&record) {
                        tracing::warn!(database = %name, error = %error, "failed to append cancellation history");
                    }
                }
                audit_action(&req, "cancel", &name, "accepted");
                return HttpResponse::Accepted()
                    .json(serde_json::json!({"name": name, "status": "cancelled"}));
            }
            CancelResult::AlreadyCancelled => {
                audit_action(&req, "cancel", &name, "already_cancelled");
                return HttpResponse::Conflict()
                    .json(serde_json::json!({"error": "Database backup is already cancelling"}));
            }
            CancelResult::NotFound => {
                audit_action(&req, "cancel", &name, "idle");
                return HttpResponse::Conflict().json(
                    serde_json::json!({"error": "Database backup is not queued or running"}),
                );
            }
        }
    }
    if action == "backup" {
        let Some(payload) = payload else {
            audit_action(&req, "backup", &name, "rejected_missing_payload");
            return HttpResponse::BadRequest()
                .json(serde_json::json!({"error": "recipient and no_encryption are required"}));
        };
        if payload.no_encryption
            && req
                .extensions()
                .get::<AuthenticatedUser>()
                .is_some_and(|user| user.role != DashboardRole::Admin)
        {
            audit_action(&req, "backup", &name, "rejected_no_encryption");
            return HttpResponse::Forbidden()
                .json(serde_json::json!({"error": "no_encryption requires administrator role"}));
        }
        if !manual_backup_available() {
            return HttpResponse::Conflict().json(
                serde_json::json!({"error": "Manual backups are unavailable in one-shot mode"}),
            );
        }
        if !known_database(&name) {
            return HttpResponse::NotFound()
                .json(serde_json::json!({"error": format!("Unknown database: {name}")}));
        }
        if !database_state_store().is_enabled(&name) {
            return HttpResponse::Conflict()
                .json(serde_json::json!({"error": "Database is disabled"}));
        }
        let Some(recipient) = users
            .list()
            .into_iter()
            .find(|user| user.chat_id == payload.chat_id)
        else {
            return HttpResponse::Conflict()
                .json(serde_json::json!({"error": "Recipient is unknown or disabled"}));
        };
        if !recipient.enabled {
            return HttpResponse::Conflict()
                .json(serde_json::json!({"error": "Recipient is unknown or disabled"}));
        }
        if !manual_backup_controller().request(ManualBackupRequest {
            database_name: name.clone(),
            chat_id: payload.chat_id.clone(),
            recipient_name: recipient.name,
            no_encryption: payload.no_encryption,
        }) {
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
        audit_action(&req, "backup", &name, "queued");
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
        audit_action(&req, &action, &name, "failed");
        tracing::warn!(database = %name, error = %error, "failed to persist database state");
        return HttpResponse::InternalServerError()
            .json(serde_json::json!({"error": "database state unavailable"}));
    }
    audit_action(&req, &action, &name, "ok");
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
        recipient: None,
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

#[derive(Debug, Deserialize)]
struct ManualBackupPayload {
    chat_id: String,
    no_encryption: bool,
}

#[derive(Debug, Deserialize)]
struct HistoryQuery {
    page: Option<usize>,
    page_size: Option<usize>,
}

/// GET /api/history/{database_name} — retained attempts and aggregate stats.
async fn api_history(
    path: web::Path<String>,
    query: web::Query<HistoryQuery>,
    history: web::Data<std::sync::Arc<HistoryStore>>,
) -> impl Responder {
    let database_name = path.into_inner();
    let page_size = query.page_size.unwrap_or(10);
    if !matches!(page_size, 10 | 20 | 50) {
        return HttpResponse::BadRequest().json(serde_json::json!({
            "error": "page_size must be one of 10, 20, or 50"
        }));
    }
    match history.summary(&database_name, query.page.unwrap_or(1), page_size) {
        Ok(summary) => HttpResponse::Ok().json(summary),
        Err(error) => {
            tracing::warn!(database = %database_name, error = %error, "failed to read database history");
            HttpResponse::InternalServerError().json(serde_json::json!({
                "error": "history is temporarily unavailable"
            }))
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum DashboardRole {
    Viewer,
    Operator,
    Admin,
}

#[derive(Clone)]
struct DashboardCredential {
    username: String,
    password: String,
    role: DashboardRole,
}

#[derive(Clone)]
struct DashboardSession {
    username: String,
    role: DashboardRole,
    csrf_token: String,
    expires_at: SystemTime,
}

#[derive(Clone)]
struct AuthenticatedUser {
    username: String,
    role: DashboardRole,
}

#[derive(Clone)]
pub struct DashboardAuth {
    credentials: Vec<DashboardCredential>,
    sessions: Arc<Mutex<HashMap<String, DashboardSession>>>,
    failures: Arc<Mutex<HashMap<String, (u32, SystemTime)>>>,
}

impl DashboardAuth {
    pub fn new(
        admin_username: String,
        admin_password: String,
        operator: Option<(String, String)>,
        viewer: Option<(String, String)>,
    ) -> Self {
        let mut credentials = vec![DashboardCredential {
            username: admin_username,
            password: admin_password,
            role: DashboardRole::Admin,
        }];
        if let Some((username, password)) = operator {
            credentials.push(DashboardCredential {
                username,
                password,
                role: DashboardRole::Operator,
            });
        }
        if let Some((username, password)) = viewer {
            credentials.push(DashboardCredential {
                username,
                password,
                role: DashboardRole::Viewer,
            });
        }
        Self {
            credentials,
            sessions: Arc::new(Mutex::new(HashMap::new())),
            failures: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn credential(&self, username: &str, password: &str) -> Option<DashboardRole> {
        self.credentials
            .iter()
            .find(|credential| credential.username == username && credential.password == password)
            .map(|credential| credential.role)
    }

    fn is_throttled(&self, source: &str) -> bool {
        let failures = self.failures.lock().expect("login failure lock poisoned");
        failures.get(source).is_some_and(|(count, since)| {
            since.elapsed().unwrap_or_default() < Duration::from_secs(60) && *count >= 5
        })
    }

    fn record_failure(&self, source: &str) {
        let mut failures = self.failures.lock().expect("login failure lock poisoned");
        let entry = failures
            .entry(source.to_string())
            .or_insert((0, SystemTime::now()));
        if entry.1.elapsed().unwrap_or_default() >= Duration::from_secs(60) {
            *entry = (0, SystemTime::now());
        }
        entry.0 = entry.0.saturating_add(1);
    }

    fn clear_failures(&self, source: &str) {
        self.failures
            .lock()
            .expect("login failure lock poisoned")
            .remove(source);
    }
}

#[cfg(test)]
async fn legacy_dashboard_auth(
    req: ServiceRequest,
    credentials: actix_web_httpauth::extractors::basic::BasicAuth,
) -> Result<ServiceRequest, (Error, ServiceRequest)> {
    let expected = req
        .app_data::<web::Data<DashboardAuth>>()
        .expect("dashboard auth state not initialized");
    if credentials
        .password()
        .and_then(|password| expected.credential(credentials.user_id(), password))
        .is_some()
    {
        Ok(req)
    } else {
        let config = actix_web_httpauth::extractors::basic::Config::default().realm("crab-dump");
        Err((
            actix_web_httpauth::extractors::AuthenticationError::from(config).into(),
            req,
        ))
    }
}

#[derive(Deserialize)]
struct LoginPayload {
    username: String,
    password: String,
}

fn token() -> String {
    static TOKEN_COUNTER: AtomicU64 = AtomicU64::new(0);
    let counter = TOKEN_COUNTER.fetch_add(1, Ordering::Relaxed);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let mut hasher = Sha256::new();
    hasher.update(now.to_le_bytes());
    hasher.update(counter.to_le_bytes());
    hex::encode(hasher.finalize())
}

async fn login(
    req: HttpRequest,
    auth: web::Data<DashboardAuth>,
    payload: web::Json<LoginPayload>,
) -> impl Responder {
    let source = req
        .connection_info()
        .realip_remote_addr()
        .unwrap_or("unknown")
        .to_string();
    if auth.is_throttled(&source) {
        return HttpResponse::TooManyRequests()
            .insert_header((header::RETRY_AFTER, "60"))
            .json(serde_json::json!({"error": "login temporarily throttled"}));
    }
    let Some(role) = auth.credential(&payload.username, &payload.password) else {
        auth.record_failure(&source);
        tracing::warn!(source = %source, "dashboard login failed");
        return HttpResponse::Unauthorized()
            .json(serde_json::json!({"error": "invalid credentials"}));
    };
    auth.clear_failures(&source);
    let session_token = token();
    let csrf_token = token();
    auth.sessions
        .lock()
        .expect("dashboard session lock poisoned")
        .insert(
            session_token.clone(),
            DashboardSession {
                username: payload.username.clone(),
                role,
                csrf_token: csrf_token.clone(),
                expires_at: SystemTime::now() + Duration::from_secs(8 * 60 * 60),
            },
        );
    HttpResponse::Ok()
        .cookie(
            Cookie::build("crab_session", session_token)
                .http_only(true)
                .same_site(SameSite::Strict)
                .path("/")
                .finish(),
        )
        .cookie(
            Cookie::build("crab_csrf", csrf_token.clone())
                .http_only(false)
                .same_site(SameSite::Strict)
                .path("/")
                .finish(),
        )
        .json(serde_json::json!({"role": format!("{role:?}").to_lowercase(), "csrf_token": csrf_token}))
}

fn minimum_role(req: &ServiceRequest) -> DashboardRole {
    let path = req.path();
    if path.starts_with("/api/telegram-users") {
        DashboardRole::Admin
    } else if path.ends_with("/backup") || path.ends_with("/enable") || path.ends_with("/disable") {
        DashboardRole::Operator
    } else if matches!(*req.method(), Method::POST | Method::PUT | Method::DELETE) {
        DashboardRole::Admin
    } else {
        DashboardRole::Viewer
    }
}

fn origin_is_same_site(req: &ServiceRequest) -> bool {
    let Some(origin) = req.headers().get(header::ORIGIN) else {
        return true;
    };
    let Some(host) = req.headers().get(header::HOST) else {
        return false;
    };
    let host = host.to_str().unwrap_or("");
    origin.to_str().ok().is_some_and(|value| {
        value == format!("http://{host}") || value == format!("https://{host}")
    })
}

async fn dashboard_auth(
    req: ServiceRequest,
    next: Next<BoxBody>,
) -> Result<ServiceResponse<BoxBody>, Error> {
    // The HTML shell must be public so its JavaScript can perform the session
    // login. All data and mutation endpoints remain protected below.
    if matches!(
        req.path(),
        "/" | "/index.html" | "/users" | "/routing" | "/healthz" | "/api/auth/login"
    ) {
        return next.call(req).await;
    }
    let Some(auth) = req.app_data::<web::Data<DashboardAuth>>().cloned() else {
        return Ok(req.into_response(HttpResponse::InternalServerError().finish()));
    };
    let Some(cookie) = req.cookie("crab_session") else {
        return Ok(req.into_response(
            HttpResponse::Unauthorized()
                .json(serde_json::json!({"error": "authentication required"})),
        ));
    };
    let session = auth
        .sessions
        .lock()
        .expect("dashboard session lock poisoned")
        .get(cookie.value())
        .cloned();
    let Some(session) = session else {
        return Ok(req.into_response(
            HttpResponse::Unauthorized().json(serde_json::json!({"error": "session expired"})),
        ));
    };
    if session.expires_at <= SystemTime::now() {
        auth.sessions
            .lock()
            .expect("dashboard session lock poisoned")
            .remove(cookie.value());
        return Ok(req.into_response(
            HttpResponse::Unauthorized().json(serde_json::json!({"error": "session expired"})),
        ));
    }
    if session.role < minimum_role(&req) {
        return Ok(req.into_response(
            HttpResponse::Forbidden()
                .json(serde_json::json!({"error": "insufficient dashboard role"})),
        ));
    }
    if matches!(*req.method(), Method::POST | Method::PUT | Method::DELETE) {
        let csrf_cookie = req
            .cookie("crab_csrf")
            .map(|cookie| cookie.value().to_string());
        let csrf_header = req
            .headers()
            .get("x-csrf-token")
            .and_then(|value| value.to_str().ok())
            .map(str::to_string);
        if csrf_cookie.as_deref() != Some(session.csrf_token.as_str())
            || csrf_header.as_deref() != Some(session.csrf_token.as_str())
            || !origin_is_same_site(&req)
        {
            return Ok(req.into_response(
                HttpResponse::Forbidden()
                    .json(serde_json::json!({"error": "CSRF validation failed"})),
            ));
        }
    }
    req.extensions_mut().insert(AuthenticatedUser {
        username: session.username,
        role: session.role,
    });
    next.call(req).await
}

async fn healthz(route: web::Data<Arc<RouteManager>>) -> impl Responder {
    if route.is_healthy() {
        HttpResponse::Ok().body("ok")
    } else {
        HttpResponse::ServiceUnavailable().body("routing core unavailable")
    }
}

async fn security_headers(
    req: ServiceRequest,
    next: Next<BoxBody>,
) -> Result<ServiceResponse<BoxBody>, Error> {
    let mut response = next.call(req).await?;
    let headers = response.headers_mut();
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        header::HeaderValue::from_static("nosniff"),
    );
    headers.insert(
        header::X_FRAME_OPTIONS,
        header::HeaderValue::from_static("DENY"),
    );
    headers.insert(
        header::REFERRER_POLICY,
        header::HeaderValue::from_static("no-referrer"),
    );
    headers.insert(
        header::CACHE_CONTROL,
        header::HeaderValue::from_static("no-store"),
    );
    headers.insert(
        header::CONTENT_SECURITY_POLICY,
        header::HeaderValue::from_static("default-src 'self'; style-src 'self' 'unsafe-inline'; script-src 'self' 'unsafe-inline'"),
    );
    Ok(response)
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
    req: HttpRequest,
    store: web::Data<Arc<TelegramUserStore>>,
    payload: web::Json<TelegramUserPayload>,
) -> impl Responder {
    let user = TelegramUser {
        name: payload.name.clone(),
        chat_id: payload.chat_id.clone(),
        enabled: payload.enabled,
    };
    match store.create(user.clone()) {
        Ok(()) => {
            audit_action(&req, "telegram_user_create", &user.chat_id, "ok");
            HttpResponse::Created().json(user)
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

async fn update_telegram_user(
    req: HttpRequest,
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
        Ok(true) => {
            audit_action(&req, "telegram_user_update", &chat_id, "ok");
            HttpResponse::Ok().json(user)
        }
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
    req: HttpRequest,
    path: web::Path<String>,
    store: web::Data<Arc<TelegramUserStore>>,
) -> impl Responder {
    let chat_id = path.into_inner();
    match store.delete(&chat_id) {
        Ok(true) => {
            audit_action(&req, "telegram_user_delete", &chat_id, "ok");
            HttpResponse::NoContent().finish()
        }
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

async fn api_routing_profiles(
    store: web::Data<Arc<ProfileStore>>,
    route: web::Data<Arc<RouteManager>>,
) -> impl Responder {
    HttpResponse::Ok().json(store.list_for_available(&route.available_cores()))
}

async fn api_routing_status(
    store: web::Data<Arc<ProfileStore>>,
    route: web::Data<Arc<RouteManager>>,
) -> impl Responder {
    HttpResponse::Ok().json(store.routing_status(route.running_core(), &route.available_cores()))
}

#[derive(Deserialize)]
struct RoutingCoreInput {
    core: String,
}

async fn select_routing_core(
    req: HttpRequest,
    store: web::Data<Arc<ProfileStore>>,
    route: web::Data<Arc<RouteManager>>,
    payload: web::Json<RoutingCoreInput>,
) -> impl Responder {
    let core = match RoutingBackend::parse(&payload.core) {
        Ok(core) => core,
        Err(_) => {
            return HttpResponse::BadRequest()
                .json(serde_json::json!({"error": "core must be sing-box or shoes"}));
        }
    };
    if !route.is_available(core) {
        return HttpResponse::Conflict().json(serde_json::json!({
            "error": format!("The {} routing core is unavailable in this image.", core.as_str())
        }));
    }
    let persisted_route_is_active =
        store.active_id().is_some() && route.is_available(store.selected_core());
    if persisted_route_is_active || route.running_core().is_some() {
        return HttpResponse::Conflict().json(serde_json::json!({
            "error": "Disable routing before changing the selected core."
        }));
    }
    match store.set_selected_core(core) {
        Ok(()) => {
            audit_action(&req, "routing_core_select", core.as_str(), "ok");
            HttpResponse::Ok()
                .json(store.routing_status(route.running_core(), &route.available_cores()))
        }
        Err(_) => HttpResponse::InternalServerError()
            .json(serde_json::json!({"error": "profile storage unavailable"})),
    }
}

fn probe_compatible_cores(route: &RouteManager, url: &str) -> Option<Vec<RoutingBackend>> {
    let mut compatible = Vec::new();
    for core in route.available_cores() {
        if route.start_temporary(url, core).is_ok() {
            compatible.push(core);
        }
    }
    if compatible.is_empty() {
        None
    } else {
        Some(compatible)
    }
}

async fn create_routing_profile(
    req: HttpRequest,
    store: web::Data<Arc<ProfileStore>>,
    route: web::Data<Arc<RouteManager>>,
    payload: web::Json<ProfileInput>,
) -> impl Responder {
    let input = payload.into_inner();
    let route_for_probe = route.get_ref().clone();
    let url = input.url.clone();
    let compatible =
        match tokio::task::spawn_blocking(move || probe_compatible_cores(&route_for_probe, &url))
            .await
        {
            Ok(Some(cores)) => cores,
            _ => {
                return HttpResponse::BadRequest().json(serde_json::json!({
                    "error": "The profile is incompatible with every routing core."
                }));
            }
        };
    match store.create_with_compatibility(input, compatible) {
        Ok(summary) => {
            audit_action(&req, "routing_profile_create", &summary.id, "ok");
            HttpResponse::Created().json(summary)
        }
        Err(_) => {
            HttpResponse::BadRequest().json(serde_json::json!({"error": "invalid routing profile"}))
        }
    }
}

async fn update_routing_profile(
    req: HttpRequest,
    path: web::Path<String>,
    store: web::Data<Arc<ProfileStore>>,
    route: web::Data<Arc<RouteManager>>,
    payload: web::Json<ProfileInput>,
) -> impl Responder {
    let id = path.into_inner();
    let input = payload.into_inner();
    let route_for_probe = route.get_ref().clone();
    let url = input.url.clone();
    let compatible =
        match tokio::task::spawn_blocking(move || probe_compatible_cores(&route_for_probe, &url))
            .await
        {
            Ok(Some(cores)) => cores,
            _ => {
                return HttpResponse::BadRequest().json(serde_json::json!({
                    "error": "The profile is incompatible with every routing core."
                }));
            }
        };
    match store.update_with_compatibility(&id, input, compatible) {
        Ok(Some(summary)) => {
            audit_action(&req, "routing_profile_update", &summary.id, "ok");
            HttpResponse::Ok().json(summary)
        }
        Ok(None) => {
            HttpResponse::NotFound().json(serde_json::json!({"error": "profile not found"}))
        }
        Err(_) => {
            HttpResponse::BadRequest().json(serde_json::json!({"error": "invalid routing profile"}))
        }
    }
}

async fn delete_routing_profile(
    req: HttpRequest,
    path: web::Path<String>,
    store: web::Data<Arc<ProfileStore>>,
) -> impl Responder {
    let id = path.into_inner();
    match store.delete(&id) {
        Ok(true) => {
            audit_action(&req, "routing_profile_delete", &id, "ok");
            HttpResponse::NoContent().finish()
        }
        Ok(false) => {
            HttpResponse::NotFound().json(serde_json::json!({"error": "profile not found"}))
        }
        Err(_) => HttpResponse::InternalServerError()
            .json(serde_json::json!({"error": "profile storage unavailable"})),
    }
}

async fn test_routing_profile(
    store: web::Data<Arc<ProfileStore>>,
    route: web::Data<Arc<RouteManager>>,
    path: web::Path<String>,
) -> impl Responder {
    let Some(url) = store.get_url(&path.into_inner()) else {
        return HttpResponse::NotFound().json(serde_json::json!({"error": "profile not found"}));
    };
    match route.test(&url) {
        Ok(parsed) => HttpResponse::Ok().json(serde_json::json!({
            "kind": parsed.kind,
            "transport": parsed.transport,
            "tls": parsed.tls,
            "valid": true
        })),
        Err(_) => {
            HttpResponse::BadRequest().json(serde_json::json!({"error": "invalid routing profile"}))
        }
    }
}

#[derive(Serialize)]
struct RoutingCheckResult {
    id: String,
    name: String,
    kind: ProfileKind,
    transport: Option<String>,
    tls: Option<bool>,
    ok: bool,
    error: Option<&'static str>,
}

#[derive(Serialize)]
struct RoutingCheckResponse {
    valid: bool,
    checked: usize,
    passed: usize,
    failed: usize,
    results: Vec<RoutingCheckResult>,
}

fn check_all_profiles(
    store: &ProfileStore,
    route: &RouteManager,
    bot_token: &str,
    api_base: &str,
) -> RoutingCheckResponse {
    let mut results = Vec::new();
    for summary in store.list_for_available(&route.available_cores()) {
        let mut result = RoutingCheckResult {
            id: summary.id.clone(),
            name: summary.name.clone(),
            kind: summary.kind,
            transport: None,
            tls: None,
            ok: false,
            error: None,
        };

        let Some(url) = store.get_url(&summary.id) else {
            result.error = Some("profile could not be checked");
            results.push(result);
            continue;
        };

        match route.start_temporary(&url, store.selected_core()) {
            Ok((parsed, temporary)) => {
                result.transport = Some(parsed.transport);
                result.tls = Some(parsed.tls);
                let client = reqwest::blocking::Client::builder()
                    .timeout(Duration::from_secs(30))
                    .proxy(match reqwest::Proxy::all(temporary.proxy()) {
                        Ok(proxy) => proxy,
                        Err(_) => {
                            result.error = Some("profile could not be checked");
                            results.push(result);
                            continue;
                        }
                    })
                    .build();
                match client {
                    Ok(client) => {
                        result.ok = telegram::test_api_at(&client, bot_token, api_base).is_ok();
                        if !result.ok {
                            result.error = Some("Telegram API check failed");
                        }
                    }
                    Err(_) => result.error = Some("profile could not be checked"),
                }
                drop(temporary);
            }
            Err(_) => result.error = Some("profile could not be checked"),
        }
        results.push(result);
    }

    let checked = results.len();
    let passed = results.iter().filter(|result| result.ok).count();
    RoutingCheckResponse {
        valid: checked == passed,
        checked,
        passed,
        failed: checked - passed,
        results,
    }
}

async fn check_all_routing_profiles(
    req: HttpRequest,
    store: web::Data<Arc<ProfileStore>>,
    route: web::Data<Arc<RouteManager>>,
    bot_token: web::Data<String>,
) -> impl Responder {
    let store = store.get_ref().clone();
    let route = route.get_ref().clone();
    let token = bot_token.get_ref().clone();
    let result = tokio::task::spawn_blocking(move || {
        check_all_profiles(&store, &route, &token, telegram::API_BASE)
    })
    .await;
    match result {
        Ok(response) => {
            audit_action(&req, "routing_profiles_check_all", "routing", "ok");
            HttpResponse::Ok().json(response)
        }
        Err(_) => HttpResponse::InternalServerError()
            .json(serde_json::json!({"error": "routing checks could not be completed"})),
    }
}

async fn apply_routing_profile(
    req: HttpRequest,
    store: web::Data<Arc<ProfileStore>>,
    route: web::Data<Arc<RouteManager>>,
    client: web::Data<Arc<RwLock<Arc<Client>>>>,
    client_drop_tx: web::Data<Sender<Arc<Client>>>,
    path: web::Path<String>,
) -> impl Responder {
    let Some(_operation_gate) = try_acquire_route_gate() else {
        return HttpResponse::Conflict().json(serde_json::json!({
            "error": "Cannot apply routing while a backup is running. Try again when it finishes."
        }));
    };
    let id = path.into_inner();
    let Some(url) = store.get_url(&id) else {
        return HttpResponse::NotFound().json(serde_json::json!({"error": "profile not found"}));
    };
    let selected_core = store.selected_core();
    if !route.is_available(selected_core) {
        return HttpResponse::BadGateway().json(serde_json::json!({
            "error": format!(
                "The selected {} routing core is unavailable in this image.",
                selected_core.as_str()
            )
        }));
    }
    if !store
        .compatible_cores(&id)
        .is_some_and(|cores| cores.contains(&selected_core))
    {
        return HttpResponse::Conflict().json(serde_json::json!({
            "error": format!(
                "This profile is not compatible with the selected {} core.",
                selected_core.as_str()
            )
        }));
    }
    if route.test(&url).is_err() {
        return HttpResponse::BadRequest().json(serde_json::json!({
            "error": "The routing profile is invalid and could not be applied."
        }));
    }
    let proxy = match route.apply(&url, selected_core) {
        Ok(proxy) => proxy,
        Err(error) => {
            let message = if error.chain().any(|cause| {
                cause
                    .to_string()
                    .contains("did not open its local listener")
            }) {
                "The managed routing service did not start its local listener."
            } else if error
                .chain()
                .any(|cause| cause.to_string().contains("starting managed routing core"))
            {
                "The managed routing service is unavailable."
            } else {
                "The routing profile could not be applied."
            };
            return HttpResponse::BadGateway().json(serde_json::json!({"error": message}));
        }
    };
    let new_client = match tokio::task::spawn_blocking(move || {
        let proxy =
            reqwest::Proxy::all(&proxy).map_err(|_| "routing profile could not be applied")?;
        Client::builder()
            .timeout(Duration::from_secs(300))
            .proxy(proxy)
            .build()
            .map(Arc::new)
            .map_err(|_| "routing profile could not be applied")
    })
    .await
    {
        Ok(Ok(client)) => client,
        _ => {
            route.rollback();
            return HttpResponse::BadGateway()
                .json(serde_json::json!({"error": "routing profile could not be applied"}));
        }
    };
    let old_client = {
        let mut current = client.write().expect("Telegram client lock poisoned");
        std::mem::replace(&mut *current, new_client)
    };
    if store.set_active(&id).is_err() {
        let failed_client = {
            let mut current = client.write().expect("Telegram client lock poisoned");
            std::mem::replace(&mut *current, old_client)
        };
        let _ = client_drop_tx.send(failed_client);
        route.rollback();
        return HttpResponse::InternalServerError()
            .json(serde_json::json!({"error": "profile storage unavailable"}));
    }
    let _ = client_drop_tx.send(old_client);
    route.commit();
    audit_action(&req, "routing_profile_apply", &id, "ok");
    let _ = proxy;
    HttpResponse::Ok().json(
        store
            .list_for_available(&route.available_cores())
            .into_iter()
            .find(|profile| profile.id == id),
    )
}

async fn disable_routing(
    req: HttpRequest,
    store: web::Data<Arc<ProfileStore>>,
    route: web::Data<Arc<RouteManager>>,
    client: web::Data<Arc<RwLock<Arc<Client>>>>,
    client_drop_tx: web::Data<Sender<Arc<Client>>>,
    fallback_proxy: web::Data<Option<String>>,
) -> impl Responder {
    let Some(_operation_gate) = try_acquire_route_gate() else {
        return HttpResponse::Conflict().json(serde_json::json!({
            "error": "Cannot change routing while a backup is running. Try again when it finishes."
        }));
    };
    if store.active_id().is_none() && route.active_proxy().is_none() {
        return HttpResponse::Ok().json(serde_json::json!({
            "active": false,
            "message": "Routing is already disabled"
        }));
    }

    let fallback = fallback_proxy.get_ref().clone();
    let new_client = match tokio::task::spawn_blocking(move || {
        let mut builder = Client::builder().timeout(Duration::from_secs(300));
        if let Some(proxy) = fallback.as_deref() {
            builder = builder
                .proxy(reqwest::Proxy::all(proxy).map_err(|_| "proxy configuration failed")?);
        }
        builder
            .build()
            .map(Arc::new)
            .map_err(|_| "client configuration failed")
    })
    .await
    {
        Ok(Ok(client)) => client,
        _ => {
            return HttpResponse::BadGateway()
                .json(serde_json::json!({"error": "routing could not be disabled"}));
        }
    };

    if store.clear_active().is_err() {
        return HttpResponse::InternalServerError()
            .json(serde_json::json!({"error": "profile storage unavailable"}));
    }
    route.stop();
    let old_client = {
        let mut current = client.write().expect("Telegram client lock poisoned");
        std::mem::replace(&mut *current, new_client)
    };
    let _ = client_drop_tx.send(old_client);
    audit_action(&req, "routing_disable", "routing", "ok");
    HttpResponse::Ok().json(serde_json::json!({
        "active": false,
        "message": "Routing disabled"
    }))
}

async fn select_routing_profile(
    req: HttpRequest,
    store: web::Data<Arc<ProfileStore>>,
    route: web::Data<Arc<RouteManager>>,
    path: web::Path<String>,
) -> impl Responder {
    let id = path.into_inner();
    match store.set_active(&id) {
        Ok(true) => {
            audit_action(&req, "routing_profile_select", &id, "ok");
            HttpResponse::Ok().json(
                store
                    .list_for_available(&route.available_cores())
                    .into_iter()
                    .find(|profile| profile.id == id),
            )
        }
        Ok(false) => {
            HttpResponse::NotFound().json(serde_json::json!({"error": "profile not found"}))
        }
        Err(_) => HttpResponse::InternalServerError()
            .json(serde_json::json!({"error": "profile storage unavailable"})),
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

async fn serve_routing() -> impl Responder {
    HttpResponse::Ok()
        .content_type("text/html; charset=utf-8")
        .body(include_str!("../routing.html"))
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
/// - `/api/status/resources` — returns CPU, memory, and WORK_DIR disk usage
///
/// All status endpoints return JSON with `state`, `message`, and `timestamp` fields.
#[allow(clippy::too_many_arguments)]
pub async fn start_server(
    host: &str,
    port: u16,
    history: std::sync::Arc<HistoryStore>,
    admin_username: String,
    admin_password: String,
    operator_credentials: Option<(String, String)>,
    viewer_credentials: Option<(String, String)>,
    telegram_users: Arc<TelegramUserStore>,
    routing_profiles: Arc<ProfileStore>,
    route_manager: Arc<RouteManager>,
    telegram_client: Arc<RwLock<Arc<Client>>>,
    client_drop_tx: Sender<Arc<Client>>,
    telegram_bot_token: String,
    fallback_proxy: Option<String>,
    work_dir: std::path::PathBuf,
) -> std::io::Result<()> {
    // Share the port via actix-web `Data` so every handler can read it.
    let port_data = web::Data::new(port);
    let auth_data = web::Data::new(DashboardAuth::new(
        admin_username,
        admin_password,
        operator_credentials,
        viewer_credentials,
    ));
    let users_data = web::Data::new(telegram_users);
    let profiles_data = web::Data::new(routing_profiles);
    let route_data = web::Data::new(route_manager);
    let client_data = web::Data::new(telegram_client);
    let client_drop_data = web::Data::new(client_drop_tx);
    let bot_token_data = web::Data::new(telegram_bot_token);
    let fallback_proxy_data = web::Data::new(fallback_proxy);
    let resource_data = web::Data::new(ResourceCollector::new(work_dir));

    HttpServer::new(move || {
        App::new()
            .app_data(port_data.clone())
            .app_data(web::Data::new(history.clone()))
            .app_data(auth_data.clone())
            .app_data(users_data.clone())
            .app_data(profiles_data.clone())
            .app_data(route_data.clone())
            .app_data(client_data.clone())
            .app_data(client_drop_data.clone())
            .app_data(bot_token_data.clone())
            .app_data(fallback_proxy_data.clone())
            .app_data(resource_data.clone())
            .route("/healthz", web::get().to(healthz))
            .route("/api/auth/login", web::post().to(login))
            .route("/api/config", web::get().to(api_config))
            .route("/api/status/service", web::get().to(api_service_status))
            .route(
                "/api/status/service/test",
                web::post().to(test_telegram_api),
            )
            .route("/api/status/process", web::get().to(api_process_status))
            .route("/api/status/database/{name}", web::get().to(api_db_status))
            .route("/api/status/databases", web::get().to(api_databases_list))
            .route("/api/status/resources", web::get().to(api_resource_status))
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
            .route("/api/routing/profiles", web::get().to(api_routing_profiles))
            .route("/api/routing/status", web::get().to(api_routing_status))
            .route("/api/routing/core", web::post().to(select_routing_core))
            .route(
                "/api/routing/profiles",
                web::post().to(create_routing_profile),
            )
            .route(
                "/api/routing/profiles/{id}",
                web::put().to(update_routing_profile),
            )
            .route(
                "/api/routing/profiles/{id}",
                web::delete().to(delete_routing_profile),
            )
            .route(
                "/api/routing/profiles/{id}/test",
                web::post().to(test_routing_profile),
            )
            .route(
                "/api/routing/profiles/check-all",
                web::post().to(check_all_routing_profiles),
            )
            .route(
                "/api/routing/profiles/{id}/apply",
                web::post().to(apply_routing_profile),
            )
            .route("/api/routing/disable", web::post().to(disable_routing))
            .route(
                "/api/routing/profiles/{id}/select",
                web::post().to(select_routing_profile),
            )
            .route("/api/info", web::get().to(api_config))
            .route("/", web::get().to(serve_dashboard))
            .route("/index.html", web::get().to(serve_dashboard))
            .route("/users", web::get().to(serve_users))
            .route("/routing", web::get().to(serve_routing))
            .wrap(from_fn(dashboard_auth))
            .wrap(from_fn(security_headers))
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
    use actix_web_httpauth::middleware::HttpAuthentication;

    /// Read one entry's fields. Tests use distinct database names because
    /// `DUMP_STATUSES` is process-global and tests run in parallel.
    fn snapshot(name: &str) -> (u8, &'static str, u64, u64, f64, usize, u64, u64, usize) {
        let statuses = DUMP_STATUSES.read().expect("dump status lock poisoned");
        let s = statuses.get(name).expect("database not tracked");
        (
            s.code,
            s.stage,
            s.bytes_done,
            s.bytes_total,
            s.speed_bps,
            s.current_chunk,
            s.current_chunk_done,
            s.current_chunk_total,
            s.chunk_count,
        )
    }

    #[test]
    fn failure_keeps_stage_and_bytes_but_zeroes_speed() {
        set_db_status("test-fail", 1, "upload", "uploading");
        set_db_transfer("test-fail", 40, 100, 20.0);
        fail_db("test-fail", "connection reset");

        // The stage it died on drives the red node in the dashboard timeline,
        // and the byte counts show how far the upload got.
        assert_eq!(
            snapshot("test-fail"),
            (2, "upload", 40, 100, 0.0, 0, 0, 0, 0)
        );
    }

    #[test]
    fn advancing_stage_resets_transfer_counters() {
        set_db_status("test-reset", 1, "upload", "uploading");
        set_db_transfer("test-reset", 40, 100, 20.0);
        set_db_status("test-reset", 1, "dump", "dumping");

        assert_eq!(snapshot("test-reset"), (1, "dump", 0, 0, 0.0, 0, 0, 0, 0));
    }

    #[test]
    fn current_chunk_progress_is_published_and_reset_with_stage() {
        set_db_status("test-chunk-progress", 1, "upload", "uploading");
        set_db_transfer_with_chunk(
            "test-chunk-progress",
            49,
            100,
            12.5,
            ChunkProgress {
                number: 2,
                count: 3,
                done: 25,
                total: 50,
            },
        );

        assert_eq!(
            snapshot("test-chunk-progress"),
            (1, "upload", 49, 100, 12.5, 2, 25, 50, 3)
        );

        set_db_status("test-chunk-progress", 0, "done", "done");
        assert_eq!(
            snapshot("test-chunk-progress"),
            (0, "done", 0, 0, 0.0, 0, 0, 0, 0)
        );
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
        let request = ManualBackupRequest {
            database_name: "app".into(),
            chat_id: "-1".into(),
            recipient_name: "Alice".into(),
            no_encryption: true,
        };
        assert!(controller.request(request.clone()));
        assert!(!controller.request(request.clone()));
        assert_eq!(controller.take_pending(), vec![request]);
        assert!(controller.is_active("app"));
        assert!(!controller.request(ManualBackupRequest {
            database_name: "app".into(),
            chat_id: "-2".into(),
            recipient_name: "Bob".into(),
            no_encryption: false,
        }));
        controller.finish("app");
        assert!(controller.request(ManualBackupRequest {
            database_name: "app".into(),
            chat_id: "-2".into(),
            recipient_name: "Bob".into(),
            no_encryption: false,
        }));
    }

    #[test]
    fn route_gate_rejects_mutations_while_backup_owns_it() {
        let guard = acquire_backup_route_gate();
        assert!(try_acquire_route_gate().is_none());
        drop(guard);
        assert!(try_acquire_route_gate().is_some());
    }

    #[test]
    fn manual_controller_preserves_independent_database_options() {
        let controller = ManualBackupController::default();
        assert!(controller.request(ManualBackupRequest {
            database_name: "first".into(),
            chat_id: "-first".into(),
            recipient_name: "First".into(),
            no_encryption: true,
        }));
        assert!(controller.request(ManualBackupRequest {
            database_name: "second".into(),
            chat_id: "-second".into(),
            recipient_name: "Second".into(),
            no_encryption: false,
        }));

        let requests = controller.take_pending();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].chat_id, "-first");
        assert!(requests[0].no_encryption);
        assert_eq!(requests[1].chat_id, "-second");
        assert!(!requests[1].no_encryption);
        for request in requests {
            controller.finish(&request.database_name);
        }
    }

    #[test]
    fn manual_controller_cancels_queued_and_active_runs_idempotently() {
        let controller = ManualBackupController::default();
        let queued = ManualBackupRequest {
            database_name: "queued".into(),
            chat_id: "-queued".into(),
            recipient_name: "Queued".into(),
            no_encryption: false,
        };
        assert!(controller.request(queued));
        assert_eq!(controller.cancel("queued"), CancelResult::Queued);
        assert_eq!(controller.cancel("queued"), CancelResult::NotFound);
        assert!(controller.take_pending().is_empty());

        assert!(controller.request(ManualBackupRequest {
            database_name: "active".into(),
            chat_id: "-active".into(),
            recipient_name: "Active".into(),
            no_encryption: false,
        }));
        let request = controller.take_pending().pop().expect("active request");
        let token = controller
            .cancellation_token("active")
            .expect("active cancellation token");
        assert!(!token.load(Ordering::SeqCst));
        assert_eq!(controller.cancel("active"), CancelResult::Active);
        assert!(token.load(Ordering::SeqCst));
        assert_eq!(controller.cancel("active"), CancelResult::AlreadyCancelled);
        controller.finish(&request.database_name);
        assert_eq!(controller.cancel("active"), CancelResult::NotFound);
        assert!(controller.request(ManualBackupRequest {
            database_name: "active".into(),
            chat_id: "-again".into(),
            recipient_name: "Again".into(),
            no_encryption: false,
        }));
    }

    fn users_test_store() -> Arc<TelegramUserStore> {
        let path = std::env::temp_dir().join(format!(
            "crab-dashboard-users-{}-{}.toml",
            std::process::id(),
            now_epoch_secs()
        ));
        let _ = std::fs::remove_file(&path);
        Arc::new(TelegramUserStore::load(path).unwrap())
    }

    #[actix_web::test]
    async fn dashboard_shell_is_public_but_api_requires_session() {
        let app = aw_test::init_service(
            App::new()
                .app_data(web::Data::new(DashboardAuth::new(
                    "admin".into(),
                    "a-strong-test-password".into(),
                    None,
                    None,
                )))
                .app_data(web::Data::new(8080_u16))
                .route("/", web::get().to(serve_dashboard))
                .route("/api/config", web::get().to(api_config))
                .route("/api/auth/login", web::post().to(login))
                .wrap(from_fn(dashboard_auth)),
        )
        .await;

        let shell =
            aw_test::call_service(&app, aw_test::TestRequest::get().uri("/").to_request()).await;
        assert_eq!(shell.status(), 200);
        assert!(shell.headers().get(header::CONTENT_TYPE).is_some());

        let protected_api = aw_test::call_service(
            &app,
            aw_test::TestRequest::get().uri("/api/config").to_request(),
        )
        .await;
        assert_eq!(protected_api.status(), 401);
    }

    #[actix_web::test]
    async fn health_endpoint_is_public_without_active_route() {
        let app = aw_test::init_service(
            App::new()
                .app_data(web::Data::new(DashboardAuth::new(
                    "admin".into(),
                    "a-strong-test-password".into(),
                    None,
                    None,
                )))
                .app_data(web::Data::new(Arc::new(RouteManager::with_backend(
                    "/tmp/crab-routing-health-web",
                    "/bin/false",
                    crate::routing::RoutingBackend::SingBox,
                ))))
                .route("/healthz", web::get().to(healthz))
                .wrap(from_fn(dashboard_auth)),
        )
        .await;

        let response = aw_test::call_service(
            &app,
            aw_test::TestRequest::get().uri("/healthz").to_request(),
        )
        .await;
        assert_eq!(response.status(), 200);
    }

    #[actix_web::test]
    async fn viewer_sessions_cannot_mutate_and_csrf_is_required() {
        let auth = DashboardAuth::new(
            "admin".into(),
            "a-strong-admin-password".into(),
            Some(("operator".into(), "a-strong-operator-password".into())),
            Some(("viewer".into(), "a-strong-viewer-password".into())),
        );
        let app = aw_test::init_service(
            App::new()
                .app_data(web::Data::new(auth))
                .route("/api/auth/login", web::post().to(login))
                .route(
                    "/api/status/database/demo/disable",
                    web::post().to(api_process_status),
                )
                .wrap(from_fn(dashboard_auth)),
        )
        .await;

        let login_response = aw_test::call_service(
            &app,
            aw_test::TestRequest::post()
                .uri("/api/auth/login")
                .set_json(serde_json::json!({
                    "username": "viewer",
                    "password": "a-strong-viewer-password"
                }))
                .to_request(),
        )
        .await;
        assert_eq!(login_response.status(), 200);
        let cookies: Vec<Cookie<'static>> = login_response
            .response()
            .headers()
            .get_all(header::SET_COOKIE)
            .filter_map(|value| value.to_str().ok())
            .filter_map(|value| Cookie::parse(value.to_string()).ok())
            .map(|cookie| cookie.into_owned())
            .collect();
        let session = cookies
            .iter()
            .find(|cookie| cookie.name() == "crab_session")
            .unwrap();
        let csrf = cookies
            .iter()
            .find(|cookie| cookie.name() == "crab_csrf")
            .unwrap();

        let forbidden = aw_test::TestRequest::post()
            .uri("/api/status/database/demo/disable")
            .cookie(session.clone())
            .cookie(csrf.clone())
            .insert_header(("x-csrf-token", csrf.value()))
            .to_request();
        assert_eq!(aw_test::call_service(&app, forbidden).await.status(), 403);

        let missing_csrf = aw_test::TestRequest::post()
            .uri("/api/status/database/demo/disable")
            .cookie(session.clone())
            .cookie(csrf.clone())
            .to_request();
        assert_eq!(
            aw_test::call_service(&app, missing_csrf).await.status(),
            403
        );
    }

    #[actix_web::test]
    async fn telegram_users_require_auth_and_support_crud() {
        let store = users_test_store();
        let app = aw_test::init_service(
            App::new()
                .app_data(web::Data::new(DashboardAuth::new(
                    "admin".into(),
                    "a-strong-test-password".into(),
                    None,
                    None,
                )))
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
                .wrap(HttpAuthentication::basic(legacy_dashboard_auth)),
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
            .insert_header((
                header::AUTHORIZATION,
                "Basic YWRtaW46YS1zdHJvbmctdGVzdC1wYXNzd29yZA==",
            ))
            .set_json(serde_json::json!({
                "name": "Alice",
                "chat_id": "-1",
                "enabled": true
            }))
            .to_request();
        assert_eq!(aw_test::call_service(&app, create).await.status(), 201);

        let update = aw_test::TestRequest::put()
            .uri("/api/telegram-users/-1")
            .insert_header((
                header::AUTHORIZATION,
                "Basic YWRtaW46YS1zdHJvbmctdGVzdC1wYXNzd29yZA==",
            ))
            .set_json(serde_json::json!({
                "name": "Alice updated",
                "chat_id": "-1",
                "enabled": false
            }))
            .to_request();
        assert_eq!(aw_test::call_service(&app, update).await.status(), 200);

        let delete = aw_test::TestRequest::delete()
            .uri("/api/telegram-users/-1")
            .insert_header((
                header::AUTHORIZATION,
                "Basic YWRtaW46YS1zdHJvbmctdGVzdC1wYXNzd29yZA==",
            ))
            .to_request();
        assert_eq!(aw_test::call_service(&app, delete).await.status(), 204);

        let missing_delete = aw_test::TestRequest::delete()
            .uri("/api/telegram-users/-1")
            .insert_header((
                header::AUTHORIZATION,
                "Basic YWRtaW46YS1zdHJvbmctdGVzdC1wYXNzd29yZA==",
            ))
            .to_request();
        assert_eq!(
            aw_test::call_service(&app, missing_delete).await.status(),
            404
        );
    }

    #[actix_web::test]
    async fn manual_backup_validates_recipient_database_and_conflicts() {
        let name = format!("manual-api-{}", std::process::id());
        let state_dir =
            std::env::temp_dir().join(format!("crab-manual-state-{}", now_epoch_secs()));
        let state = Arc::new(DatabaseStateStore::load(
            &state_dir,
            std::slice::from_ref(&name),
        ));
        set_database_state_store(state);
        register_database(&name, true);
        set_manual_backup_available(true);

        let users = users_test_store();
        users
            .create(TelegramUser {
                name: "Enabled".into(),
                chat_id: "-enabled".into(),
                enabled: true,
            })
            .unwrap();
        users
            .create(TelegramUser {
                name: "Disabled".into(),
                chat_id: "-disabled".into(),
                enabled: false,
            })
            .unwrap();
        let history = Arc::new(HistoryStore::new(
            std::env::temp_dir().join(format!("crab-manual-history-{}", now_epoch_secs())),
            1,
        ));
        let app = aw_test::init_service(
            App::new()
                .app_data(web::Data::new(history))
                .app_data(web::Data::new(users))
                .route(
                    "/api/status/database/{name}/{action}",
                    web::post().to(api_database_action),
                ),
        )
        .await;

        let post = |database: &str, chat_id: &str| {
            aw_test::TestRequest::post()
                .uri(&format!("/api/status/database/{database}/backup"))
                .set_json(serde_json::json!({
                    "chat_id": chat_id,
                    "no_encryption": true
                }))
                .to_request()
        };

        assert_eq!(
            aw_test::call_service(&app, post(&name, "-missing"))
                .await
                .status(),
            409
        );
        assert_eq!(
            aw_test::call_service(&app, post(&name, "-disabled"))
                .await
                .status(),
            409
        );
        assert_eq!(
            aw_test::call_service(&app, post("unknown-database", "-enabled"))
                .await
                .status(),
            404
        );

        let accepted = aw_test::call_service(&app, post(&name, "-enabled")).await;
        assert_eq!(accepted.status(), 202);
        let duplicate = aw_test::call_service(&app, post(&name, "-enabled")).await;
        assert_eq!(duplicate.status(), 409);
        let body = aw_test::read_body(duplicate).await;
        assert!(!String::from_utf8_lossy(&body).contains("-enabled"));

        for request in manual_backup_controller().take_pending() {
            assert_eq!(request.database_name, name);
            assert_eq!(request.chat_id, "-enabled");
            assert_eq!(request.recipient_name, "Enabled");
            assert!(request.no_encryption);
            manual_backup_controller().finish(&request.database_name);
        }
        set_manual_backup_available(false);
        let _ = std::fs::remove_dir_all(state_dir);
    }

    #[actix_web::test]
    async fn history_api_paginates_and_rejects_invalid_page_sizes() {
        let name = format!("history-api-{}", std::process::id());
        let directory = std::env::temp_dir().join(format!("crab-history-api-{}", now_epoch_secs()));
        let history = Arc::new(HistoryStore::new(&directory, 12));
        for ordinal in 1..=25 {
            let timestamp = format!("2026-08-{ordinal:02}T00:00:00Z");
            history
                .append(&crate::history::HistoryRecord {
                    started_at: timestamp.clone(),
                    ended_at: timestamp,
                    database_index: 0,
                    database_name: name.clone(),
                    source: "scheduled".into(),
                    recipient: None,
                    status: "success".into(),
                    error: None,
                    dump_bytes: ordinal,
                    packaged_bytes: ordinal,
                    chunk_count: 1,
                    sha256: None,
                    encrypted: false,
                    duration_secs: 1.0,
                    upload_duration_secs: 0.0,
                    upload_attempts: 1,
                    upload_retries: 0,
                })
                .unwrap();
        }

        let app = aw_test::init_service(
            App::new()
                .app_data(web::Data::new(history))
                .route("/api/history/{database_name}", web::get().to(api_history)),
        )
        .await;

        let response = aw_test::call_service(
            &app,
            aw_test::TestRequest::get()
                .uri(&format!("/api/history/{name}"))
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), 200);
        let body: serde_json::Value = aw_test::read_body_json(response).await;
        assert_eq!(body["page"], 1);
        assert_eq!(body["page_size"], 10);
        assert_eq!(body["total_records"], 25);
        assert_eq!(body["total_pages"], 3);
        assert_eq!(body["records"].as_array().unwrap().len(), 10);
        assert_eq!(body["records"][0]["started_at"], "2026-08-25T00:00:00Z");
        assert_eq!(body["stats"]["attempts"], 25);

        for page_size in [10, 20, 50] {
            let response = aw_test::call_service(
                &app,
                aw_test::TestRequest::get()
                    .uri(&format!("/api/history/{name}?page=2&page_size={page_size}"))
                    .to_request(),
            )
            .await;
            assert_eq!(response.status(), 200);
            let body: serde_json::Value = aw_test::read_body_json(response).await;
            assert_eq!(body["page_size"], page_size);
            assert_eq!(body["stats"]["attempts"], 25);
        }

        let response = aw_test::call_service(
            &app,
            aw_test::TestRequest::get()
                .uri(&format!("/api/history/{name}?page_size=15"))
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), 400);
        let _ = std::fs::remove_dir_all(directory);
    }
}
