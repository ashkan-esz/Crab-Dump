//! HTTP status dashboard server.
//!
//! Serves `index.html` as a static page and provides API endpoints:
//! - `GET /api/config` — returns `{ "port": <port> }` so the JS can discover the port
//! - `GET /api/status/service` — Telegram API connection status
//! - `GET /api/status/process` — aggregated PostgreSQL dump status (max across DBs)
//! - `GET /api/status/database/{name}` — per-database dump status
//! - `GET /api/status/databases` — all database statuses as a JSON array
//!
//! The dashboard polls these endpoints every 4 seconds and updates the UI.

use actix_web::{web, App, HttpResponse, HttpServer, Responder};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::{LazyLock, RwLock};
use std::time::SystemTime;

// ===========================================================================
// Global status atoms (Telegram service — single value)
// ===========================================================================

/// Atomic status values (0=UP, 1=DEGRADED, 2=DOWN).
static TELEGRAM_STATUS: AtomicU8 = AtomicU8::new(0);

/// Seconds since UNIX epoch when the server first received a request.
/// Set lazily on the first `api_config` call to avoid static initialization
/// with non-const expressions (which Rust forbids).
static START_EPOCH_SECS: AtomicU64 = AtomicU64::new(0);

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

// ===========================================================================
// Public API — Telegram service status
// ===========================================================================

/// Set the Telegram service status. 0=UP, 1=DEGRADED, 2=DOWN.
#[allow(dead_code)]
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
pub fn register_database(db_name: &str) {
    set_db_status(db_name, 0, "queued", "Queued — waiting to start");
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

/// Response shape for the `/api/config` endpoint (includes uptime and server details).
#[derive(Serialize, Deserialize)]
pub struct ConfigResponse {
    /// Port the dashboard is listening on.
    pub port: u16,
    /// Server uptime in seconds.
    pub uptime_seconds: u64,
    /// Hostname the server is running on.
    pub hostname: String,
}

/// GET /api/config — returns the current dashboard port.
///
/// Exposes server metadata (port, uptime, hostname) so the dashboard can
/// display a rich overview panel alongside the live status cards.
async fn api_config(cfg: web::Data<u16>) -> impl Responder {
    // Lazily store the start time on the first request. `compare_exchange`
    // makes a concurrent first request settle on a single start value.
    let now_secs = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("system time before UNIX epoch")
        .as_secs();
    let start = match START_EPOCH_SECS.compare_exchange(
        0,
        now_secs,
        Ordering::Relaxed,
        Ordering::Relaxed,
    ) {
        Ok(_) => now_secs,       // this request set the start time
        Err(existing) => existing, // another request already set it
    };

    let hostname = hostname();
    let uptime_seconds = now_secs.saturating_sub(start);
    HttpResponse::Ok().json(ConfigResponse {
        port: **cfg,
        uptime_seconds,
        hostname,
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

/// Start the HTTP server and block until it stops.
///
/// Serves:
/// - `/` — the dashboard HTML
/// - `/index.html` — same dashboard HTML
/// - `/api/config` — returns `{ "port": <port>, "uptime_seconds": ..., "hostname": ... }`
/// - `/api/status/service` — returns Telegram API connection status
/// - `/api/status/process` — returns aggregated PostgreSQL dump status
/// - `/api/status/database/{name}` — returns per-database dump status
/// - `/api/status/databases` — returns all tracked database statuses as an array
///
/// All status endpoints return JSON with `state`, `message`, and `timestamp` fields.
pub async fn start_server(port: u16) -> std::io::Result<()> {
    // Share the port via actix-web `Data` so every handler can read it.
    let port_data = web::Data::new(port);

    HttpServer::new(move || {
        App::new()
            .app_data(port_data.clone())
            .route("/api/config", web::get().to(api_config))
            .route("/api/status/service", web::get().to(api_service_status))
            .route("/api/status/process", web::get().to(api_process_status))
            .route("/api/status/database/{name}", web::get().to(api_db_status))
            .route("/api/status/databases", web::get().to(api_databases_list))
            .route("/api/info", web::get().to(api_config))
            .route("/", web::get().to(serve_dashboard))
            .route("/index.html", web::get().to(serve_dashboard))
    })
    .bind(format!("127.0.0.1:{port}"))?
    .run()
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn transfer_update_for_unknown_database_is_ignored() {
        set_db_transfer("test-absent", 1, 2, 3.0);
        let statuses = DUMP_STATUSES.read().expect("dump status lock poisoned");
        assert!(statuses.get("test-absent").is_none());
    }
}
