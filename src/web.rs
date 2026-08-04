//! HTTP status dashboard server.
//!
//! Serves `index.html` as a static page and provides API endpoints:
//! - `GET /api/config` — returns `{ "port": <port> }` so the JS can discover the port
//! - `GET /api/status/service` — Telegram API connection status
//! - `GET /api/status/process` — PostgreSQL dump status
//!
//! The dashboard polls these endpoints every 4 seconds and updates the UI.

use actix_web::{web, App, HttpResponse, HttpServer, Responder};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::time::SystemTime;

// Atomic status values (0=UP, 1=DEGRADED, 2=DOWN)
static TELEGRAM_STATUS: AtomicU8 = AtomicU8::new(0);
static DUMP_STATUS: AtomicU8 = AtomicU8::new(0);

/// Seconds since UNIX epoch when the server first received a request.
/// Set lazily on the first `api_config` call to avoid static initialization
/// with non-const expressions (which Rust forbids).
static START_EPOCH_SECS: AtomicU64 = AtomicU64::new(0);

/// Set the Telegram service status. 0=UP, 1=DEGRADED, 2=DOWN.
#[allow(dead_code)]
pub fn set_telegram_status(state: u8) {
    TELEGRAM_STATUS.store(state, Ordering::SeqCst);
}

/// Set the dump process status. 0=UP, 1=DEGRADED, 2=DOWN.
#[allow(dead_code)]
pub fn set_dump_status(state: u8) {
    DUMP_STATUS.store(state, Ordering::SeqCst);
}

/// Read the current Telegram service status.
pub fn get_telegram_status() -> u8 {
    TELEGRAM_STATUS.load(Ordering::SeqCst)
}

/// Read the current dump process status.
pub fn get_dump_status() -> u8 {
    DUMP_STATUS.load(Ordering::SeqCst)
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
    // Lazily store the start time on the first request.
    // Uses CAS loop to handle concurrent first-request safely.
    let now_secs = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("system time before UNIX epoch")
        .as_secs();
    let start = START_EPOCH_SECS.load(Ordering::Relaxed);
    if start == 0 {
        START_EPOCH_SECS.store(now_secs, Ordering::Relaxed);
    }

    let hostname = hostname();
    let uptime_seconds = now_secs.saturating_sub(start.max(now_secs));
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

/// Map atomic status to human-readable label and message.
fn status_entry(code: u8, _label: &str, message: &str) -> StatusResponse {
    let (state, msg) = match code {
        0 => ("UP", message),
        1 => ("DEGRADED", message),
        _ => ("DOWN", message),
    };
    StatusResponse {
        state,
        message: format!("[{}] {}", "now", msg),
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
    HttpResponse::Ok().json(status_entry(code, "Telegram API", &msg))
}

/// GET /api/status/process — returns PostgreSQL dump process status.
///
/// Returns a JSON object with:
/// - `state`: one of "UP", "DEGRADED", "DOWN"
/// - `message`: human-readable description
/// - `timestamp`: ISO 8601 timestamp of the status update
///
/// States:
/// - **UP**: No active dump or dump completed successfully.
/// - **DEGRADED**: Dump process is running or waiting for next backup.
/// - **DOWN**: Dump process failed or is in error state.
async fn api_process_status() -> impl Responder {
    let code = get_dump_status();
    let msg = match code {
        0 => "No active dump process or dump completed successfully".to_string(),
        1 => "Dump process is running or waiting for next backup".to_string(),
        _ => "Dump process failed or is in error state".to_string(),
    };
    HttpResponse::Ok().json(status_entry(code, "PostgreSQL Dump", &msg))
}

/// Resolve the local hostname; fall back to "unknown" on failure.
fn hostname() -> String {
    std::fs::read_to_string("/etc/hostname")
        .map(|s| s.trim().to_string())
        .or_else(|_| {
            std::env::var("HOSTNAME")
                .map(|s| s.trim().to_string())
        })
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
/// - `/api/status/process` — returns PostgreSQL dump process status
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
            .route("/api/info", web::get().to(api_config))
            .route("/", web::get().to(serve_dashboard))
            .route("/index.html", web::get().to(serve_dashboard))
    })
    .bind(format!("127.0.0.1:{port}"))?
    .run()
    .await
}
