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
use std::sync::atomic::{AtomicU8, Ordering};

// Atomic status values (0=UP, 1=DEGRADED, 2=DOWN)
static TELEGRAM_STATUS: AtomicU8 = AtomicU8::new(0);
static DUMP_STATUS: AtomicU8 = AtomicU8::new(0);

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

/// Response shape for the `/api/config` endpoint.
#[derive(Serialize, Deserialize)]
pub struct ConfigResponse {
    /// Port the dashboard is listening on.
    pub port: u16,
}

/// GET /api/config — returns the current dashboard port.
async fn api_config(cfg: web::Data<u16>) -> impl Responder {
    HttpResponse::Ok().json(ConfigResponse { port: **cfg })
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
async fn api_process_status() -> impl Responder {
    let code = get_dump_status();
    let msg = match code {
        0 => "No active dump process or dump completed successfully".to_string(),
        1 => "Dump process is running or waiting for next backup".to_string(),
        _ => "Dump process failed or is in error state".to_string(),
    };
    HttpResponse::Ok().json(status_entry(code, "PostgreSQL Dump", &msg))
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
/// - `/api/config` — returns `{ "port": <port> }` so the JS can discover the port
/// - `/api/status/service` — Telegram status endpoint
/// - `/api/status/process` — dump process status endpoint
pub async fn start_server(port: u16) -> std::io::Result<()> {
    // Share the port via actix-web `Data` so every handler can read it.
    let port_data = web::Data::new(port);

    HttpServer::new(move || {
        App::new()
            .app_data(port_data.clone())
            .route("/api/config", web::get().to(api_config))
            .route("/api/status/service", web::get().to(api_service_status))
            .route("/api/status/process", web::get().to(api_process_status))
            .route("/", web::get().to(serve_dashboard))
            .route("/index.html", web::get().to(serve_dashboard))
    })
    .bind(format!("127.0.0.1:{port}"))?
    .run()
    .await
}
