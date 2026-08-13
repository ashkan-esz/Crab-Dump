//! Telegram Bot API upload (chunked `sendDocument`) with retries.

use std::path::Path;
use std::sync::Mutex;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use reqwest::blocking::{multipart, Client};
use serde::Deserialize;

use crate::web;

const API_BASE: &str = "https://api.telegram.org";
/// Max attempts per chunk (1 initial + retries).
const MAX_ATTEMPTS: u32 = 5;
/// Upper bound on a server-supplied `retry_after`, so a bad value can't park
/// the backup for hours.
const MAX_RETRY_AFTER_SECS: u64 = 300;

/// Serializes every upload in the process.
///
/// Telegram rate-limits per chat (~20 messages/minute), so concurrent uploads
/// buy no throughput — they only convert into 429s. Queueing here costs
/// nothing real and keeps the dump and packaging stages parallel.
static UPLOAD_LOCK: Mutex<()> = Mutex::new(());

#[derive(Debug, Clone, Copy, Default)]
pub struct UploadStats {
    pub attempts: u64,
    pub retries: u64,
}

pub fn format_backup_summary(
    database: &str,
    filename: &str,
    total_bytes: u64,
    parts: usize,
    encrypted: bool,
) -> String {
    format_backup_summary_with_label(
        "📦 <b>Manual backup ready</b>",
        database,
        filename,
        total_bytes,
        parts,
        encrypted,
    )
}

pub fn format_scheduled_backup_summary(
    database: &str,
    filename: &str,
    total_bytes: u64,
    parts: usize,
    encrypted: bool,
) -> String {
    format_backup_summary_with_label(
        "📦 <b>Scheduled backup ready</b>",
        database,
        filename,
        total_bytes,
        parts,
        encrypted,
    )
}

fn format_backup_summary_with_label(
    label: &str,
    database: &str,
    filename: &str,
    total_bytes: u64,
    parts: usize,
    encrypted: bool,
) -> String {
    let part_label = if parts == 1 { "part" } else { "parts" };
    let encryption = if encrypted { "age enabled" } else { "disabled" };
    let size_mb = total_bytes as f64 / (1024.0 * 1024.0);
    format!(
        "{label}\n\n\
         🗄️ <b>Database:</b> <code>{}</code>\n\
         📄 <b>File:</b> <code>{}</code>\n\
         📏 <b>Packaged size:</b> <code>{size_mb:.2} MB</code>\n\
         🧩 <b>Parts:</b> <code>{parts} {part_label}</code>\n\
         🗜️ <b>Compression:</b> <code>zstd</code>\n\
         🔐 <b>Encryption:</b> <code>{encryption}</code>",
        escape_html(database),
        escape_html(filename),
    )
}

pub fn format_backup_completion(database: &str, filename: &str, parts: usize) -> String {
    format_backup_completion_with_label(
        "✅ <b>Manual backup uploaded</b>",
        database,
        filename,
        parts,
    )
}

pub fn format_scheduled_backup_completion(database: &str, filename: &str, parts: usize) -> String {
    format_backup_completion_with_label(
        "✅ <b>Scheduled backup uploaded</b>",
        database,
        filename,
        parts,
    )
}

fn format_backup_completion_with_label(
    label: &str,
    database: &str,
    filename: &str,
    parts: usize,
) -> String {
    format!(
        "{label}\n\n\
         🗄️ <b>Database:</b> <code>{}</code>\n\
         📄 <b>File:</b> <code>{}</code>\n\
         🎉 All <code>{parts}</code> parts uploaded successfully.",
        escape_html(database),
        escape_html(filename),
    )
}

fn escape_html(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

#[derive(Debug, Deserialize)]
struct ApiResponse {
    ok: bool,
    description: Option<String>,
    error_code: Option<i64>,
    parameters: Option<ResponseParameters>,
}

/// Telegram's `responseParameters`; on 429 it carries how long to wait.
#[derive(Debug, Deserialize)]
struct ResponseParameters {
    retry_after: Option<u64>,
}

/// Upload a single file part to the configured chat.
///
/// Retries with exponential backoff on transient failures (network errors,
/// 429, 5xx), preferring Telegram's own `retry_after` hint when it sends one.
/// Uploads are serialized process-wide via [`UPLOAD_LOCK`].
pub fn send_document(
    client: &Client,
    bot_token: &str,
    chat_id: &str,
    path: &Path,
    stats: &mut UploadStats,
) -> Result<()> {
    // The guard protects no data, so a poisoned lock is safe to adopt.
    let _upload_guard = UPLOAD_LOCK.lock().unwrap_or_else(|e| e.into_inner());

    let url = format!("{API_BASE}/bot{bot_token}/sendDocument");
    let file_name = path
        .file_name()
        .and_then(|s| s.to_str())
        .ok_or_else(|| anyhow!("chunk path has no file name: {}", path.display()))?
        .to_string();

    let mut attempt = 0u32;
    loop {
        attempt += 1;
        stats.attempts += 1;
        let part = multipart::Part::file(path)
            .with_context(|| format!("opening chunk {}", path.display()))?
            .file_name(file_name.clone())
            .mime_str("application/octet-stream")
            .context("setting mime")?;
        let form = multipart::Form::new()
            .text("chat_id", chat_id.to_string())
            // Disable TG-side server preview to keep the chat clean.
            .text("disable_content_type_detection", "true")
            .part("document", part);

        let send_result = client
            .post(&url)
            .multipart(form)
            .send()
            .context("sending chunk to Telegram");

        let outcome = match send_result {
            Ok(resp) => {
                let status = resp.status();
                let body: ApiResponse = resp.json().context("parsing Telegram JSON response")?;
                if body.ok {
                    Ok(())
                } else {
                    Err((
                        status.as_u16(),
                        body.error_code,
                        body.description,
                        body.parameters.and_then(|p| p.retry_after),
                    ))
                }
            }
            Err(e) => Err((0, None, Some(e.to_string()), None)),
        };

        match outcome {
            Ok(()) => {
                tracing::info!(
                    attempt, file = %path.display(),
                    "uploaded chunk"
                );
                web::set_telegram_status(0);
                return Ok(());
            }
            Err((http_status, tg_code, desc, retry_after)) => {
                let transient = is_transient(http_status, tg_code);
                let exhausted = attempt >= MAX_ATTEMPTS;
                tracing::warn!(
                    attempt,
                    max_attempts = MAX_ATTEMPTS,
                    http_status,
                    tg_code,
                    retry_after,
                    desc = desc.as_deref().unwrap_or(""),
                    transient,
                    "sendDocument failed"
                );
                if !transient || exhausted {
                    web::set_telegram_status(2);
                    bail!(
                        "sendDocument for {} failed permanently after {attempt} attempt(s) \
                         (http={http_status}, tg_code={:?}, desc={})",
                        path.display(),
                        tg_code,
                        desc.as_deref().unwrap_or("?")
                    );
                }
                web::set_telegram_status(1);
                stats.retries += 1;
                std::thread::sleep(Duration::from_secs(backoff_secs(attempt, retry_after)));
            }
        }
    }
}

/// Send an informational text message to the configured chat.
///
/// Messages use the same process-wide upload lock and retry policy as
/// document uploads. Callers intentionally decide whether a notice failure is
/// fatal; dashboard notices are best-effort.
pub fn send_message(client: &Client, bot_token: &str, chat_id: &str, text: &str) -> Result<()> {
    let _upload_guard = UPLOAD_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let url = format!("{API_BASE}/bot{bot_token}/sendMessage");
    let mut attempt = 0u32;

    loop {
        attempt += 1;
        let send_result = client
            .post(&url)
            .form(&[("chat_id", chat_id), ("text", text), ("parse_mode", "HTML")])
            .send()
            .context("sending Telegram message");

        let outcome = match send_result {
            Ok(resp) => {
                let status = resp.status();
                let body: ApiResponse = resp.json().context("parsing Telegram JSON response")?;
                if body.ok {
                    Ok(())
                } else {
                    Err((
                        status.as_u16(),
                        body.error_code,
                        body.parameters.and_then(|p| p.retry_after),
                    ))
                }
            }
            Err(_) => Err((0, None, None)),
        };

        match outcome {
            Ok(()) => {
                web::set_telegram_status(0);
                return Ok(());
            }
            Err((http_status, tg_code, retry_after)) => {
                let transient = is_transient(http_status, tg_code);
                let exhausted = attempt >= MAX_ATTEMPTS;
                tracing::warn!(
                    attempt,
                    max_attempts = MAX_ATTEMPTS,
                    http_status,
                    tg_code,
                    retry_after,
                    transient,
                    "sendMessage failed"
                );
                if !transient || exhausted {
                    web::set_telegram_status(2);
                    bail!(
                        "sendMessage failed permanently after {attempt} attempt(s) \
                         (http={http_status}, tg_code={tg_code:?})"
                    );
                }
                web::set_telegram_status(1);
                std::thread::sleep(Duration::from_secs(backoff_secs(attempt, retry_after)));
            }
        }
    }
}

/// How long to wait before retrying attempt `attempt`.
///
/// Telegram knows how long the limit actually runs, so its `retry_after` hint
/// beats a guess — clamped by [`MAX_RETRY_AFTER_SECS`] so a bad value can't
/// park the backup for hours. Without a hint, back off exponentially: 1, 2, 4,
/// 8s over the four sleeps `MAX_ATTEMPTS` allows.
fn backoff_secs(attempt: u32, retry_after: Option<u64>) -> u64 {
    match retry_after {
        Some(n) => n.min(MAX_RETRY_AFTER_SECS),
        None => 1u64 << attempt.saturating_sub(1).min(5),
    }
}

/// A failure is worth retrying if it's a network error or a server-side /
/// rate-limit Telegram response.
fn is_transient(http_status: u16, tg_code: Option<i64>) -> bool {
    if http_status == 0 {
        return true; // transport-level error
    }
    if http_status == 429 || (500..600).contains(&http_status) {
        return true;
    }
    // Telegram-specific retryable codes: 429 (rate limit) and 5xx-style
    // server codes as reported in the JSON body.
    if let Some(c) = tg_code {
        if c == 429 || (500..600).contains(&c) {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    /// D8: Telegram's own hint beats the computed backoff, but a hostile or
    /// buggy value must not park the backup for hours.
    #[test]
    fn retry_after_hint_wins_and_is_clamped() {
        assert_eq!(backoff_secs(1, Some(45)), 45);
        assert_eq!(
            backoff_secs(4, Some(7)),
            7,
            "hint wins over a larger backoff"
        );
        assert_eq!(backoff_secs(1, Some(86_400)), MAX_RETRY_AFTER_SECS);
    }

    #[test]
    fn backoff_doubles_without_a_hint() {
        let sleeps: Vec<u64> = (1..MAX_ATTEMPTS).map(|a| backoff_secs(a, None)).collect();
        assert_eq!(sleeps, vec![1, 2, 4, 8]);
    }

    #[test]
    fn transient_covers_rate_limits_and_transport_errors() {
        assert!(is_transient(0, None), "transport error");
        assert!(is_transient(429, None));
        assert!(is_transient(503, None));
        assert!(is_transient(200, Some(429)), "code in the JSON body");
        assert!(!is_transient(400, None), "bad request is permanent");
        assert!(!is_transient(401, None), "bad token is permanent");
    }

    #[test]
    fn backup_summary_formats_encrypted_multipart_notice() {
        let message = format_backup_summary(
            "analytics",
            "analytics_20260812T031500Z.sql.zst.age",
            100_000_000,
            3,
            true,
        );
        assert!(message.contains("analytics"));
        assert!(message.contains("analytics_20260812T031500Z.sql.zst.age"));
        assert!(message.contains("95.37 MB"));
        assert!(message.contains("3 parts"));
        assert!(message.contains("zstd"));
        assert!(message.contains("age"));
    }

    #[test]
    fn backup_summary_formats_unencrypted_singlepart_notice() {
        let message =
            format_backup_summary("orders", "orders_20260812T031500Z.sql.zst", 42, 1, false);
        assert!(message.contains("orders"));
        assert!(message.contains("0.00 MB"));
        assert!(message.contains("1 part"));
        assert!(message.contains("zstd"));
        assert!(message.contains("disabled"));
        assert!(message.contains("Encryption:</b> <code>disabled"));
    }

    #[test]
    fn backup_completion_mentions_all_uploaded_parts() {
        let message =
            format_backup_completion("analytics", "analytics_20260812T031500Z.sql.zst.age", 3);
        assert!(message.contains("analytics"));
        assert!(message.contains("All <code>3</code> parts"));
        assert!(message.contains("uploaded"));
    }

    #[test]
    fn scheduled_messages_are_distinguishable_and_include_multipart_details() {
        let summary = format_scheduled_backup_summary(
            "analytics",
            "analytics_20260812T031500Z.sql.zst.age",
            100_000_000,
            3,
            true,
        );
        assert!(summary.contains("Scheduled backup ready"));
        assert!(summary.contains("analytics"));
        assert!(summary.contains("95.37 MB"));
        assert!(summary.contains("3 parts"));
        assert!(summary.contains("zstd"));
        assert!(summary.contains("age"));

        let completion = format_scheduled_backup_completion(
            "analytics",
            "analytics_20260812T031500Z.sql.zst.age",
            3,
        );
        assert!(completion.contains("Scheduled backup uploaded"));
        assert!(completion.contains("All <code>3</code> parts"));
    }
}
