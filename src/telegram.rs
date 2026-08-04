//! Telegram Bot API upload (chunked `sendDocument`) with retries.

use std::path::Path;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use reqwest::blocking::{multipart, Client};
use serde::Deserialize;

const API_BASE: &str = "https://api.telegram.org";
/// Max attempts per chunk (1 initial + retries).
const MAX_ATTEMPTS: u32 = 5;

#[derive(Debug, Deserialize)]
struct ApiResponse {
    ok: bool,
    description: Option<String>,
    error_code: Option<i64>,
}

/// Upload a single file part to the configured chat.
///
/// Retries with exponential backoff on transient failures (network errors,
/// 429, 5xx). Returns the `document.file_id` Telegram assigned (useful for
/// logging / a manifest).
pub fn send_document(client: &Client, bot_token: &str, chat_id: &str, path: &Path) -> Result<()> {
    let url = format!("{API_BASE}/bot{bot_token}/sendDocument");
    let file_name = path
        .file_name()
        .and_then(|s| s.to_str())
        .ok_or_else(|| anyhow!("chunk path has no file name: {}", path.display()))?
        .to_string();

    let mut attempt = 0u32;
    loop {
        attempt += 1;
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
                    Err((status.as_u16(), body.error_code, body.description))
                }
            }
            Err(e) => Err((0, None, Some(e.to_string()))),
        };

        match outcome {
            Ok(()) => {
                tracing::info!(
                    attempt, file = %path.display(),
                    "uploaded chunk"
                );
                return Ok(());
            }
            Err((http_status, tg_code, desc)) => {
                let transient = is_transient(http_status, tg_code);
                let exhausted = attempt >= MAX_ATTEMPTS;
                tracing::warn!(
                    attempt,
                    max_attempts = MAX_ATTEMPTS,
                    http_status,
                    tg_code,
                    desc = desc.as_deref().unwrap_or(""),
                    transient,
                    "sendDocument failed"
                );
                if !transient || exhausted {
                    bail!(
                        "sendDocument for {} failed permanently after {attempt} attempt(s) \
                         (http={http_status}, tg_code={:?}, desc={})",
                        path.display(),
                        tg_code,
                        desc.as_deref().unwrap_or("?")
                    );
                }
                // Exponential backoff: 2^attempt seconds, capped at 32s.
                let secs = 1u64 << attempt.saturating_sub(1).min(5);
                std::thread::sleep(Duration::from_secs(secs));
            }
        }
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
