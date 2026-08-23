//! Lightweight Telegram command bot using the application's shared client.

use anyhow::{Context, Result};
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time::Duration;

use crate::config::DatabaseConfig;
use crate::database_registry::DatabaseRegistry;
use crate::database_state::DatabaseStateStore;
use crate::telegram;
use crate::telegram_users::TelegramUserStore;
use crate::web::{self, ManualBackupRequest};

const MAX_ATTEMPTS: u32 = 5;
const POLL_TIMEOUT_SECS: u64 = 25;
const MAX_RETRY_AFTER_SECS: u64 = 300;

pub type ClientHandle = Arc<RwLock<Arc<Client>>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum BotState {
    Configured,
    Running,
    Stopped,
}

#[derive(Debug, Clone, Serialize)]
pub struct BotStatus {
    pub state: BotState,
    pub username: Option<String>,
    pub last_success_at: Option<String>,
    pub last_error: Option<String>,
    pub update_count: u64,
    pub last_update_id: Option<i64>,
}

impl Default for BotStatus {
    fn default() -> Self {
        Self {
            state: BotState::Configured,
            username: None,
            last_success_at: None,
            last_error: None,
            update_count: 0,
            last_update_id: None,
        }
    }
}

pub type BotStatusHandle = Arc<Mutex<BotStatus>>;

pub fn new_status() -> BotStatusHandle {
    Arc::new(Mutex::new(BotStatus::default()))
}

pub fn status_snapshot(status: &BotStatusHandle) -> BotStatus {
    status.lock().expect("bot status lock poisoned").clone()
}

pub fn safe_error(error: &str, token: &str) -> String {
    let value = if token.is_empty() {
        error.to_string()
    } else {
        error.replace(token, "[REDACTED]")
    }
    .replace('\n', " ");
    let value = value.trim();
    if value.is_empty() {
        "Telegram bot request failed".to_string()
    } else {
        value.chars().take(240).collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Help,
    Status,
    Backup(String),
}

pub fn parse_command(text: &str) -> Option<Command> {
    let mut words = text.split_whitespace();
    let command = words.next()?.strip_prefix('/')?;
    let command = command.split('@').next()?.to_ascii_lowercase();
    match command.as_str() {
        "help" if words.next().is_none() => Some(Command::Help),
        "status" if words.next().is_none() => Some(Command::Status),
        "backup" => {
            let database = words.next()?.to_string();
            (words.next().is_none() && !database.is_empty()).then_some(Command::Backup(database))
        }
        _ => None,
    }
}

pub fn help_message() -> &'static str {
    "<b>crab-dump commands</b>\n\n/help — list commands\n/status — application and Telegram status\n/backup &lt;database&gt; — queue a database backup"
}

#[derive(Debug, Clone, Serialize)]
struct InlineKeyboardMarkup {
    inline_keyboard: Vec<Vec<InlineButton>>,
}

#[derive(Debug, Clone, Serialize)]
struct InlineButton {
    text: String,
    callback_data: String,
}

fn action_markup() -> String {
    serde_json::to_string(&InlineKeyboardMarkup {
        inline_keyboard: vec![vec![
            InlineButton {
                text: "📊 Status".into(),
                callback_data: "bot:status".into(),
            },
            InlineButton {
                text: "💾 Backup".into(),
                callback_data: "bot:backup".into(),
            },
        ]],
    })
    .expect("static bot keyboard serializes")
}

fn database_markup(
    registry: &DatabaseRegistry,
    database_states: &DatabaseStateStore,
) -> Option<String> {
    let buttons = registry
        .config_snapshot()
        .into_iter()
        .map(|database| database.display_name())
        .filter(|name| database_states.is_enabled(name))
        .filter(|name| format!("bot:backup:{name}").len() <= 64)
        .map(|name| InlineButton {
            text: name.clone(),
            callback_data: format!("bot:backup:{name}"),
        })
        .collect::<Vec<_>>();
    (!buttons.is_empty()).then(|| {
        serde_json::to_string(&InlineKeyboardMarkup {
            inline_keyboard: buttons.chunks(2).map(|row| row.to_vec()).collect(),
        })
        .expect("database bot keyboard serializes")
    })
}

pub fn command_status_message(status: &BotStatusHandle) -> String {
    let snapshot = status_snapshot(status);
    let state = match snapshot.state {
        BotState::Configured => "configured",
        BotState::Running => "running",
        BotState::Stopped => "stopped",
    };
    let bot = snapshot
        .username
        .as_deref()
        .map(|name| format!("@{}", telegram::escape_html(name)))
        .unwrap_or_else(|| "unknown".to_string());
    let telegram_api = match web::get_telegram_status() {
        0 => "up",
        1 => "degraded",
        _ => "down",
    };
    let backup_mode = if web::manual_backup_available() {
        "scheduled"
    } else {
        "one-shot"
    };
    format!(
        "<b>crab-dump status</b>\n\nBot: <code>{bot}</code>\nState: <code>{state}</code>\nTelegram API: <code>{telegram_api}</code>\nBackup mode: <code>{backup_mode}</code>\nUpdates processed: <code>{}</code>",
        snapshot.update_count,
    )
}

#[derive(Debug, Deserialize)]
struct ApiEnvelope<T> {
    ok: bool,
    result: Option<T>,
    description: Option<String>,
    error_code: Option<i64>,
    parameters: Option<ApiParameters>,
}

#[derive(Debug, Deserialize)]
struct ApiParameters {
    retry_after: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct Me {
    username: Option<String>,
}

#[derive(Debug, Deserialize)]
struct Update {
    update_id: i64,
    message: Option<Message>,
    callback_query: Option<CallbackQuery>,
}

#[derive(Debug, Deserialize)]
struct Message {
    chat: Chat,
    text: Option<String>,
}

#[derive(Debug, Deserialize)]
struct Chat {
    id: i64,
}

#[derive(Debug, Deserialize)]
struct CallbackQuery {
    id: String,
    data: Option<String>,
    message: Option<Message>,
}

#[derive(Debug, Serialize)]
struct GetUpdatesQuery {
    offset: Option<i64>,
    timeout: u64,
    allowed_updates: &'static str,
}

pub struct BotRuntime {
    status: BotStatusHandle,
    stop: Arc<AtomicBool>,
}

impl BotRuntime {
    pub fn spawn(
        client: ClientHandle,
        token: String,
        users: Arc<TelegramUserStore>,
        registry: Arc<DatabaseRegistry>,
        database_states: Arc<DatabaseStateStore>,
    ) -> Arc<Self> {
        let status = new_status();
        let stop = Arc::new(AtomicBool::new(false));
        let runtime = Arc::new(Self {
            status: Arc::clone(&status),
            stop: Arc::clone(&stop),
        });
        let thread_status = Arc::clone(&status);
        thread::spawn(move || {
            run(
                client,
                token,
                users,
                registry,
                database_states,
                thread_status,
                stop,
            );
        });
        runtime
    }

    pub fn status(&self) -> BotStatusHandle {
        Arc::clone(&self.status)
    }
}

impl Drop for BotRuntime {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
    }
}

pub fn check(client: &Client, token: &str) -> Result<String> {
    let response: ApiEnvelope<Me> = client
        .get(format!("{}/bot{token}/getMe", telegram::API_BASE))
        .send()
        .context("checking Telegram bot")?
        .json()
        .context("parsing Telegram bot check response")?;
    if !response.ok {
        anyhow::bail!(
            "Telegram bot check failed (description={}, tg_code={:?})",
            response.description.as_deref().unwrap_or("unknown"),
            response.error_code
        );
    }
    response
        .result
        .and_then(|me| me.username)
        .ok_or_else(|| anyhow::anyhow!("Telegram bot did not return a username"))
}

fn run(
    client: ClientHandle,
    token: String,
    users: Arc<TelegramUserStore>,
    registry: Arc<DatabaseRegistry>,
    database_states: Arc<DatabaseStateStore>,
    status: BotStatusHandle,
    stop: Arc<AtomicBool>,
) {
    set_state(&status, BotState::Running);
    if let Ok(client) = client.read().map(|guard| guard.clone()) {
        match check(&client, &token) {
            Ok(username) => {
                let mut current = status.lock().expect("bot status lock poisoned");
                current.username = Some(username);
                current.last_success_at = Some(now());
                current.last_error = None;
            }
            Err(error) => set_error(&status, &safe_error(&error.to_string(), &token)),
        }
    }

    let mut offset = None;
    while !stop.load(Ordering::SeqCst) {
        let api_client = match client.read() {
            Ok(client) => client.clone(),
            Err(_) => {
                set_error(&status, "Telegram client lock poisoned");
                thread::sleep(Duration::from_secs(1));
                continue;
            }
        };
        let result = get_updates(&api_client, &token, offset);
        match result {
            Ok(updates) => {
                mark_success(&status);
                for update in updates {
                    offset = Some(update.update_id.saturating_add(1));
                    {
                        let mut current = status.lock().expect("bot status lock poisoned");
                        current.update_count = current.update_count.saturating_add(1);
                        current.last_update_id = Some(update.update_id);
                    }
                    if let Some(message) = update.message {
                        handle_message(
                            &api_client,
                            &token,
                            &users,
                            &registry,
                            &database_states,
                            &status,
                            message,
                        );
                    }
                    if let Some(callback) = update.callback_query {
                        handle_callback(
                            &api_client,
                            &token,
                            &users,
                            &registry,
                            &database_states,
                            &status,
                            callback,
                        );
                    }
                }
            }
            Err(error) => {
                set_error(&status, &safe_error(&error.to_string(), &token));
                thread::sleep(Duration::from_secs(1));
            }
        }
    }
    set_state(&status, BotState::Stopped);
}

fn get_updates(client: &Client, token: &str, offset: Option<i64>) -> Result<Vec<Update>> {
    let mut attempt = 0;
    loop {
        attempt += 1;
        let response = match client
            .get(format!("{}/bot{token}/getUpdates", telegram::API_BASE))
            .query(&GetUpdatesQuery {
                offset,
                timeout: POLL_TIMEOUT_SECS,
                allowed_updates: "[\"message\",\"callback_query\"]",
            })
            .send()
        {
            Ok(response) => response,
            Err(error) if attempt < MAX_ATTEMPTS => {
                thread::sleep(Duration::from_secs(retry_delay(attempt, None)));
                continue;
            }
            Err(error) => return Err(error).context("polling Telegram updates"),
        };
        let status = response.status();
        let body: ApiEnvelope<Vec<Update>> = response
            .json()
            .context("parsing Telegram updates response")?;
        if body.ok {
            return Ok(body.result.unwrap_or_default());
        }
        if attempt >= MAX_ATTEMPTS || !is_retryable(status.as_u16(), body.error_code) {
            anyhow::bail!(
                "Telegram polling failed (http={}, tg_code={:?}, description={})",
                status.as_u16(),
                body.error_code,
                body.description.as_deref().unwrap_or("unknown")
            );
        }
        thread::sleep(Duration::from_secs(retry_delay(
            attempt,
            body.parameters.and_then(|p| p.retry_after),
        )));
    }
}

fn handle_message(
    client: &Client,
    token: &str,
    users: &TelegramUserStore,
    registry: &DatabaseRegistry,
    database_states: &DatabaseStateStore,
    status: &BotStatusHandle,
    message: Message,
) {
    let chat_id = message.chat.id.to_string();
    let Some(user) = users
        .list()
        .into_iter()
        .find(|user| user.enabled && user.chat_id == chat_id)
    else {
        return;
    };
    let Some(command) = message.text.as_deref().and_then(parse_command) else {
        return;
    };
    let (response, markup) = match command {
        Command::Help => (help_message().to_string(), Some(action_markup())),
        Command::Status => (command_status_message(status), Some(action_markup())),
        Command::Backup(database) => (
            queue_backup(&chat_id, &user.name, &database, registry, database_states),
            Some(action_markup()),
        ),
    };
    if let Err(error) = telegram::send_message_with_markup(
        client,
        token,
        &chat_id,
        &response,
        markup.as_deref(),
        None,
    ) {
        set_error(status, &safe_error(&error.to_string(), token));
    }
}

fn handle_callback(
    client: &Client,
    token: &str,
    users: &TelegramUserStore,
    registry: &DatabaseRegistry,
    database_states: &DatabaseStateStore,
    status: &BotStatusHandle,
    callback: CallbackQuery,
) {
    let Some(message) = callback.message else {
        return;
    };
    let chat_id = message.chat.id.to_string();
    let Some(user) = users
        .list()
        .into_iter()
        .find(|user| user.enabled && user.chat_id == chat_id)
    else {
        return;
    };
    let _ = telegram::answer_callback_query(client, token, &callback.id);
    let data = callback.data.as_deref().unwrap_or_default();
    let (response, markup) = match data {
        "bot:status" => (command_status_message(status), Some(action_markup())),
        "bot:backup" => {
            let response = if web::manual_backup_available() {
                "Choose a database to back up:".to_string()
            } else {
                "Manual backups are unavailable in one-shot mode.".to_string()
            };
            (response, database_markup(registry, database_states))
        }
        database if database.starts_with("bot:backup:") => {
            let name = &database["bot:backup:".len()..];
            (
                queue_backup(&chat_id, &user.name, name, registry, database_states),
                Some(action_markup()),
            )
        }
        _ => return,
    };
    if let Err(error) = telegram::send_message_with_markup(
        client,
        token,
        &chat_id,
        &response,
        markup.as_deref(),
        None,
    ) {
        set_error(status, &safe_error(&error.to_string(), token));
    }
}

fn queue_backup(
    chat_id: &str,
    recipient_name: &str,
    database: &str,
    registry: &DatabaseRegistry,
    database_states: &DatabaseStateStore,
) -> String {
    if !web::manual_backup_available() {
        return "Manual backups are unavailable in one-shot mode.".to_string();
    }
    let known = registry
        .config_snapshot()
        .into_iter()
        .map(|db: DatabaseConfig| db.display_name())
        .any(|name| name == database);
    if !known {
        return format!(
            "Unknown database: <code>{}</code>",
            telegram::escape_html(database)
        );
    }
    if !database_states.is_enabled(database) {
        return "That database is disabled.".to_string();
    }
    if !web::manual_backup_controller().request(ManualBackupRequest {
        database_name: database.to_string(),
        chat_id: chat_id.to_string(),
        recipient_name: recipient_name.to_string(),
        no_encryption: false,
    }) {
        return "That database already has a backup queued or running.".to_string();
    }
    web::set_db_status(
        database,
        1,
        "queued",
        "Telegram command backup queued — waiting to start",
    );
    format!(
        "✅ Backup for <code>{}</code> queued.",
        telegram::escape_html(database)
    )
}

fn set_state(status: &BotStatusHandle, state: BotState) {
    status.lock().expect("bot status lock poisoned").state = state;
}

fn set_error(status: &BotStatusHandle, error: &str) {
    status.lock().expect("bot status lock poisoned").last_error = Some(error.to_string());
}

fn mark_success(status: &BotStatusHandle) {
    let mut status = status.lock().expect("bot status lock poisoned");
    status.last_success_at = Some(now());
    status.last_error = None;
}

fn now() -> String {
    chrono::Utc::now().to_rfc3339()
}

fn is_retryable(http_status: u16, telegram_code: Option<i64>) -> bool {
    http_status == 0
        || http_status == 429
        || (500..600).contains(&http_status)
        || telegram_code == Some(429)
        || telegram_code.is_some_and(|code| (500..600).contains(&code))
}

fn retry_delay(attempt: u32, retry_after: Option<u64>) -> u64 {
    retry_after
        .map(|seconds| seconds.min(MAX_RETRY_AFTER_SECS))
        .unwrap_or_else(|| 1u64 << attempt.saturating_sub(1).min(5))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_supported_commands_and_rejects_bad_arguments() {
        assert_eq!(parse_command("/help"), Some(Command::Help));
        assert_eq!(parse_command("/status@crab_dump"), Some(Command::Status));
        assert_eq!(
            parse_command("/backup analytics"),
            Some(Command::Backup("analytics".into()))
        );
        assert_eq!(parse_command("/backup"), None);
        assert_eq!(parse_command("/backup a b"), None);
        assert_eq!(
            parse_command("/backup unknown"),
            Some(Command::Backup("unknown".into()))
        );
    }

    #[test]
    fn retry_after_is_obeyed_and_bounded() {
        assert_eq!(retry_delay(1, Some(7)), 7);
        assert_eq!(retry_delay(1, Some(86_400)), MAX_RETRY_AFTER_SECS);
        assert_eq!(retry_delay(4, None), 8);
    }

    #[test]
    fn status_transitions_are_visible() {
        let status = new_status();
        assert_eq!(status_snapshot(&status).state, BotState::Configured);
        set_state(&status, BotState::Running);
        mark_success(&status);
        assert_eq!(status_snapshot(&status).state, BotState::Running);
        assert!(status_snapshot(&status).last_success_at.is_some());
        set_state(&status, BotState::Stopped);
        assert_eq!(status_snapshot(&status).state, BotState::Stopped);
    }

    #[test]
    fn html_and_errors_are_safe() {
        assert!(help_message().contains("&lt;database&gt;"));
        assert_eq!(
            safe_error("bad token secret", "secret"),
            "bad token [REDACTED]"
        );
        assert!(command_status_message(&new_status()).contains("crab-dump status"));
    }

    #[test]
    fn action_keyboard_contains_status_and_backup_buttons() {
        let markup: serde_json::Value =
            serde_json::from_str(&action_markup()).expect("valid inline keyboard JSON");
        let buttons = markup["inline_keyboard"][0]
            .as_array()
            .expect("keyboard row");
        assert_eq!(buttons[0]["callback_data"], "bot:status");
        assert_eq!(buttons[1]["callback_data"], "bot:backup");
    }
}
