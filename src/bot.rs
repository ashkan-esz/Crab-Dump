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
use crate::resource_usage::ResourceCollector;
use crate::restore::{
    new_request_id, ManifestStore, RestoreController, RestoreMode, RestoreRequest, RestoreStatus,
};
use crate::telegram;
use crate::telegram_users::{TelegramUser, TelegramUserStore, SOURCE_TELEGRAM};
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
    AddMe,
    Help,
    Status,
    Backup(String),
    Restore,
}

pub fn parse_command(text: &str) -> Option<Command> {
    let mut words = text.split_whitespace();
    let command = words.next()?.strip_prefix('/')?;
    let mut command_parts = command.split('@');
    let command = command_parts.next()?;
    if command_parts.next().is_some_and(|bot| bot.is_empty()) || command_parts.next().is_some() {
        return None;
    }
    let command = command.to_ascii_lowercase();
    match command.as_str() {
        "add_me" | "add-me" if words.next().is_none() => Some(Command::AddMe),
        "help" if words.next().is_none() => Some(Command::Help),
        "status" if words.next().is_none() => Some(Command::Status),
        "backup" => {
            let database = words.next()?.to_string();
            (words.next().is_none() && !database.is_empty()).then_some(Command::Backup(database))
        }
        "restore" if words.next().is_none() => Some(Command::Restore),
        _ => None,
    }
}

pub fn help_message() -> &'static str {
    "<b>crab-dump commands</b>\n\n/add_me — register this Telegram account\n/help — list commands\n/status — application and Telegram status\n/backup &lt;database&gt; — queue a database backup\n/restore — request a restore from a restorable backup"
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
            InlineButton {
                text: "♻️ Restore".into(),
                callback_data: "bot:restore".into(),
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

pub fn command_status_message(status: &BotStatusHandle, resources: &ResourceCollector) -> String {
    let snapshot = web::runtime_status_snapshot(status, resources);
    let mut message = format!(
        "🦀 <b>crab-dump status</b>\n\
         {} <b>Overall:</b> {}\n\
         🕒 <b>Updated:</b> {}\n\n\
         <b>🤖 Telegram bot</b>\n\
         • Bot: <code>{}</code>\n\
         • State: <code>{}</code>\n\
         • API: {} {}\n\
         • Polling: {} {}\n\
         • Updates processed: <code>{}</code>\n",
        severity_emoji(snapshot.overall_code),
        severity_label(snapshot.overall_code),
        format_timestamp(&snapshot.updated),
        snapshot
            .bot
            .username
            .as_deref()
            .map(|name| format!("@{}", safe_html(name)))
            .unwrap_or_else(|| "unknown".into()),
        bot_state_label(snapshot.bot.state),
        severity_emoji(snapshot.telegram_code),
        severity_label(snapshot.telegram_code).to_ascii_lowercase(),
        if snapshot.bot.state == BotState::Running && snapshot.bot.last_error.is_none() {
            "🟢"
        } else {
            "🟡"
        },
        if snapshot.bot.state == BotState::Running && snapshot.bot.last_error.is_none() {
            "healthy"
        } else {
            "degraded"
        },
        snapshot.bot.update_count,
    );
    if let Some(last_success) = snapshot.bot.last_success_at.as_deref() {
        message.push_str(&format!(
            "• Last success: <code>{}</code>\n",
            format_timestamp(last_success)
        ));
    }
    if snapshot.overall_code > 0 {
        if let Some(error) = snapshot.bot.last_error.as_deref() {
            message.push_str(&format!("• Error: <code>{}</code>\n", safe_html(error)));
        }
    }

    let schedule = if snapshot.schedule_label.is_empty() {
        "one-shot".to_string()
    } else {
        "scheduled".to_string()
    };
    let schedule_label = if snapshot.schedule_label.is_empty() {
        "one-shot".to_string()
    } else {
        safe_html(&snapshot.schedule_label)
    };
    message.push_str(&format!(
        "\n<b>💾 Backups</b>\n\
         • Mode: <code>{schedule}</code>\n\
         • Schedule: <code>{}</code>\n\
         • Databases: <code>{} enabled / {} disabled</code>\n",
        schedule_label, snapshot.enabled_databases, snapshot.disabled_databases,
    ));
    if let Some(epoch) = snapshot
        .next_backup_run_epoch
        .filter(|_| !snapshot.cycle_running)
    {
        message.push_str(&format!(
            "• Next run: <code>{}</code>\n",
            format_epoch(epoch)
        ));
    }

    message.push_str("\n<b>🗄 Databases</b>\n");
    let database_start = message.len();
    for (index, database) in snapshot.databases.iter().enumerate() {
        let line = format_database_line(database);
        if message.len() + line.len() > 3200 {
            let remaining = snapshot.databases.len().saturating_sub(index);
            message.push_str(&format!("• <i>{remaining} more databases</i>\n"));
            break;
        }
        message.push_str(&line);
    }
    if message.len() == database_start {
        message.push_str("⚪ <i>No databases configured</i>\n");
    }

    message.push_str("\n<b>🖥 Resources</b>\n");
    message.push_str(&format_resource_line("CPU", &snapshot.resources.cpu));
    message.push_str(&format_resource_line("Memory", &snapshot.resources.memory));
    message.push_str(&format_resource_line("Disk", &snapshot.resources.disk));
    message.push_str("\n<i>Use the buttons below to refresh or start a backup.</i>");
    message
}

fn severity_emoji(code: u8) -> &'static str {
    match code {
        0 => "🟢",
        1 => "🟡",
        _ => "🔴",
    }
}

fn severity_label(code: u8) -> &'static str {
    match code {
        0 => "HEALTHY",
        1 => "DEGRADED",
        _ => "DOWN",
    }
}

fn bot_state_label(state: BotState) -> &'static str {
    match state {
        BotState::Configured => "configured",
        BotState::Running => "running",
        BotState::Stopped => "stopped",
    }
}

fn stage_label(stage: &str) -> &'static str {
    match stage {
        "queued" => "queued",
        "dump" => "dumping",
        "compression" => "compressing",
        "encryption" => "encrypting",
        "upload" => "uploading",
        "done" => "done",
        "cancelled" => "cancelled",
        "disabled" => "disabled",
        _ => "unknown",
    }
}

fn format_database_line(database: &web::DatabaseStatusSnapshot) -> String {
    let emoji = if !database.enabled {
        "⚪"
    } else {
        severity_emoji(database.code)
    };
    let mut detail = format!(
        "{} · {}",
        stage_label(database.stage),
        safe_html(&database.detail)
    );
    if database.stage == "upload" && database.bytes_total > 0 {
        let percent = database.bytes_done.saturating_mul(100) / database.bytes_total;
        detail = format!(
            "{detail} · {}/{} chunks · {percent}% ({} / {})",
            database.current_chunk,
            database.chunk_count,
            format_bytes(database.current_chunk_done),
            format_bytes(database.current_chunk_total)
        );
    }
    let updated = format_timestamp(&database.updated);
    format!(
        "{emoji} <code>{}</code> — {detail} · <i>{updated}</i>\n",
        safe_html(&database.name),
    )
}

fn format_resource_line(name: &str, metric: &crate::resource_usage::ResourceMetric) -> String {
    let value = match (metric.used_bytes, metric.total_bytes, metric.percent) {
        (Some(used), Some(total), Some(percent)) => {
            format!(
                "{} / {} · {:.0}%",
                format_bytes(used),
                format_bytes(total),
                percent
            )
        }
        (Some(used), Some(total), None) => {
            format!("{} / {}", format_bytes(used), format_bytes(total))
        }
        (None, None, Some(percent)) => format!("{percent:.0}%"),
        _ => "unavailable".into(),
    };
    format!("• {name}: <code>{value}</code>\n")
}

fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} {}", UNITS[unit])
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

fn format_timestamp(value: &str) -> String {
    chrono::DateTime::parse_from_rfc3339(value)
        .map(|date| {
            date.with_timezone(&chrono::Utc)
                .format("%Y-%m-%d %H:%M:%S UTC")
                .to_string()
        })
        .unwrap_or_else(|_| safe_html(value))
}

fn format_epoch(epoch: u64) -> String {
    chrono::DateTime::<chrono::Utc>::from_timestamp(epoch as i64, 0)
        .map(|date| date.format("%Y-%m-%d %H:%M:%S UTC").to_string())
        .unwrap_or_else(|| "unknown".into())
}

fn safe_html(value: &str) -> String {
    let value = value
        .split_whitespace()
        .map(|word| {
            if word.contains("://")
                || word.contains("password=")
                || word.contains("token=")
                || word.contains("chat_id=")
            {
                "[REDACTED]"
            } else {
                word
            }
        })
        .collect::<Vec<_>>()
        .join(" ");
    telegram::escape_html(&value)
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

#[derive(Debug, Serialize)]
struct BotCommand {
    command: &'static str,
    description: &'static str,
}

#[derive(Debug, Serialize)]
struct SetMyCommandsRequest {
    commands: Vec<BotCommand>,
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
    from: Option<MessageSender>,
    text: Option<String>,
}

#[derive(Debug, Deserialize)]
struct Chat {
    id: i64,
}

#[derive(Debug, Deserialize)]
struct MessageSender {
    first_name: Option<String>,
    last_name: Option<String>,
    username: Option<String>,
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
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        client: ClientHandle,
        token: String,
        users: Arc<TelegramUserStore>,
        registry: Arc<DatabaseRegistry>,
        database_states: Arc<DatabaseStateStore>,
        resources: Arc<ResourceCollector>,
        manifests: Arc<ManifestStore>,
        restores: Arc<RestoreController>,
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
                resources,
                manifests,
                restores,
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

fn set_my_commands(client: &Client, token: &str) -> Result<()> {
    let response: ApiEnvelope<bool> = client
        .post(format!("{}/bot{token}/setMyCommands", telegram::API_BASE))
        .json(&SetMyCommandsRequest {
            commands: vec![
                BotCommand {
                    command: "add_me",
                    description: "Register this Telegram account",
                },
                BotCommand {
                    command: "help",
                    description: "List available commands",
                },
                BotCommand {
                    command: "status",
                    description: "Show application and Telegram status",
                },
                BotCommand {
                    command: "backup",
                    description: "Queue a database backup",
                },
                BotCommand {
                    command: "restore",
                    description: "Request a backup restore",
                },
            ],
        })
        .send()
        .context("registering Telegram bot commands")?
        .json()
        .context("parsing Telegram bot commands response")?;
    if !response.ok {
        anyhow::bail!(
            "Telegram bot command registration failed (description={}, tg_code={:?})",
            response.description.as_deref().unwrap_or("unknown"),
            response.error_code
        );
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn run(
    client: ClientHandle,
    token: String,
    users: Arc<TelegramUserStore>,
    registry: Arc<DatabaseRegistry>,
    database_states: Arc<DatabaseStateStore>,
    resources: Arc<ResourceCollector>,
    manifests: Arc<ManifestStore>,
    restores: Arc<RestoreController>,
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
                drop(current);
                if let Err(error) = set_my_commands(&client, &token) {
                    set_error(&status, &safe_error(&error.to_string(), &token));
                }
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
                            &resources,
                            &manifests,
                            &restores,
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
                            &resources,
                            &manifests,
                            &restores,
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

#[allow(clippy::too_many_arguments)]
fn handle_message(
    client: &Client,
    token: &str,
    users: &TelegramUserStore,
    registry: &DatabaseRegistry,
    database_states: &DatabaseStateStore,
    resources: &ResourceCollector,
    manifests: &ManifestStore,
    _restores: &RestoreController,
    status: &BotStatusHandle,
    message: Message,
) {
    let chat_id = message.chat.id.to_string();
    let Some(command) = message.text.as_deref().and_then(parse_command) else {
        return;
    };
    if command == Command::AddMe {
        let response = if users.list().iter().any(|user| user.chat_id == chat_id) {
            "You are already registered.".to_string()
        } else {
            let user = TelegramUser {
                name: display_name(message.from.as_ref(), &chat_id),
                chat_id: chat_id.clone(),
                enabled: false,
                source: SOURCE_TELEGRAM.into(),
            };
            match users.create(user) {
                Ok(()) => {
                    "✅ You are registered. An administrator must enable your account before you can use bot commands.".to_string()
                }
                Err(error) if error.to_string().contains("already exists") => {
                    "You are already registered.".to_string()
                }
                Err(error) => {
                    set_error(status, &safe_error(&error.to_string(), token));
                    "Registration failed. Please try again later.".to_string()
                }
            }
        };
        if let Err(error) =
            telegram::send_message_with_markup(client, token, &chat_id, &response, None, None)
        {
            set_error(status, &safe_error(&error.to_string(), token));
        }
        return;
    }
    let Some(user) = users
        .list()
        .into_iter()
        .find(|user| user.enabled && user.chat_id == chat_id)
    else {
        return;
    };
    let (response, markup) = match command {
        Command::AddMe => unreachable!("handled before authorization"),
        Command::Help => (help_message().to_string(), Some(action_markup())),
        Command::Status => (
            command_status_message(status, resources),
            Some(action_markup()),
        ),
        Command::Backup(database) => (
            queue_backup(&chat_id, &user.name, &database, registry, database_states),
            Some(action_markup()),
        ),
        Command::Restore => restore_menu(manifests),
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

fn display_name(sender: Option<&MessageSender>, chat_id: &str) -> String {
    let Some(sender) = sender else {
        return chat_id.to_string();
    };
    let first = sender
        .first_name
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let last = sender
        .last_name
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    match (first, last) {
        (Some(first), Some(last)) => format!("{first} {last}"),
        (Some(first), None) => first.to_string(),
        (None, Some(last)) => last.to_string(),
        (None, None) => sender
            .username
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
            .unwrap_or_else(|| chat_id.to_string()),
    }
}

#[allow(clippy::too_many_arguments)]
fn handle_callback(
    client: &Client,
    token: &str,
    users: &TelegramUserStore,
    registry: &DatabaseRegistry,
    database_states: &DatabaseStateStore,
    resources: &ResourceCollector,
    manifests: &ManifestStore,
    restores: &RestoreController,
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
        "bot:status" => (
            command_status_message(status, resources),
            Some(action_markup()),
        ),
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
        "bot:restore" => restore_menu(manifests),
        backup if backup.starts_with("bot:restore:") => {
            let backup_id = &backup["bot:restore:".len()..];
            queue_restore(&chat_id, &user.name, backup_id, manifests, restores)
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

fn restore_menu(manifests: &ManifestStore) -> (String, Option<String>) {
    let backups = match manifests.restorable() {
        Ok(backups) => backups,
        Err(error) => {
            tracing::warn!(error = %error, "failed to list restorable backups");
            return (
                "Restore is temporarily unavailable.".into(),
                Some(action_markup()),
            );
        }
    };
    if backups.is_empty() {
        return (
            "No restorable backups are available.".into(),
            Some(action_markup()),
        );
    }
    let buttons = backups
        .iter()
        .take(20)
        .filter(|backup| format!("bot:restore:{}", backup.backup_id).len() <= 64)
        .map(|backup| InlineButton {
            text: format!("{} · {}", backup.database_name, backup.timestamp),
            callback_data: format!("bot:restore:{}", backup.backup_id),
        })
        .collect::<Vec<_>>();
    (
        "Choose a backup. The request will wait for dashboard approval.".into(),
        Some(
            serde_json::to_string(&InlineKeyboardMarkup {
                inline_keyboard: buttons.chunks(1).map(|row| row.to_vec()).collect(),
            })
            .expect("restore keyboard serializes"),
        ),
    )
}

fn queue_restore(
    chat_id: &str,
    user_name: &str,
    backup_id: &str,
    manifests: &ManifestStore,
    restores: &RestoreController,
) -> (String, Option<String>) {
    let backup = match manifests.find_restorable(backup_id) {
        Ok(backup) => backup,
        Err(_) => {
            return (
                "That backup is no longer restorable.".into(),
                Some(action_markup()),
            )
        }
    };
    let request = RestoreRequest {
        request_id: new_request_id(),
        backup_id: backup.backup_id.clone(),
        database_name: backup.database_name.clone(),
        requested_by: chat_id.to_string(),
        mode: RestoreMode::Safe,
        status: RestoreStatus::Queued,
        audit: vec![format!("requested by {user_name}")],
        error: None,
    };
    match restores.queue(request) {
        Ok(()) => (
            format!(
                "✅ Restore for <code>{}</code> queued in safe mode. An operator must approve it in the dashboard.",
                telegram::escape_html(&backup.database_name)
            ),
            Some(action_markup()),
        ),
        Err(_) => (
            "A restore is already queued or running for that database.".into(),
            Some(action_markup()),
        ),
    }
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
        assert_eq!(parse_command("/add_me"), Some(Command::AddMe));
        assert_eq!(parse_command("/add_me@crab_dump"), Some(Command::AddMe));
        assert_eq!(parse_command("/add-me"), Some(Command::AddMe));
        assert_eq!(parse_command("/add-me@crab_dump"), Some(Command::AddMe));
        assert_eq!(parse_command("/add_me@"), None);
        assert_eq!(parse_command("/add_me@a@b"), None);
        assert_eq!(parse_command("/add_me now"), None);
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
        let resources = ResourceCollector::new(std::env::temp_dir());
        assert!(command_status_message(&new_status(), &resources).contains("crab-dump status"));
    }

    #[test]
    fn database_status_formats_stages_progress_and_secrets_safely() {
        let database = web::DatabaseStatusSnapshot {
            name: "<production>".into(),
            enabled: true,
            code: 1,
            stage: "upload",
            detail: "uploading postgres://user:password@db.example/secret".into(),
            bytes_done: 64 * 1024 * 1024,
            bytes_total: 100 * 1024 * 1024,
            current_chunk: 2,
            current_chunk_done: 8 * 1024 * 1024,
            current_chunk_total: 49 * 1024 * 1024,
            chunk_count: 4,
            updated: "2026-08-23T21:42:10Z".into(),
        };
        let line = format_database_line(&database);
        assert!(line.contains("&lt;production&gt;"));
        assert!(line.contains("uploading"));
        assert!(line.contains("64%"));
        assert!(line.contains("2/4 chunks"));
        assert!(!line.contains("postgres://"));
        assert!(!line.contains("password@"));
    }

    #[test]
    fn resource_and_timestamp_formatting_handles_unavailable_values() {
        let unavailable = crate::resource_usage::ResourceMetric {
            percent: None,
            used_bytes: None,
            total_bytes: None,
        };
        assert!(format_resource_line("Disk", &unavailable).contains("unavailable"));
        let cpu = crate::resource_usage::ResourceMetric {
            percent: Some(18.0),
            used_bytes: None,
            total_bytes: None,
        };
        assert!(format_resource_line("CPU", &cpu).contains("18%"));
        assert_eq!(format_epoch(0), "1970-01-01 00:00:00 UTC");
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

    #[test]
    fn command_suggestions_publish_all_supported_commands() {
        let request = SetMyCommandsRequest {
            commands: vec![
                BotCommand {
                    command: "add_me",
                    description: "Register this Telegram account",
                },
                BotCommand {
                    command: "help",
                    description: "List available commands",
                },
                BotCommand {
                    command: "status",
                    description: "Show application and Telegram status",
                },
                BotCommand {
                    command: "backup",
                    description: "Queue a database backup",
                },
            ],
        };
        let payload: serde_json::Value =
            serde_json::to_value(request).expect("command registration serializes");
        let commands = payload["commands"].as_array().expect("commands array");
        assert_eq!(commands.len(), 4);
        assert_eq!(commands[0]["command"], "add_me");
        assert_eq!(commands[3]["command"], "backup");
    }

    #[test]
    fn derives_sender_display_name_in_priority_order() {
        let sender = MessageSender {
            first_name: Some("Ada".into()),
            last_name: Some("Lovelace".into()),
            username: Some("ada".into()),
        };
        assert_eq!(display_name(Some(&sender), "99"), "Ada Lovelace");

        let sender = MessageSender {
            first_name: None,
            last_name: None,
            username: Some("ada".into()),
        };
        assert_eq!(display_name(Some(&sender), "99"), "ada");
        assert_eq!(display_name(None, "99"), "99");
    }
}
