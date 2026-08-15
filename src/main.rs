//! crab-dump: stream compressed, optionally encrypted Postgres dumps to Telegram.
//!
//! Multi-database aware: spawns independent pipelines per configured server,
//! each running `pg_dump → zstd → age? → chunk → upload`, at most
//! `MAX_PARALLEL_DATABASES` of them at a time. A database that fails is
//! reported and skipped — never fatal to its peers.
//!
//! Runs once and exits by default (for cron / systemd timers). Set
//! `BACKUP_INTERVAL` to keep the process alive and repeat the cycle itself.

use std::io::{BufReader, Read, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex, RwLock};
use std::time::SystemTime;

use anyhow::{anyhow, bail, Context, Result};
use chrono::NaiveDateTime;
use clap::Parser;
use reqwest::blocking::Client;

mod chunk;
mod compress;
mod config;
mod cron;
mod database_state;
mod dump;
mod encrypt;
mod history;
mod resource_usage;
mod routing;
mod telegram;
mod telegram_users;
mod web;

use chunk::ChunkWriter;
use config::{Config, DatabaseConfig, Schedule, SharedConfig, DATA_DIR};
use database_state::DatabaseStateStore;
use history::{HistoryRecord, HistoryStore};
use routing::{ProfileStore, RouteManager, DEFAULT_SHOES_PATH, DEFAULT_SING_BOX_PATH};

type ClientHandle = Arc<RwLock<Arc<Client>>>;

/// Load `.env` / `.env.local` before config resolution so environment-based
/// config loading picks up credentials without manual export.
fn load_env() {
    // `dotenv` returns Err if no file is found — we treat that as success
    // because env vars can also be set directly.
    if let Err(e) = dotenv::from_path(".env.local") {
        // .env.local is optional — don't warn if missing.
        tracing::debug!(error = %e, ".env.local not found, skipping");
    } else {
        tracing::info!("loaded .env.local");
    }
    // .env is the primary dotenv file — dotenv::dotenv() loads .env from CWD.
    if let Err(e) = dotenv::dotenv() {
        tracing::warn!(error = %e, ".env not found; continuing with env vars only");
    } else {
        tracing::info!("loaded .env");
    }
}

/// CLI argument parser.
#[derive(Debug, Parser)]
struct Cli {
    /// Validate config and check pg_dump availability without dumping or uploading.
    #[arg(long)]
    dry_run: bool,

    /// Disable age encryption for this run, even if AGE_RECIPIENT is set.
    #[arg(long)]
    no_encryption: bool,
}

fn main() -> Result<()> {
    // ── Logging setup ───────────────────────────────────────────────────────
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .try_init();

    let cli = Cli::parse();
    load_env();

    // ── Resolve configuration ───────────────────────────────────────────────
    let (shared_cfg, databases) =
        Config::resolve_databases().context("loading configuration from env")?;
    let names = databases
        .iter()
        .map(DatabaseConfig::display_name)
        .collect::<Vec<_>>();
    let data_dir = PathBuf::from(DATA_DIR);
    let database_states = Arc::new(DatabaseStateStore::load(&data_dir, &names));
    web::set_database_state_store(Arc::clone(&database_states));
    let telegram_users = Arc::new(
        telegram_users::TelegramUserStore::load(data_dir.join("telegram_users.toml"))
            .context("loading Telegram users directory")?,
    );
    let routing_profiles =
        Arc::new(ProfileStore::load(&data_dir).context("loading routing profiles")?);
    let route_manager = Arc::new(RouteManager::with_paths(
        shared_cfg.work_dir.join("routing"),
        std::env::var("SING_BOX_PATH").unwrap_or_else(|_| DEFAULT_SING_BOX_PATH.to_string()),
        std::env::var("SHOES_PATH").unwrap_or_else(|_| DEFAULT_SHOES_PATH.to_string()),
    ));
    let routing_backend = routing_profiles.selected_core();
    if let Some(active_url) = routing_profiles.active_url() {
        if route_manager.is_available(routing_backend) && shared_cfg.socks_proxy.is_some() {
            anyhow::bail!(
                "SOCKS_PROXY cannot be combined with an active dashboard routing profile"
            );
        }
        if !route_manager.is_available(routing_backend) {
            tracing::warn!(
                core = routing_backend.as_str(),
                "active dashboard routing profile is persisted but its routing core is unavailable; routing starts disabled"
            );
        } else if !cli.dry_run {
            if let Err(error) = route_manager.apply(&active_url, routing_backend) {
                let executable_missing = error.chain().any(|cause| {
                    cause
                        .downcast_ref::<std::io::Error>()
                        .is_some_and(|io| io.kind() == std::io::ErrorKind::NotFound)
                });
                if executable_missing {
                    tracing::warn!(
                        core = routing_backend.as_str(),
                        "active dashboard routing profile could not find its routing executable; routing starts disabled"
                    );
                } else {
                    return Err(error).context("starting active dashboard routing profile");
                }
            }
        }
    }
    let route_proxy = route_manager.active_proxy();
    let telegram_client: ClientHandle = Arc::new(RwLock::new(Arc::new(
        build_http_client_for_proxy(route_proxy.as_deref().or(shared_cfg.socks_proxy.as_deref()))?,
    )));
    let (client_drop_tx, client_drop_rx) = mpsc::channel::<Arc<Client>>();
    std::thread::spawn(move || {
        while let Ok(client) = client_drop_rx.recv() {
            drop(client);
        }
    });

    // ── Spawn status dashboard (unchanged logic) ────────────────────────────
    // The dashboard runs in a dedicated thread with its own tokio runtime
    // because actix_web::HttpServer is not Send.
    let dashboard_port = shared_cfg.api_port;
    let dashboard_host = shared_cfg.dashboard_host.clone();
    let dashboard_username = shared_cfg.dashboard_username.clone();
    let dashboard_password = shared_cfg.dashboard_password.clone();
    let dashboard_operator = shared_cfg
        .dashboard_operator_username
        .clone()
        .zip(shared_cfg.dashboard_operator_password.clone());
    let dashboard_viewer = shared_cfg
        .dashboard_viewer_username
        .clone()
        .zip(shared_cfg.dashboard_viewer_password.clone());
    let dashboard_users = Arc::clone(&telegram_users);
    let dashboard_profiles = Arc::clone(&routing_profiles);
    let dashboard_route = Arc::clone(&route_manager);
    let dashboard_client = Arc::clone(&telegram_client);
    let dashboard_history = std::sync::Arc::clone(&shared_cfg.history);
    let dashboard_bot_token = shared_cfg.tg_bot_token.clone();
    let dashboard_fallback_proxy = shared_cfg.socks_proxy.clone();
    let dashboard_work_dir = shared_cfg.work_dir.clone();
    web::set_manual_backup_available(shared_cfg.backup_schedule.is_some());
    web::set_max_parallel_databases(shared_cfg.max_parallel_databases);
    web::set_telegram_chat_count(shared_cfg.tg_chat_ids.len());
    tracing::info!(port = dashboard_port, "spawning status dashboard server");
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("create tokio runtime for web server");
        rt.block_on(async move {
            if let Err(e) = web::start_server(
                &dashboard_host,
                dashboard_port,
                dashboard_history,
                dashboard_username,
                dashboard_password,
                dashboard_operator,
                dashboard_viewer,
                dashboard_users,
                dashboard_profiles,
                dashboard_route,
                dashboard_client,
                client_drop_tx,
                dashboard_bot_token,
                dashboard_fallback_proxy,
                dashboard_work_dir,
            )
            .await
            {
                eprintln!("web server error: {e}");
            }
        });
    });

    // ── Verify pg_dump availability ─────────────────────────────────────────
    if !config::pg_dump_available() {
        anyhow::bail!("`pg_dump` was not found on PATH; install the postgres client tools");
    }

    // ── Log resolved configuration ──────────────────────────────────────────
    tracing::info!(
        count = databases.len(),
        work_dir = %shared_cfg.work_dir.display(),
        history_dir = %shared_cfg.history.directory_display(),
        chunk_mb = shared_cfg.chunk_size_mb,
        max_parallel = shared_cfg.max_parallel_databases,
        "configuration resolved",
    );

    for (i, db) in databases.iter().enumerate() {
        // Register up front so the dashboard lists every configured database
        // as "queued" before any pipeline starts.
        web::register_database(
            &db.display_name(),
            database_states.is_enabled(&db.display_name()),
        );
        tracing::info!(
            db_index = i,
            db_name = %db.display_name(),
            "loaded database configuration",
        );
    }

    // ── Dry-run mode ────────────────────────────────────────────────────────
    // Config validation only: everything above already ran (config resolution,
    // pg_dump lookup), so reaching here means the config is good. Exit rather
    // than idling — `docker compose run --rm crab-dump --dry-run` must return.
    if cli.dry_run {
        let suffix = if databases.len() == 1 { "" } else { "s" };
        println!(
            "dry-run OK — {} database{} configured",
            databases.len(),
            suffix,
        );
        return Ok(());
    }

    // ── Scheduled mode ──────────────────────────────────────────────────────
    // With BACKUP_INTERVAL set the process stays alive and repeats the cycle
    // itself, so no external cron/systemd timer is needed (and the dashboard
    // keeps serving between runs). Failures never end the loop — a database
    // that is down now may well be up at the next cycle.
    if let Some(schedule) = &shared_cfg.backup_schedule {
        if let Some(history_schedule) = &shared_cfg.history_upload_schedule {
            web::set_history_schedule_label(schedule_label(history_schedule));
            spawn_history_upload_worker(
                &shared_cfg,
                history_schedule,
                Arc::clone(&telegram_client),
            );
        }
        run_scheduled(
            &shared_cfg,
            &databases,
            cli.no_encryption,
            schedule,
            &telegram_client,
        );
        // `run_scheduled` only returns if the loop is ever made to terminate;
        // today it runs until the process is signalled.
        return Ok(());
    }

    // ── One-shot mode ───────────────────────────────────────────────────────
    let _operation_gate = web::acquire_backup_route_gate();
    web::set_cycle_running(true);
    let (results, failures) = run_cycle(
        &shared_cfg,
        &databases,
        cli.no_encryption,
        &telegram_client,
        BackupSource::OneShot,
    );
    web::set_cycle_running(false);

    // A partial run must not look like a clean one to cron/systemd, but the
    // databases that did work are already uploaded — the exit code is the only
    // thing left to report.
    if !failures.is_empty() && results.is_empty() {
        anyhow::bail!(
            "all {} database backups failed (see the manifest above)",
            failures.len(),
        );
    }
    if !failures.is_empty() {
        // Partial failure: non-zero so a timer's failure handling still fires,
        // after every other database has been backed up and reported.
        anyhow::bail!(
            "{} of {} database backups failed (see the manifest above)",
            failures.len(),
            results.len() + failures.len(),
        );
    }
    Ok(())
}

/// Start the independent history uploader alongside the backup scheduler.
///
/// The worker has its own wall-clock loop, but shares the process-wide
/// Telegram upload lock used by database backups. It intentionally is not
/// started in one-shot mode.
fn spawn_history_upload_worker(cfg: &SharedConfig, schedule: &Schedule, client: ClientHandle) {
    let Schedule::Cron(cron) = schedule else {
        tracing::error!("history upload schedule must be a five-field cron expression");
        return;
    };
    let history = Arc::clone(&cfg.history);
    let work_dir = cfg.work_dir.clone();
    let chunk_size = cfg.chunk_size_bytes();
    let bot_token = cfg.tg_bot_token.clone();
    let chat_ids = cfg.tg_chat_ids.clone();
    let cron = cron.clone();
    std::thread::spawn(move || {
        tracing::info!(cron = %cron, "history upload worker started");
        for occurrence in 1u64.. {
            sleep_until_cron(
                &cron,
                occurrence,
                "history upload",
                web::set_next_history_run_in,
                &web::ManualBackupController::default(),
            );
            if let Err(error) = upload_active_history(
                &history, &work_dir, chunk_size, &client, &bot_token, &chat_ids,
            ) {
                tracing::error!(error = %error, "scheduled history upload failed");
            }
        }
    });
}

fn upload_active_history(
    history: &HistoryStore,
    work_dir: &std::path::Path,
    chunk_size: u64,
    client: &ClientHandle,
    bot_token: &str,
    chat_ids: &[String],
) -> Result<()> {
    let Some(snapshot) = history.snapshot_active(work_dir)? else {
        tracing::info!("history upload skipped — active monthly history is empty");
        return Ok(());
    };
    let stamp = match dump_timestamp(SystemTime::now()) {
        Ok(stamp) => stamp,
        Err(error) => {
            let _ = std::fs::remove_file(&snapshot.path);
            return Err(error).context("naming history upload parts");
        }
    };
    let prefix = format!("history-{}-{}", snapshot.month, stamp);
    let result = (|| -> Result<()> {
        let (parts, _, total_bytes) =
            chunk_history_snapshot(&snapshot.path, work_dir, &prefix, chunk_size)?;
        tracing::info!(
            month = %snapshot.month,
            parts = parts.len(),
            total_bytes,
            "uploading monthly history snapshot",
        );
        let mut stats = telegram::UploadStats::default();
        upload_chunks_to_destinations(
            &parts,
            chat_ids,
            |chat_id, part, stats| {
                let client = client.read().expect("Telegram client lock poisoned");
                telegram::send_document(&client, bot_token, chat_id, part, stats)
            },
            |_index| {},
            &mut stats,
            None,
        )
        .context("uploading history snapshot")?;
        tracing::info!(
            upload_attempts = stats.attempts,
            upload_retries = stats.retries,
            "monthly history snapshot uploaded",
        );
        Ok(())
    })();
    chunk::cleanup_prefix(work_dir, &prefix);
    let _ = std::fs::remove_file(&snapshot.path);
    result
}

fn chunk_history_snapshot(
    snapshot: &std::path::Path,
    work_dir: &std::path::Path,
    prefix: &str,
    chunk_size: u64,
) -> Result<(Vec<std::path::PathBuf>, [u8; 32], u64)> {
    let input = std::fs::File::open(snapshot)
        .with_context(|| format!("opening history snapshot {}", snapshot.display()))?;
    let mut reader = BufReader::new(input);
    let mut chunker = ChunkWriter::new(work_dir, prefix, chunk_size);
    let mut buffer = vec![0u8; 64 * 1024];
    loop {
        let read = reader
            .read(&mut buffer)
            .with_context(|| "reading history snapshot")?;
        if read == 0 {
            break;
        }
        chunker
            .write_all(&buffer[..read])
            .with_context(|| "splitting history snapshot into Telegram parts")?;
    }
    chunker.finish().context("finalizing history chunks")
}

/// Run backup cycles on `schedule`, forever, and never stop on failure.
///
/// The two schedule forms differ only in when the next cycle starts:
///
/// - [`Schedule::Every`] backs up immediately, then keeps cycles one interval
///   apart measured from the *start* of each cycle (so `6h` means six hours
///   apart rather than six hours of idling).
/// - [`Schedule::Cron`] waits for the next wall-clock time matching the
///   expression, exactly like crontab — nothing runs at startup.
///
/// Either way a cycle that overruns its own slot does not stack a second cycle
/// on top of itself: two concurrent cycles would double the `pg_dump` load and
/// the `WORK_DIR` peak. An interval schedule starts the next cycle immediately;
/// a cron schedule skips the firing times that went by while it was busy,
/// rather than running them back to back.
fn run_scheduled(
    cfg: &SharedConfig,
    databases: &[DatabaseConfig],
    no_encryption: bool,
    schedule: &Schedule,
    client: &ClientHandle,
) {
    match schedule {
        Schedule::Every(interval) => tracing::info!(
            interval_secs = interval.as_secs(),
            databases = databases.len(),
            "scheduled mode — running the first cycle now, then every interval",
        ),
        Schedule::Cron(expr) => tracing::info!(
            cron = %expr,
            databases = databases.len(),
            "scheduled mode — waiting for the first matching time (nothing runs now)",
        ),
    }
    web::set_schedule_label(schedule_label(schedule));

    let controller = web::manual_backup_controller();
    let mut next_interval = None;
    for cycle in 1u64.. {
        if run_manual_requests(cfg, databases, client, &controller) {
            continue;
        }

        // A cron schedule has to reach its first matching time before the first
        // cycle; an interval schedule backs up straight away.
        if let Schedule::Cron(expr) = schedule {
            if sleep_until_cron(
                expr,
                cycle,
                "backup",
                web::set_next_backup_run_in,
                &controller,
            ) {
                continue;
            }
        } else if let Some(remaining) = next_interval.take() {
            web::set_next_backup_run_in(remaining);
            if controller.wait_for_wake(remaining) {
                continue;
            }
        }

        let _operation_gate = web::acquire_backup_route_gate();
        let cycle_started = std::time::Instant::now();
        tracing::info!(cycle, "backup cycle starting");
        web::set_cycle_running(true);

        // Reset every card to "queued" so the dashboard shows this cycle's
        // progress rather than the previous cycle's outcome.
        for db in databases {
            web::register_database(
                &db.display_name(),
                web::database_state_store().is_enabled(&db.display_name()),
            );
        }

        let (results, failures) = run_cycle(
            cfg,
            databases,
            no_encryption,
            client,
            BackupSource::Scheduled,
        );
        web::set_cycle_running(false);

        // A failing database is reported and then forgotten: the next cycle
        // retries it from scratch. This is the whole point of running on a
        // schedule instead of exiting non-zero and waiting for a human.
        if failures.is_empty() {
            tracing::info!(cycle, successes = results.len(), "backup cycle complete");
        } else {
            tracing::warn!(
                cycle,
                successes = results.len(),
                failures = failures.len(),
                "backup cycle finished with failures — retrying them next cycle",
            );
        }

        // An interval schedule sleeps off whatever is left of its interval; a
        // cron schedule computes its next firing time at the top of the loop,
        // from the clock as it is once the cycle has actually finished.
        if let Schedule::Every(interval) = schedule {
            let elapsed = cycle_started.elapsed();
            match interval.checked_sub(elapsed) {
                Some(remaining) => {
                    tracing::info!(
                        cycle,
                        elapsed_secs = elapsed.as_secs_f64(),
                        sleep_secs = remaining.as_secs(),
                        "sleeping until the next backup cycle",
                    );
                    web::set_next_backup_run_in(remaining);
                    next_interval = Some(remaining);
                }
                None => tracing::warn!(
                    cycle,
                    elapsed_secs = elapsed.as_secs_f64(),
                    interval_secs = interval.as_secs(),
                    "cycle took longer than BACKUP_INTERVAL — starting the next one \
                     immediately; raise the interval or lower MAX_PARALLEL_DATABASES",
                ),
            }
        }
    }
}

/// Drain all dashboard requests that arrived while the scheduler was asleep
/// or while a scheduled cycle was running.
fn run_manual_requests(
    cfg: &SharedConfig,
    databases: &[DatabaseConfig],
    client: &ClientHandle,
    controller: &Arc<web::ManualBackupController>,
) -> bool {
    let requests = controller.take_pending();
    if requests.is_empty() {
        return false;
    }
    let indices = requests
        .iter()
        .filter_map(|request| {
            databases
                .iter()
                .position(|db| db.display_name() == request.database_name)
        })
        .collect::<Vec<_>>();
    if !indices.is_empty() {
        let options = requests
            .iter()
            .map(|request| {
                (
                    request.database_name.clone(),
                    BackupOptions {
                        chat_ids: vec![request.chat_id.clone()],
                        recipient: Some(request.recipient_name.clone()),
                        no_encryption: request.no_encryption,
                    },
                )
            })
            .collect::<std::collections::HashMap<_, _>>();
        let _operation_gate = web::acquire_backup_route_gate();
        web::set_cycle_running(true);
        let _ = execute_database_indices(
            cfg,
            databases,
            &indices,
            client,
            BackupSource::Manual,
            true,
            &options,
        );
        web::set_cycle_running(false);
    }
    for request in requests {
        controller.finish(&request.database_name);
    }
    true
}

/// Sleep until the next local time matching `expr`.
///
/// The target is computed **once** and then compared against the wall clock in
/// bounded slices. Recomputing it each slice would never fire: a sleep normally
/// wakes a hair past the target, and `next_after` is strictly-after, so the
/// target would keep advancing to the following firing time.
///
/// Sleeping in slices rather than one long call means a clock adjustment (NTP
/// step, DST change, a resumed container) is noticed within [`SLEEP_SLICE`]
/// instead of firing that far off — a jump forward past the target fires
/// immediately, and a jump backwards simply waits longer.
fn sleep_until_cron(
    expr: &cron::Cron,
    cycle: u64,
    schedule_name: &str,
    set_next_run: fn(std::time::Duration),
    controller: &web::ManualBackupController,
) -> bool {
    let now = chrono::Local::now().naive_local();
    // `Cron::parse` rejects expressions that can never fire, so `None` here
    // would mean the clock has run past the four-year search window. Fire now
    // rather than sleeping forever on an unanswerable question.
    let Some(target) = expr.next_after(now) else {
        tracing::error!(
            cycle,
            cron = %expr,
            "cannot determine the next firing time from the current clock; \
             running this schedule immediately",
        );
        return false;
    };

    tracing::info!(
        cycle,
        cron = %expr,
        next_run = %target.format("%Y-%m-%d %H:%M:%S"),
        wait_secs = target.signed_duration_since(now).num_seconds(),
        schedule = schedule_name,
        "waiting for the next scheduled execution",
    );

    if let Ok(wait) = target.signed_duration_since(now).to_std() {
        set_next_run(wait);
    }

    while let Some(slice) = sleep_slice(chrono::Local::now().naive_local(), target) {
        if controller.wait_for_wake(slice) {
            return true;
        }
    }
    false
}

/// How the dashboard should describe the schedule it is running on.
fn schedule_label(schedule: &Schedule) -> String {
    match schedule {
        Schedule::Every(interval) => format!("every {}", format_secs(interval.as_secs())),
        Schedule::Cron(expr) => format!("cron {expr}"),
    }
}

/// Render a whole number of seconds as the compact `1d 2h 3m` form used in the
/// dashboard, dropping zero units. Zero itself is `0s`.
fn format_secs(total: u64) -> String {
    let parts = [
        (total / 86_400, 'd'),
        (total % 86_400 / 3_600, 'h'),
        (total % 3_600 / 60, 'm'),
        (total % 60, 's'),
    ];
    let out: Vec<String> = parts
        .iter()
        .filter(|(n, _)| *n > 0)
        .map(|(n, unit)| format!("{n}{unit}"))
        .collect();
    if out.is_empty() {
        return "0s".to_string();
    }
    out.join(" ")
}

/// Longest single sleep [`sleep_until_cron`] takes before re-reading the clock.
const SLEEP_SLICE: std::time::Duration = std::time::Duration::from_secs(30);

/// How long to sleep before re-checking the clock, or `None` once `target` has
/// arrived (or already passed, after a forward clock jump).
fn sleep_slice(now: NaiveDateTime, target: NaiveDateTime) -> Option<std::time::Duration> {
    // `to_std` fails for a negative span — i.e. the target is behind us.
    let remaining = target.signed_duration_since(now).to_std().ok()?;
    if remaining.is_zero() {
        return None;
    }
    Some(remaining.min(SLEEP_SLICE))
}

/// Run one full backup cycle over every database and print its manifest.
///
/// Never fails as a whole: an individual database's failure is collected and
/// reported, and the remaining databases are backed up regardless. The caller
/// decides what a failure means (exit code in one-shot mode, "try again next
/// cycle" in scheduled mode).
fn run_cycle(
    cfg: &SharedConfig,
    databases: &[DatabaseConfig],
    no_encryption: bool,
    client: &ClientHandle,
    source: BackupSource,
) -> (Vec<BackupResult>, Vec<DatabaseFailure>) {
    let controller = web::manual_backup_controller();
    let active = databases
        .iter()
        .enumerate()
        .filter(|(_, db)| {
            web::database_state_store().is_enabled(&db.display_name())
                && controller.claim_scheduled(&db.display_name())
        })
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    let options = databases
        .iter()
        .map(|db| {
            (
                db.display_name(),
                BackupOptions {
                    chat_ids: cfg.tg_chat_ids.clone(),
                    recipient: None,
                    no_encryption,
                },
            )
        })
        .collect::<std::collections::HashMap<_, _>>();
    let (results, failures) = execute_database_indices(
        cfg,
        databases,
        &active,
        client,
        source,
        source != BackupSource::Manual,
        &options,
    );

    print_manifest(&results, &failures);

    tracing::info!(
        successes = results.len(),
        failures = failures.len(),
        "backup complete",
    );

    (results, failures)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BackupSource {
    Manual,
    Scheduled,
    OneShot,
}

impl BackupSource {
    fn as_str(self) -> &'static str {
        match self {
            Self::Manual => "manual",
            Self::Scheduled => "scheduled",
            Self::OneShot => "one-shot",
        }
    }

    fn sends_notifications(self) -> bool {
        matches!(self, Self::Manual | Self::Scheduled)
    }

    fn notification_chat_ids(self, options: &BackupOptions) -> &[String] {
        match self {
            Self::Scheduled => &options.chat_ids,
            Self::Manual => options.chat_ids.get(..1).unwrap_or_default(),
            Self::OneShot => &[],
        }
    }
}

/// Result produced by a single database backup pipeline.
///
/// Holds metadata needed for the consolidated manifest output.
struct BackupResult {
    /// Human-readable display name of the database.
    db_name: String,
    /// Total number of bytes streamed through the pipeline.
    total_bytes: u64,
    /// Whether age encryption was applied to the stream.
    encrypted: bool,
    /// SHA-256 digest of the final byte stream (reassembly verification).
    sha256: [u8; 32],
    /// Wall-clock time elapsed for the entire backup cycle.
    elapsed_secs: f64,
    /// Number of uploaded chunks.
    chunks_count: usize,
}

#[derive(Debug, Default)]
struct AttemptMetrics {
    dump_bytes: u64,
    packaged_bytes: u64,
    chunks_count: usize,
    sha256: Option<[u8; 32]>,
    encrypted: bool,
    upload_duration_secs: f64,
    upload_attempts: u64,
    upload_retries: u64,
}

#[derive(Debug, Clone)]
struct BackupOptions {
    chat_ids: Vec<String>,
    recipient: Option<String>,
    no_encryption: bool,
}

type CancellationToken = Arc<std::sync::atomic::AtomicBool>;

fn cancellation_requested(token: &CancellationToken) -> bool {
    token.load(Ordering::SeqCst)
}

fn cancelled() -> anyhow::Error {
    anyhow::Error::new(web::CancellationError)
}

fn is_cancelled(error: &anyhow::Error) -> bool {
    error.downcast_ref::<web::CancellationError>().is_some()
}

/// Upload every chunk to each destination that has not failed yet.
///
/// A destination is complete only if it receives every chunk. Failed
/// destinations are removed from later attempts, but the current chunk stays
/// on disk until all destinations still active for it have been attempted.
fn upload_chunks_to_destinations<F, C>(
    chunks: &[std::path::PathBuf],
    chat_ids: &[String],
    mut send: F,
    mut after_chunk: C,
    stats: &mut telegram::UploadStats,
    cancellation: Option<&CancellationToken>,
) -> Result<()>
where
    F: FnMut(&str, &std::path::Path, &mut telegram::UploadStats) -> Result<()>,
    C: FnMut(usize),
{
    if chat_ids.is_empty() {
        bail!("no Telegram destinations configured");
    }

    let mut active = vec![true; chat_ids.len()];
    let mut completed = vec![true; chat_ids.len()];
    let mut failures = Vec::new();

    for (chunk_index, path) in chunks.iter().enumerate() {
        if cancellation.is_some_and(cancellation_requested) {
            return Err(cancelled());
        }
        if !active.iter().any(|is_active| *is_active) {
            break;
        }

        for (destination, is_active) in active.iter_mut().enumerate() {
            if cancellation.is_some_and(cancellation_requested) {
                return Err(cancelled());
            }
            if !*is_active {
                continue;
            }
            if let Err(error) = send(&chat_ids[destination], path, stats) {
                *is_active = false;
                completed[destination] = false;
                failures.push(format!(
                    "destination {} failed on chunk {}: {error:#}",
                    destination,
                    chunk_index + 1
                ));
                tracing::warn!(
                    destination,
                    chunk = chunk_index + 1,
                    error = %error,
                    "Telegram destination marked incomplete",
                );
            }
        }

        chunk::remove(path);
        after_chunk(chunk_index);
    }

    if completed.iter().any(|is_complete| *is_complete) {
        return Ok(());
    }

    let detail = failures
        .first()
        .cloned()
        .unwrap_or_else(|| "all destinations were incomplete".to_string());
    Err(anyhow!(
        "no Telegram destination received every chunk; {detail}"
    ))
}

/// A database whose backup did not complete.
///
/// Recorded so the manifest reports failures explicitly instead of only
/// listing what happened to succeed.
struct DatabaseFailure {
    /// Position of the database in the resolved configuration.
    index: usize,
    /// Human-readable display name of the database.
    db_name: String,
    /// The failure, formatted with its full context chain.
    error: String,
}

/// Execute the full backup pipeline for a single database.
///
/// Steps:
/// 1. Spawn `pg_dump` for this database with configured extra args.
/// 2. Stream stdout through `[zstd → age?] → ChunkWriter` producing a bare
///    file when it fits, otherwise `.partNNNN` files.
/// 3. Upload each chunk to every active Telegram destination (with retries).
/// 4. Return a [`BackupResult`] with metadata.
///
/// Reports "running" / "done" / "error" status to the dashboard at each phase.
///
/// On failure the partial chunks are swept from `work_dir` unless
/// `cfg.keep_failed_dumps` asks to keep them for debugging.
fn run_database(
    cfg: &SharedConfig,
    db: &DatabaseConfig,
    db_index: usize,
    client: &ClientHandle,
    source: BackupSource,
    options: &BackupOptions,
    cancellation: CancellationToken,
) -> Result<BackupResult> {
    let started = SystemTime::now();
    let db_name = db.display_name();
    let mut metrics = AttemptMetrics {
        encrypted: !options.no_encryption && cfg.age_recipient.is_some(),
        ..Default::default()
    };

    // Wire-contract filename: `{db}_{utc_ts}.sql.zst[.age]`. Duplicate
    // display names are rejected at config time, so the timestamped prefix
    // remains unique for a database's run.
    let stamp = dump_timestamp(started)?;
    let extension = if !options.no_encryption && cfg.age_recipient.is_some() {
        ".sql.zst.age"
    } else {
        ".sql.zst"
    };
    let base_name = format!("{db_name}_{stamp}{extension}");

    let result = backup_pipeline(
        cfg,
        db,
        &db_name,
        &base_name,
        started,
        source,
        options,
        client,
        &mut metrics,
        &cancellation,
    );

    let ended = SystemTime::now();
    let duration_secs = ended
        .duration_since(started)
        .unwrap_or_default()
        .as_secs_f64();
    match &result {
        Ok(success) => record_history(
            &cfg.history,
            HistoryRecord {
                started_at: history::timestamp(started),
                ended_at: history::timestamp(ended),
                database_index: db_index,
                database_name: db_name.clone(),
                source: source.as_str().into(),
                recipient: options.recipient.clone(),
                status: "success".into(),
                error: None,
                dump_bytes: metrics.dump_bytes,
                packaged_bytes: success.total_bytes,
                chunk_count: success.chunks_count,
                sha256: Some(hex::encode(success.sha256)),
                encrypted: success.encrypted,
                duration_secs,
                upload_duration_secs: metrics.upload_duration_secs,
                upload_attempts: metrics.upload_attempts,
                upload_retries: metrics.upload_retries,
            },
            &db.url,
            cfg,
            &options.chat_ids,
        ),
        Err(error) => record_history(
            &cfg.history,
            HistoryRecord {
                started_at: history::timestamp(started),
                ended_at: history::timestamp(ended),
                database_index: db_index,
                database_name: db_name.clone(),
                source: source.as_str().into(),
                recipient: options.recipient.clone(),
                status: if is_cancelled(error) {
                    "cancelled"
                } else {
                    "failure"
                }
                .into(),
                error: Some(history::sanitize_error(
                    &if is_cancelled(error) {
                        "cancelled by dashboard operator".to_string()
                    } else {
                        format!("{error:#}")
                    },
                    &db.url,
                    &cfg.tg_bot_token,
                    &options.chat_ids,
                )),
                dump_bytes: metrics.dump_bytes,
                packaged_bytes: metrics.packaged_bytes,
                chunk_count: metrics.chunks_count,
                sha256: metrics.sha256.map(hex::encode),
                encrypted: metrics.encrypted,
                duration_secs,
                upload_duration_secs: metrics.upload_duration_secs,
                upload_attempts: metrics.upload_attempts,
                upload_retries: metrics.upload_retries,
            },
            &db.url,
            cfg,
            &options.chat_ids,
        ),
    }

    // A failure can happen anywhere — mid-dump, mid-upload — so sweep by
    // prefix: the chunk path list only exists once the pipeline finished.
    if result.is_err() {
        if cfg.keep_failed_dumps && !result.as_ref().err().is_some_and(is_cancelled) {
            tracing::warn!(
                db = db_name,
                work_dir = %cfg.work_dir.display(),
                prefix = base_name,
                "KEEP_FAILED_DUMPS is set — leaving partial chunks on disk; remove them yourself",
            );
        } else {
            chunk::cleanup_prefix(&cfg.work_dir, &base_name);
        }
    }

    result
}

/// The pipeline itself, split out so [`run_database`] can clean up after it on
/// every failure path with a single check.
#[allow(clippy::too_many_arguments)]
fn backup_pipeline(
    cfg: &SharedConfig,
    db: &DatabaseConfig,
    db_name: &str,
    base_name: &str,
    started: SystemTime,
    source: BackupSource,
    options: &BackupOptions,
    client: &ClientHandle,
    metrics: &mut AttemptMetrics,
    cancellation: &CancellationToken,
) -> Result<BackupResult> {
    // Report "running" status to the dashboard before starting heavy work.
    web::set_db_status(db_name, 1, "dump", "Dumping PostgreSQL via pg_dump …");

    // ── Resolve encryption mode ────────────────────────────────────────────
    let age_recipient = if options.no_encryption {
        None
    } else {
        cfg.age_recipient.as_deref()
    };
    let encrypted = age_recipient.is_some();

    // ── Start pg_dump subprocess ───────────────────────────────────────────
    // Keep the DumpPipe alive so we can verify pg_dump's exit status after
    // draining stdout; a non-zero exit means the dump is incomplete.
    tracing::info!(db = db_name, "starting pg_dump");
    let mut pipe = dump::spawn_pg_dump(&db.url, db.pg_dump_extra_args.as_deref())
        .with_context(|| format!("starting pg_dump for {db_name}"))?;
    if cancellation_requested(cancellation) {
        pipe.cancel().context("terminating cancelled pg_dump")?;
        return Err(cancelled());
    }

    // ── Build the streaming pipeline ───────────────────────────────────────
    //   encrypted:  pg_dump | zstd | age  | chunked files
    //   plain:      pg_dump | zstd        | chunked files
    let mut sink = match age_recipient {
        Some(recipient) => {
            tracing::info!(db = db_name, "encryption enabled (age X25519)");
            let enc = encrypt::encryptor_for(recipient).context("building age encryptor")?;
            let age_writer = encrypt::wrap(
                enc,
                ChunkWriter::new(&cfg.work_dir, base_name, cfg.chunk_size_bytes()),
            )
            .context("wrapping chunker in age StreamWriter")?;
            let zstd = compress::encoder(age_writer).context("building zstd encoder")?;
            Sink::Encrypted(zstd)
        }
        None => {
            if options.no_encryption {
                tracing::warn!(
                    db = db_name,
                    "--no-encryption — dump will be compressed but NOT encrypted"
                );
            } else {
                tracing::warn!(
                    db = db_name,
                    "AGE_RECIPIENT not set — dump will be compressed but NOT encrypted"
                );
            }
            let chunker = ChunkWriter::new(&cfg.work_dir, base_name, cfg.chunk_size_bytes());
            let zstd = compress::encoder(chunker).context("building zstd encoder")?;
            Sink::Plain(zstd)
        }
    };

    // ── Stream pg_dump stdout through the pipeline ─────────────────────────
    // The 64 KiB buffer is chosen as a reasonable trade-off: large enough to
    // amortize syscalls, small enough to avoid excessive buffering.
    let mut buf = vec![0u8; 64 * 1024];
    // Uncompressed bytes read so far — published to the dashboard as the
    // logical size of this dump. Publishing on every read would take the
    // global status lock (and format a timestamp) tens of thousands of times
    // per GiB, so only report once per `DUMP_REPORT_EVERY` bytes.
    const DUMP_REPORT_EVERY: u64 = 4 * 1024 * 1024;
    let mut raw_bytes: u64 = 0;
    let mut reported_at: u64 = 0;
    loop {
        if cancellation_requested(cancellation) {
            pipe.cancel().context("terminating cancelled pg_dump")?;
            return Err(cancelled());
        }
        let n = pipe
            .stdout
            .read(&mut buf)
            .with_context(|| format!("reading from pg_dump stdout (db={db_name})"))?;
        if n == 0 {
            break;
        } // EOF reached
        if cancellation_requested(cancellation) {
            pipe.cancel().context("terminating cancelled pg_dump")?;
            return Err(cancelled());
        }
        sink.write(&buf[..n])
            .with_context(|| format!("writing through pipeline (db={db_name})"))?;
        raw_bytes += n as u64;
        if raw_bytes - reported_at >= DUMP_REPORT_EVERY {
            web::set_db_dump_bytes(db_name, raw_bytes);
            reported_at = raw_bytes;
        }
    }
    // Exact total, including the tail below the reporting threshold.
    web::set_db_dump_bytes(db_name, raw_bytes);
    metrics.dump_bytes = raw_bytes;

    // ── Finalize pipeline stages ───────────────────────────────────────────
    // Unwind the writer stack in reverse order:
    //   1. zstd::Encoder::finish(self) → io::Result<InnerWriter>
    //   2. age::StreamWriter::finish() → Result<ChunkWriter> (if encrypted)
    //   3. ChunkWriter::finish() → Result<(paths, hash, total)>
    web::set_db_status(
        db_name,
        1,
        "package",
        if encrypted {
            "Flushing compression + encryption, writing chunks …"
        } else {
            "Flushing compression, writing chunks …"
        },
    );
    if cancellation_requested(cancellation) {
        pipe.cancel().context("terminating cancelled pg_dump")?;
        return Err(cancelled());
    }
    let (chunks, hash, total_bytes) = sink
        .finish()
        .with_context(|| format!("finalizing pipeline stages (db={db_name})"))?;

    // Now that stdout is drained, confirm pg_dump exited successfully.
    pipe.finish()
        .with_context(|| format!("pg_dump did not complete successfully (db={db_name})"))?;

    let elapsed = started.elapsed().unwrap_or_default();
    let chunks_count = chunks.len();
    metrics.packaged_bytes = total_bytes;
    metrics.chunks_count = chunks_count;
    metrics.sha256 = Some(hash);

    // Dump + packaging done; the upload stage starts next. `total_bytes` is
    // the post-compression (and post-encryption) size — i.e. exactly what
    // goes over the wire to Telegram.
    web::set_db_status(
        db_name,
        1,
        "upload",
        format!("Uploading to Telegram — 0/{chunks_count} chunks"),
    );
    web::set_db_transfer(db_name, 0, total_bytes, 0.0);

    tracing::info!(
        db = db_name,
        chunks = chunks_count,
        total_bytes,
        encrypted,
        sha256 = hex::encode(hash),
        elapsed_secs = elapsed.as_secs_f64(),
        "pipeline complete; uploading",
    );

    if source.sends_notifications() {
        let message = if source == BackupSource::Scheduled {
            telegram::format_scheduled_backup_summary(
                db_name,
                base_name,
                total_bytes,
                chunks_count,
                encrypted,
            )
        } else {
            telegram::format_backup_summary(
                db_name,
                base_name,
                total_bytes,
                chunks_count,
                encrypted,
            )
        };
        for chat_id in source.notification_chat_ids(options) {
            if cancellation_requested(cancellation) {
                return Err(cancelled());
            }
            let client_guard = client.read().expect("Telegram client lock poisoned");
            if let Err(error) = telegram::send_message_with_cancel(
                &client_guard,
                &cfg.tg_bot_token,
                chat_id,
                &message,
                Some(cancellation),
            ) {
                tracing::warn!(
                    db = db_name,
                    error = %error,
                    source = source.as_str(),
                    "failed to send backup summary; continuing upload",
                );
            }
        }
    }

    // ── Upload chunks to Telegram ──────────────────────────────────────────
    // Each chunk is retained until every currently active destination has been
    // attempted. A failed destination is skipped for subsequent chunks.
    let upload_started = SystemTime::now();
    let sent_bytes = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let mut upload_stats = telegram::UploadStats::default();
    upload_chunks_to_destinations(
        &chunks,
        &options.chat_ids,
        |chat_id, path, stats| {
            let client_guard = client.read().expect("Telegram client lock poisoned");
            let Some(chunk_index) = chunks.iter().position(|chunk| chunk == path) else {
                return Err(anyhow::anyhow!(
                    "upload chunk {} is not part of the current backup",
                    path.display()
                ));
            };
            let chunk_total = std::fs::metadata(path)
                .map(|metadata| metadata.len())
                .unwrap_or(0);
            let db_name = db_name.to_string();
            let base_done = sent_bytes.load(std::sync::atomic::Ordering::Relaxed);
            web::set_db_transfer_with_chunk(
                &db_name,
                base_done,
                total_bytes,
                rate(base_done, &upload_started),
                web::ChunkProgress {
                    number: chunk_index + 1,
                    count: chunks_count,
                    done: 0,
                    total: chunk_total,
                },
            );
            let progress = std::sync::Arc::new(move |chunk_done| {
                web::set_db_transfer_with_chunk(
                    &db_name,
                    base_done,
                    total_bytes,
                    rate(base_done + chunk_done, &upload_started),
                    web::ChunkProgress {
                        number: chunk_index + 1,
                        count: chunks_count,
                        done: chunk_done,
                        total: chunk_total,
                    },
                );
            });
            telegram::send_document_with_progress(
                &client_guard,
                &cfg.tg_bot_token,
                chat_id,
                path,
                stats,
                Some(progress),
                Some(cancellation),
            )
        },
        |i| {
            let path = &chunks[i];
            let chunk_bytes = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
            sent_bytes.fetch_add(chunk_bytes, std::sync::atomic::Ordering::Relaxed);
            let sent = sent_bytes.load(std::sync::atomic::Ordering::Relaxed);
            web::set_db_status(
                db_name,
                1,
                "upload",
                format!("Uploading to Telegram — {}/{chunks_count} chunks", i + 1),
            );
            web::set_db_transfer(db_name, sent, total_bytes, rate(sent, &upload_started));
        },
        &mut upload_stats,
        Some(cancellation),
    )
    .with_context(|| format!("uploading backup chunks (db={db_name})"))?;
    metrics.upload_attempts += upload_stats.attempts;
    metrics.upload_retries += upload_stats.retries;
    metrics.upload_duration_secs = upload_started.elapsed().unwrap_or_default().as_secs_f64();

    if source.sends_notifications() && chunks_count > 1 {
        let message = if source == BackupSource::Scheduled {
            telegram::format_scheduled_backup_completion(db_name, base_name, chunks_count)
        } else {
            telegram::format_backup_completion(db_name, base_name, chunks_count)
        };
        for chat_id in source.notification_chat_ids(options) {
            let client_guard = client.read().expect("Telegram client lock poisoned");
            if let Err(error) =
                telegram::send_message(&client_guard, &cfg.tg_bot_token, chat_id, &message)
            {
                tracing::warn!(
                    db = db_name,
                    error = %error,
                    source = source.as_str(),
                    "failed to send backup completion notice; backup succeeded",
                );
            }
        }
    }

    // Mark database as done in the dashboard.
    web::set_db_status(
        db_name,
        0,
        "done",
        format!("Backup complete — {chunks_count} chunks uploaded"),
    );
    web::set_db_transfer(
        db_name,
        total_bytes,
        total_bytes,
        rate(total_bytes, &upload_started),
    );

    Ok(BackupResult {
        db_name: db_name.to_string(),
        total_bytes,
        encrypted,
        sha256: hash,
        elapsed_secs: elapsed.as_secs_f64(),
        chunks_count,
    })
}

fn record_history(
    store: &HistoryStore,
    record: HistoryRecord,
    database_url: &str,
    cfg: &SharedConfig,
    chat_ids: &[String],
) {
    if let Err(error) = store.append(&record) {
        let sanitized = history::sanitize_error(
            &error.to_string(),
            database_url,
            &cfg.tg_bot_token,
            chat_ids,
        );
        tracing::warn!(error = %sanitized, "failed to write backup history; continuing");
    }
}

/// Average throughput in bytes/second for `bytes` transferred since `since`.
///
/// Returns 0.0 when no measurable time has passed, rather than infinity.
fn rate(bytes: u64, since: &SystemTime) -> f64 {
    let secs = since.elapsed().unwrap_or_default().as_secs_f64();
    if secs > 0.0 {
        bytes as f64 / secs
    } else {
        0.0
    }
}

/// Execute selected databases with the existing bounded worker pool.
///
/// Runs at most `cfg.max_parallel_databases` pipelines at the same time; the
/// rest wait for a free slot. Individual failures never cancel other databases
/// and never abort the run — including the case where every database fails, so
/// a scheduled run keeps going and retries next cycle.
///
/// Returns the successes and the per-database failures; the caller reports both
/// and decides the exit code.
fn execute_database_indices(
    cfg: &SharedConfig,
    databases: &[DatabaseConfig],
    indices: &[usize],
    client: &ClientHandle,
    source: BackupSource,
    already_claimed: bool,
    options_by_db: &std::collections::HashMap<String, BackupOptions>,
) -> (Vec<BackupResult>, Vec<DatabaseFailure>) {
    // Never start more workers than there is work for them. With a single
    // database this is one worker, which is the sequential path — no special
    // case needed, and no path that skips failure collection.
    if indices.is_empty() {
        return (Vec::new(), Vec::new());
    }
    let workers = cfg.max_parallel_databases.min(indices.len()).max(1);
    tracing::info!(
        count = indices.len(),
        max_parallel = cfg.max_parallel_databases,
        workers,
        "spawning parallel database backups"
    );

    // Each worker runs the full blocking pipeline for one database at a time,
    // taking the next queued database as soon as it frees up. A panic inside a
    // pipeline is caught here so the remaining databases keep going and the
    // failure is still attributed to the database that caused it.
    let outcomes = run_bounded(indices, workers, |job_index, db_index| {
        let db = &databases[*db_index];
        let name = db.display_name();
        if !already_claimed && !web::manual_backup_controller().claim_scheduled(&name) {
            return None;
        }
        let options = options_by_db
            .get(&name)
            .expect("backup options must exist for configured database");
        let cancellation = web::manual_backup_controller()
            .cancellation_token(&name)
            .unwrap_or_else(|| Arc::new(std::sync::atomic::AtomicBool::new(false)));
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            run_database(cfg, db, *db_index, client, source, options, cancellation)
        }));
        if source != BackupSource::Manual {
            web::manual_backup_controller().finish(&name);
        }
        Some((job_index, outcome))
    });

    let outcomes = outcomes.into_iter().flatten().collect::<Vec<_>>();

    // Collect results, tracking successes and failures independently.
    let mut results = Vec::new();
    let mut errors: Vec<DatabaseFailure> = Vec::new();

    for (job_index, outcome) in outcomes {
        let i = indices[job_index];
        let db_name = databases[i].display_name();
        match outcome {
            Ok(Ok(result)) => {
                tracing::info!(db_index = i, db_name = %db_name, "database backup succeeded");
                results.push(result);
            }
            Ok(Err(e)) => {
                tracing::error!(
                    db_index = i,
                    db_name = %db_name,
                    error = %e,
                    "database backup failed",
                );
                if is_cancelled(&e) {
                    web::cancel_db(&db_name);
                    tracing::info!(
                        db_index = i,
                        db_name = %db_name,
                        "database backup cancelled by operator",
                    );
                } else {
                    web::fail_db(&db_name, e.to_string()); // keeps the failed stage
                    errors.push(DatabaseFailure {
                        index: i,
                        db_name,
                        error: format!("{e:#}"),
                    });
                }
            }
            Err(payload) => {
                // Attempt to extract a human-readable panic message.
                let panic_msg = if let Some(s) = payload.downcast_ref::<String>() {
                    s.clone()
                } else if let Some(s) = payload.downcast_ref::<&str>() {
                    s.to_string()
                } else {
                    "unknown panic".to_string()
                };
                tracing::error!(
                    db_index = i,
                    db_name = %db_name,
                    error = %panic_msg,
                    "backup thread panicked",
                );
                web::fail_db(&db_name, format!("panicked: {panic_msg}"));
                errors.push(DatabaseFailure {
                    index: i,
                    db_name,
                    error: format!("panicked: {panic_msg}"),
                });
                let now = SystemTime::now();
                record_history(
                    &cfg.history,
                    HistoryRecord {
                        started_at: history::timestamp(now),
                        ended_at: history::timestamp(now),
                        database_index: i,
                        database_name: databases[i].display_name(),
                        source: source.as_str().into(),
                        recipient: options_by_db
                            .get(&databases[i].display_name())
                            .and_then(|options| options.recipient.clone()),
                        status: "failure".into(),
                        error: Some(history::sanitize_error(
                            &format!("panicked: {panic_msg}"),
                            &databases[i].url,
                            &cfg.tg_bot_token,
                            options_by_db
                                .get(&databases[i].display_name())
                                .map(|options| options.chat_ids.as_slice())
                                .unwrap_or(&[]),
                        )),
                        dump_bytes: 0,
                        packaged_bytes: 0,
                        chunk_count: 0,
                        sha256: None,
                        encrypted: !options_by_db
                            .get(&databases[i].display_name())
                            .is_some_and(|options| options.no_encryption)
                            && cfg.age_recipient.is_some(),
                        duration_secs: 0.0,
                        upload_duration_secs: 0.0,
                        upload_attempts: 0,
                        upload_retries: 0,
                    },
                    &databases[i].url,
                    cfg,
                    &options_by_db
                        .get(&databases[i].display_name())
                        .map(|options| options.chat_ids.clone())
                        .unwrap_or_else(|| cfg.tg_chat_ids.clone()),
                );
            }
        }
    }

    if !errors.is_empty() {
        tracing::warn!(
            successes = results.len(),
            failures = errors.len(),
            "some database backups failed (continued)",
        );
    }

    (results, errors)
}

/// Run `f` over every item with at most `workers` running at the same time.
///
/// A shared cursor hands out the next index, so a worker that finishes early
/// picks up the next database immediately instead of waiting for its peers —
/// what a fixed batch-per-round split would do. Results come back in input
/// order regardless of completion order, which keeps the manifest deterministic.
///
/// Scoped threads let the workers borrow the config and database list, so
/// nothing has to be cloned per database.
fn run_bounded<T: Sync, R: Send>(
    items: &[T],
    workers: usize,
    f: impl Fn(usize, &T) -> R + Sync,
) -> Vec<R> {
    let next = AtomicUsize::new(0);
    let out: Mutex<Vec<(usize, R)>> = Mutex::new(Vec::with_capacity(items.len()));

    std::thread::scope(|scope| {
        for _ in 0..workers {
            scope.spawn(|| loop {
                // `fetch_add` is the whole queue: each worker claims exactly one
                // index, and the first claim past the end ends that worker.
                let i = next.fetch_add(1, Ordering::Relaxed);
                let Some(item) = items.get(i) else { return };
                let result = f(i, item);
                out.lock().expect("result lock poisoned").push((i, result));
            });
        }
    });

    let mut collected = out.into_inner().expect("result lock poisoned");
    collected.sort_by_key(|(i, _)| *i);
    collected.into_iter().map(|(_, r)| r).collect()
}

fn build_http_client_for_proxy(socks_proxy: Option<&str>) -> Result<Client> {
    let mut builder = Client::builder().timeout(std::time::Duration::from_secs(300));
    if let Some(proxy) = socks_proxy {
        tracing::info!(proxy = %proxy, "routing Telegram traffic through SOCKS5 proxy");
        builder = builder.proxy(reqwest::Proxy::all(proxy).context("parsing SOCKS_PROXY URL")?);
    }
    builder.build().context("building HTTP client")
}

/// Print the consolidated manifest to stdout for downstream consumers.
///
/// Produces a structured block containing per-database summaries, any
/// failures. Designed for machine-parsable consumption by operators.
fn print_manifest(results: &[BackupResult], failures: &[DatabaseFailure]) {
    println!("# crab-dump manifest");
    println!(
        "servers: {} ({} ok, {} failed)",
        results.len() + failures.len(),
        results.len(),
        failures.len(),
    );

    for (i, r) in results.iter().enumerate() {
        println!(
            "server {}: {} (bytes={}, chunks={}, encrypted={}, sha256={}, duration={:.1}s)",
            i,
            r.db_name,
            r.total_bytes,
            r.chunks_count,
            r.encrypted,
            hex::encode(r.sha256),
            r.elapsed_secs,
        );
    }

    // Failures are part of the manifest: a reader that sees only successes
    // cannot tell a complete run from a partial one.
    for f in failures {
        println!("FAILED [{}] {}: {}", f.index, f.db_name, f.error);
    }
}

// =============================================================================
// Streaming pipeline finalization (kept intact from original implementation)
// =============================================================================

/// Finalization sink: owns the zstd encoder and its inner writer(s).
///
/// Implementing `Write` delegates to the zstd encoder for the main loop.
/// Unwinding through `finish()` peels off each layer in reverse order.
enum Sink {
    Encrypted(zstd::Encoder<'static, age::stream::StreamWriter<ChunkWriter>>),
    Plain(zstd::Encoder<'static, ChunkWriter>),
}

impl Write for Sink {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Sink::Encrypted(e) => e.write(buf),
            Sink::Plain(e) => e.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Sink::Encrypted(e) => e.flush(),
            Sink::Plain(e) => e.flush(),
        }
    }
}

impl Sink {
    /// Finalize the zstd encoder (flushing into age→chunker), then unwrap
    /// the inner layers back to the ChunkWriter, finalize it, and return
    /// the chunk list, sha256, and total bytes.
    ///
    /// Pipeline unwinding order:
    ///   1. `zstd::Encoder::finish(self)` → `io::Result<InnerWriter>`
    ///   2. `age::StreamWriter::finish()` → `Result<ChunkWriter>`
    ///   3. `ChunkWriter::finish()` → `Result<(paths, hash, total)>`
    fn finish(self) -> Result<(Vec<PathBuf>, [u8; 32], u64)> {
        match self {
            Sink::Plain(encoder) => {
                // Finish zstd, get back the owned ChunkWriter, finalize it.
                let chunker = encoder
                    .finish()
                    .map_err(|e| anyhow::anyhow!("zstd finish: {e}"))?;
                chunker
                    .finish()
                    .map_err(|e| anyhow::anyhow!("chunker finish: {e}"))
            }
            Sink::Encrypted(encoder) => {
                // Finish zstd → get age StreamWriter, finish that → get ChunkWriter.
                let age_writer = encoder
                    .finish()
                    .map_err(|e| anyhow::anyhow!("zstd finish: {e}"))?;
                let chunker = age_writer
                    .finish()
                    .map_err(|e| anyhow::anyhow!("age writer finish: {e}"))?;
                chunker
                    .finish()
                    .map_err(|e| anyhow::anyhow!("chunker finish: {e}"))
            }
        }
    }
}

/// Convert a Unix epoch timestamp to a readable, filename-safe UTC timestamp
/// (`YYYY-MM-DD_HH:mm:ss`) for dump and history filenames.
fn dump_timestamp(t: SystemTime) -> Result<String> {
    let (year, month, day, hour, minute, second) = utc_timestamp_parts(t)?;
    Ok(format!(
        "{year:04}-{month:02}-{day:02}_{hour:02}:{minute:02}:{second:02}"
    ))
}

/// Return UTC timestamp components using Howard Hinnant's civil date
/// algorithm — no external crates required.
fn utc_timestamp_parts(t: SystemTime) -> Result<(i64, i64, i64, i64, i64, i64)> {
    use std::time::UNIX_EPOCH;
    let secs = t
        .duration_since(UNIX_EPOCH)
        .context("time before epoch")?
        .as_secs();

    // Convert epoch seconds to UTC Y-M-D H:M:S components.
    let days = (secs / 86400) as i64;
    let secs_of_day = (secs % 86400) as i64;
    let h = secs_of_day / 3600;
    let m = (secs_of_day % 3600) / 60;
    let s = secs_of_day % 60;

    // Days since 1970-01-01 → civil date (Howard Hinnant's algorithm).
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097; // [0, 146097)
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = doy - (153 * mp + 2) / 5 + 1; // [1, 31]
    let month = if mp < 10 { mp + 3 } else { mp - 9 }; // [1, 12]
    let year = if month <= 2 { y + 1 } else { y };

    Ok((year, month, d, h, m, s))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn history_snapshot_chunks_reassemble_in_order_and_cleanly() {
        let root =
            std::env::temp_dir().join(format!("crab-dump-history-chunks-{}", std::process::id()));
        std::fs::remove_dir_all(&root).ok();
        std::fs::create_dir_all(&root).unwrap();
        let snapshot = root.join("snapshot.jsonl");
        let original: Vec<u8> = (0..100_000).map(|n| (n % 251) as u8).collect();
        std::fs::write(&snapshot, &original).unwrap();

        let (parts, _, total) =
            chunk_history_snapshot(&snapshot, &root, "history-2026-08-123456", 1024).unwrap();
        assert_eq!(total, original.len() as u64);
        assert!(parts.len() > 90);
        assert!(parts
            .iter()
            .all(|part| { std::fs::metadata(part).unwrap().len() <= 1024 }));
        let mut reassembled = Vec::new();
        for part in &parts {
            reassembled.extend(std::fs::read(part).unwrap());
        }
        assert_eq!(reassembled, original);
        chunk::cleanup_prefix(&root, "history-2026-08-123456");
        assert!(parts.iter().all(|part| !part.exists()));
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn dump_timestamp_is_readable_and_utc() {
        let timestamp = std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_786_568_132);
        assert_eq!(dump_timestamp(timestamp).unwrap(), "2026-08-12_20:55:32");
    }

    #[test]
    fn dump_timestamps_sort_lexically_across_time_boundaries() {
        let seconds = [
            1_786_568_132, // 2026-08-12 20:55:32
            1_786_568_133, // next second
            1_786_579_200, // 2026-08-13 00:00:00, next day
            1_788_220_800, // 2026-09-01 00:00:00, next month
            1_798_761_600, // 2027-01-01 00:00:00, next year
        ];
        let timestamps: Vec<_> = seconds
            .into_iter()
            .map(|seconds| {
                dump_timestamp(std::time::UNIX_EPOCH + std::time::Duration::from_secs(seconds))
                    .unwrap()
            })
            .collect();

        assert!(timestamps.windows(2).all(|pair| pair[0] < pair[1]));
    }

    #[test]
    fn small_history_snapshot_uses_bare_file() {
        let root =
            std::env::temp_dir().join(format!("crab-dump-history-small-{}", std::process::id()));
        std::fs::remove_dir_all(&root).ok();
        std::fs::create_dir_all(&root).unwrap();
        let snapshot = root.join("snapshot.jsonl");
        let original = b"{\"status\":\"success\"}\n";
        std::fs::write(&snapshot, original).unwrap();

        let (paths, _, total) =
            chunk_history_snapshot(&snapshot, &root, "history-2026-08-123456", 1024).unwrap();

        assert_eq!(total, original.len() as u64);
        assert_eq!(paths, vec![root.join("history-2026-08-123456")]);
        assert_eq!(std::fs::read(&paths[0]).unwrap(), original);
        assert!(!root.join("history-2026-08-123456.part0000").exists());
        chunk::cleanup_prefix(&root, "history-2026-08-123456");
        assert!(!paths[0].exists());
        std::fs::remove_dir_all(&root).ok();
    }

    /// The concurrency limit is the whole point of the parameter: never more
    /// than `workers` pipelines in flight, every database still processed, and
    /// results in input order so the manifest does not depend on timing.
    #[test]
    fn bounded_runner_caps_concurrency_and_keeps_input_order() {
        let items: Vec<usize> = (0..20).collect();
        let live = AtomicUsize::new(0);
        let peak = AtomicUsize::new(0);

        let out = run_bounded(&items, 3, |i, item| {
            let now = live.fetch_add(1, Ordering::SeqCst) + 1;
            peak.fetch_max(now, Ordering::SeqCst);
            // Hold the slot long enough that an unbounded implementation would
            // overlap all 20 items and blow past the cap.
            std::thread::sleep(std::time::Duration::from_millis(5));
            live.fetch_sub(1, Ordering::SeqCst);
            i * 10 + item
        });

        assert!(
            peak.load(Ordering::SeqCst) <= 3,
            "concurrency exceeded the cap: {}",
            peak.load(Ordering::SeqCst),
        );
        assert_eq!(out, items.iter().map(|v| v * 11).collect::<Vec<_>>());
    }

    /// A panicking pipeline must not take its peers down: the panic is caught
    /// per item, so later databases still run and the failure stays attributed.
    #[test]
    fn bounded_runner_isolates_panics() {
        let items = vec![0usize, 1, 2];
        let out = run_bounded(&items, 2, |_, item| {
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                assert_ne!(*item, 1, "boom");
                *item
            }))
        });

        assert!(out[0].is_ok());
        assert!(
            out[1].is_err(),
            "the panicking item must be reported as such"
        );
        assert!(out[2].is_ok(), "a peer panic must not skip later items");
    }

    /// The cron wait must actually end. Slicing the sleep is only safe because
    /// the target is fixed up front — recomputing it each slice pushed it to the
    /// next firing minute every time the sleep woke a moment late, so the
    /// scheduler waited forever and never ran a cycle.
    #[test]
    fn cron_sleep_slices_converge_on_the_target() {
        let target = chrono::NaiveDate::from_ymd_opt(2026, 8, 12)
            .unwrap()
            .and_hms_opt(9, 30, 0)
            .unwrap();

        // Walk the loop the way `sleep_until_cron` does, advancing a simulated
        // clock by each returned slice — including a final wake a hair late.
        let mut now = target - chrono::Duration::seconds(74);
        let mut slices = 0;
        while let Some(slice) = sleep_slice(now, target) {
            assert!(slice <= SLEEP_SLICE, "a slice must stay bounded: {slice:?}");
            now += chrono::Duration::from_std(slice).unwrap() + chrono::Duration::milliseconds(3);
            slices += 1;
            assert!(slices < 10, "the wait must terminate, not loop");
        }

        // 30s + 30s + the 14s remainder, then the overshoot ends it.
        assert_eq!(slices, 3);
        assert!(now >= target, "the loop must not exit before the target");
    }

    /// A forward clock jump past the target (NTP step, resumed container) fires
    /// immediately rather than waiting out another full period.
    #[test]
    fn cron_sleep_fires_when_the_target_already_passed() {
        let target = chrono::NaiveDate::from_ymd_opt(2026, 8, 12)
            .unwrap()
            .and_hms_opt(9, 30, 0)
            .unwrap();
        assert_eq!(sleep_slice(target, target), None);
        assert_eq!(
            sleep_slice(target + chrono::Duration::hours(2), target),
            None
        );
    }

    /// The dashboard shows these strings verbatim, so both forms have to read
    /// like something an operator recognises from their own config.
    #[test]
    fn schedule_labels_describe_both_forms() {
        assert_eq!(
            schedule_label(&Schedule::Every(std::time::Duration::from_secs(21_600))),
            "every 6h",
        );
        assert_eq!(
            schedule_label(&Schedule::Every(std::time::Duration::from_secs(90))),
            "every 1m 30s",
        );
        assert_eq!(
            schedule_label(&Schedule::Cron(Box::new(
                cron::Cron::parse("0 */4 * * *").unwrap()
            ))),
            "cron 0 */4 * * *",
        );
    }

    #[test]
    fn seconds_render_as_compact_units_without_zero_parts() {
        assert_eq!(format_secs(0), "0s");
        assert_eq!(format_secs(45), "45s");
        assert_eq!(format_secs(3_600), "1h");
        assert_eq!(format_secs(90_061), "1d 1h 1m 1s");
    }

    #[test]
    fn notification_eligibility_is_separate_from_one_shot_behavior() {
        assert!(BackupSource::Manual.sends_notifications());
        assert!(BackupSource::Scheduled.sends_notifications());
        assert!(!BackupSource::OneShot.sends_notifications());

        let options = BackupOptions {
            chat_ids: vec!["primary".into(), "archive".into()],
            recipient: None,
            no_encryption: false,
        };
        assert_eq!(
            BackupSource::Manual.notification_chat_ids(&options),
            &["primary".to_string()]
        );
        assert_eq!(
            BackupSource::Scheduled.notification_chat_ids(&options),
            &["primary".to_string(), "archive".to_string()]
        );
        assert!(BackupSource::OneShot
            .notification_chat_ids(&options)
            .is_empty());
    }

    fn upload_test_chunks(label: &str, count: usize) -> (PathBuf, Vec<PathBuf>) {
        let root =
            std::env::temp_dir().join(format!("crab-dump-upload-{label}-{}", std::process::id()));
        std::fs::remove_dir_all(&root).ok();
        std::fs::create_dir_all(&root).unwrap();
        let paths = (0..count)
            .map(|index| {
                let path = root.join(format!("chunk-{index}"));
                std::fs::write(&path, [index as u8]).unwrap();
                path
            })
            .collect();
        (root, paths)
    }

    #[test]
    fn destinations_all_receive_all_chunks_and_stats_aggregate() {
        let (root, chunks) = upload_test_chunks("all-success", 2);
        let destinations = vec!["primary".into(), "backup".into()];
        let mut stats = telegram::UploadStats::default();
        let mut completed_chunks = Vec::new();
        upload_chunks_to_destinations(
            &chunks,
            &destinations,
            |_destination, _path, stats| {
                stats.attempts += 1;
                stats.retries += 1;
                Ok(())
            },
            |index| completed_chunks.push(index),
            &mut stats,
            None,
        )
        .unwrap();
        assert_eq!(stats.attempts, 4);
        assert_eq!(stats.retries, 4);
        assert_eq!(completed_chunks, vec![0, 1]);
        assert!(chunks.iter().all(|path| !path.exists()));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn failed_destination_is_skipped_while_peer_completes() {
        let (root, chunks) = upload_test_chunks("one-fails", 3);
        let destinations = vec!["primary".into(), "backup".into()];
        let mut stats = telegram::UploadStats::default();
        let mut calls = Vec::new();
        upload_chunks_to_destinations(
            &chunks,
            &destinations,
            |destination, _path, stats| {
                calls.push(destination.to_string());
                stats.attempts += 1;
                if destination == "backup" {
                    anyhow::bail!("unavailable");
                }
                Ok(())
            },
            |_| {},
            &mut stats,
            None,
        )
        .unwrap();
        assert_eq!(stats.attempts, 4);
        assert_eq!(calls, vec!["primary", "backup", "primary", "primary"]);
        assert!(chunks.iter().all(|path| !path.exists()));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn all_destinations_failing_returns_error_after_cleanup_attempts() {
        let (root, chunks) = upload_test_chunks("all-fail", 2);
        let destinations = vec!["primary".into(), "backup".into()];
        let mut stats = telegram::UploadStats::default();
        let error = upload_chunks_to_destinations(
            &chunks,
            &destinations,
            |_destination, _path, stats| {
                stats.attempts += 1;
                anyhow::bail!("unavailable");
            },
            |_| {},
            &mut stats,
            None,
        )
        .unwrap_err();
        assert!(error.to_string().contains("no Telegram destination"));
        assert_eq!(stats.attempts, 2);
        assert!(!chunks[0].exists());
        assert!(chunks[1].exists());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn cancelled_upload_stops_before_the_next_chunk() {
        let (root, chunks) = upload_test_chunks("cancelled", 2);
        let token = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let mut stats = telegram::UploadStats::default();
        let error = upload_chunks_to_destinations(
            &chunks,
            &["primary".into()],
            |_destination, _path, _stats| Ok(()),
            |_| {},
            &mut stats,
            Some(&token),
        )
        .unwrap_err();
        assert!(is_cancelled(&error));
        assert!(chunks.iter().all(|path| path.exists()));
        std::fs::remove_dir_all(root).unwrap();
    }
}
