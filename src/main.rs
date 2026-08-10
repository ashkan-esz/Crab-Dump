//! crab-dump: stream compressed, optionally encrypted Postgres dumps to Telegram.
//!
//! Multi-database aware: spawns independent pipelines per configured server,
//! each running `pg_dump → zstd → age? → chunk → upload`, at most
//! `MAX_PARALLEL_DATABASES` of them at a time.

use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::SystemTime;

use anyhow::{Context, Result};
use clap::Parser;
use reqwest::blocking::Client;

mod chunk;
mod compress;
mod config;
mod dump;
mod encrypt;
mod telegram;
mod web;

use chunk::ChunkWriter;
use config::{Config, DatabaseConfig, SharedConfig};

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
    // resolve_databases tries three sources in descending priority:
    //   1. TOML [[databases]] arrays from config.toml
    //   2. Indexed env vars (DATABASE_URL_0, DATABASE_URL_1, …)
    //   3. Single legacy DATABASE_URL (backward compat)
    let (shared_cfg, databases) =
        Config::resolve_databases().context("loading configuration from env")?;

    // ── Spawn status dashboard (unchanged logic) ────────────────────────────
    // The dashboard runs in a dedicated thread with its own tokio runtime
    // because actix_web::HttpServer is not Send.
    let dashboard_port = shared_cfg.api_port;
    web::set_max_parallel_databases(shared_cfg.max_parallel_databases);
    tracing::info!(port = dashboard_port, "spawning status dashboard server");
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("create tokio runtime for web server");
        rt.block_on(async move {
            if let Err(e) = web::start_server(dashboard_port).await {
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
        chunk_mb = shared_cfg.chunk_size_mb,
        max_parallel = shared_cfg.max_parallel_databases,
        "configuration resolved",
    );

    for (i, db) in databases.iter().enumerate() {
        // Register up front so the dashboard lists every configured database
        // as "queued" before any pipeline starts.
        web::register_database(&db.display_name());
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

    // ── Execute backups ─────────────────────────────────────────────────────
    let (results, failures) = execute_all_databases(&shared_cfg, &databases, cli.no_encryption)
        .context("running database backups")?;

    // ── Print consolidated manifest ─────────────────────────────────────────
    print_manifest(&results, &failures);

    // ── Cleanup temp chunks on success ──────────────────────────────────────
    for r in &results {
        chunk::cleanup(&r.chunk_paths);
    }

    tracing::info!(
        successes = results.len(),
        failures = failures.len(),
        "backup complete",
    );

    // A partial run must not look like a clean one to cron/systemd.
    if !failures.is_empty() {
        anyhow::bail!(
            "{} of {} database backups failed (see the manifest above)",
            failures.len(),
            results.len() + failures.len(),
        );
    }
    Ok(())
}

/// Result produced by a single database backup pipeline.
///
/// Holds metadata needed for the consolidated manifest output.
struct BackupResult {
    /// Human-readable display name of the database.
    db_name: String,
    /// Chunk filename prefix (`db{index}-{name}-{stamp}`) — the glob stem
    /// operators need for reassembly, which `db_name` alone does not
    /// reconstruct.
    base_name: String,
    /// Paths to the temporary chunk files written to disk.
    chunk_paths: Vec<PathBuf>,
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
/// 2. Stream stdout through `[zstd → age?] → ChunkWriter` producing `.partNNNN` files.
/// 3. Upload each chunk to Telegram (with retries).
/// 4. Return a [`BackupResult`] with metadata.
///
/// Reports "running" / "done" / "error" status to the dashboard at each phase.
fn run_database(
    cfg: &SharedConfig,
    db: &DatabaseConfig,
    db_index: usize,
    no_encryption: bool,
) -> Result<BackupResult> {
    let started = SystemTime::now();
    let db_name = db.display_name();

    // Namespaced prefix prevents collisions when multiple databases share
    // the same working directory. Format: `db{index}-{name}-{YYYYMMDD-HHMMSS}`.
    // Duplicate display names are rejected at config time; the index is belt
    // and braces, so a future resolution path cannot make two pipelines write
    // the same `.partNNNN` files.
    let stamp = ymdhms(started)?;
    let base_name = format!("db{db_index}-{db_name}-{stamp}");

    // Report "running" status to the dashboard before starting heavy work.
    web::set_db_status(&db_name, 1, "dump", "Dumping PostgreSQL via pg_dump …");

    // ── Resolve encryption mode ────────────────────────────────────────────
    let age_recipient = if no_encryption {
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

    // ── Build the streaming pipeline ───────────────────────────────────────
    //   encrypted:  pg_dump | zstd | age  | chunked files
    //   plain:      pg_dump | zstd        | chunked files
    let mut sink = match age_recipient {
        Some(recipient) => {
            tracing::info!(db = db_name, "encryption enabled (age X25519)");
            let enc = encrypt::encryptor_for(recipient).context("building age encryptor")?;
            let age_writer = encrypt::wrap(
                enc,
                ChunkWriter::new(&cfg.work_dir, &base_name, cfg.chunk_size_bytes()),
            )
            .context("wrapping chunker in age StreamWriter")?;
            let zstd = compress::encoder(age_writer).context("building zstd encoder")?;
            Sink::Encrypted(zstd)
        }
        None => {
            if no_encryption {
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
            let chunker = ChunkWriter::new(&cfg.work_dir, &base_name, cfg.chunk_size_bytes());
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
        let n = pipe
            .stdout
            .read(&mut buf)
            .with_context(|| format!("reading from pg_dump stdout (db={db_name})"))?;
        if n == 0 {
            break;
        } // EOF reached
        sink.write(&buf[..n])
            .with_context(|| format!("writing through pipeline (db={db_name})"))?;
        raw_bytes += n as u64;
        if raw_bytes - reported_at >= DUMP_REPORT_EVERY {
            web::set_db_dump_bytes(&db_name, raw_bytes);
            reported_at = raw_bytes;
        }
    }
    // Exact total, including the tail below the reporting threshold.
    web::set_db_dump_bytes(&db_name, raw_bytes);

    // ── Finalize pipeline stages ───────────────────────────────────────────
    // Unwind the writer stack in reverse order:
    //   1. zstd::Encoder::finish(self) → io::Result<InnerWriter>
    //   2. age::StreamWriter::finish() → Result<ChunkWriter> (if encrypted)
    //   3. ChunkWriter::finish() → Result<(paths, hash, total)>
    web::set_db_status(
        &db_name,
        1,
        "package",
        if encrypted {
            "Flushing compression + encryption, writing chunks …"
        } else {
            "Flushing compression, writing chunks …"
        },
    );
    let (chunks, hash, total_bytes) = sink
        .finish()
        .with_context(|| format!("finalizing pipeline stages (db={db_name})"))?;

    // Now that stdout is drained, confirm pg_dump exited successfully.
    pipe.finish()
        .with_context(|| format!("pg_dump did not complete successfully (db={db_name})"))?;

    let elapsed = started.elapsed().unwrap_or_default();
    let chunks_count = chunks.len();

    // Dump + packaging done; the upload stage starts next. `total_bytes` is
    // the post-compression (and post-encryption) size — i.e. exactly what
    // goes over the wire to Telegram.
    web::set_db_status(
        &db_name,
        1,
        "upload",
        format!("Uploading to Telegram — 0/{chunks_count} chunks"),
    );
    web::set_db_transfer(&db_name, 0, total_bytes, 0.0);

    tracing::info!(
        db = db_name,
        chunks = chunks_count,
        total_bytes,
        encrypted,
        sha256 = hex::encode(hash),
        elapsed_secs = elapsed.as_secs_f64(),
        "pipeline complete; uploading",
    );

    // ── Upload chunks to Telegram ──────────────────────────────────────────
    let client = build_http_client(cfg)?;
    let upload_started = SystemTime::now();
    let mut sent_bytes: u64 = 0;
    for (i, p) in chunks.iter().enumerate() {
        telegram::send_document(&client, &cfg.tg_bot_token, &cfg.tg_chat_id, p).with_context(
            || {
                format!(
                    "uploading chunk {}/{} (db={})",
                    i + 1,
                    chunks_count,
                    db_name
                )
            },
        )?;
        // Report progress from the chunk sizes actually on disk; throughput is
        // wall-clock over the upload stage, so retries and stalls show up in it.
        sent_bytes += std::fs::metadata(p).map(|m| m.len()).unwrap_or(0);
        web::set_db_status(
            &db_name,
            1,
            "upload",
            format!("Uploading to Telegram — {}/{chunks_count} chunks", i + 1),
        );
        web::set_db_transfer(
            &db_name,
            sent_bytes,
            total_bytes,
            rate(sent_bytes, &upload_started),
        );
    }

    // Mark database as done in the dashboard.
    web::set_db_status(
        &db_name,
        0,
        "done",
        format!("Backup complete — {chunks_count} chunks uploaded"),
    );
    web::set_db_transfer(
        &db_name,
        total_bytes,
        total_bytes,
        rate(total_bytes, &upload_started),
    );

    Ok(BackupResult {
        db_name,
        base_name,
        chunk_paths: chunks,
        total_bytes,
        encrypted,
        sha256: hash,
        elapsed_secs: elapsed.as_secs_f64(),
        chunks_count,
    })
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

/// Execute backups for all configured databases.
///
/// Runs at most `cfg.max_parallel_databases` pipelines at the same time; the
/// rest wait for a free slot. Individual failures do not cancel other databases
/// (failure policy: continue). The operation fails only when ALL databases fail.
///
/// Returns the successes and the per-database failures; the caller reports both
/// and sets the exit code.
fn execute_all_databases(
    cfg: &SharedConfig,
    databases: &[DatabaseConfig],
    no_encryption: bool,
) -> Result<(Vec<BackupResult>, Vec<DatabaseFailure>)> {
    // Fast path: single database skips threading overhead.
    if databases.len() == 1 {
        let db_name = databases[0].display_name();
        let result = run_database(cfg, &databases[0], 0, no_encryption)
            .inspect_err(|e| web::fail_db(&db_name, e.to_string()))?;
        return Ok((vec![result], Vec::new()));
    }

    // Never start more workers than there is work for them.
    let workers = cfg.max_parallel_databases.min(databases.len()).max(1);
    tracing::info!(
        count = databases.len(),
        max_parallel = cfg.max_parallel_databases,
        workers,
        "spawning parallel database backups"
    );

    // Each worker runs the full blocking pipeline for one database at a time,
    // taking the next queued database as soon as it frees up. A panic inside a
    // pipeline is caught here so the remaining databases keep going and the
    // failure is still attributed to the database that caused it.
    let outcomes = run_bounded(databases, workers, |i, db| {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            run_database(cfg, db, i, no_encryption)
        }))
    });

    // Collect results, tracking successes and failures independently.
    let mut results = Vec::new();
    let mut errors: Vec<DatabaseFailure> = Vec::new();

    for (i, outcome) in outcomes.into_iter().enumerate() {
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
                web::fail_db(&db_name, e.to_string()); // keeps the failed stage
                errors.push(DatabaseFailure {
                    index: i,
                    db_name,
                    error: format!("{e:#}"),
                });
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
            }
        }
    }

    // Fatal only when ALL databases failed.
    if results.is_empty() && !errors.is_empty() {
        let details = errors
            .iter()
            .map(|f| format!("  [{}] {}: {}", f.index, f.db_name, f.error))
            .collect::<Vec<_>>()
            .join("\n");
        anyhow::bail!("All {} database backups failed:\n{details}", errors.len());
    }

    if !errors.is_empty() {
        tracing::warn!(
            successes = results.len(),
            failures = errors.len(),
            "some database backups failed (continued)",
        );
    }

    Ok((results, errors))
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

/// Build an HTTP client configured for Telegram Bot API uploads.
///
/// Applies the optional SOCKS5 proxy setting and a 5-minute request timeout
/// to accommodate large document uploads over unreliable connections.
fn build_http_client(cfg: &SharedConfig) -> Result<Client> {
    let mut builder = Client::builder().timeout(std::time::Duration::from_secs(300));
    if let Some(proxy) = &cfg.socks_proxy {
        tracing::info!(proxy = %proxy, "routing Telegram traffic through SOCKS5 proxy");
        builder = builder.proxy(reqwest::Proxy::all(proxy).context("parsing SOCKS_PROXY URL")?);
    }
    builder.build().context("building HTTP client")
}

/// Print the consolidated manifest to stdout for downstream consumers.
///
/// Produces a structured block containing per-database summaries, any
/// failures, and restore command templates. Designed for machine-parsable
/// consumption by operators.
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

    println!();

    // Produce a restore command template for each database. The glob is on
    // `base_name`, the real chunk prefix — `db_name` alone matches nothing.
    for r in results {
        println!("{}", restore_line(r));
    }
}

/// Restore command template for one database.
///
/// The glob stem must be `base_name`: it is the exact prefix
/// [`ChunkWriter`] gives the `.partNNNN` files, and nothing else on
/// [`BackupResult`] reconstructs it.
fn restore_line(r: &BackupResult) -> String {
    let decrypt = if r.encrypted { "age -d | " } else { "" };
    format!(
        "# restore [{}]: cat {}.part* | {decrypt}zstd -d | pg_restore --dbname=...",
        r.db_name, r.base_name
    )
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

/// Convert a Unix epoch timestamp to a UTC timestamp string (`YYYYMMDD-HHMMSS`).
///
/// Uses Howard Hinnant's civil date algorithm — no external crates required.
/// Deterministic across platforms; used for generating unique backup filenames.
fn ymdhms(t: SystemTime) -> Result<String> {
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

    Ok(format!("{year:04}{month:02}{d:02}-{h:02}{m:02}{s:02}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn result_with(base_name: &str, encrypted: bool) -> BackupResult {
        BackupResult {
            db_name: "mvpcore".into(),
            base_name: base_name.into(),
            chunk_paths: Vec::new(),
            total_bytes: 0,
            encrypted,
            sha256: [0u8; 32],
            elapsed_secs: 0.0,
            chunks_count: 0,
        }
    }

    /// D5: the manifest glob must match the files `ChunkWriter` actually
    /// wrote. Globbing the bare `db_name` expanded to nothing.
    #[test]
    fn restore_glob_matches_real_chunk_files() {
        let dir = std::env::temp_dir().join(format!("crab-dump-d5-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let base_name = "db0-mvpcore-20260810-004521";

        let mut w = ChunkWriter::new(&dir, base_name, 8);
        w.write_all(&[b'x'; 20]).unwrap();
        let (paths, _, _) = w.finish().unwrap();

        let line = restore_line(&result_with(base_name, false));
        let (_, glob) = line.split_once("cat ").unwrap();
        let stem = glob.split(".part*").next().unwrap();

        for p in &paths {
            let name = p.file_name().unwrap().to_str().unwrap();
            assert!(
                name.starts_with(stem),
                "manifest glob `{stem}.part*` does not match chunk `{name}`"
            );
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn restore_line_decrypts_only_when_encrypted() {
        assert!(restore_line(&result_with("db0-x-1", true)).contains("age -d | zstd -d"));
        assert!(!restore_line(&result_with("db0-x-1", false)).contains("age -d"));
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
}
