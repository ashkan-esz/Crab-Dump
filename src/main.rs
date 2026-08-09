//! crab-dump: stream compressed, optionally encrypted Postgres dumps to Telegram.
//!
//! Multi-database aware: spawns independent pipelines per configured server,
//! each running `pg_dump → zstd → age? → chunk → upload` concurrently.

use std::io::{Read, Write};
use std::path::PathBuf;
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
    if cli.dry_run {
        let suffix = if databases.len() == 1 { "" } else { "s" };
        println!(
            "dry-run OK — {} database{} configured, dashboard on http://127.0.0.1:{dashboard_port}",
            databases.len(),
            suffix,
        );

        // Keep the main thread alive so the web server stays responsive.
        // The web server runs in a background thread with its own runtime.
        loop {
            std::thread::sleep(std::time::Duration::from_secs(1));
        }
    }

    // ── Execute backups ─────────────────────────────────────────────────────
    let results = execute_all_databases(&shared_cfg, &databases, cli.no_encryption)
        .context("running database backups")?;

    // ── Print consolidated manifest ─────────────────────────────────────────
    print_manifest(&results);

    // ── Cleanup temp chunks on success ──────────────────────────────────────
    for r in &results {
        chunk::cleanup(&r.chunk_paths);
    }

    tracing::info!(successes = results.len(), "backup complete");
    Ok(())
}

/// Result produced by a single database backup pipeline.
///
/// Holds metadata needed for the consolidated manifest output.
struct BackupResult {
    /// Human-readable display name of the database.
    db_name: String,
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

/// Execute the full backup pipeline for a single database.
///
/// Steps:
/// 1. Spawn `pg_dump` for this database with configured extra args.
/// 2. Stream stdout through `[zstd → age?] → ChunkWriter` producing `.partNN` files.
/// 3. Upload each chunk to Telegram (with retries).
/// 4. Return a [`BackupResult`] with metadata.
///
/// Reports "running" / "done" / "error" status to the dashboard at each phase.
fn run_database(
    cfg: &SharedConfig,
    db: &DatabaseConfig,
    no_encryption: bool,
) -> Result<BackupResult> {
    let started = SystemTime::now();
    let db_name = db.display_name();

    // Namespaced prefix prevents collisions when multiple databases share
    // the same working directory.  Format: `db-{name}-{YYYYMMDD-HHMMSS}`.
    let stamp = ymdhms(started)?;
    let base_name = format!("db-{db_name}-{stamp}");

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
/// Runs each database backup as a separate OS thread via `std::thread::spawn`.
/// Individual failures do not cancel other databases (failure policy: continue).
/// The operation fails only when ALL databases fail.
fn execute_all_databases(
    cfg: &SharedConfig,
    databases: &[DatabaseConfig],
    no_encryption: bool,
) -> Result<Vec<BackupResult>> {
    // Fast path: single database skips threading overhead.
    if databases.len() == 1 {
        let db_name = databases[0].display_name();
        let result = run_database(cfg, &databases[0], no_encryption)
            .inspect_err(|e| web::fail_db(&db_name, e.to_string()))?;
        return Ok(vec![result]);
    }

    tracing::info!(
        count = databases.len(),
        "spawning parallel database backups"
    );

    // One thread per database — each runs the full blocking pipeline.
    let handles: Vec<_> = databases
        .iter()
        .map(|db| {
            let db = db.clone();
            let cfg = cfg.clone();
            let no_enc = no_encryption;
            std::thread::spawn(move || run_database(&cfg, &db, no_enc))
        })
        .collect();

    // Collect results, tracking successes and failures independently.
    let mut results = Vec::new();
    let mut errors: Vec<(usize, String, anyhow::Error)> = Vec::new();

    for (i, handle) in handles.into_iter().enumerate() {
        let db_name = databases[i].display_name();
        match handle.join() {
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
                errors.push((i, db_name, e));
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
                errors.push((i, db_name, anyhow::anyhow!("panicked: {panic_msg}")));
            }
        }
    }

    // Fatal only when ALL databases failed.
    if results.is_empty() && !errors.is_empty() {
        let details = errors
            .iter()
            .map(|(idx, name, e)| format!("  [{idx}] {name}: {e}"))
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

    Ok(results)
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
/// Produces a structured block containing per-database summaries and restore
/// command templates. Designed for machine-parsable consumption by operators.
fn print_manifest(results: &[BackupResult]) {
    println!("# crab-dump manifest");
    println!("servers: {}", results.len());

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

    println!();

    // Produce a restore command template for each database.
    for r in results {
        let cmd = if r.encrypted {
            format!(
                "# restore [{}]: cat {}*.part* | age -d | zstd -d | pg_restore --dbname=...",
                r.db_name, r.db_name
            )
        } else {
            format!(
                "# restore [{}]: cat {}*.part* | zstd -d | pg_restore --dbname=...",
                r.db_name, r.db_name
            )
        };
        println!("{cmd}");
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
