//! crab-dump: stream a compressed, optionally encrypted Postgres dump to Telegram.

use std::io::{Read, Write};
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
use config::Config;

/// Load `.env` (`.env.local` takes precedence over `.env`) so that environment-based
/// config loading in `config.rs` picks up credentials without manual export.
fn load_env() {
    // `dotenv` returns Err if no file is found — we treat that as success because
    // env vars can also be set directly.
    if let Err(e) = dotenv::from_path(".env.local") {
        // .env.local is optional — don't warn if it's missing.
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

/// Stream a compressed, optionally age-encrypted PostgreSQL dump to Telegram in chunks.
#[derive(Debug, Parser)]
struct Cli {
    /// Validate config and check that pg_dump exists, but do not dump or upload.
    #[arg(long)]
    dry_run: bool,

    /// Disable age encryption for this run, even if AGE_RECIPIENT is set.
    #[arg(long)]
    no_encryption: bool,
}

fn main() -> Result<()> {
    // RFC3339-ish timestamp for log lines.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .try_init();

    let cli = Cli::parse();

    // Load .env / .env.local before config resolution so env vars are available.
    load_env();

    let cfg = Config::from_env().context("loading configuration from env")?;

    // Spawn the status dashboard web server in a dedicated thread with its own
    // tokio runtime (actix_web::HttpServer is not Send, so it cannot be spawned
    // on the shared tokio runtime via Handle::spawn).
    tracing::info!(port = cfg.api_port, "spawning status dashboard server");
    let dashboard_port = cfg.api_port;
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

    if !config::pg_dump_available() {
        anyhow::bail!("`pg_dump` was not found on PATH; install the postgres client tools");
    }
    tracing::info!(work_dir = %cfg.work_dir.display(), chunk_mb = cfg.chunk_size_mb, "configuration loaded");

    if cli.dry_run {
        tracing::info!(
            "--dry-run: configuration valid and pg_dump found; dashboard on http://127.0.0.1:{dashboard_port}"
        );
        println!("dry-run OK — dashboard available at http://127.0.0.1:{dashboard_port}");

        // Keep the main thread alive so the web server stays responsive.
        // The web server runs in a background thread with its own runtime.
        loop {
            std::thread::sleep(std::time::Duration::from_secs(1));
        }
    }

    if let Err(e) = run_backup(&cfg, cli.no_encryption) {
        tracing::error!(error = %e, "backup failed");
        return Err(e);
    }
    Ok(())
}

fn run_backup(cfg: &Config, no_encryption: bool) -> Result<()> {
    let started = SystemTime::now();
    let stamp = ymdhms(started)?;
    let base_name = format!("pgdump-{stamp}");
    let age_recipient = if no_encryption {
        None
    } else {
        cfg.age_recipient.as_deref()
    };
    let encrypted = age_recipient.is_some();

    // ---- streaming pipeline ----
    //   encrypted:  pg_dump | zstd | age  | chunked files
    //   plain:      pg_dump | zstd        | chunked files
    //
    // Keep the DumpPipe alive so we can check pg_dump's exit status after
    // draining stdout; a non-zero exit means the dump is incomplete.
    let mut pipe = dump::spawn_pg_dump(&cfg.database_url, cfg.pg_dump_extra_args.as_deref())
        .context("starting pg_dump")?;

    // Build the pipeline: zstd wraps age (encrypted) or chunker directly (plain).
    let mut sink = match age_recipient {
        Some(recipient) => {
            tracing::info!("encryption enabled (age X25519)");
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
                tracing::warn!("--no-encryption set — dump will be compressed but NOT encrypted");
            } else {
                tracing::warn!("AGE_RECIPIENT not set — dump will be compressed but NOT encrypted");
            }
            let chunker = ChunkWriter::new(&cfg.work_dir, &base_name, cfg.chunk_size_bytes());
            let zstd = compress::encoder(chunker).context("building zstd encoder")?;
            Sink::Plain(zstd)
        }
    };

    // Drive pg_dump stdout through the pipeline.
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = pipe
            .stdout
            .read(&mut buf)
            .context("reading from pg_dump stdout")?;
        if n == 0 {
            break;
        }
        sink.write(&buf[..n]).context("writing through pipeline")?;
    }

    // Finalize zstd and inner layers, extract the chunk list + hash.
    let (chunks, hash, total_bytes) = sink.finish().context("finalizing pipeline")?;

    // Now that stdout is drained, wait on pg_dump and confirm success.
    pipe.finish()
        .context("pg_dump did not complete successfully")?;

    let elapsed = started.elapsed().unwrap_or_default();
    tracing::info!(
        chunks = chunks.len(),
        total_bytes,
        encrypted,
        sha256 = hex::encode(hash),
        elapsed_secs = elapsed.as_secs_f64(),
        "pipeline complete; uploading"
    );

    // ---- upload ----
    let mut client_builder = Client::builder().timeout(std::time::Duration::from_secs(300));
    if let Some(proxy) = &cfg.socks_proxy {
        tracing::info!(proxy = %proxy, "routing Telegram traffic through SOCKS5 proxy");
        client_builder =
            client_builder.proxy(reqwest::Proxy::all(proxy).context("parsing SOCKS_PROXY URL")?);
    }
    let client = client_builder.build().context("building HTTP client")?;

    for (i, p) in chunks.iter().enumerate() {
        telegram::send_document(&client, &cfg.tg_bot_token, &cfg.tg_chat_id, p)
            .with_context(|| format!("uploading chunk {}/{}", i + 1, chunks.len()))?;
    }

    // Manifest to stdout for the receiving side.
    println!("# crab-dump manifest");
    println!("base:   {base_name}");
    println!("chunks: {}", chunks.len());
    println!("bytes:  {total_bytes}");
    println!("encrypted: {encrypted}");
    println!("sha256: {}", hex::encode(hash));
    println!("parts:");
    for p in &chunks {
        println!("  {}", p.file_name().unwrap().to_string_lossy());
    }
    println!();
    let restore = if encrypted {
        format!("# restore: cat {base_name}.part* | age -d | zstd -d | pg_restore --dbname=...")
    } else {
        format!("# restore: cat {base_name}.part* | zstd -d | pg_restore --dbname=...")
    };
    println!("{restore}");

    // Cleanup temp chunks on success.
    chunk::cleanup(&chunks);

    tracing::info!("backup complete");
    Ok(())
}

/// Finalization sink: owns the zstd encoder and its inner writer(s).
/// Implementing `Write` delegates to the zstd encoder for the main loop.
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
    /// zstd::Encoder::finish(self) → io::Result<InnerWriter>
    /// age::stream::StreamWriter::finish() → Result<ChunkWriter>
    /// ChunkWriter::finish() → Result<(paths, hash, total)>
    fn finish(self) -> Result<(Vec<std::path::PathBuf>, [u8; 32], u64)> {
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

fn ymdhms(t: SystemTime) -> Result<String> {
    use std::time::UNIX_EPOCH;
    let secs = t
        .duration_since(UNIX_EPOCH)
        .context("time before epoch")?
        .as_secs();
    // Convert epoch seconds to UTC Y-M-D H:M:S without pulling chrono.
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
