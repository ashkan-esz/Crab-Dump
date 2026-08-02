//! pg-backup-tg: stream a compressed, optionally encrypted Postgres dump to Telegram.

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

use chunk::ChunkWriter;
use config::Config;

/// Stream a compressed, optionally age-encrypted PostgreSQL dump to Telegram in chunks.
#[derive(Debug, Parser)]
struct Cli {
    /// Validate config and check that pg_dump exists, but do not dump or upload.
    #[arg(long)]
    dry_run: bool,

    /// Disable age encryption for this backup, even if AGE_RECIPIENT is set.
    #[arg(long)]
    no_encryption: bool,
}

fn main() -> Result<()> {
    // RFC3339-ish timestamp for log lines.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .try_init();

    let cli = Cli::parse();
    let cfg = Config::from_env().context("loading configuration from env")?;

    if !config::pg_dump_available() {
        anyhow::bail!("`pg_dump` was not found on PATH; install the postgres client tools");
    }
    tracing::info!(work_dir = %cfg.work_dir.display(), chunk_mb = cfg.chunk_size_mb, "configuration loaded");

    if cli.dry_run {
        tracing::info!("--dry-run: configuration valid and pg_dump found; not running backup");
        println!("dry-run OK");
        return Ok(());
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
    let mut pipe =
        dump::spawn_pg_dump(&cfg.database_url, cfg.pg_dump_extra_args.as_deref())
            .context("starting pg_dump")?;

    // Innermost writer: the rolling chunk writer (final destination of bytes).
    let chunker = ChunkWriter::new(&cfg.work_dir, &base_name, cfg.chunk_size_bytes());

    // Build the zstd encoder wrapping either the age StreamWriter (encrypted)
    // or the chunker directly (plain). `sink` remembers how to finalize.
    let sink;
    let mut zstd_writer: zstd::Encoder<'static, Box<dyn std::io::Write + Send>> = match age_recipient {
        Some(recipient) => {
            tracing::info!("encryption enabled (age X25519)");
            let enc =
                encrypt::encryptor_for(recipient).context("building age encryptor")?;
            let age_writer =
                encrypt::wrap(enc, chunker).context("wrapping chunker in age StreamWriter")?;
            sink = Sink::Encrypted(age_writer);
            compress::encoder(Box::new(sink.writer())).context("building zstd encoder")?
        }
        None => {
            if no_encryption {
                tracing::warn!("--no-encryption set — dump will be compressed but NOT encrypted");
            } else {
                tracing::warn!("AGE_RECIPIENT not set — dump will be compressed but NOT encrypted");
            }
            sink = Sink::Plain(chunker);
            compress::encoder(Box::new(sink.writer())).context("building zstd encoder")?
        }
    };

    // Drive pg_dump stdout through the whole stack.
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = pipe
            .stdout
            .read(&mut buf)
            .context("reading from pg_dump stdout")?;
        if n == 0 {
            break;
        }
        zstd_writer
            .write_all(&buf[..n])
            .context("writing through pipeline")?;
    }

    // Finalize zstd first (flushes into the sink), then the sink itself.
    zstd_writer.finish().context("finalizing zstd encoder")?;
    let (chunks, hash, total_bytes) = sink.finish()?;

    // Now that stdout is drained, wait on pg_dump and confirm success.
    pipe.finish().context("pg_dump did not complete successfully")?;

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
    let mut client_builder = Client::builder()
        .timeout(std::time::Duration::from_secs(300));
    if let Some(proxy) = &cfg.socks_proxy {
        tracing::info!(proxy = %proxy, "routing Telegram traffic through SOCKS5 proxy");
        client_builder = client_builder
            .proxy(reqwest::Proxy::all(proxy).context("parsing SOCKS_PROXY URL")?);
    }
    let client = client_builder.build().context("building HTTP client")?;

    for (i, p) in chunks.iter().enumerate() {
        telegram::send_document(&client, &cfg.tg_bot_token, &cfg.tg_chat_id, p)
            .with_context(|| format!("uploading chunk {}/{}", i + 1, chunks.len()))?;
    }

    // Manifest to stdout for the receiving side.
    println!("# pg-backup-tg manifest");
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
        format!(
            "# restore: cat {base_name}.part* | age -d | zstd -d | pg_restore --dbname=..."
        )
    } else {
        format!("# restore: cat {base_name}.part* | zstd -d | pg_restore --dbname=...")
    };
    println!("{restore}");

    // Cleanup temp chunks on success.
    chunk::cleanup(&chunks);

    tracing::info!("backup complete");
    Ok(())
}

/// Finalization sink: holds either the age StreamWriter (encrypted) or the
/// bare ChunkWriter (plain), and finalizes them in the correct order.
///
/// The `writer()` method borrows the inner writer mutably so the zstd encoder
/// (built before `finish` is called) can write into it; `finish()` then drains
/// the appropriate layer and returns the chunk list + hash.
enum Sink {
    Encrypted(age::stream::StreamWriter<ChunkWriter>),
    Plain(ChunkWriter),
}

impl Sink {
    /// Borrow the innermost writable destination (the chunker or the age
    /// writer sitting on top of it) as a trait object for the zstd encoder.
    fn writer(&mut self) -> &mut (dyn std::io::Write + Send) {
        match self {
            Sink::Encrypted(w) => w,
            Sink::Plain(w) => w,
        }
    }

    /// Finalize the sink layers (zstd has already been finished by the caller)
    /// and return the chunk list, full-stream sha256, and total bytes.
    fn finish(self) -> Result<(Vec<std::path::PathBuf>, [u8; 32], u64)> {
        match self {
            Sink::Encrypted(w) => {
                let chunker = w.finish().context("finalizing age StreamWriter")?;
                chunker.finish().context("finalizing chunk writer")
            }
            Sink::Plain(chunker) => chunker.finish().context("finalizing chunk writer"),
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
    let doe = (z - era * 146_097) as i64; // [0, 146097)
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = doy - (153 * mp + 2) / 5 + 1; // [1, 31]
    let month = if mp < 10 { mp + 3 } else { mp - 9 }; // [1, 12]
    let year = if month <= 2 { y + 1 } else { y };

    Ok(format!(
        "{year:04}{month:02}{d:02}-{h:02}{m:02}{s:02}"
    ))
}
