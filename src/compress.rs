//! zstd compression stage.

use std::io::Write;

use anyhow::{Context, Result};

/// Compression level. zstd default is 3; 19 is the max. We use 3 for speed
/// (dumps are already fairly compressible and we care about wall time).
pub const ZSTD_LEVEL: i32 = 3;

/// Wrap `inner` in a zstd [`Encoder`]. The returned writer must be finalized
/// via `finish()` — see the orchestrator in `main.rs`.
pub fn encoder<W: Write>(inner: W) -> Result<zstd::Encoder<'static, W>> {
    let mut e = zstd::Encoder::new(inner, ZSTD_LEVEL).context("creating zstd encoder")?;
    e.include_checksum(true)
        .context("enabling zstd checksum")?;
    // 4 MiB window (2^22): plenty for dumps, bounds peak memory.
    e.window_log(22).context("setting zstd window log")?;
    Ok(e)
}
