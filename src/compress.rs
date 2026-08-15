//! Streaming compression stage.

use std::io::Write;

use anyhow::{bail, Context, Result};

/// Supported global compression codecs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionCodec {
    Zstd,
    Gzip,
    Brotli,
}

impl CompressionCodec {
    pub fn parse(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "zstd" => Ok(Self::Zstd),
            "gzip" => Ok(Self::Gzip),
            "brotli" => Ok(Self::Brotli),
            other => bail!("COMPRESSION_CODEC must be one of zstd, gzip, or brotli, got `{other}`"),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Zstd => "zstd",
            Self::Gzip => "gzip",
            Self::Brotli => "brotli",
        }
    }

    pub fn display_name(self) -> &'static str {
        self.as_str()
    }

    pub fn suffix(self) -> &'static str {
        match self {
            Self::Zstd => ".zst",
            Self::Gzip => ".gz",
            Self::Brotli => ".br",
        }
    }

    pub fn default_level(self) -> i32 {
        match self {
            Self::Zstd => 3,
            Self::Gzip => 6,
            Self::Brotli => 5,
        }
    }

    pub fn validate_level(self, level: i32) -> Result<()> {
        let valid = match self {
            Self::Zstd => (1..=22).contains(&level),
            Self::Gzip => (0..=9).contains(&level),
            Self::Brotli => (0..=11).contains(&level),
        };
        if !valid {
            bail!(
                "COMPRESSION_LEVEL must be {} for {}, got {level}",
                match self {
                    Self::Zstd => "between 1 and 22",
                    Self::Gzip => "between 0 and 9",
                    Self::Brotli => "between 0 and 11",
                },
                self.as_str()
            );
        }
        Ok(())
    }
}

/// An owned streaming compressor. Each variant returns its owned downstream
/// writer from [`Self::finish`], preserving the pipeline's constant-memory
/// finalization contract.
pub enum Encoder<'a, W: Write> {
    Zstd(zstd::Encoder<'a, W>),
    Gzip(flate2::write::GzEncoder<W>),
    Brotli(Box<brotli::CompressorWriter<W>>),
}

pub fn encoder<'a, W: Write + Send + 'a>(
    inner: W,
    codec: CompressionCodec,
    level: i32,
    checksum: bool,
) -> Result<Encoder<'a, W>> {
    codec.validate_level(level)?;
    let encoder = match codec {
        CompressionCodec::Zstd => {
            let mut encoder = zstd::Encoder::new(inner, level).context("creating zstd encoder")?;
            encoder
                .include_checksum(checksum)
                .context("configuring zstd checksum")?;
            encoder.window_log(22).context("setting zstd window log")?;
            Encoder::Zstd(encoder)
        }
        CompressionCodec::Gzip => Encoder::Gzip(flate2::write::GzEncoder::new(
            inner,
            flate2::Compression::new(level as u32),
        )),
        CompressionCodec::Brotli => Encoder::Brotli(Box::new(brotli::CompressorWriter::new(
            inner,
            64 * 1024,
            level as u32,
            22,
        ))),
    };
    Ok(encoder)
}

impl<'a, W: Write> Write for Encoder<'a, W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Self::Zstd(inner) => inner.write(buf),
            Self::Gzip(inner) => inner.write(buf),
            Self::Brotli(inner) => inner.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Self::Zstd(inner) => inner.flush(),
            Self::Gzip(inner) => inner.flush(),
            Self::Brotli(inner) => inner.flush(),
        }
    }
}

impl<'a, W: Write> Encoder<'a, W> {
    pub fn finish(self) -> Result<W> {
        match self {
            Self::Zstd(inner) => inner.finish().context("zstd finish"),
            Self::Gzip(inner) => inner.finish().context("gzip finish"),
            Self::Brotli(inner) => Ok(inner.into_inner()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    fn round_trip(codec: CompressionCodec, level: i32, checksum: bool) -> Result<()> {
        let input = b"streaming compression keeps the dump off the heap\n".repeat(10_000);
        let mut encoded = Vec::new();
        {
            let mut writer = encoder(&mut encoded, codec, level, checksum)?;
            for chunk in input.chunks(97) {
                writer.write_all(chunk)?;
            }
            writer.finish()?;
        }

        let mut decoded = Vec::new();
        match codec {
            CompressionCodec::Zstd => {
                zstd::stream::copy_decode(encoded.as_slice(), &mut decoded)?;
            }
            CompressionCodec::Gzip => {
                flate2::read::GzDecoder::new(encoded.as_slice()).read_to_end(&mut decoded)?;
            }
            CompressionCodec::Brotli => {
                brotli::Decompressor::new(encoded.as_slice(), 64 * 1024)
                    .read_to_end(&mut decoded)?;
            }
        }
        assert_eq!(decoded, input);
        Ok(())
    }

    #[test]
    fn all_codecs_round_trip_streaming_input() -> Result<()> {
        round_trip(CompressionCodec::Zstd, 3, true)?;
        round_trip(CompressionCodec::Gzip, 6, false)?;
        round_trip(CompressionCodec::Brotli, 5, false)?;
        Ok(())
    }

    #[test]
    fn zstd_levels_start_at_one() {
        assert!(CompressionCodec::Zstd.validate_level(1).is_ok());
        assert!(CompressionCodec::Zstd.validate_level(22).is_ok());
        assert!(CompressionCodec::Zstd.validate_level(0).is_err());
        assert!(CompressionCodec::Zstd.validate_level(-1).is_err());
    }
}
