//! Rolling writer that splits a byte stream into fixed-size temp files.

use std::fs::File;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use sha2::{Digest, Sha256};

/// A writer that rotates a new file every `max_bytes` bytes.
///
/// Files are created in `work_dir` and named `prefix.partNNNN`, where `NNNN`
/// is zero-padded and starts at `0000`. The rolling is transparent to the
/// caller: just [`Write::write`] the stream and call [`ChunkWriter::finish`].
///
/// The pad width matters: the documented restore path is a shell glob
/// (`cat prefix.part* | …`), which orders lexically. A width narrower than the
/// part count sorts `part100` before `part99` and silently reassembles the
/// stream out of order. Four digits covers 10000 parts — ~478 GiB at the
/// default 49 MiB chunk size; widen it before that ceiling, not after.
pub struct ChunkWriter {
    work_dir: PathBuf,
    prefix: String,
    max_bytes: u64,

    current: Option<File>,
    current_len: u64,
    index: usize,
    paths: Vec<PathBuf>,
    hasher: Sha256,
    total_written: u64,
}

impl ChunkWriter {
    /// `prefix` should not include an extension; parts get `.partNNNN` appended.
    pub fn new(work_dir: impl Into<PathBuf>, prefix: impl Into<String>, max_bytes: u64) -> Self {
        ChunkWriter {
            work_dir: work_dir.into(),
            prefix: prefix.into(),
            max_bytes,
            current: None,
            current_len: 0,
            index: 0,
            paths: Vec::new(),
            hasher: Sha256::new(),
            total_written: 0,
        }
    }

    fn next_path(&self, index: usize) -> PathBuf {
        self.work_dir
            .join(format!("{}.part{:04}", self.prefix, index))
    }

    fn roll_if_needed(&mut self) -> io::Result<()> {
        if self.current_len >= self.max_bytes {
            if let Some(f) = self.current.take() {
                drop(f);
            }
        }
        if self.current.is_none() {
            let path = self.next_path(self.index);
            let f = File::create(&path).map_err(|e| {
                io::Error::other(format!("creating chunk file {}: {e}", path.display()))
            })?;
            tracing::debug!(index = self.index, path = %path.display(), "rolling new chunk");
            self.paths.push(path.clone());
            self.current = Some(f);
            self.current_len = 0;
            self.index += 1;
        }
        Ok(())
    }

    /// Finalize: flush the last part and return the list of chunk paths,
    /// in order. Also returns the sha256 of the *entire* encrypted stream
    /// (so the receiving side can verify reassembly before decrypting).
    ///
    /// Always produces at least one chunk file (possibly empty) so that an
    /// empty stream still has a representable artifact to upload.
    pub fn finish(mut self) -> Result<(Vec<PathBuf>, [u8; 32], u64)> {
        // Ensure at least one file exists even if nothing was written.
        self.roll_if_needed().map_err(|e| anyhow::anyhow!("{e}"))?;
        if let Some(f) = self.current.take() {
            f.sync_all().context("syncing final chunk")?;
            drop(f);
        }
        let digest = self.hasher.finalize_reset();
        let mut hash = [0u8; 32];
        hash.copy_from_slice(&digest);
        // `ChunkWriter: Drop`, so we can't move `paths` out; take it instead.
        let paths = std::mem::take(&mut self.paths);
        Ok((paths, hash, self.total_written))
    }
}

impl Write for ChunkWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        // Defensive: with empty state and the file can't be created, surface
        // as io::Error so async/sync plumbing still works as expected.
        self.roll_if_needed()
            .map_err(|e| io::Error::other(e.to_string()))?;

        // Never write past the current chunk's limit.
        let room = (self.max_bytes - self.current_len) as usize;
        let n = buf.len().min(room);

        let f = self
            .current
            .as_mut()
            .expect("roll_if_needed guarantees a current file");
        let written = f.write(&buf[..n])?;
        self.current_len += written as u64;
        self.total_written += written as u64;
        self.hasher.update(&buf[..written]);
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        if let Some(f) = self.current.as_mut() {
            f.flush()?;
        }
        Ok(())
    }
}

impl Drop for ChunkWriter {
    fn drop(&mut self) {
        // Flush the active file on drop so its bytes are durable if the
        // caller aborts mid-stream. Path tracking is done eagerly in
        // roll_if_needed(), so finish() and drop() agree on the file list.
        if let Some(f) = self.current.as_mut() {
            let _ = f.sync_all();
        }
    }
}

/// Best-effort removal of a list of chunk files. Errors are logged, not fatal.
pub fn cleanup(paths: &[PathBuf]) {
    for p in paths {
        if let Err(e) = std::fs::remove_file(p) {
            if e.kind() != std::io::ErrorKind::NotFound {
                tracing::warn!(path = %p.display(), error = %e, "failed to remove chunk");
            }
        }
    }
}

/// Concatenate `paths` in order into `out`. Used in tests and as a reference
/// for the receiving side's reassembly step.
#[allow(dead_code)]
pub fn reassemble(paths: &[PathBuf], out: &Path) -> Result<()> {
    let mut o = std::fs::File::create(out).context("creating reassembled output")?;
    for p in paths {
        let mut f = std::fs::File::open(p).context("opening chunk for reassemble")?;
        std::io::copy(&mut f, &mut o).context("copying chunk")?;
    }
    o.sync_all().context("syncing reassembled output")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmpdir() -> PathBuf {
        let d = std::env::temp_dir().join(format!(
            "crab-dump-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    /// Drive a writer to consume the whole buffer, since `Write::write` is
    /// allowed to do short writes (and `ChunkWriter` deliberately caps each
    /// call to the remaining space in the current chunk).
    fn write_all<W: Write>(w: &mut W, mut buf: &[u8]) {
        while !buf.is_empty() {
            let n = w.write(buf).expect("write");
            assert!(n > 0, "writer made no progress");
            buf = &buf[n..];
        }
    }

    #[test]
    fn rolls_at_boundary_and_reassembles_exactly() {
        let dir = tmpdir();
        let max = 1024u64; // small so we exercise rolling
        let mut w = ChunkWriter::new(&dir, "blob", max);

        // Write 2.5 chunks worth of deterministic data.
        let total = (max as usize) * 5 / 2; // 2560 bytes
        let mut expected = Vec::with_capacity(total);
        let mut block = [0u8; 100];
        for (i, b) in block.iter_mut().enumerate() {
            *b = (i % 251) as u8; // prime → avoid repeating patterns
        }
        let mut written = 0;
        while written < total {
            let n = block.len().min(total - written);
            write_all(&mut w, &block[..n]);
            expected.extend_from_slice(&block[..n]);
            written += n;
        }
        w.flush().unwrap();
        let (paths, hash, total_written) = w.finish().unwrap();

        assert_eq!(paths.len(), 3, "expected 3 chunks");
        assert_eq!(total_written, total as u64);

        // Each file is at most `max` bytes.
        for p in &paths {
            let len = std::fs::metadata(p).unwrap().len();
            assert!(
                len <= max,
                "chunk {} is {} bytes (> {max})",
                p.display(),
                len
            );
        }

        // Reassembly is byte-exact and the hash matches.
        let reassembled = dir.join("reassembled");
        reassemble(&paths, &reassembled).unwrap();
        let got = std::fs::read(&reassembled).unwrap();
        assert_eq!(got, expected);

        let mut h = Sha256::new();
        h.update(&expected);
        let mut want = [0u8; 32];
        want.copy_from_slice(&h.finalize());
        assert_eq!(hash, want);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn empty_stream_produces_single_empty_chunk() {
        let dir = tmpdir();
        let w = ChunkWriter::new(&dir, "empty", 1024);
        let (paths, hash, total) = w.finish().unwrap();
        assert_eq!(paths.len(), 1);
        assert_eq!(std::fs::metadata(&paths[0]).unwrap().len(), 0);
        assert_eq!(total, 0);
        // SHA-256 of empty input is a fixed, non-zero constant.
        let mut want = [0u8; 32];
        want.copy_from_slice(&Sha256::digest(b""));
        assert_eq!(hash, want);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// The documented restore path is a shell glob, which sorts lexically, so
    /// generation order and lexical order must agree past the 100th part.
    #[test]
    fn part_names_sort_lexically_past_100() {
        let w = ChunkWriter::new("/tmp", "blob", 1);
        let names: Vec<String> = (0..1000)
            .map(|i| {
                w.next_path(i)
                    .file_name()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .to_owned()
            })
            .collect();
        let mut sorted = names.clone();
        sorted.sort();
        assert_eq!(names, sorted, "part names must sort in generation order");
    }

    #[test]
    fn exact_boundary_creates_new_chunk_on_next_write() {
        let dir = tmpdir();
        let max = 16u64;
        let mut w = ChunkWriter::new(&dir, "boundary", max);
        // Fill exactly to the boundary.
        let buf = vec![0xAA; max as usize];
        write_all(&mut w, &buf);
        // Next byte must go to a *new* file.
        write_all(&mut w, &[0xBB]);
        let (paths, _, _) = w.finish().unwrap();
        assert_eq!(paths.len(), 2);
        assert_eq!(std::fs::metadata(&paths[0]).unwrap().len(), max);
        assert_eq!(std::fs::metadata(&paths[1]).unwrap().len(), 1);
        std::fs::remove_dir_all(&dir).ok();
    }
}
