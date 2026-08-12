//! Spawns `pg_dump` and exposes its stdout as a reader.

use std::io::Read;
use std::process::{Child, Command, Stdio};

use anyhow::{Context, Result};
use shell_words::split;

/// A live `pg_dump` child plus its piped stdout.
///
/// `stdout` is the byte source; `child` must be kept alive until the stream
/// is fully consumed, then its exit status checked via [`DumpPipe::finish`].
pub struct DumpPipe {
    pub stdout: Box<dyn Read + Send>,
    child: Child,
}

impl DumpPipe {
    /// Wait for the child to exit and return an error if it failed.
    pub fn finish(mut self) -> Result<()> {
        // Make sure we've drained stdout so the child isn't blocked writing.
        drop(self.stdout);
        let status = self.child.wait().context("waiting for pg_dump to exit")?;
        if !status.success() {
            anyhow::bail!("pg_dump exited with status {status}");
        }
        Ok(())
    }
}

/// Spawn `pg_dump` connected to `database_url`.
///
/// Default args: `--format=custom` (so `pg_restore` can do selective restores),
/// unless the caller supplies extra args that already specify a format.
pub fn spawn_pg_dump(database_url: &str, extra_args: Option<&str>) -> Result<DumpPipe> {
    let mut args = vec!["--format=custom".to_string(), "--no-owner".to_string()];
    let user_args = match extra_args {
        Some(s) if !s.trim().is_empty() => {
            split(s).context("PG_DUMP_EXTRA_ARGS is not valid shell syntax")?
        }
        _ => vec![],
    };
    args.extend(user_args);
    args.push("--dbname".to_string());
    args.push(database_url.to_string());

    let mut child = Command::new("pg_dump")
        .args(&args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .context("failed to spawn pg_dump (is it on PATH?)")?;

    let stdout = child
        .stdout
        .take()
        .context("pg_dump stdout was not captured")?;

    Ok(DumpPipe {
        stdout: Box::new(stdout),
        child,
    })
}
