//! Spawns `pg_dump` and exposes its stdout as a reader.

use std::io::{self, Read};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};

use crate::config::parse_pg_dump_extra_args;

/// A live `pg_dump` child plus its piped stdout.
///
/// `stdout` is the byte source; `child` must be kept alive until the stream
/// is fully consumed, then its exit status checked via [`DumpPipe::finish`].
pub struct DumpPipe {
    pub stdout: Box<dyn Read + Send>,
    child: Arc<Mutex<Child>>,
    timed_out: Arc<AtomicBool>,
    watchdog_stop: Arc<(Mutex<bool>, Condvar)>,
    watchdog: Option<JoinHandle<()>>,
}

impl DumpPipe {
    /// Terminate and reap `pg_dump` after cancellation.
    pub fn cancel(&mut self) -> Result<()> {
        drop(std::mem::replace(&mut self.stdout, Box::new(io::empty())));
        self.stop_watchdog();
        let mut child = self
            .child
            .lock()
            .map_err(|_| anyhow!("pg_dump child lock poisoned"))?;
        if child
            .try_wait()
            .context("checking pg_dump after cancellation")?
            .is_none()
        {
            child
                .kill()
                .context("terminating pg_dump after cancellation")?;
        }
        child.wait().context("reaping pg_dump after cancellation")?;
        Ok(())
    }

    /// Wait for the child to exit and return an error if it failed.
    pub fn finish(mut self) -> Result<()> {
        // Make sure we've drained stdout so the child isn't blocked writing.
        drop(std::mem::replace(&mut self.stdout, Box::new(io::empty())));
        let status = loop {
            let status = self
                .child
                .lock()
                .map_err(|_| anyhow!("pg_dump child lock poisoned"))?
                .try_wait()
                .context("waiting for pg_dump to exit")?;
            if let Some(status) = status {
                break status;
            }
            std::thread::sleep(Duration::from_millis(25));
        };
        self.stop_watchdog();
        if self.timed_out.load(Ordering::SeqCst) {
            anyhow::bail!("pg_dump exceeded its timeout");
        }
        if !status.success() {
            anyhow::bail!("pg_dump exited with status {status}");
        }
        Ok(())
    }

    fn stop_watchdog(&mut self) {
        let (lock, wake) = &*self.watchdog_stop;
        if let Ok(mut stopped) = lock.lock() {
            *stopped = true;
            wake.notify_one();
        }
        if let Some(handle) = self.watchdog.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for DumpPipe {
    fn drop(&mut self) {
        self.stop_watchdog();
        if let Ok(mut child) = self.child.lock() {
            if child.try_wait().ok().flatten().is_none() {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
    }
}

/// Spawn `pg_dump` connected to `database_url`.
///
/// Default args: `--format=custom` (so `pg_restore` can do selective restores),
/// unless the caller supplies extra args that already specify a format.
pub fn spawn_pg_dump(
    database_url: &str,
    extra_args: Option<&str>,
    timeout: Duration,
) -> Result<DumpPipe> {
    let mut args = vec![
        "--format=custom".to_string(),
        "--no-owner".to_string(),
        "--no-privileges".to_string(),
        "--lock-wait-timeout=5000".to_string(),
    ];
    let user_args = match extra_args {
        Some(s) if !s.trim().is_empty() => parse_pg_dump_extra_args(s)?,
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

    let child = Arc::new(Mutex::new(child));
    let timed_out = Arc::new(AtomicBool::new(false));
    let watchdog_stop = Arc::new((Mutex::new(false), Condvar::new()));
    let watchdog_child = Arc::clone(&child);
    let watchdog_timed_out = Arc::clone(&timed_out);
    let watchdog_signal = Arc::clone(&watchdog_stop);
    let watchdog = std::thread::spawn(move || {
        let (lock, wake) = &*watchdog_signal;
        let Ok(stopped) = lock.lock() else { return };
        let Ok((stopped, _)) = wake.wait_timeout(stopped, timeout) else {
            return;
        };
        if !*stopped {
            let Ok(mut child) = watchdog_child.lock() else {
                return;
            };
            if child.try_wait().ok().flatten().is_none() {
                let _ = child.kill();
                watchdog_timed_out.store(true, Ordering::SeqCst);
            }
        }
    });

    Ok(DumpPipe {
        stdout: Box::new(stdout),
        child,
        timed_out,
        watchdog_stop,
        watchdog: Some(watchdog),
    })
}
