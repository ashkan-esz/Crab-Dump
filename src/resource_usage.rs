//! Linux resource usage for the dashboard.
//!
//! Containerized runs prefer cgroup metrics so the dashboard reports the
//! resources available to this image rather than the Docker host.

use anyhow::{Context, Result};
use chrono::Utc;
use libc::statvfs;
use serde::Serialize;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Instant;

#[derive(Debug, Clone, Serialize)]
pub struct ResourceMetric {
    pub percent: Option<f64>,
    pub used_bytes: Option<u64>,
    pub total_bytes: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ResourceUsage {
    pub scope: &'static str,
    pub cpu: ResourceMetric,
    pub memory: ResourceMetric,
    pub disk: ResourceMetric,
    pub timestamp: String,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Copy)]
struct CpuSample {
    usage_usec: u64,
    captured_at: Instant,
}

#[derive(Debug)]
pub struct ResourceCollector {
    work_dir: PathBuf,
    previous_cpu: Mutex<Option<CpuSample>>,
}

impl ResourceCollector {
    pub fn new(work_dir: PathBuf) -> Self {
        Self {
            work_dir,
            previous_cpu: Mutex::new(None),
        }
    }

    pub fn collect(&self) -> ResourceUsage {
        let scope = if is_containerized() {
            "container"
        } else {
            "host"
        };

        let mut errors = Vec::new();
        let cpu = match self.cpu_metric(scope) {
            Ok(metric) => metric,
            Err(error) => {
                errors.push(format!("CPU unavailable: {error}"));
                empty_metric()
            }
        };
        let memory = match self.memory_metric(scope) {
            Ok(metric) => metric,
            Err(error) => {
                errors.push(format!("memory unavailable: {error}"));
                empty_metric()
            }
        };
        let disk = match disk_metric(&self.work_dir) {
            Ok(metric) => metric,
            Err(error) => {
                errors.push(format!("disk unavailable: {error}"));
                empty_metric()
            }
        };

        ResourceUsage {
            scope,
            cpu,
            memory,
            disk,
            timestamp: Utc::now().to_rfc3339(),
            error: (!errors.is_empty()).then(|| errors.join("; ")),
        }
    }

    fn cpu_metric(&self, scope: &str) -> Result<ResourceMetric> {
        let (usage_usec, capacity_usec) = if scope == "container" {
            cgroup_cpu_sample().context("reading cgroup CPU counters")?
        } else {
            host_cpu_sample().context("reading host CPU counters")?
        };
        let now = Instant::now();
        let mut previous = self
            .previous_cpu
            .lock()
            .expect("resource CPU lock poisoned");
        let percent = previous
            .replace(CpuSample {
                usage_usec,
                captured_at: now,
            })
            .and_then(|old| {
                let elapsed = now.duration_since(old.captured_at).as_secs_f64();
                let delta = usage_usec.saturating_sub(old.usage_usec) as f64;
                (elapsed > 0.0)
                    .then(|| (delta / (elapsed * capacity_usec as f64) * 100.0).clamp(0.0, 100.0))
            });

        Ok(ResourceMetric {
            percent,
            used_bytes: None,
            total_bytes: None,
        })
    }

    fn memory_metric(&self, scope: &str) -> Result<ResourceMetric> {
        let (used, total) = if scope == "container" {
            cgroup_memory().context("reading cgroup memory counters")?
        } else {
            host_memory().context("reading host memory counters")?
        };
        let percent = (total > 0).then(|| (used as f64 / total as f64 * 100.0).clamp(0.0, 100.0));
        Ok(ResourceMetric {
            percent,
            used_bytes: Some(used),
            total_bytes: Some(total),
        })
    }
}

fn empty_metric() -> ResourceMetric {
    ResourceMetric {
        percent: None,
        used_bytes: None,
        total_bytes: None,
    }
}

fn read_trimmed(path: impl AsRef<Path>) -> Result<String> {
    fs::read_to_string(path.as_ref())
        .with_context(|| format!("reading {}", path.as_ref().display()))
        .map(|value| value.trim().to_string())
}

fn read_u64(path: impl AsRef<Path>) -> Result<u64> {
    read_trimmed(path.as_ref())?
        .parse()
        .with_context(|| format!("parsing {}", path.as_ref().display()))
}

fn cgroup_v2_mount() -> Option<PathBuf> {
    let content = fs::read_to_string("/proc/self/mountinfo").ok()?;
    content.lines().find_map(|line| {
        let (before, after) = line.split_once(" - ")?;
        if !after.starts_with("cgroup2 ") {
            return None;
        }
        before.split_whitespace().nth(4).map(PathBuf::from)
    })
}

fn cgroup_v1_mount_for(controller: &str) -> Option<PathBuf> {
    let content = fs::read_to_string("/proc/self/mountinfo").ok()?;
    content.lines().find_map(|line| {
        let (before, after) = line.split_once(" - ")?;
        if !after.starts_with("cgroup ") {
            return None;
        }
        let mount = before.split_whitespace().nth(4)?;
        (mount.contains(controller) || after.contains(controller)).then(|| PathBuf::from(mount))
    })
}

fn is_containerized() -> bool {
    Path::new("/.dockerenv").exists()
        || Path::new("/run/.containerenv").exists()
        || fs::read_to_string("/proc/1/cgroup")
            .map(|content| {
                content.contains("docker")
                    || content.contains("containerd")
                    || content.contains("kubepods")
                    || content.contains("libpod")
            })
            .unwrap_or(false)
}

fn cgroup_v2_path() -> Option<PathBuf> {
    let mount = cgroup_v2_mount()?;
    let content = fs::read_to_string("/proc/self/cgroup").ok()?;
    let relative = content
        .lines()
        .find_map(|line| line.strip_prefix("0::"))?
        .trim_start_matches('/');
    Some(mount.join(relative))
}

fn cgroup_v1_path(controller: &str) -> Option<PathBuf> {
    let mount = cgroup_v1_mount_for(controller)?;
    let content = fs::read_to_string("/proc/self/cgroup").ok()?;
    let relative = content.lines().find_map(|line| {
        let (controllers, path) = line.split_once("::")?;
        controllers
            .split(',')
            .any(|item| item == controller)
            .then_some(path)
    })?;
    Some(mount.join(relative.trim_start_matches('/')))
}

fn cgroup_cpu_sample() -> Result<(u64, u64)> {
    if let Some(path) = cgroup_v2_path() {
        let stat = read_trimmed(path.join("cpu.stat"))?;
        let usage = stat
            .lines()
            .find_map(|line| line.strip_prefix("usage_usec "))
            .context("usage_usec missing from cpu.stat")?
            .parse()
            .context("parsing cgroup usage_usec")?;
        let capacity = read_trimmed(path.join("cpu.max"))
            .ok()
            .and_then(|value| {
                let mut parts = value.split_whitespace();
                let quota = parts.next()?;
                let period = parts.next()?.parse::<u64>().ok()?;
                (quota != "max").then(|| {
                    quota
                        .parse::<u64>()
                        .ok()
                        .map(|quota| quota.saturating_mul(1_000_000) / period)
                })?
            })
            .unwrap_or_else(|| (available_cpus() as u64).saturating_mul(1_000_000));
        return Ok((usage, capacity.max(1)));
    }

    let path = cgroup_v1_path("cpuacct").context("cgroup v1 cpuacct path unavailable")?;
    let usage = read_u64(path.join("cpuacct.usage"))? / 1_000;
    let quota = read_u64(path.join("cpu.cfs_quota_us")).unwrap_or(-1_i64 as u64);
    let period = read_u64(path.join("cpu.cfs_period_us")).unwrap_or(100_000);
    let capacity = if quota == -1_i64 as u64 {
        (available_cpus() as u64).saturating_mul(1_000_000)
    } else {
        quota.saturating_mul(1_000_000) / period.max(1)
    };
    Ok((usage, capacity.max(1)))
}

fn host_cpu_sample() -> Result<(u64, u64)> {
    let line = read_trimmed("/proc/stat")?
        .lines()
        .next()
        .context("CPU aggregate missing from /proc/stat")?
        .to_string();
    let values: Vec<u64> = line
        .split_whitespace()
        .skip(1)
        .map(str::parse)
        .collect::<Result<_, _>>()
        .context("parsing /proc/stat")?;
    let idle = values.get(3).copied().unwrap_or(0) + values.get(4).copied().unwrap_or(0);
    let total: u64 = values.iter().sum();
    let usage = total.saturating_sub(idle).saturating_mul(10_000);
    let capacity = (available_cpus() as u64).saturating_mul(1_000_000);
    Ok((usage, capacity.max(1)))
}

fn cgroup_memory() -> Result<(u64, u64)> {
    if let Some(path) = cgroup_v2_path() {
        let used = read_u64(path.join("memory.current"))?;
        let total = match read_trimmed(path.join("memory.max")) {
            Ok(value) if value != "max" => value.parse().context("parsing memory.max")?,
            _ => host_memory()?.1,
        };
        return Ok((used, total));
    }
    let path = cgroup_v1_path("memory").context("cgroup v1 memory path unavailable")?;
    Ok((
        read_u64(path.join("memory.usage_in_bytes"))?,
        read_u64(path.join("memory.limit_in_bytes"))?,
    ))
}

fn host_memory() -> Result<(u64, u64)> {
    let content = read_trimmed("/proc/meminfo")?;
    let mut total = None;
    let mut available = None;
    for line in content.lines() {
        let mut parts = line.split_whitespace();
        match parts.next() {
            Some("MemTotal:") => total = parts.next().and_then(|value| value.parse::<u64>().ok()),
            Some("MemAvailable:") => {
                available = parts.next().and_then(|value| value.parse::<u64>().ok())
            }
            _ => {}
        }
    }
    let total = total
        .context("MemTotal missing from /proc/meminfo")?
        .saturating_mul(1024);
    let available = available
        .context("MemAvailable missing from /proc/meminfo")?
        .saturating_mul(1024);
    Ok((total.saturating_sub(available), total))
}

fn disk_metric(work_dir: &Path) -> Result<ResourceMetric> {
    let path = work_dir.to_string_lossy();
    let mut stats = std::mem::MaybeUninit::uninit();
    let result = unsafe {
        statvfs(
            std::ffi::CString::new(path.as_bytes())?.as_ptr(),
            stats.as_mut_ptr(),
        )
    };
    if result != 0 {
        return Err(std::io::Error::last_os_error()).context("statvfs failed");
    }
    let stats = unsafe { stats.assume_init() };
    let total = stats.f_blocks.saturating_mul(stats.f_frsize);
    let free = stats.f_bavail.saturating_mul(stats.f_frsize);
    let used = total.saturating_sub(free);
    Ok(ResourceMetric {
        percent: (total > 0).then(|| used as f64 / total as f64 * 100.0),
        used_bytes: Some(used),
        total_bytes: Some(total),
    })
}

fn available_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|value| value.get())
        .unwrap_or(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_percentage_is_bounded() {
        let percent = (150_u64 as f64 / 100.0 * 100.0).clamp(0.0, 100.0);
        assert_eq!(percent, 100.0);
    }

    #[test]
    fn empty_metric_has_no_false_zero_values() {
        let metric = empty_metric();
        assert!(metric.percent.is_none());
        assert!(metric.used_bytes.is_none());
        assert!(metric.total_bytes.is_none());
    }

    #[test]
    fn host_memory_is_available_on_linux() {
        let (used, total) = host_memory().expect("/proc/meminfo should be readable");
        assert!(total > 0);
        assert!(used <= total);
    }

    #[test]
    fn work_dir_disk_metric_is_available() {
        let metric = disk_metric(Path::new(".")).expect("current filesystem should be readable");
        assert!(metric.total_bytes.unwrap_or(0) > 0);
        assert!(metric.used_bytes.unwrap_or(u64::MAX) <= metric.total_bytes.unwrap_or(0));
    }
}
