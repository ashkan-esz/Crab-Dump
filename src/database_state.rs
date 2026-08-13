//! Persistent enablement state for configured databases.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

#[derive(Debug)]
pub struct DatabaseStateStore {
    path: PathBuf,
    states: Mutex<HashMap<String, bool>>,
}

#[derive(Debug, Serialize, Deserialize)]
struct StateFile {
    databases: HashMap<String, bool>,
}

impl DatabaseStateStore {
    pub fn load(history_dir: impl Into<PathBuf>, configured_names: &[String]) -> Self {
        let path = history_dir.into().join("database-state.json");
        let states = match fs::read_to_string(&path) {
            Ok(contents) => match serde_json::from_str::<StateFile>(&contents) {
                Ok(file) => filter_known(file.databases, configured_names),
                Err(error) => {
                    tracing::warn!(path = %path.display(), error = %error, "malformed database state; defaulting databases to enabled");
                    HashMap::new()
                }
            },
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => HashMap::new(),
            Err(error) => {
                tracing::warn!(path = %path.display(), error = %error, "cannot read database state; defaulting databases to enabled");
                HashMap::new()
            }
        };
        Self {
            path,
            states: Mutex::new(states),
        }
    }

    pub fn is_enabled(&self, name: &str) -> bool {
        self.states
            .lock()
            .expect("database state lock poisoned")
            .get(name)
            .copied()
            .unwrap_or(true)
    }

    pub fn set_enabled(&self, name: &str, enabled: bool) -> Result<()> {
        let mut states = self.states.lock().expect("database state lock poisoned");
        states.insert(name.to_string(), enabled);
        self.persist(&states)
    }

    fn persist(&self, states: &HashMap<String, bool>) -> Result<()> {
        fs::create_dir_all(self.path.parent().unwrap_or_else(|| Path::new("."))).with_context(
            || format!("creating database state directory {}", self.path.display()),
        )?;
        let tmp = self.path.with_extension("json.tmp");
        let data = serde_json::to_vec_pretty(&StateFile {
            databases: states.clone(),
        })
        .context("serializing database state")?;
        fs::write(&tmp, data)
            .with_context(|| format!("writing temporary database state {}", tmp.display()))?;
        fs::rename(&tmp, &self.path).with_context(|| {
            format!(
                "atomically replacing database state {}",
                self.path.display()
            )
        })?;
        Ok(())
    }
}

fn filter_known(states: HashMap<String, bool>, names: &[String]) -> HashMap<String, bool> {
    let known = names.iter().collect::<HashSet<_>>();
    states
        .into_iter()
        .filter(|(name, _)| known.contains(name))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_and_stale_state_defaults_to_enabled() {
        let dir = std::env::temp_dir().join(format!("crab-dump-state-{}-load", std::process::id()));
        fs::remove_dir_all(&dir).ok();
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            dir.join("database-state.json"),
            r#"{"databases":{"app":false,"old":false}}"#,
        )
        .unwrap();
        let store = DatabaseStateStore::load(&dir, &["app".into(), "new".into()]);
        assert!(!store.is_enabled("app"));
        assert!(store.is_enabled("new"));
        assert!(store.is_enabled("old"));
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn save_replaces_state_atomically() {
        let dir = std::env::temp_dir().join(format!("crab-dump-state-{}-save", std::process::id()));
        fs::remove_dir_all(&dir).ok();
        let store = DatabaseStateStore::load(&dir, &["app".into()]);
        store.set_enabled("app", false).unwrap();
        let parsed: StateFile =
            serde_json::from_str(&fs::read_to_string(dir.join("database-state.json")).unwrap())
                .unwrap();
        assert_eq!(parsed.databases.get("app"), Some(&false));
        assert!(!dir.join("database-state.json.tmp").exists());
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn malformed_state_defaults_to_enabled() {
        let dir =
            std::env::temp_dir().join(format!("crab-dump-state-{}-malformed", std::process::id()));
        fs::remove_dir_all(&dir).ok();
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("database-state.json"), b"not json").unwrap();
        let store = DatabaseStateStore::load(&dir, &["app".into()]);
        assert!(store.is_enabled("app"));
        fs::remove_dir_all(dir).ok();
    }
}
