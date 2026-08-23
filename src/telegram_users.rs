//! Persistent, dashboard-managed Telegram user directory.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TelegramUser {
    pub name: String,
    pub chat_id: String,
    pub enabled: bool,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct TelegramUsersFile {
    #[serde(default)]
    users: Vec<TelegramUser>,
}

#[derive(Debug)]
pub struct TelegramUserStore {
    path: PathBuf,
    users: RwLock<Vec<TelegramUser>>,
}

impl TelegramUserStore {
    pub fn load(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let users = match fs::read_to_string(&path) {
            Ok(content) => {
                toml::from_str::<TelegramUsersFile>(&content)
                    .with_context(|| format!("parsing Telegram users file {}", path.display()))?
                    .users
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("reading Telegram users file {}", path.display()))
            }
        };
        validate_users(&users)?;
        Ok(Self {
            path,
            users: RwLock::new(users),
        })
    }

    pub fn list(&self) -> Vec<TelegramUser> {
        self.users
            .read()
            .expect("Telegram users lock poisoned")
            .clone()
    }

    pub fn create(&self, user: TelegramUser) -> Result<()> {
        let mut users = self.users.write().expect("Telegram users lock poisoned");
        validate_user(&user)?;
        if users
            .iter()
            .any(|existing| existing.chat_id == user.chat_id)
        {
            anyhow::bail!("chat ID already exists");
        }
        let mut next = users.clone();
        next.push(user);
        persist(&self.path, &next)?;
        *users = next;
        Ok(())
    }

    pub fn update(&self, chat_id: &str, replacement: TelegramUser) -> Result<bool> {
        let mut users = self.users.write().expect("Telegram users lock poisoned");
        validate_user(&replacement)?;
        let Some(index) = users.iter().position(|user| user.chat_id == chat_id) else {
            return Ok(false);
        };
        if replacement.chat_id != chat_id
            && users.iter().any(|user| user.chat_id == replacement.chat_id)
        {
            anyhow::bail!("chat ID already exists");
        }
        let mut next = users.clone();
        next[index] = replacement;
        persist(&self.path, &next)?;
        *users = next;
        Ok(true)
    }

    pub fn delete(&self, chat_id: &str) -> Result<bool> {
        let mut users = self.users.write().expect("Telegram users lock poisoned");
        let original_len = users.len();
        let next = users
            .iter()
            .filter(|user| user.chat_id != chat_id)
            .cloned()
            .collect::<Vec<_>>();
        if next.len() == original_len {
            return Ok(false);
        }
        persist(&self.path, &next)?;
        *users = next;
        Ok(true)
    }

    pub fn replace(&self, users: Vec<TelegramUser>) -> Result<()> {
        validate_users(&users)?;
        let mut current = self.users.write().expect("Telegram users lock poisoned");
        persist(&self.path, &users)?;
        *current = users;
        Ok(())
    }
}

fn validate_user(user: &TelegramUser) -> Result<()> {
    if user.name.trim().is_empty() {
        anyhow::bail!("name must not be blank");
    }
    if user.chat_id.trim().is_empty() {
        anyhow::bail!("chat ID must not be blank");
    }
    Ok(())
}

fn validate_users(users: &[TelegramUser]) -> Result<()> {
    let mut chat_ids = std::collections::HashSet::new();
    for user in users {
        validate_user(user)?;
        if !chat_ids.insert(&user.chat_id) {
            anyhow::bail!("chat ID already exists");
        }
    }
    Ok(())
}

fn persist(path: &Path, users: &[TelegramUser]) -> Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)
        .with_context(|| format!("creating Telegram users directory {}", parent.display()))?;
    let content = toml::to_string_pretty(&TelegramUsersFile {
        users: users.to_vec(),
    })
    .context("serializing Telegram users")?;
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    let temp_path = parent.join(format!(".telegram_users.{nonce}.tmp"));
    let mut temp = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp_path)
        .with_context(|| {
            format!(
                "creating temporary Telegram users file in {}",
                parent.display()
            )
        })?;
    temp.write_all(content.as_bytes())
        .context("writing temporary Telegram users file")?;
    temp.sync_all()
        .context("flushing temporary Telegram users file")?;
    drop(temp);
    if let Err(error) = fs::rename(&temp_path, path) {
        let _ = fs::remove_file(&temp_path);
        return Err(error).with_context(|| {
            format!(
                "atomically replacing Telegram users file {}",
                path.display()
            )
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn user(id: &str) -> TelegramUser {
        TelegramUser {
            name: "Alice".into(),
            chat_id: id.into(),
            enabled: true,
        }
    }

    #[test]
    fn load_save_round_trip() {
        let dir = std::env::temp_dir().join(format!("crab-users-{}-roundtrip", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let path = dir.join("data").join("../data/telegram_users.toml");
        let _ = fs::remove_file(&path);
        let store = TelegramUserStore::load(&path).unwrap();
        store.create(user("-1")).unwrap();
        let loaded = TelegramUserStore::load(&path).unwrap();
        assert_eq!(loaded.list(), vec![user("-1")]);
        assert!(path.exists());
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn blank_fields_and_duplicates_are_rejected() {
        let dir = std::env::temp_dir().join(format!("crab-users-{}", std::process::id()));
        let _ = fs::create_dir_all(&dir);
        let path = dir.join("users-blank.toml");
        let _ = fs::remove_file(&path);
        let store = TelegramUserStore::load(path).unwrap();
        assert!(store
            .create(TelegramUser {
                name: " ".into(),
                ..user("-1")
            })
            .is_err());
        assert!(store
            .create(TelegramUser {
                chat_id: " ".into(),
                ..user("-1")
            })
            .is_err());
        store.create(user("-1")).unwrap();
        assert!(store.create(user("-1")).is_err());
    }

    #[test]
    fn enabled_flag_updates() {
        let dir = std::env::temp_dir().join(format!("crab-users-{}", std::process::id()));
        let _ = fs::create_dir_all(&dir);
        let path = dir.join("users-enabled.toml");
        let _ = fs::remove_file(&path);
        let store = TelegramUserStore::load(path).unwrap();
        store.create(user("-1")).unwrap();
        let mut updated = user("-1");
        updated.enabled = false;
        assert!(store.update("-1", updated).unwrap());
        assert!(!store.list()[0].enabled);
    }

    #[test]
    fn failed_persistence_does_not_change_memory() {
        let dir = std::env::temp_dir().join(format!("crab-users-{}", std::process::id()));
        let _ = fs::create_dir_all(&dir);
        let path = dir.join("users-failed.toml");
        let _ = fs::remove_file(&path);
        let store = TelegramUserStore::load(&path).unwrap();
        store.create(user("-1")).unwrap();
        let bad_parent = dir.join("missing");
        let bad_path = bad_parent.join("users.toml");
        let bad_store = TelegramUserStore::load(&bad_path).unwrap();
        let _ = fs::write(&bad_parent, b"not a directory");
        assert!(bad_store.create(user("-2")).is_err());
        assert!(bad_store.list().is_empty());
        assert_eq!(store.list().len(), 1);
        let _ = fs::remove_file(bad_parent);
    }
}
