//! Runtime and dashboard-managed compression configuration.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use crate::compress::CompressionCodec;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CompressionSettings {
    pub codec: Option<CompressionCodec>,
    pub level: Option<i32>,
    pub checksum: Option<bool>,
}

impl CompressionSettings {
    pub fn from_parts(
        codec: Option<&str>,
        level: Option<i32>,
        checksum: Option<bool>,
    ) -> Result<Self> {
        let codec = codec.map(CompressionCodec::parse).transpose()?;
        let settings = Self {
            codec,
            level,
            checksum,
        };
        settings.validate()?;
        Ok(settings.normalized())
    }

    pub fn validate(self) -> Result<()> {
        match (self.codec, self.level, self.checksum) {
            (None, None, None) => Ok(()),
            (None, Some(_), _) => anyhow::bail!("compression level requires a codec"),
            (None, None, Some(_)) => anyhow::bail!("compression checksum requires a codec"),
            (Some(codec), None, _) => anyhow::bail!(
                "compression level is required when {} compression is enabled",
                codec.as_str()
            ),
            (Some(codec), Some(level), checksum) => {
                codec.validate_level(level)?;
                if codec != CompressionCodec::Zstd && checksum.is_some() {
                    anyhow::bail!(
                        "compression checksum is only supported with zstd, not {}",
                        codec.as_str()
                    );
                }
                Ok(())
            }
        }
    }

    fn normalized(self) -> Self {
        match self.codec {
            None => Self {
                codec: None,
                level: None,
                checksum: None,
            },
            Some(codec) => Self {
                codec: Some(codec),
                level: Some(self.level.unwrap_or_else(|| codec.default_level())),
                checksum: (codec == CompressionCodec::Zstd).then(|| self.checksum.unwrap_or(true)),
            },
        }
    }

    pub fn codec_name(self) -> &'static str {
        self.codec.map_or("none", CompressionCodec::as_str)
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct PersistedCompressionSettings {
    codec: String,
    level: Option<i32>,
    checksum: Option<bool>,
}

pub struct CompressionConfigStore {
    path: PathBuf,
    fallback: CompressionSettings,
    settings: RwLock<CompressionSettings>,
    overridden: RwLock<bool>,
}

impl CompressionConfigStore {
    pub fn load(path: PathBuf, fallback: CompressionSettings) -> Result<Arc<Self>> {
        fallback.validate()?;
        let (settings, overridden) = match fs::read_to_string(&path) {
            Ok(content) => match serde_json::from_str::<PersistedCompressionSettings>(&content) {
                Ok(saved) => {
                    let settings = CompressionSettings::from_parts(
                        (!saved.codec.eq_ignore_ascii_case("none")).then_some(saved.codec.as_str()),
                        saved.level,
                        saved.checksum,
                    )
                    .with_context(|| format!("validating {}", path.display()))?;
                    (settings, true)
                }
                Err(error) => {
                    tracing::warn!(
                        error = %error,
                        path = %path.display(),
                        "ignoring malformed dashboard compression override"
                    );
                    (fallback.normalized(), false)
                }
            },
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                (fallback.normalized(), false)
            }
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("reading dashboard compression override {}", path.display())
                })
            }
        };
        Ok(Arc::new(Self {
            path,
            fallback: fallback.normalized(),
            settings: RwLock::new(settings),
            overridden: RwLock::new(overridden),
        }))
    }

    pub fn current(&self) -> CompressionSettings {
        *self
            .settings
            .read()
            .expect("compression settings lock poisoned")
    }

    pub fn is_overridden(&self) -> bool {
        *self
            .overridden
            .read()
            .expect("compression source lock poisoned")
    }

    pub fn update(&self, settings: CompressionSettings) -> Result<CompressionSettings> {
        let settings = settings.normalized();
        settings.validate()?;
        let persisted = PersistedCompressionSettings {
            codec: settings.codec_name().to_string(),
            level: settings.level,
            checksum: settings.checksum,
        };
        let content = serde_json::to_vec_pretty(&persisted)
            .context("serializing dashboard compression override")?;
        let parent = self.path.parent().unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent).with_context(|| {
            format!("creating compression config directory {}", parent.display())
        })?;
        let temp = self.path.with_extension("json.tmp");
        {
            let mut file =
                fs::File::create(&temp).with_context(|| format!("creating {}", temp.display()))?;
            use std::io::Write;
            file.write_all(&content)
                .with_context(|| format!("writing {}", temp.display()))?;
            file.sync_all()
                .with_context(|| format!("syncing {}", temp.display()))?;
        }
        set_owner_only(&temp)?;
        fs::rename(&temp, &self.path)
            .with_context(|| format!("replacing {}", self.path.display()))?;
        set_owner_only(&self.path)?;
        *self
            .settings
            .write()
            .expect("compression settings lock poisoned") = settings;
        *self
            .overridden
            .write()
            .expect("compression source lock poisoned") = true;
        Ok(settings)
    }

    pub fn snapshot(&self) -> (CompressionSettings, bool) {
        (self.current(), self.is_overridden())
    }

    pub fn replace(&self, settings: CompressionSettings, overridden: bool) -> Result<()> {
        settings.validate()?;
        let mut current = self
            .settings
            .write()
            .expect("compression settings lock poisoned");
        let mut source = self
            .overridden
            .write()
            .expect("compression source lock poisoned");
        if overridden {
            let persisted = PersistedCompressionSettings {
                codec: settings.codec_name().to_string(),
                level: settings.level,
                checksum: settings.checksum,
            };
            let content = serde_json::to_vec_pretty(&persisted)
                .context("serializing dashboard compression override")?;
            let parent = self.path.parent().unwrap_or_else(|| Path::new("."));
            fs::create_dir_all(parent).with_context(|| {
                format!("creating compression config directory {}", parent.display())
            })?;
            let temp = self.path.with_extension("json.tmp");
            fs::write(&temp, content).with_context(|| format!("writing {}", temp.display()))?;
            set_owner_only(&temp)?;
            fs::rename(&temp, &self.path)
                .with_context(|| format!("replacing {}", self.path.display()))?;
            set_owner_only(&self.path)?;
        } else if self.path.exists() {
            fs::remove_file(&self.path)
                .with_context(|| format!("removing {}", self.path.display()))?;
        }
        *current = settings.normalized();
        *source = overridden;
        Ok(())
    }

    pub fn clear_override(&self) -> Result<()> {
        if self.path.exists() {
            fs::remove_file(&self.path)
                .with_context(|| format!("removing {}", self.path.display()))?;
        }
        *self
            .settings
            .write()
            .expect("compression settings lock poisoned") = self.fallback;
        *self
            .overridden
            .write()
            .expect("compression source lock poisoned") = false;
        Ok(())
    }
}

#[cfg(unix)]
fn set_owner_only(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
        .with_context(|| format!("restricting permissions on {}", path.display()))
}

#[cfg(not(unix))]
fn set_owner_only(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_and_normalizes_codec_settings() {
        let settings = CompressionSettings::from_parts(Some("zstd"), Some(3), None).unwrap();
        assert_eq!(settings.codec_name(), "zstd");
        assert_eq!(settings.checksum, Some(true));

        let raw = CompressionSettings::from_parts(None, None, None).unwrap();
        assert_eq!(
            raw,
            CompressionSettings {
                codec: None,
                level: None,
                checksum: None
            }
        );
    }

    #[test]
    fn rejects_codec_specific_invalid_values() {
        assert!(CompressionSettings::from_parts(Some("gzip"), Some(10), None).is_err());
        assert!(CompressionSettings::from_parts(Some("gzip"), Some(6), Some(true)).is_err());
        assert!(CompressionSettings::from_parts(None, Some(3), None).is_err());
    }

    #[test]
    fn persists_and_reloads_dashboard_override() {
        let root = std::env::temp_dir().join(format!("crab-compression-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        let path = root.join("data").join("compression-config.json");
        let fallback = CompressionSettings::from_parts(Some("zstd"), Some(3), Some(true)).unwrap();
        let store = CompressionConfigStore::load(path.clone(), fallback).unwrap();
        let updated = CompressionSettings::from_parts(Some("brotli"), Some(7), None).unwrap();
        store.update(updated).unwrap();
        let reloaded = CompressionConfigStore::load(path, fallback).unwrap();
        assert_eq!(reloaded.current(), updated);
        assert!(reloaded.is_overridden());
        let _ = fs::remove_dir_all(root);
    }
}
