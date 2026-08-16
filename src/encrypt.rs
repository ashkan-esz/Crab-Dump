//! age encryption stage.

use std::io::Write;

use anyhow::{Context, Result};

/// The configured encryption mode. Secrets are kept inside the enum so callers
/// cannot accidentally confuse a recipient with a passphrase.
#[derive(Clone, Default, PartialEq, Eq)]
pub enum EncryptionMode {
    #[default]
    None,
    AgeRecipient(String),
    AgePassphrase(String),
}

impl EncryptionMode {
    pub fn label(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::AgeRecipient(_) => "age-recipient",
            Self::AgePassphrase(_) => "age-passphrase",
        }
    }

    pub fn encrypted(&self) -> bool {
        !matches!(self, Self::None)
    }
}

impl std::fmt::Debug for EncryptionMode {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.label())
    }
}

/// Build an age [`Encryptor`] for the given X25519 recipient public key.
pub fn encryptor_for(recipient: &str) -> Result<age::Encryptor> {
    let pk: age::x25519::Recipient = recipient
        .parse()
        .map_err(|e: &str| anyhow::anyhow!("parsing age recipient: {e}"))?;
    age::Encryptor::with_recipients(std::iter::once(&pk as _))
        .context("age returned no recipients (this should not happen)")
}

/// Build an age encryptor for the configured mode without exposing its secret
/// in errors or logs.
pub fn encryptor_for_mode(mode: &EncryptionMode) -> Result<Option<age::Encryptor>> {
    match mode {
        EncryptionMode::None => Ok(None),
        EncryptionMode::AgeRecipient(recipient) => encryptor_for(recipient).map(Some),
        EncryptionMode::AgePassphrase(passphrase) => {
            Ok(Some(age::Encryptor::with_user_passphrase(
                age::secrecy::SecretString::from(passphrase.clone()),
            )))
        }
    }
}

/// Wrap `inner` in an age [`StreamWriter`].
///
/// **Important:** the returned writer *must* be finalized with `.finish()`
/// before `inner` is closed, otherwise the trailer is not written and the
/// file is unreadable.
pub fn wrap<W: Write>(enc: age::Encryptor, inner: W) -> Result<age::stream::StreamWriter<W>> {
    enc.wrap_output(inner).context("creating age StreamWriter")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};

    const TEST_RECIPIENT: &str = "age1t7rxyev2z3rw82stdlrrepyc39nvn86l5078zqkf5uasdy86jp6svpy7pa";
    const TEST_IDENTITY: &str =
        "AGE-SECRET-KEY-1GQ9778VQXMMJVE8SK7J6VT8UJ4HDQAJUVSFCWCM02D8GEWQ72PVQ2Y5J33";

    fn round_trip(mode: EncryptionMode) {
        let plaintext = b"streamed backup payload";
        let encryptor = encryptor_for_mode(&mode).unwrap().unwrap();
        let mut ciphertext = Vec::new();
        let mut writer = wrap(encryptor, &mut ciphertext).unwrap();
        writer.write_all(plaintext).unwrap();
        writer.finish().unwrap();

        let decryptor = age::Decryptor::new(&ciphertext[..]).unwrap();
        let mut output = Vec::new();
        match mode {
            EncryptionMode::AgeRecipient(_) => {
                let identity: age::x25519::Identity = TEST_IDENTITY.parse().unwrap();
                let mut reader = decryptor
                    .decrypt(std::iter::once(&identity as &dyn age::Identity))
                    .unwrap();
                reader.read_to_end(&mut output).unwrap();
            }
            EncryptionMode::AgePassphrase(passphrase) => {
                let identity =
                    age::scrypt::Identity::new(age::secrecy::SecretString::from(passphrase));
                let mut reader = decryptor
                    .decrypt(std::iter::once(&identity as &dyn age::Identity))
                    .unwrap();
                reader.read_to_end(&mut output).unwrap();
            }
            EncryptionMode::None => unreachable!(),
        }
        assert_eq!(output, plaintext);
    }

    #[test]
    fn recipient_mode_creates_stream_writer_and_round_trips() {
        round_trip(EncryptionMode::AgeRecipient(TEST_RECIPIENT.into()));
    }

    #[test]
    fn passphrase_mode_creates_stream_writer_and_round_trips() {
        round_trip(EncryptionMode::AgePassphrase(
            "correct horse battery staple".into(),
        ));
    }
}
