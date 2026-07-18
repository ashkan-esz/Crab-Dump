//! age encryption stage.

use std::io::Write;

use anyhow::{Context, Result};

/// Build an age [`Encryptor`] for the given X25519 recipient public key.
pub fn encryptor_for(recipient: &str) -> Result<age::Encryptor> {
    let pk: age::x25519::Recipient = recipient
        .parse()
        .map_err(|e: &str| anyhow::anyhow!("parsing age recipient `{recipient}`: {e}"))?;
    age::Encryptor::with_recipients(std::iter::once(&pk as _))
        .context("age returned no recipients (this should not happen)")
}

/// Wrap `inner` in an age [`StreamWriter`].
///
/// **Important:** the returned writer *must* be finalized with `.finish()`
/// before `inner` is closed, otherwise the trailer is not written and the
/// file is unreadable.
pub fn wrap<W: Write>(enc: age::Encryptor, inner: W) -> Result<age::stream::StreamWriter<W>> {
    enc.wrap_output(inner).context("creating age StreamWriter")
}
