use anyhow::{bail, Context, Result};
use reqwest::blocking::Client;
use serde::Deserialize;
use std::process::Command;

const GCS_API: &str = "https://storage.googleapis.com/storage/v1";
const KEY_OBJECT: &str = "cube-key.json";
const SERVICE: &str = "cube-cli";
const ACCOUNT: &str = "encryption-key";

#[derive(Deserialize)]
struct CubeKey {
    version: u32,
    key: String,
}

/// Fetch the encryption key from GCS bucket.
pub fn fetch_key_from_gcs(bucket: &str, token: &str) -> Result<(u32, String)> {
    let client = Client::new();
    let encoded = urlencoding::encode(KEY_OBJECT);
    let url = format!("{GCS_API}/b/{bucket}/o/{encoded}?alt=media");

    let resp = client.get(&url).bearer_auth(token).send()?;

    if resp.status() == reqwest::StatusCode::NOT_FOUND {
        bail!(
            "Clé de chiffrement introuvable dans gs://{bucket}/{KEY_OBJECT}. \
             Contactez l'équipe UNISIS."
        );
    }

    let cube_key: CubeKey = resp
        .error_for_status()?
        .json()
        .context("Format invalide pour cube-key.json")?;

    Ok((cube_key.version, cube_key.key))
}

// ── Platform-specific keychain operations ───────────────────────────

#[cfg(target_os = "macos")]
pub fn store_key(key: &str) -> Result<()> {
    // Delete existing entry (ignore errors if not found)
    let _ = Command::new("security")
        .args(["delete-generic-password", "-s", SERVICE, "-a", ACCOUNT])
        .output();

    let output = Command::new("security")
        .args([
            "add-generic-password",
            "-A", // allow access from any app (no prompts)
            "-s",
            SERVICE,
            "-a",
            ACCOUNT,
            "-w",
            key,
        ])
        .output()
        .context("Impossible d'exécuter 'security'")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("Impossible de stocker la clé dans le keychain : {stderr}");
    }
    Ok(())
}

#[cfg(target_os = "macos")]
pub fn read_key() -> Result<String> {
    let output = Command::new("security")
        .args([
            "find-generic-password",
            "-s",
            SERVICE,
            "-a",
            ACCOUNT,
            "-w", // output password only
        ])
        .output()
        .context("Impossible d'exécuter 'security'")?;

    if !output.status.success() {
        bail!("Clé de chiffrement introuvable dans le keychain. Exécutez 'cube sync' ou 'cube key --refresh'.");
    }

    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

#[cfg(target_os = "macos")]
pub fn delete_key() -> Result<()> {
    let output = Command::new("security")
        .args(["delete-generic-password", "-s", SERVICE, "-a", ACCOUNT])
        .output()
        .context("Impossible d'exécuter 'security'")?;

    // Ignore "item not found" errors
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        if !stderr.contains("could not be found") {
            bail!("Impossible de supprimer la clé : {stderr}");
        }
    }
    Ok(())
}

#[cfg(target_os = "macos")]
pub fn has_key() -> bool {
    Command::new("security")
        .args(["find-generic-password", "-s", SERVICE, "-a", ACCOUNT])
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

// ── Linux: use kernel keyring (keyctl) ──────────────────────────────

#[cfg(target_os = "linux")]
pub fn store_key(key: &str) -> Result<()> {
    let entry = keyring::Entry::new(SERVICE, ACCOUNT).context("Impossible d'accéder au keyring")?;
    entry
        .set_password(key)
        .context("Impossible de stocker la clé")?;
    Ok(())
}

#[cfg(target_os = "linux")]
pub fn read_key() -> Result<String> {
    let entry = keyring::Entry::new(SERVICE, ACCOUNT).context("Impossible d'accéder au keyring")?;
    entry
        .get_password()
        .context("Clé de chiffrement introuvable. Exécutez 'cube sync' ou 'cube key --refresh'.")
}

#[cfg(target_os = "linux")]
pub fn delete_key() -> Result<()> {
    let entry = keyring::Entry::new(SERVICE, ACCOUNT).context("Impossible d'accéder au keyring")?;
    match entry.delete_credential() {
        Ok(()) => Ok(()),
        Err(keyring::Error::NoEntry) => Ok(()),
        Err(e) => Err(anyhow::anyhow!("Impossible de supprimer la clé : {e}")),
    }
}

#[cfg(target_os = "linux")]
pub fn has_key() -> bool {
    let entry = match keyring::Entry::new(SERVICE, ACCOUNT) {
        Ok(e) => e,
        Err(_) => return false,
    };
    entry.get_password().is_ok()
}

// ── Common ──────────────────────────────────────────────────────────

/// Run the `cube key` command.
pub fn run(refresh: bool, delete: bool, dev: bool) -> Result<()> {
    if delete {
        delete_key()?;
        eprintln!("Clé supprimée du keychain.");
        return Ok(());
    }

    if refresh {
        let bucket = super::sync::bucket_for(dev);
        let token = super::sync::get_access_token()?;
        let (version, key) = fetch_key_from_gcs(bucket, &token)?;
        store_key(&key)?;
        eprintln!("Clé v{version} stockée dans le keychain.");
        return Ok(());
    }

    // Status
    if has_key() {
        eprintln!("Clé de chiffrement : présente dans le keychain.");
    } else {
        eprintln!("Clé de chiffrement : absente du keychain.");
        eprintln!("Exécutez 'cube key --refresh' ou 'cube sync' pour récupérer la clé.");
    }
    Ok(())
}
