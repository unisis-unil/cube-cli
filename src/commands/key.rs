use anyhow::{bail, Context, Result};
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;

const GCS_API: &str = "https://storage.googleapis.com/storage/v1";
const KEY_OBJECT: &str = "cube-key.json";
const KEY_FILENAME: &str = ".key.json";

#[derive(Deserialize, Serialize, Clone)]
pub struct CubeKey {
    pub version: u32,
    pub key: String,
}

impl CubeKey {
    /// Decode the hex key into 32 raw bytes (v2 AES-256-GCM).
    pub fn decode_aes_key(&self) -> Result<[u8; 32]> {
        if self.version < 2 {
            bail!(
                "decode_aes_key() requires key version >= 2, got v{}",
                self.version
            );
        }
        let bytes = hex::decode(&self.key).context("Clé v2 invalide : hex mal formé")?;
        let arr: [u8; 32] = bytes.try_into().map_err(|v: Vec<u8>| {
            anyhow::anyhow!("Clé v2 invalide : {} bytes au lieu de 32", v.len())
        })?;
        Ok(arr)
    }
}

/// Fetch the encryption key from GCS bucket.
pub fn fetch_key_from_gcs(bucket: &str, token: &str) -> Result<CubeKey> {
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

    Ok(cube_key)
}

// ── File-based key storage ──────────────────────────────────────────

fn key_file_path() -> PathBuf {
    let cache_dir = dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".unisis-cube");
    cache_dir.join(KEY_FILENAME)
}

pub fn store_key(cube_key: &CubeKey) -> Result<()> {
    let path = key_file_path();

    // Ensure parent directory exists
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("Impossible de créer {}", parent.display()))?;
    }

    let json = serde_json::to_string_pretty(cube_key)?;
    fs::write(&path, &json).with_context(|| format!("Impossible d'écrire {}", path.display()))?;

    // Set permissions to 0600 (owner read/write only)
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600))?;
    }

    Ok(())
}

pub fn read_key() -> Result<CubeKey> {
    let path = key_file_path();
    let json = fs::read_to_string(&path).with_context(|| {
        format!(
            "Clé introuvable ({}). Exécutez 'cube sync' ou 'cube key --refresh'.",
            path.display()
        )
    })?;
    let cube_key: CubeKey =
        serde_json::from_str(&json).context("Format invalide pour .key.json")?;
    Ok(cube_key)
}

pub fn delete_key() -> Result<()> {
    let path = key_file_path();
    if path.exists() {
        fs::remove_file(&path)
            .with_context(|| format!("Impossible de supprimer {}", path.display()))?;
    }
    Ok(())
}

pub fn has_key() -> bool {
    key_file_path().exists()
}

// ── Common ──────────────────────────────────────────────────────────

/// Run the `cube key` command.
pub fn run(refresh: bool, delete: bool, dev: bool) -> Result<()> {
    if delete {
        delete_key()?;
        eprintln!("Clé supprimée.");
        return Ok(());
    }

    if refresh {
        let bucket = super::sync::bucket_for(dev);
        let token = super::sync::get_access_token()?;
        let cube_key = fetch_key_from_gcs(bucket, &token)?;
        let version = cube_key.version;
        store_key(&cube_key)?;
        eprintln!(
            "🔑 Clé v{version} stockée dans {}.",
            key_file_path().display()
        );
        return Ok(());
    }

    // Status
    if has_key() {
        match read_key() {
            Ok(k) => eprintln!(
                "Clé de chiffrement : v{} ({})",
                k.version,
                key_file_path().display()
            ),
            Err(e) => eprintln!("Clé présente mais illisible : {e}"),
        }
    } else {
        eprintln!("Clé de chiffrement : absente.");
        eprintln!("Exécutez 'cube key --refresh' ou 'cube sync' pour récupérer la clé.");
    }
    Ok(())
}
