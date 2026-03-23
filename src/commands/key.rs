use anyhow::{bail, Context, Result};
use reqwest::blocking::Client;
use serde::Deserialize;

const GCS_API: &str = "https://storage.googleapis.com/storage/v1";
const ROOT_KEY_OBJECT: &str = "cube-key.json";

#[derive(Deserialize, Clone)]
pub struct CubeKey {
    pub version: u32,
    pub key: String,
}

impl CubeKey {
    /// Decode the hex key into 32 raw bytes (v2 AES-256-CBC).
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

/// Fetch the encryption key from a specific GCS path.
pub fn fetch_key_from_gcs(bucket: &str, token: &str, key_path: &str) -> Result<CubeKey> {
    let client = Client::new();
    let encoded = urlencoding::encode(key_path);
    let url = format!("{GCS_API}/b/{bucket}/o/{encoded}?alt=media");

    let resp = client.get(&url).bearer_auth(token).send()?;

    if resp.status() == reqwest::StatusCode::NOT_FOUND {
        bail!("Clé introuvable : gs://{bucket}/{key_path}");
    }

    let cube_key: CubeKey = resp
        .error_for_status()?
        .json()
        .context("Format invalide pour cube-key.json")?;

    Ok(cube_key)
}

/// Fetch the key for a snapshot, falling back to the root key.
pub fn fetch_snapshot_key(bucket: &str, token: &str, snapshot_prefix: &str) -> Result<CubeKey> {
    let snapshot_key_path = format!("{snapshot_prefix}cube-key.json");
    fetch_key_from_gcs(bucket, token, &snapshot_key_path)
        .or_else(|_| fetch_key_from_gcs(bucket, token, ROOT_KEY_OBJECT))
        .context("Clé introuvable (ni dans le snapshot, ni à la racine du bucket)")
}

/// Clean up legacy .key.json file if it exists.
pub fn cleanup_legacy_key_file() {
    let path = dirs::home_dir()
        .unwrap_or_default()
        .join(".unisis-cube")
        .join(".key.json");
    if path.exists() {
        let _ = std::fs::remove_file(&path);
    }
}

/// Run the `cube key` command — shows key status on GCS.
pub fn run(dev: bool) -> Result<()> {
    let bucket = super::sync::bucket_for(dev);
    let token = super::sync::get_access_token()?;

    match fetch_key_from_gcs(bucket, &token, ROOT_KEY_OBJECT) {
        Ok(k) => eprintln!("Clé de chiffrement : v{} (sur GCS)", k.version),
        Err(_) => eprintln!("Aucune clé de chiffrement trouvée sur GCS."),
    }
    Ok(())
}
