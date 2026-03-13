use anyhow::{bail, Context, Result};
use chrono::Utc;
use console::style;
use indicatif::{ProgressBar, ProgressStyle};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::process::Command;

const SYNC_METADATA_FILE: &str = ".sync_metadata.json";
const CHECK_TTL_SECONDS: i64 = 3 * 3600; // 3 hours
const BUCKET_PROD: &str = "unisis-data.unisis.ch";
const BUCKET_DEV: &str = "unisis-data-dev.unisis.ch";

pub fn bucket_for(dev: bool) -> &'static str {
    if dev { BUCKET_DEV } else { BUCKET_PROD }
}

#[derive(Serialize, Deserialize, Default)]
struct SyncMetadata {
    /// Timestamp du répertoire GCS utilisé lors du dernier sync
    remote_timestamp: String,
    /// ISO 8601 datetime of the last GCS check
    #[serde(default)]
    last_checked_at: Option<String>,
}

fn default_cache_dir(dev: bool) -> Result<std::path::PathBuf> {
    super::schema::default_cache_dir(dev)
}

fn read_sync_metadata(cache: &Path) -> SyncMetadata {
    let path = cache.join(SYNC_METADATA_FILE);
    std::fs::read_to_string(&path)
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_default()
}

fn write_sync_metadata(cache: &Path, metadata: &SyncMetadata) -> Result<()> {
    let path = cache.join(SYNC_METADATA_FILE);
    let json = serde_json::to_string_pretty(metadata)?;
    std::fs::write(&path, json)?;
    Ok(())
}

/// Extrait le timestamp du chemin GCS d'un répertoire.
/// Ex: "gs://bucket/cubes/2026-03-12T231707/" -> "2026-03-12T231707"
fn extract_timestamp(dir_path: &str) -> &str {
    dir_path
        .trim_end_matches('/')
        .rsplit('/')
        .next()
        .unwrap_or(dir_path)
}

/// Fetch the latest remote timestamp from GCS.
/// Returns None on any failure (no network, gsutil missing, etc.)
fn fetch_latest_remote_timestamp(bucket: &str, prefix: &str) -> Option<String> {
    let output = Command::new("gsutil")
        .args(["ls", &format!("gs://{bucket}/{prefix}")])
        .stderr(std::process::Stdio::null())
        .output()
        .ok()?;

    if !output.status.success() {
        return None;
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let latest = stdout.lines().filter(|l| !l.is_empty()).max()?;
    Some(extract_timestamp(latest).to_string())
}

/// Check if cubes need updating. Called before schema/query/sql commands.
/// Only contacts GCS if the last check was more than CHECK_TTL_SECONDS ago.
/// Prints a warning to stderr if an update is available. Silently ignores errors.
pub fn check_for_updates(dev: bool) {
    let cache = match default_cache_dir(dev) {
        Ok(c) => c,
        Err(_) => return,
    };

    let mut meta = read_sync_metadata(&cache);

    // Check TTL
    if let Some(ref last_checked) = meta.last_checked_at {
        if let Ok(last) = chrono::DateTime::parse_from_rfc3339(last_checked) {
            let elapsed = Utc::now().signed_duration_since(last);
            if elapsed.num_seconds() < CHECK_TTL_SECONDS {
                return;
            }
        }
    }

    // TTL expired — check GCS
    let remote_ts = match fetch_latest_remote_timestamp(bucket_for(dev), "cubes/") {
        Some(ts) => ts,
        None => return, // no network or gsutil error — skip silently
    };

    // Update last_checked_at regardless of result
    meta.last_checked_at = Some(Utc::now().to_rfc3339());
    let _ = write_sync_metadata(&cache, &meta);

    // Compare
    let sync_cmd = if dev { "cube --dev sync" } else { "cube sync" };
    if !meta.remote_timestamp.is_empty() && remote_ts != meta.remote_timestamp {
        eprintln!(
            "cube: mise à jour disponible ({} → {}). Exécutez '{sync_cmd}' pour télécharger.",
            meta.remote_timestamp, remote_ts
        );
    } else if meta.remote_timestamp.is_empty() {
        eprintln!(
            "cube: des cubes sont disponibles sur GCS. Exécutez '{sync_cmd}' pour télécharger."
        );
    }
}

pub fn run(bucket: &str, prefix: &str, cache_dir: Option<&Path>, force: bool) -> Result<()> {
    let dev = bucket == BUCKET_DEV;
    let cache = match cache_dir {
        Some(p) => p.to_path_buf(),
        None => default_cache_dir(dev)?,
    };
    std::fs::create_dir_all(&cache)?;

    let mut meta = read_sync_metadata(&cache);

    eprintln!("Listing gs://{bucket}/{prefix} ...");

    let output = Command::new("gsutil")
        .args(["ls", &format!("gs://{bucket}/{prefix}")])
        .output()
        .context("gsutil introuvable. Installez le SDK Google Cloud et exécutez 'gcloud auth application-default login'.")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("gsutil ls a échoué : {stderr}");
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let dirs: Vec<&str> = stdout.lines().filter(|l| !l.is_empty()).collect();

    if dirs.is_empty() {
        bail!("Aucun objet trouvé sous gs://{bucket}/{prefix}");
    }

    let latest = dirs
        .iter()
        .max()
        .context("Aucun répertoire trouvé")?;

    let remote_ts = extract_timestamp(latest).to_string();
    eprintln!("Répertoire le plus récent : {latest} (timestamp: {remote_ts})");

    if !force && meta.remote_timestamp == remote_ts {
        eprintln!("Le cache est à jour (timestamp: {remote_ts}).");
        meta.last_checked_at = Some(Utc::now().to_rfc3339());
        write_sync_metadata(&cache, &meta)?;
        return Ok(());
    }

    if force && meta.remote_timestamp == remote_ts {
        eprintln!("Re-synchronisation forcée (timestamp: {remote_ts}).");
    } else {
        eprintln!(
            "Mise à jour détectée : {} → {}",
            if meta.remote_timestamp.is_empty() {
                "(aucun)"
            } else {
                &meta.remote_timestamp
            },
            remote_ts
        );
    }

    // gsutil rsync compares hashes and only downloads changed files.
    // -m: parallel transfers, -d: delete local files not in remote, -x: exclude metadata file
    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::with_template("{prefix:.bold} {spinner:.cyan} {msg}")
            .unwrap()
            .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏ "),
    );
    pb.set_prefix("sync");
    pb.set_message("rsync en cours...");
    pb.enable_steady_tick(std::time::Duration::from_millis(100));

    let source = format!("{latest}");
    let exclude_pattern = format!(r"^\.sync_metadata\.json$");

    let rsync = Command::new("gsutil")
        .args([
            "-m", "-q", "rsync",
            "-d",
            "-x", &exclude_pattern,
            &source,
            &cache.to_string_lossy(),
        ])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .output()?;

    pb.finish_and_clear();

    if !rsync.status.success() {
        let stderr = String::from_utf8_lossy(&rsync.stderr);
        bail!("gsutil rsync a échoué : {stderr}");
    }

    // Count .sqlite files in cache
    let cube_count = std::fs::read_dir(&cache)?
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path()
                .extension()
                .and_then(|ext| ext.to_str())
                == Some("sqlite")
        })
        .count();

    meta.remote_timestamp = remote_ts;
    meta.last_checked_at = Some(Utc::now().to_rfc3339());
    write_sync_metadata(&cache, &meta)?;

    eprintln!(
        "{} Synchronisation terminée ({} cubes). Cache : {}",
        style("✓").green().bold(),
        cube_count,
        cache.display()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_extract_timestamp() {
        assert_eq!(
            extract_timestamp("gs://bucket/cubes/2026-03-12T231707/"),
            "2026-03-12T231707"
        );
        assert_eq!(
            extract_timestamp("gs://bucket/cubes/2026-03-15T100000/"),
            "2026-03-15T100000"
        );
        assert_eq!(extract_timestamp("simple"), "simple");
    }

    #[test]
    fn test_sync_metadata_roundtrip() {
        let tmp = TempDir::new().unwrap();
        let meta = SyncMetadata {
            remote_timestamp: "2026-03-12T231707".to_string(),
            last_checked_at: Some("2026-03-12T23:17:07+00:00".to_string()),
        };

        write_sync_metadata(tmp.path(), &meta).unwrap();
        let loaded = read_sync_metadata(tmp.path());

        assert_eq!(loaded.remote_timestamp, "2026-03-12T231707");
        assert_eq!(loaded.last_checked_at.unwrap(), "2026-03-12T23:17:07+00:00");
    }

    #[test]
    fn test_sync_metadata_missing_last_checked() {
        let tmp = TempDir::new().unwrap();
        let json = r#"{"remote_timestamp": "ts1"}"#;
        std::fs::write(tmp.path().join(SYNC_METADATA_FILE), json).unwrap();
        let meta = read_sync_metadata(tmp.path());
        assert_eq!(meta.remote_timestamp, "ts1");
        assert!(meta.last_checked_at.is_none());
    }

    #[test]
    fn test_read_sync_metadata_missing_file() {
        let tmp = TempDir::new().unwrap();
        let meta = read_sync_metadata(tmp.path());
        assert!(meta.remote_timestamp.is_empty());
        assert!(meta.last_checked_at.is_none());
    }

    #[test]
    fn test_read_sync_metadata_corrupted() {
        let tmp = TempDir::new().unwrap();
        std::fs::write(tmp.path().join(SYNC_METADATA_FILE), "not json").unwrap();
        let meta = read_sync_metadata(tmp.path());
        assert!(meta.remote_timestamp.is_empty());
    }

    #[test]
    fn test_bucket_for() {
        assert_eq!(bucket_for(false), "unisis-data.unisis.ch");
        assert_eq!(bucket_for(true), "unisis-data-dev.unisis.ch");
    }
}
