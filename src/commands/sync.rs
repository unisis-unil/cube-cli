use anyhow::{bail, Context, Result};
use base64::Engine;
use chrono::Utc;
use console::style;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use std::io::Write;
use std::path::Path;
use std::process::Command;

const SYNC_METADATA_FILE: &str = ".sync_metadata.json";
const CHECK_TTL_SECONDS: i64 = 3 * 3600; // 3 hours
const BUCKET_PROD: &str = "unisis-data.unisis.ch";
const BUCKET_DEV: &str = "unisis-data-dev.unisis.ch";
const GCS_API: &str = "https://storage.googleapis.com/storage/v1";

pub fn bucket_for(dev: bool) -> &'static str {
    if dev { BUCKET_DEV } else { BUCKET_PROD }
}

// ── Metadata ────────────────────────────────────────────────────────

#[derive(Serialize, Deserialize, Default)]
struct SyncMetadata {
    remote_timestamp: String,
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

// ── GCS API ─────────────────────────────────────────────────────────

fn get_access_token() -> Result<String> {
    let output = Command::new("gcloud")
        .args(["auth", "application-default", "print-access-token"])
        .stderr(std::process::Stdio::piped())
        .output()
        .context(
            "gcloud introuvable. Installez le SDK Google Cloud et exécutez \
             'gcloud auth application-default login'.",
        )?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!(
            "Impossible d'obtenir un token d'accès. \
             Exécutez 'gcloud auth application-default login'.\n{stderr}"
        );
    }

    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

#[derive(Deserialize)]
struct GcsListResponse {
    #[serde(default)]
    prefixes: Vec<String>,
    #[serde(default)]
    items: Vec<GcsObject>,
    #[serde(rename = "nextPageToken")]
    next_page_token: Option<String>,
}

#[derive(Deserialize, Clone)]
struct GcsObject {
    name: String,
    #[serde(deserialize_with = "deserialize_string_u64")]
    size: u64,
    crc32c: String,
}

fn deserialize_string_u64<'de, D>(deserializer: D) -> std::result::Result<u64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    s.parse().map_err(serde::de::Error::custom)
}

/// List "subdirectories" at a given prefix (using delimiter=/).
fn gcs_list_prefixes(client: &Client, token: &str, bucket: &str, prefix: &str) -> Result<Vec<String>> {
    let mut all_prefixes = Vec::new();
    let mut page_token: Option<String> = None;

    loop {
        let mut req = client
            .get(format!("{GCS_API}/b/{bucket}/o"))
            .bearer_auth(token)
            .query(&[("prefix", prefix), ("delimiter", "/")]);

        if let Some(ref pt) = page_token {
            req = req.query(&[("pageToken", pt.as_str())]);
        }

        let resp: GcsListResponse = req.send()?.error_for_status()?.json()?;
        all_prefixes.extend(resp.prefixes);

        match resp.next_page_token {
            Some(pt) => page_token = Some(pt),
            None => break,
        }
    }

    Ok(all_prefixes)
}

/// List all objects under a prefix.
fn gcs_list_objects(client: &Client, token: &str, bucket: &str, prefix: &str) -> Result<Vec<GcsObject>> {
    let mut all_items = Vec::new();
    let mut page_token: Option<String> = None;

    loop {
        let mut req = client
            .get(format!("{GCS_API}/b/{bucket}/o"))
            .bearer_auth(token)
            .query(&[("prefix", prefix)]);

        if let Some(ref pt) = page_token {
            req = req.query(&[("pageToken", pt.as_str())]);
        }

        let resp: GcsListResponse = req.send()?.error_for_status()?.json()?;
        all_items.extend(resp.items);

        match resp.next_page_token {
            Some(pt) => page_token = Some(pt),
            None => break,
        }
    }

    Ok(all_items)
}

/// Find the latest timestamp directory under prefix.
fn find_latest_timestamp(client: &Client, token: &str, bucket: &str, prefix: &str) -> Result<String> {
    let prefixes = gcs_list_prefixes(client, token, bucket, prefix)?;

    if prefixes.is_empty() {
        bail!("Aucun répertoire trouvé sous gs://{bucket}/{prefix}");
    }

    let latest = prefixes
        .iter()
        .max()
        .context("Aucun répertoire trouvé")?;

    let ts = latest.trim_end_matches('/').rsplit('/').next().unwrap_or(latest);
    Ok(ts.to_string())
}

// ── CRC32C ──────────────────────────────────────────────────────────

/// Compute CRC32C of a local file and return it base64-encoded (same format as GCS).
fn local_crc32c_b64(path: &Path) -> Result<String> {
    let data = std::fs::read(path)?;
    let hash = crc32c::crc32c(&data);
    let bytes = hash.to_be_bytes();
    Ok(base64::engine::general_purpose::STANDARD.encode(bytes))
}

// ── SQLite integrity ────────────────────────────────────────────────

/// Quick integrity check: open the file, verify the schema metadata is readable.
fn sqlite_integrity_ok(path: &Path) -> bool {
    let conn = match rusqlite::Connection::open_with_flags(
        path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    ) {
        Ok(c) => c,
        Err(_) => return false,
    };
    // Check that SQLite can read the file and the metadata table exists
    conn.query_row(
        "SELECT value FROM metadata WHERE key = 'schema'",
        [],
        |row| row.get::<_, String>(0),
    )
    .is_ok()
}

// ── Download ────────────────────────────────────────────────────────

fn download_object(
    client: &Client,
    token: &str,
    bucket: &str,
    object: &GcsObject,
    dest: &Path,
    pb: &ProgressBar,
) -> Result<()> {
    let encoded_name = urlencoding::encode(&object.name);
    let url = format!("{GCS_API}/b/{bucket}/o/{encoded_name}?alt=media");

    let mut resp = client
        .get(&url)
        .bearer_auth(token)
        .send()?
        .error_for_status()?;

    let mut file = std::fs::File::create(dest)?;
    let mut downloaded: u64 = 0;
    let mut buf = vec![0u8; 256 * 1024]; // 256 KB chunks

    loop {
        let n = std::io::Read::read(&mut resp, &mut buf)?;
        if n == 0 {
            break;
        }
        file.write_all(&buf[..n])?;
        downloaded += n as u64;
        pb.set_position(downloaded);
    }

    Ok(())
}

// ── Progress styles ─────────────────────────────────────────────────

fn style_overall() -> ProgressStyle {
    ProgressStyle::with_template(
        "{prefix:.bold.cyan} [{bar:30.cyan/dim}] {pos}/{len} cubes  {msg}",
    )
    .unwrap()
    .progress_chars("━╸─")
}

fn style_download() -> ProgressStyle {
    ProgressStyle::with_template(
        "  [{bar:25.green/dim}] {bytes}/{total_bytes} {bytes_per_sec}  {prefix}",
    )
    .unwrap()
    .progress_chars("━╸─")
}

fn style_done() -> ProgressStyle {
    ProgressStyle::with_template("  {msg} {prefix}").unwrap()
}

fn format_size(bytes: u64) -> String {
    if bytes < 1024 {
        format!("{bytes} B")
    } else if bytes < 1024 * 1024 {
        format!("{:.0} KB", bytes as f64 / 1024.0)
    } else {
        format!("{:.1} MB", bytes as f64 / (1024.0 * 1024.0))
    }
}

// ── check_for_updates ───────────────────────────────────────────────

pub fn check_for_updates(dev: bool) {
    let cache = match default_cache_dir(dev) {
        Ok(c) => c,
        Err(_) => return,
    };

    let mut meta = read_sync_metadata(&cache);

    if let Some(ref last_checked) = meta.last_checked_at {
        if let Ok(last) = chrono::DateTime::parse_from_rfc3339(last_checked) {
            let elapsed = Utc::now().signed_duration_since(last);
            if elapsed.num_seconds() < CHECK_TTL_SECONDS {
                return;
            }
        }
    }

    // Try to check via API (fast, no gsutil dependency)
    let remote_ts = match check_latest_timestamp_quiet(bucket_for(dev), "cubes/") {
        Some(ts) => ts,
        None => return,
    };

    meta.last_checked_at = Some(Utc::now().to_rfc3339());
    let _ = write_sync_metadata(&cache, &meta);

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

/// Quick timestamp check — silently returns None on any error.
fn check_latest_timestamp_quiet(bucket: &str, prefix: &str) -> Option<String> {
    let token = get_access_token().ok()?;
    let client = Client::new();
    find_latest_timestamp(&client, &token, bucket, prefix).ok()
}

// ── run ─────────────────────────────────────────────────────────────

pub fn run(bucket: &str, prefix: &str, cache_dir: Option<&Path>, force: bool) -> Result<()> {
    let dev = bucket == BUCKET_DEV;
    let cache = match cache_dir {
        Some(p) => p.to_path_buf(),
        None => default_cache_dir(dev)?,
    };
    std::fs::create_dir_all(&cache)?;

    // Clean up any leftover .tmp files from a previous interrupted sync
    if let Ok(entries) = std::fs::read_dir(&cache) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) == Some("tmp") {
                let _ = std::fs::remove_file(&path);
            }
        }
    }

    let mut meta = read_sync_metadata(&cache);

    eprintln!("Authentification...");
    let token = get_access_token()?;
    let client = Client::new();

    eprintln!("Recherche du dernier snapshot sur gs://{bucket}/{prefix} ...");
    let remote_ts = find_latest_timestamp(&client, &token, bucket, prefix)?;
    let remote_prefix = format!("{prefix}{remote_ts}/");
    eprintln!("Snapshot le plus récent : {remote_ts}");

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
            "Mise à jour : {} → {}",
            if meta.remote_timestamp.is_empty() { "(aucun)" } else { &meta.remote_timestamp },
            remote_ts
        );
    }

    // List all .sqlite objects in the latest snapshot
    let objects = gcs_list_objects(&client, &token, bucket, &remote_prefix)?;
    let sqlite_objects: Vec<GcsObject> = objects
        .into_iter()
        .filter(|o| o.name.ends_with(".sqlite"))
        .collect();

    if sqlite_objects.is_empty() {
        bail!("Aucun fichier .sqlite trouvé dans gs://{bucket}/{remote_prefix}");
    }

    // Set up multi-progress display
    let mp = MultiProgress::new();
    let overall = mp.add(ProgressBar::new(sqlite_objects.len() as u64));
    overall.set_style(style_overall());
    overall.set_prefix("sync");

    let mut downloaded: u64 = 0;
    let mut skipped: u64 = 0;
    let mut downloaded_bytes: u64 = 0;

    let remote_filenames: Vec<String> = sqlite_objects
        .iter()
        .filter_map(|o| o.name.rsplit('/').next().map(|s| s.to_string()))
        .collect();

    for obj in &sqlite_objects {
        let filename = obj.name.rsplit('/').next().unwrap_or(&obj.name);
        let display_name = filename.strip_suffix(".sqlite").unwrap_or(filename);
        let local_path = cache.join(filename);

        overall.set_message(display_name.to_string());

        // Check if local file matches remote (CRC32C comparison)
        if local_path.exists() {
            if let Ok(local_hash) = local_crc32c_b64(&local_path) {
                if local_hash == obj.crc32c {
                    let done_pb = mp.add(ProgressBar::new(0));
                    done_pb.set_style(style_done());
                    done_pb.set_prefix(display_name.to_string());
                    done_pb.finish_with_message(format!(
                        "{} à jour ({})",
                        style("✓").green(),
                        format_size(obj.size)
                    ));
                    skipped += 1;
                    overall.inc(1);
                    continue;
                }
            }
        }

        // Download to a temp file, then rename atomically to avoid partial files
        let tmp_path = cache.join(format!(".{filename}.tmp"));
        let file_pb = mp.add(ProgressBar::new(obj.size));
        file_pb.set_style(style_download());
        file_pb.set_prefix(display_name.to_string());

        if let Err(e) = download_object(&client, &token, bucket, obj, &tmp_path, &file_pb) {
            let _ = std::fs::remove_file(&tmp_path);
            file_pb.set_style(style_done());
            file_pb.finish_with_message(format!("{} erreur", style("✗").red()));
            bail!(e);
        }

        // Verify CRC32C matches the remote object
        if let Ok(local_hash) = local_crc32c_b64(&tmp_path) {
            if local_hash != obj.crc32c {
                let _ = std::fs::remove_file(&tmp_path);
                file_pb.set_style(style_done());
                file_pb.finish_with_message(format!(
                    "{} hash incorrect, ignoré",
                    style("✗").yellow()
                ));
                overall.inc(1);
                continue;
            }
        }

        // Verify SQLite integrity before accepting the file
        if !sqlite_integrity_ok(&tmp_path) {
            let _ = std::fs::remove_file(&tmp_path);
            file_pb.set_style(style_done());
            file_pb.finish_with_message(format!(
                "{} corrompu, ignoré",
                style("✗").yellow()
            ));
            overall.inc(1);
            continue;
        }

        std::fs::rename(&tmp_path, &local_path)?;

        file_pb.set_style(style_done());
        file_pb.finish_with_message(format!(
            "{} téléchargé ({})",
            style("✓").green(),
            format_size(obj.size)
        ));

        downloaded += 1;
        downloaded_bytes += obj.size;
        overall.inc(1);
    }

    overall.finish_and_clear();

    // Delete local .sqlite files not in the remote snapshot
    for entry in std::fs::read_dir(&cache)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) == Some("sqlite") {
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if !remote_filenames.contains(&name.to_string()) {
                    std::fs::remove_file(&path)?;
                    eprintln!("  {} supprimé (absent du remote)", name);
                }
            }
        }
    }

    meta.remote_timestamp = remote_ts;
    meta.last_checked_at = Some(Utc::now().to_rfc3339());
    write_sync_metadata(&cache, &meta)?;

    eprintln!(
        "\n{} Synchronisation terminée — {} téléchargé(s) ({}), {} à jour. Cache : {}",
        style("✓").green().bold(),
        downloaded,
        format_size(downloaded_bytes),
        skipped,
        cache.display()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

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

    #[test]
    fn test_local_crc32c_b64() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("test.bin");
        std::fs::write(&path, b"hello world").unwrap();
        let hash = local_crc32c_b64(&path).unwrap();
        // crc32c("hello world") = 0xc99465aa → base64 = "yZRlqg=="
        assert_eq!(hash, "yZRlqg==");
    }

    #[test]
    fn test_sqlite_integrity_ok_valid() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("valid.sqlite");
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute_batch(
            "CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT);
             INSERT INTO metadata VALUES ('schema', '{\"cube\": \"Test\"}');
             CREATE TABLE data (x TEXT, indicateur REAL);",
        )
        .unwrap();
        drop(conn);
        assert!(sqlite_integrity_ok(&path));
    }

    #[test]
    fn test_sqlite_integrity_ok_corrupted() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("bad.sqlite");
        std::fs::write(&path, b"this is not a sqlite file").unwrap();
        assert!(!sqlite_integrity_ok(&path));
    }

    #[test]
    fn test_sqlite_integrity_ok_missing_metadata() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("no_meta.sqlite");
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute_batch("CREATE TABLE data (x TEXT);").unwrap();
        drop(conn);
        assert!(!sqlite_integrity_ok(&path));
    }

    #[test]
    fn test_format_size() {
        assert_eq!(format_size(500), "500 B");
        assert_eq!(format_size(2048), "2 KB");
        assert_eq!(format_size(5_500_000), "5.2 MB");
    }
}
