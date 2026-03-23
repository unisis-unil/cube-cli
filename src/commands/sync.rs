use aes::Aes256;
use anyhow::{bail, Context, Result};
use cbc::cipher::{BlockDecryptMut, KeyIvInit};
type Aes256CbcDec = cbc::Decryptor<Aes256>;
use base64::Engine;
use chrono::Utc;
use console::style;
use flate2::read::GzDecoder;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::Write;
use std::path::Path;
use std::process::Command;

const SYNC_METADATA_FILE: &str = ".sync_metadata.json";
const CHECK_TTL_SECONDS: i64 = 3 * 3600; // 3 hours
const BUCKET_PROD: &str = "unisis-data.unisis.ch";
const BUCKET_DEV: &str = "unisis-data-dev.unisis.ch";
const GCS_API: &str = "https://storage.googleapis.com/storage/v1";

pub fn bucket_for(dev: bool) -> &'static str {
    if dev {
        BUCKET_DEV
    } else {
        BUCKET_PROD
    }
}

// ── Metadata ────────────────────────────────────────────────────────

#[derive(Serialize, Deserialize, Default)]
struct SyncMetadata {
    remote_timestamp: String,
    #[serde(default)]
    last_checked_at: Option<String>,
    /// Remote CRC32C (base64) per local filename, to skip re-download when
    /// files are stored decompressed locally (.sqlite) but compressed on GCS (.sqlite.gz).
    #[serde(default)]
    file_checksums: HashMap<String, String>,
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

pub fn get_access_token() -> Result<String> {
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
fn gcs_list_prefixes(
    client: &Client,
    token: &str,
    bucket: &str,
    prefix: &str,
) -> Result<Vec<String>> {
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

// ── Manifest ────────────────────────────────────────────────────────

const MANIFEST_FILE: &str = "manifest.json";

#[derive(Deserialize, Clone)]
struct Manifest {
    files: Vec<ManifestFile>,
}

#[derive(Deserialize, Clone)]
struct ManifestFile {
    name: String,
    crc32c: String,
    #[serde(deserialize_with = "deserialize_string_u64")]
    size: u64,
}

/// Fetch manifest.json from a snapshot prefix. Returns None if the file doesn't exist (export in progress).
fn fetch_manifest(
    client: &Client,
    token: &str,
    bucket: &str,
    prefix: &str,
) -> Result<Option<Manifest>> {
    let object_name = format!("{prefix}{MANIFEST_FILE}");
    let encoded = urlencoding::encode(&object_name);
    let url = format!("{GCS_API}/b/{bucket}/o/{encoded}?alt=media");

    let resp = client.get(&url).bearer_auth(token).send()?;

    if resp.status() == reqwest::StatusCode::NOT_FOUND {
        return Ok(None);
    }

    let manifest: Manifest = resp.error_for_status()?.json()?;
    Ok(Some(manifest))
}

/// Find the latest timestamp directory under prefix that has a manifest.
/// Returns (timestamp, manifest). If the most recent snapshot has no manifest,
/// warns about an export in progress and falls back to the previous one.
fn find_latest_ready_snapshot(
    client: &Client,
    token: &str,
    bucket: &str,
    prefix: &str,
) -> Result<Option<(String, Manifest)>> {
    let prefixes = gcs_list_prefixes(client, token, bucket, prefix)?;

    if prefixes.is_empty() {
        bail!("Aucun répertoire trouvé sous gs://{bucket}/{prefix}");
    }

    let mut sorted: Vec<&String> = prefixes.iter().collect();
    sorted.sort();
    sorted.reverse(); // most recent first

    for (i, p) in sorted.iter().enumerate() {
        let ts = p.trim_end_matches('/').rsplit('/').next().unwrap_or(p);
        let snapshot_prefix = format!("{prefix}{ts}/");

        match fetch_manifest(client, token, bucket, &snapshot_prefix)? {
            Some(manifest) => {
                if i > 0 {
                    eprintln!(
                        "{} Un export semble en cours (snapshot sans manifeste ignoré). \
                         Utilisation du dernier snapshot complet : {ts}",
                        style("⚠").yellow()
                    );
                }
                return Ok(Some((ts.to_string(), manifest)));
            }
            None => {
                eprintln!("  Snapshot {ts} : pas de manifeste (export en cours ou échoué).");
            }
        }
    }

    Ok(None)
}

/// Find the latest timestamp directory under prefix (without manifest check).
fn find_latest_timestamp(
    client: &Client,
    token: &str,
    bucket: &str,
    prefix: &str,
) -> Result<String> {
    let prefixes = gcs_list_prefixes(client, token, bucket, prefix)?;

    if prefixes.is_empty() {
        bail!("Aucun répertoire trouvé sous gs://{bucket}/{prefix}");
    }

    let latest = prefixes.iter().max().context("Aucun répertoire trouvé")?;

    let ts = latest
        .trim_end_matches('/')
        .rsplit('/')
        .next()
        .unwrap_or(latest);
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

/// Quick integrity check: open the file and verify the schema metadata is readable.
#[allow(dead_code)]
fn sqlite_integrity_ok(path: &Path) -> bool {
    let conn = match rusqlite::Connection::open_with_flags(
        path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    ) {
        Ok(c) => c,
        Err(_) => return false,
    };
    conn.query_row(
        "SELECT value FROM metadata WHERE key = 'schema'",
        [],
        |row| row.get::<_, String>(0),
    )
    .is_ok()
}

// ── Gzip decompression ─────────────────────────────────────────────

/// Decompress a .gz file to a destination path.
fn decompress_gz(gz_path: &Path, dest: &Path) -> Result<()> {
    let gz_file = std::fs::File::open(gz_path)?;
    let mut decoder = GzDecoder::new(gz_file);
    let mut out = std::fs::File::create(dest)?;
    std::io::copy(&mut decoder, &mut out)?;
    Ok(())
}

/// Decompress gzip bytes (already in memory) to a file.
fn decompress_gz_bytes(gz_bytes: &[u8], dest: &Path) -> Result<()> {
    let mut decoder = GzDecoder::new(gz_bytes);
    let mut out = std::fs::File::create(dest)?;
    std::io::copy(&mut decoder, &mut out)?;
    Ok(())
}

/// Decrypt AES-256-CBC encrypted data.
/// Format: [1 byte version 0x02][16 bytes IV][ciphertext PKCS7-padded]
fn decrypt_aes_cbc(data: &[u8], key: &[u8; 32]) -> Result<Vec<u8>> {
    if data.len() < 1 + 16 + 16 {
        bail!("Fichier chiffré trop court ({} bytes)", data.len());
    }
    let version = data[0];
    if version != 0x02 {
        bail!("Version de chiffrement non supportée : {version:#04x} (attendu: 0x02)");
    }
    let iv = &data[1..17];
    let ciphertext = &data[17..];

    let decryptor = Aes256CbcDec::new(key.into(), iv.into());
    decryptor
        .decrypt_padded_vec_mut::<cbc::cipher::block_padding::Pkcs7>(ciphertext)
        .map_err(|_| {
            anyhow::anyhow!("Déchiffrement AES-CBC échoué (clé incorrecte ou données corrompues)")
        })
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
    ProgressStyle::with_template("{prefix:.bold.cyan} [{bar:30.cyan/dim}] {pos}/{len} cubes  {msg}")
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

    eprintln!("Recherche du dernier snapshot complet sur gs://{bucket}/{prefix} ...");
    let (remote_ts, manifest) = match find_latest_ready_snapshot(&client, &token, bucket, prefix)? {
        Some(result) => result,
        None => {
            return Err(crate::error::CubeError::unavailable(
                "Aucun snapshot complet trouvé (aucun manifeste disponible). \
                 Un export est peut-être en cours. Réessayez plus tard.",
            ));
        }
    };
    let remote_prefix = format!("{prefix}{remote_ts}/");

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
            if meta.remote_timestamp.is_empty() {
                "(aucun)"
            } else {
                &meta.remote_timestamp
            },
            remote_ts
        );
    }

    let sqlite_objects: Vec<GcsObject> = manifest
        .files
        .iter()
        .filter(|f| {
            f.name.ends_with(".sqlite.gz.enc")
                || f.name.ends_with(".sqlite.gz")
                || f.name.ends_with(".sqlite")
        })
        .map(|f| GcsObject {
            name: format!("{remote_prefix}{}", f.name),
            size: f.size,
            crc32c: f.crc32c.clone(),
        })
        .collect();

    if sqlite_objects.is_empty() {
        bail!("Aucun fichier .sqlite dans le manifeste de gs://{bucket}/{remote_prefix}");
    }

    eprintln!(
        "Manifeste OK — {} cube(s) attendu(s).",
        sqlite_objects.len()
    );

    // Pre-fetch encryption key for AES-256-CBC decryption (v2 files)
    let has_enc_files = sqlite_objects.iter().any(|o| o.name.ends_with(".gz.enc"));
    let aes_key: [u8; 32] = if has_enc_files {
        let cube_key = super::key::fetch_snapshot_key(bucket, &token, &remote_prefix)
            .context("Clé requise pour déchiffrer les cubes .gz.enc")?;
        if cube_key.version >= 2 {
            cube_key.decode_aes_key()?
        } else {
            bail!(
                "Les fichiers .gz.enc requièrent une clé v2, mais le bucket contient une clé v{}",
                cube_key.version
            );
        }
    } else {
        [0u8; 32] // unused placeholder for v1 snapshots
    };

    // Set up multi-progress display
    let mp = MultiProgress::new();
    let overall = mp.add(ProgressBar::new(sqlite_objects.len() as u64));
    overall.set_style(style_overall());
    overall.set_prefix("sync");

    let mut downloaded: u64 = 0;
    let mut skipped: u64 = 0;
    let mut downloaded_bytes: u64 = 0;

    // Map remote names to local .sqlite names
    // .sqlite.gz.enc -> .sqlite, .sqlite.gz -> .sqlite, .sqlite -> .sqlite
    fn strip_remote_suffix(name: &str) -> &str {
        name.strip_suffix(".gz.enc")
            .or_else(|| name.strip_suffix(".gz"))
            .unwrap_or(name)
    }

    let local_filenames: Vec<String> = sqlite_objects
        .iter()
        .filter_map(|o| {
            o.name
                .rsplit('/')
                .next()
                .map(|f| strip_remote_suffix(f).to_string())
        })
        .collect();

    for obj in &sqlite_objects {
        let remote_filename = obj.name.rsplit('/').next().unwrap_or(&obj.name);
        let is_enc = remote_filename.ends_with(".gz.enc");
        let is_gz = remote_filename.ends_with(".gz") && !is_enc;
        let local_filename = strip_remote_suffix(remote_filename);
        let display_name = local_filename
            .strip_suffix(".sqlite")
            .unwrap_or(local_filename);
        let local_path = cache.join(local_filename);

        overall.set_message(display_name.to_string());

        // Skip if local file exists and remote CRC32C hasn't changed (stored in metadata)
        if local_path.exists() {
            if let Some(stored_crc) = meta.file_checksums.get(local_filename) {
                if *stored_crc == obj.crc32c {
                    let done_pb = mp.add(ProgressBar::new(0));
                    done_pb.set_style(style_done());
                    done_pb.set_prefix(display_name.to_string());
                    let local_size = std::fs::metadata(&local_path).map(|m| m.len()).unwrap_or(0);
                    done_pb.finish_with_message(format!(
                        "{} à jour ({})",
                        style("✓").green(),
                        format_size(local_size)
                    ));
                    skipped += 1;
                    overall.inc(1);
                    continue;
                }
            }
        }

        // Download to a temp file
        let tmp_download = cache.join(format!(".{remote_filename}.tmp"));
        let file_pb = mp.add(ProgressBar::new(obj.size));
        file_pb.set_style(style_download());
        file_pb.set_prefix(display_name.to_string());

        if let Err(e) = download_object(&client, &token, bucket, obj, &tmp_download, &file_pb) {
            let _ = std::fs::remove_file(&tmp_download);
            file_pb.set_style(style_done());
            file_pb.finish_with_message(format!("{} erreur", style("✗").red()));
            bail!(e);
        }

        // Verify CRC32C of the downloaded file
        if let Ok(local_hash) = local_crc32c_b64(&tmp_download) {
            if local_hash != obj.crc32c {
                let _ = std::fs::remove_file(&tmp_download);
                file_pb.set_style(style_done());
                file_pb
                    .finish_with_message(format!("{} hash incorrect, ignoré", style("✗").yellow()));
                overall.inc(1);
                continue;
            }
        }

        // Decrypt (if .enc) then decompress (if .gz) to get the plain .sqlite
        let tmp_sqlite = if is_enc {
            // AES-256-CBC: decrypt → gzip bytes → decompress
            let enc_bytes = std::fs::read(&tmp_download).context("Lecture du fichier chiffré")?;
            let _ = std::fs::remove_file(&tmp_download);
            match decrypt_aes_cbc(&enc_bytes, &aes_key) {
                Ok(gz_bytes) => {
                    let tmp_out = cache.join(format!(".{local_filename}.tmp"));
                    match decompress_gz_bytes(&gz_bytes, &tmp_out) {
                        Ok(()) => tmp_out,
                        Err(e) => {
                            let _ = std::fs::remove_file(&tmp_out);
                            file_pb.set_style(style_done());
                            file_pb.finish_with_message(format!(
                                "{} décompression échouée: {e}",
                                style("✗").yellow(),
                            ));
                            overall.inc(1);
                            continue;
                        }
                    }
                }
                Err(e) => {
                    file_pb.set_style(style_done());
                    file_pb.finish_with_message(format!(
                        "{} déchiffrement échoué: {e}",
                        style("✗").yellow(),
                    ));
                    overall.inc(1);
                    continue;
                }
            }
        } else if is_gz {
            let tmp_out = cache.join(format!(".{local_filename}.tmp"));
            if let Err(e) = decompress_gz(&tmp_download, &tmp_out) {
                let _ = std::fs::remove_file(&tmp_download);
                let _ = std::fs::remove_file(&tmp_out);
                file_pb.set_style(style_done());
                file_pb.finish_with_message(format!(
                    "{} décompression échouée: {e}",
                    style("✗").yellow(),
                ));
                overall.inc(1);
                continue;
            }
            let _ = std::fs::remove_file(&tmp_download);
            tmp_out
        } else {
            tmp_download
        };

        let final_size = std::fs::metadata(&tmp_sqlite).map(|m| m.len()).unwrap_or(0);

        std::fs::rename(&tmp_sqlite, &local_path)?;
        meta.file_checksums
            .insert(local_filename.to_string(), obj.crc32c.clone());

        file_pb.set_style(style_done());
        if is_gz {
            file_pb.finish_with_message(format!(
                "{} téléchargé ({} gz → {})",
                style("✓").green(),
                format_size(obj.size),
                format_size(final_size)
            ));
        } else {
            file_pb.finish_with_message(format!(
                "{} téléchargé ({})",
                style("✓").green(),
                format_size(final_size)
            ));
        }

        downloaded += 1;
        downloaded_bytes += final_size;
        overall.inc(1);
    }

    overall.finish_and_clear();

    // Delete local .sqlite files not in the remote snapshot
    for entry in std::fs::read_dir(&cache)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) == Some("sqlite") {
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if !local_filenames.contains(&name.to_string()) {
                    meta.file_checksums.remove(name);
                    std::fs::remove_file(&path)?;
                    eprintln!("  {} supprimé (absent du remote)", name);
                }
            }
        }
    }

    meta.remote_timestamp = remote_ts;
    meta.last_checked_at = Some(Utc::now().to_rfc3339());
    write_sync_metadata(&cache, &meta)?;

    // Clean up legacy key file (SQLCipher v1 is no longer supported)
    super::key::cleanup_legacy_key_file();

    eprintln!(
        "\n{} Synchronisation terminée — {} téléchargé(s) ({}), {} à jour. Cache : {}",
        style("✓").green().bold(),
        downloaded,
        format_size(downloaded_bytes),
        skipped,
        cache.display()
    );

    // Post-sync verification: open each cube and check schema + data table
    let fail_count = verify_cubes(&cache, &mut meta)?;

    if fail_count > 0 {
        // Persist invalidated checksums so next sync retries failed cubes
        write_sync_metadata(&cache, &meta)?;
        bail!(
            "{fail_count} cube(s) en erreur. Exécutez 'cube sync' pour retélécharger les cubes corrompus."
        );
    }

    Ok(())
}

// ── Post-sync verification ──────────────────────────────────────────

pub(crate) const CATALOGUE_FILE: &str = ".catalogue.json";

fn verify_cubes(cache: &Path, meta: &mut SyncMetadata) -> Result<u32> {
    let mut cubes: Vec<std::path::PathBuf> = std::fs::read_dir(cache)?
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("sqlite"))
        .collect();
    cubes.sort();

    if cubes.is_empty() {
        return Ok(0);
    }

    eprintln!("\nVérification des cubes...");

    let mut ok_count = 0u32;
    let mut fail_count = 0u32;
    let mut catalogue = Vec::new();

    for path in &cubes {
        let name = path.file_stem().and_then(|s| s.to_str()).unwrap_or("?");
        let filename = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or_default();

        match verify_and_catalogue_cube(path) {
            Ok((row_count, entry)) => {
                ok_count += 1;
                eprintln!("  {} {} ({} lignes)", style("✓").green(), name, row_count);
                catalogue.push(entry);
            }
            Err(e) => {
                fail_count += 1;
                eprintln!("  {} {} : {}", style("✗").red(), name, e);
                // Invalidate checksum so next sync retries this cube
                meta.file_checksums.remove(filename);
            }
        }
    }

    // Write catalogue cache
    catalogue.sort_by(|a, b| {
        let na = a["name"].as_str().unwrap_or("");
        let nb = b["name"].as_str().unwrap_or("");
        na.cmp(nb)
    });
    let catalogue_path = cache.join(CATALOGUE_FILE);
    let json = serde_json::to_string_pretty(&catalogue)?;
    std::fs::write(&catalogue_path, json)?;

    eprintln!(
        "\n{} Vérification : {ok_count} OK, {fail_count} en erreur sur {} cube(s).",
        if fail_count == 0 {
            style("✓").green().bold()
        } else {
            style("⚠").yellow().bold()
        },
        cubes.len()
    );

    if fail_count > 0 {
        eprintln!(
            "Les cubes en erreur peuvent être corrompus. \
             Un prochain 'cube sync' retéléchargera les cubes affectés."
        );
    }

    Ok(fail_count)
}

/// Verify a single cube and build its catalogue entry.
fn verify_and_catalogue_cube(path: &Path) -> Result<(i64, serde_json::Value)> {
    let conn = crate::db::open(path)?;

    let schema = crate::db::read_metadata_schema(&conn).context("Schéma métadonnées illisible")?;

    let columns =
        crate::db::get_table_columns(&conn, "data").context("Table 'data' inaccessible")?;
    if columns.is_empty() {
        bail!("Table 'data' sans colonnes");
    }

    let row_count = crate::db::get_row_count(&conn).context("Impossible de compter les lignes")?;

    let indicator_col = schema
        .get("indicator_column")
        .and_then(|v| v.as_str())
        .unwrap_or("indicateur");
    let dimension_count = columns.iter().filter(|c| c.name != indicator_col).count();

    let file_stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("?");

    let entry = serde_json::json!({
        "name": file_stem,
        "cube": schema.get("cube").unwrap_or(&serde_json::Value::Null),
        "cube_description": schema.get("cube_description").unwrap_or(&serde_json::Value::Null),
        "measure": schema.get("measure").unwrap_or(&serde_json::Value::Null),
        "measure_description": schema.get("measure_description").unwrap_or(&serde_json::Value::Null),
        "aggregation": schema.get("aggregation").unwrap_or(&serde_json::Value::Null),
        "row_count": row_count,
        "dimension_count": dimension_count,
    });

    Ok((row_count, entry))
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
            file_checksums: HashMap::new(),
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

    #[test]
    fn test_decompress_gz() {
        use flate2::write::GzEncoder;
        use flate2::Compression;

        let tmp = TempDir::new().unwrap();
        let gz_path = tmp.path().join("test.sqlite.gz");
        let out_path = tmp.path().join("test.sqlite");

        // Create a gzipped file
        let original = b"hello gzip world";
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(original).unwrap();
        let compressed = encoder.finish().unwrap();
        std::fs::write(&gz_path, &compressed).unwrap();

        // Decompress
        decompress_gz(&gz_path, &out_path).unwrap();
        let result = std::fs::read(&out_path).unwrap();
        assert_eq!(result, original);
    }

    #[test]
    fn test_decompress_gz_invalid() {
        let tmp = TempDir::new().unwrap();
        let gz_path = tmp.path().join("bad.gz");
        let out_path = tmp.path().join("bad.sqlite");

        std::fs::write(&gz_path, b"not gzip data").unwrap();
        assert!(decompress_gz(&gz_path, &out_path).is_err());
    }

    #[test]
    fn test_sync_metadata_with_checksums() {
        let tmp = TempDir::new().unwrap();
        let mut checksums = HashMap::new();
        checksums.insert("cube_a.sqlite".to_string(), "abc123==".to_string());
        checksums.insert("cube_b.sqlite".to_string(), "def456==".to_string());

        let meta = SyncMetadata {
            remote_timestamp: "2026-03-13T120000".to_string(),
            last_checked_at: None,
            file_checksums: checksums,
        };

        write_sync_metadata(tmp.path(), &meta).unwrap();
        let loaded = read_sync_metadata(tmp.path());

        assert_eq!(loaded.file_checksums.len(), 2);
        assert_eq!(
            loaded.file_checksums.get("cube_a.sqlite").unwrap(),
            "abc123=="
        );
    }

    #[test]
    fn test_sync_metadata_without_checksums_field() {
        let tmp = TempDir::new().unwrap();
        // Simulate old metadata format without file_checksums
        let json = r#"{"remote_timestamp": "ts1", "last_checked_at": "2026-01-01T00:00:00Z"}"#;
        std::fs::write(tmp.path().join(SYNC_METADATA_FILE), json).unwrap();
        let meta = read_sync_metadata(tmp.path());
        assert_eq!(meta.remote_timestamp, "ts1");
        assert!(meta.file_checksums.is_empty());
    }

    #[test]
    fn test_manifest_deserialization() {
        let json = r#"{
            "files": [
                {"name": "cube_a.sqlite.gz", "crc32c": "abc123==", "size": "12345"},
                {"name": "cube_b.sqlite.gz", "crc32c": "def456==", "size": "67890"}
            ]
        }"#;
        let manifest: Manifest = serde_json::from_str(json).unwrap();
        assert_eq!(manifest.files.len(), 2);
        assert_eq!(manifest.files[0].name, "cube_a.sqlite.gz");
        assert_eq!(manifest.files[0].crc32c, "abc123==");
        assert_eq!(manifest.files[0].size, 12345);
        assert_eq!(manifest.files[1].name, "cube_b.sqlite.gz");
        assert_eq!(manifest.files[1].size, 67890);
    }

    #[test]
    fn test_manifest_to_gcs_objects() {
        let json = r#"{
            "files": [
                {"name": "cube_a.sqlite.gz", "crc32c": "abc==", "size": "100"},
                {"name": "cube_b.sqlite.gz", "crc32c": "def==", "size": "200"},
                {"name": "other.txt", "crc32c": "ghi==", "size": "50"}
            ]
        }"#;
        let manifest: Manifest = serde_json::from_str(json).unwrap();
        let prefix = "cubes/2026-03-13T120000/";

        let objects: Vec<GcsObject> = manifest
            .files
            .iter()
            .filter(|f| f.name.ends_with(".sqlite.gz"))
            .map(|f| GcsObject {
                name: format!("{prefix}{}", f.name),
                size: f.size,
                crc32c: f.crc32c.clone(),
            })
            .collect();

        assert_eq!(objects.len(), 2);
        assert_eq!(objects[0].name, "cubes/2026-03-13T120000/cube_a.sqlite.gz");
        assert_eq!(objects[0].crc32c, "abc==");
        assert_eq!(objects[1].size, 200);
    }

    #[test]
    fn test_verify_and_catalogue_cube_valid() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("test.sqlite");
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute_batch(
            "CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT);
             INSERT INTO metadata VALUES ('schema', '{\"cube\": \"Test\", \"measure\": \"Count\"}');
             CREATE TABLE data (\"Nom\" TEXT, indicateur REAL);
             INSERT INTO data VALUES ('A', 1.0);
             INSERT INTO data VALUES ('B', 2.0);",
        )
        .unwrap();
        drop(conn);

        let (row_count, entry) = verify_and_catalogue_cube(&path).unwrap();
        assert_eq!(row_count, 2);
        assert_eq!(entry["name"], "test");
        assert_eq!(entry["cube"], "Test");
        assert_eq!(entry["dimension_count"], 1);
    }

    #[test]
    fn test_verify_and_catalogue_cube_no_metadata() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("no_meta.sqlite");
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute_batch("CREATE TABLE data (x TEXT);").unwrap();
        drop(conn);

        assert!(verify_and_catalogue_cube(&path).is_err());
    }

    #[test]
    fn test_verify_and_catalogue_cube_no_data_table() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("no_data.sqlite");
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute_batch(
            "CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT);
             INSERT INTO metadata VALUES ('schema', '{\"cube\": \"Test\"}');",
        )
        .unwrap();
        drop(conn);

        assert!(verify_and_catalogue_cube(&path).is_err());
    }

    #[test]
    fn test_verify_and_catalogue_cube_corrupted() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("corrupt.sqlite");
        std::fs::write(&path, b"not a database").unwrap();

        assert!(verify_and_catalogue_cube(&path).is_err());
    }

    #[test]
    fn test_verify_cubes_mixed() {
        let tmp = TempDir::new().unwrap();

        // Valid cube
        let good = tmp.path().join("good.sqlite");
        let conn = rusqlite::Connection::open(&good).unwrap();
        conn.execute_batch(
            "CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT);
             INSERT INTO metadata VALUES ('schema', '{\"cube\": \"Good\"}');
             CREATE TABLE data (x TEXT, indicateur REAL);
             INSERT INTO data VALUES ('A', 1.0);",
        )
        .unwrap();
        drop(conn);

        // Invalid cube
        let bad = tmp.path().join("bad.sqlite");
        std::fs::write(&bad, b"corrupted").unwrap();

        // Non-sqlite file (should be ignored)
        std::fs::write(tmp.path().join("readme.txt"), b"ignored").unwrap();

        let mut meta = SyncMetadata::default();
        meta.file_checksums
            .insert("good.sqlite".to_string(), "aaa==".to_string());
        meta.file_checksums
            .insert("bad.sqlite".to_string(), "bbb==".to_string());

        let fail_count = verify_cubes(tmp.path(), &mut meta).unwrap();
        assert_eq!(fail_count, 1);
        // Checksum of the failed cube should be invalidated
        assert!(meta.file_checksums.contains_key("good.sqlite"));
        assert!(!meta.file_checksums.contains_key("bad.sqlite"));
    }
}
