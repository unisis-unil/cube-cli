use chrono::Utc;
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

const CHECK_FILE: &str = ".version_check.json";
const CHECK_TTL_SECONDS: i64 = 3 * 3600; // 3 hours
const GITHUB_LATEST: &str = "https://api.github.com/repos/unisis-unil/cube-cli/releases/latest";

#[derive(Serialize, Deserialize, Default)]
struct VersionCheck {
    latest_version: String,
    checked_at: String,
}

#[derive(Deserialize)]
struct GitHubRelease {
    tag_name: String,
}

/// Directory for version check cache (~/.unisis-cube/).
fn cache_dir() -> Option<PathBuf> {
    let home = dirs::home_dir()?;
    let dir = home.join(".unisis-cube");
    if !dir.exists() {
        std::fs::create_dir_all(&dir).ok()?;
    }
    Some(dir)
}

fn read_check(dir: &Path) -> VersionCheck {
    let path = dir.join(CHECK_FILE);
    std::fs::read_to_string(&path)
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_default()
}

fn write_check(dir: &Path, check: &VersionCheck) {
    let path = dir.join(CHECK_FILE);
    if let Ok(json) = serde_json::to_string(check) {
        let _ = std::fs::write(&path, json);
    }
}

/// Returns true if the binary was installed via Homebrew.
fn is_homebrew_install() -> bool {
    let exe = match std::env::current_exe() {
        Ok(p) => p,
        Err(_) => return false,
    };
    // Resolve symlinks to get the real path (Homebrew symlinks from /opt/homebrew/bin/)
    let real = exe.canonicalize().unwrap_or(exe);
    let path = real.to_string_lossy();
    path.contains("/homebrew/") || path.contains("/Cellar/") || path.contains("/Linuxbrew/")
}

/// Parse a version string like "v1.2.3" or "1.2.3" into (major, minor, patch).
fn parse_semver(s: &str) -> Option<(u32, u32, u32)> {
    let s = s.strip_prefix('v').unwrap_or(s);
    let parts: Vec<&str> = s.split('.').collect();
    if parts.len() != 3 {
        return None;
    }
    Some((
        parts[0].parse().ok()?,
        parts[1].parse().ok()?,
        parts[2].parse().ok()?,
    ))
}

/// Returns true if `latest` is strictly newer than `current`.
fn is_newer(current: &str, latest: &str) -> bool {
    match (parse_semver(current), parse_semver(latest)) {
        (Some(c), Some(l)) => l > c,
        _ => false,
    }
}

/// Check for a newer CLI version and print a message if available.
/// Silently does nothing on any error (network, parse, etc.).
pub fn check_for_new_version() {
    let dir = match cache_dir() {
        Some(d) => d,
        None => return,
    };

    let mut check = read_check(&dir);

    // TTL: skip if recently checked
    if !check.checked_at.is_empty() {
        if let Ok(last) = chrono::DateTime::parse_from_rfc3339(&check.checked_at) {
            let elapsed = Utc::now().signed_duration_since(last);
            if elapsed.num_seconds() < CHECK_TTL_SECONDS {
                // Still show message if we already know a newer version exists
                if !check.latest_version.is_empty() {
                    show_update_message(&check.latest_version);
                }
                return;
            }
        }
    }

    // Fetch latest release from GitHub
    let latest = match fetch_latest_version() {
        Some(v) => v,
        None => return,
    };

    check.checked_at = Utc::now().to_rfc3339();
    check.latest_version = latest.clone();
    write_check(&dir, &check);

    show_update_message(&latest);
}

fn show_update_message(latest: &str) {
    let current = env!("CARGO_PKG_VERSION");
    if !is_newer(current, latest) {
        return;
    }
    let display_latest = latest.strip_prefix('v').unwrap_or(latest);
    if is_homebrew_install() {
        eprintln!(
            "cube: nouvelle version disponible ({current} → {display_latest}). \
             Mettez à jour avec : brew upgrade cube"
        );
    } else {
        eprintln!(
            "cube: nouvelle version disponible ({current} → {display_latest}). \
             Voir https://github.com/unisis-unil/cube-cli/releases/latest"
        );
    }
}

/// Fetch the latest release tag from GitHub. Returns None on any error.
fn fetch_latest_version() -> Option<String> {
    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()
        .ok()?;

    let resp = client
        .get(GITHUB_LATEST)
        .header("User-Agent", "cube-cli")
        .send()
        .ok()?;

    if !resp.status().is_success() {
        return None;
    }

    let release: GitHubRelease = resp.json().ok()?;
    Some(release.tag_name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_semver() {
        assert_eq!(parse_semver("1.2.3"), Some((1, 2, 3)));
        assert_eq!(parse_semver("v1.2.3"), Some((1, 2, 3)));
        assert_eq!(parse_semver("0.0.1"), Some((0, 0, 1)));
        assert_eq!(parse_semver("invalid"), None);
        assert_eq!(parse_semver("1.2"), None);
    }

    #[test]
    fn test_is_newer() {
        assert!(is_newer("1.0.0", "1.0.1"));
        assert!(is_newer("1.0.0", "1.1.0"));
        assert!(is_newer("1.0.0", "2.0.0"));
        assert!(is_newer("1.0.8", "v1.1.0"));
        assert!(!is_newer("1.1.0", "1.1.0"));
        assert!(!is_newer("1.1.0", "1.0.9"));
        assert!(!is_newer("2.0.0", "1.9.9"));
    }
}
