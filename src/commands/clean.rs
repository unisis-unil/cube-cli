use anyhow::{Context, Result};
use console::style;
use std::io::Write;

/// Run the `cube clean` command — removes all cached cubes and metadata.
pub fn run(dev: bool, yes: bool) -> Result<()> {
    let cache = super::schema::default_cache_dir(dev)?;

    if !cache.exists() {
        eprintln!(
            "Le cache {} n'existe pas, rien à nettoyer.",
            cache.display()
        );
        return Ok(());
    }

    let (file_count, total_size) = cache_stats(&cache);

    if file_count == 0 {
        eprintln!("Le cache {} est déjà vide.", cache.display());
        return Ok(());
    }

    eprintln!(
        "Cache à supprimer : {} ({} fichier{}, {})",
        cache.display(),
        file_count,
        if file_count > 1 { "s" } else { "" },
        format_size(total_size)
    );

    if !yes {
        eprint!("Confirmer la suppression ? [y/N] ");
        std::io::stderr().flush().ok();
        let mut input = String::new();
        std::io::stdin().read_line(&mut input)?;
        let answer = input.trim().to_lowercase();
        if answer != "y" && answer != "yes" && answer != "o" && answer != "oui" {
            eprintln!("Annulé.");
            return Ok(());
        }
    }

    std::fs::remove_dir_all(&cache)
        .with_context(|| format!("Impossible de supprimer {}", cache.display()))?;

    eprintln!(
        "{} Cache supprimé : {}",
        style("✓").green().bold(),
        cache.display()
    );
    Ok(())
}

fn cache_stats(cache: &std::path::Path) -> (usize, u64) {
    let mut count = 0usize;
    let mut total = 0u64;
    if let Ok(entries) = std::fs::read_dir(cache) {
        for entry in entries.flatten() {
            if let Ok(meta) = entry.metadata() {
                if meta.is_file() {
                    count += 1;
                    total += meta.len();
                }
            }
        }
    }
    (count, total)
}

fn format_size(bytes: u64) -> String {
    if bytes < 1024 {
        format!("{bytes} B")
    } else if bytes < 1024 * 1024 {
        format!("{:.0} KB", bytes as f64 / 1024.0)
    } else if bytes < 1024 * 1024 * 1024 {
        format!("{:.1} MB", bytes as f64 / (1024.0 * 1024.0))
    } else {
        format!("{:.2} GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    }
}
