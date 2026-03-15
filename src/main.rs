mod commands;
mod db;
mod error;
mod formatter;
mod version;

use chrono::Local;
use clap::{Parser, Subcommand};
use std::io::IsTerminal;
use std::path::PathBuf;
use std::process::ExitCode;

/// cube — UNISIS S3 Cubes CLI
///
/// Query SQLite cubes from the UNISIS S3 (Statistiques en Self-Service) platform
/// at Université de Lausanne.
///
/// USAGE:
///     cube schema [--search TERM] [<cube> [<dimension>]]
///     cube query <cube> --group-by <dim> [flags]
///     cube sql <cube> "SELECT ..."
///     cube key [--refresh | --delete]
///     cube sync
///     cube feedback "message"
///
/// EXAMPLES:
///     cube schema
///     cube schema --search "réussite"
///     cube schema infrastructures_surface
///     cube schema infrastructures_surface "Faculté"
///     cube query etudiants_nombre_d_etudiants_plan_principal --group-by Faculté --filter Semestre=2025A
///     cube query etudiants_nombre_d_etudiants_plan_principal --group-by Faculté Sexe --arrange indicateur:desc --limit 10 --format json
///     cube sql infrastructures_surface "SELECT Faculté, SUM(indicateur) FROM data GROUP BY Faculté"
///     cube sync
///     cube feedback "La commande query est très pratique !"
///
/// CACHE:
///     ~/.unisis-cube/cache/        Local cache for PROD cubes
///     ~/.unisis-cube/cache-dev/    Local cache for DEV cubes (--dev)
///
/// Each SQLite file contains:
///     data       Table with dimension columns (TEXT) and an indicator column (numeric)
///     metadata   Table with key='schema' holding a JSON description of the cube
#[derive(Parser)]
#[command(
    name = "cube",
    version,
    verbatim_doc_comment,
    after_help = "SOURCE:\n    PROD: gs://unisis-data.unisis.ch/cubes/\n    DEV:  gs://unisis-data-dev.unisis.ch/cubes/\n    Use 'cube sync' (or 'cube --dev sync') to download the latest version."
)]
struct Cli {
    /// Use the DEV bucket (unisis-data-dev) instead of PROD
    #[arg(long, global = true)]
    dev: bool,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Progressive drill-down into cube metadata
    ///
    /// Level 0 — no arguments: compact catalogue of all cubes
    ///     (name, description, measure — no dimensions).
    /// Level 1 — cube name: dimensions with types, cardinality, and a
    ///     value preview (full sorted list if ≤ 20, else first/last 10).
    /// Level 2 — cube + dimension: all distinct values for that dimension.
    ///
    /// Use --search to filter the catalogue (level 0) by name or description.
    /// Supports regex (e.g. --search "réussite|cohorte").
    ///
    /// EXAMPLES:
    ///     cube schema
    ///     cube schema --search "réussite"
    ///     cube schema --search "réussite|cohorte"
    ///     cube schema infrastructures_surface
    ///     cube schema infrastructures_surface "Faculté"
    ///     cube schema ./path/to/file.sqlite
    #[command(verbatim_doc_comment)]
    Schema {
        /// Cube name (e.g. infrastructures_surface) or path to .sqlite file
        name: Option<String>,

        /// Dimension name to list all distinct values for
        dimension: Option<String>,

        /// Filter cubes by name or description (level 0 only, supports regex)
        #[arg(long, value_name = "PATTERN")]
        search: Option<String>,
    },

    /// Query cube data with aggregation (PREFERRED over 'cube sql')
    ///
    /// Always prefer 'cube query' over 'cube sql'. The structured flags
    /// guarantee correct quoting, aggregation and column names — eliminating
    /// a whole class of errors that hand-written SQL is prone to (wrong
    /// column names, missing quotes on accented identifiers, incorrect
    /// GROUP BY clauses, etc.). Reserve 'cube sql' only for queries that
    /// cannot be expressed with the flags below (e.g. sub-queries, CASE
    /// expressions, joins across cubes).
    ///
    /// Builds a SQL query with SUM aggregation on the indicator column,
    /// applying filters, grouping, sorting and limiting as specified.
    ///
    /// FILTER LOGIC:
    ///     --filter on the SAME column  → OR  (WHERE col IN (v1, v2))
    ///     --filter on DIFFERENT columns → AND (WHERE c1=v1 AND c2=v2)
    ///     --exclude follows the same logic (NOT IN / AND NOT IN)
    ///
    /// MULTI-VALUE FLAGS:
    ///     All list flags (--group-by, --filter, --exclude, --select, --arrange)
    ///     accept multiple values in two equivalent ways:
    ///       --group-by Faculté Sexe            (space-separated after one flag)
    ///       --group-by Faculté --group-by Sexe  (repeated flag)
    ///     Both forms can be mixed freely.
    ///
    /// EXAMPLES:
    ///     # Group by Faculté
    ///     cube query infrastructures_surface --group-by Faculté
    ///
    ///     # Multiple group-by (two equivalent forms)
    ///     cube query infrastructures_surface --group-by Faculté Sexe
    ///     cube query infrastructures_surface --group-by Faculté --group-by Sexe
    ///
    ///     # Two faculties (OR): Faculté IN ('FBM', 'SSP')
    ///     cube query infrastructures_surface --group-by Faculté --filter Faculté=FBM Faculté=SSP
    ///
    ///     # Cross-dimension (AND): Faculté='FBM' AND Type='Bureau'
    ///     cube query infrastructures_surface --group-by Faculté Type --filter Faculté=FBM Type=Bureau
    ///
    ///     # Exclude labs, sorted descending, top 5
    ///     cube query infrastructures_surface --group-by Faculté --exclude Type=Labo --arrange indicateur:desc --limit 5
    ///
    ///     # For complex boolean logic, use 'cube sql' instead
    #[command(verbatim_doc_comment)]
    Query {
        /// Cube name or path to .sqlite file
        file: PathBuf,

        /// Columns to display (default: group-by columns + indicator).
        /// Accepts multiple values: --select A B or --select A --select B
        #[arg(long, num_args = 1.., value_name = "COL")]
        select: Vec<String>,

        /// Include filter: col=val (same column → OR, different columns → AND).
        /// Accepts multiple values: --filter A=1 B=2 or --filter A=1 --filter B=2
        #[arg(long, num_args = 1.., value_name = "EXPR")]
        filter: Vec<String>,

        /// Exclude filter: col=val (same column → OR, different columns → AND).
        /// Accepts multiple values: --exclude A=1 B=2 or --exclude A=1 --exclude B=2
        #[arg(long, num_args = 1.., value_name = "EXPR")]
        exclude: Vec<String>,

        /// Group-by columns (required unless --no-aggregate).
        /// Accepts multiple values: --group-by A B or --group-by A --group-by B
        #[arg(long, num_args = 1.., value_name = "COL")]
        group_by: Vec<String>,

        /// Sort order: col or col:asc or col:desc.
        /// Accepts multiple values: --arrange A:asc B:desc or --arrange A:asc --arrange B:desc
        #[arg(long, num_args = 1.., value_name = "SPEC")]
        arrange: Vec<String>,

        /// Maximum number of rows to return
        #[arg(long, value_name = "N")]
        limit: Option<usize>,

        /// Name of the indicator column
        #[arg(long, value_name = "COL", default_value = "indicateur")]
        indicator: String,

        /// Return raw rows without aggregation
        #[arg(long)]
        no_aggregate: bool,

        /// Output format: table, csv, json
        #[arg(long, value_name = "FMT", default_value = "table")]
        format: String,
    },

    /// Execute arbitrary SQL against the cube file
    ///
    /// WARNING: Prefer 'cube query' whenever possible. The structured query
    /// interface handles quoting, aggregation and column names automatically,
    /// which prevents errors. Use 'cube sql' only when 'cube query' cannot
    /// express the needed logic (e.g. sub-queries, CASE, HAVING, joins).
    ///
    /// TABLES:
    ///     data       Dimension columns (TEXT) + indicator column (numeric).
    ///                Always quote accented column names, e.g. "Faculté".
    ///     metadata   key='schema' → JSON description of the cube.
    ///
    /// EXAMPLES:
    ///     # Aggregate by one dimension
    ///     cube sql infrastructures_surface "SELECT \"Faculté\", SUM(indicateur) FROM data GROUP BY \"Faculté\""
    ///
    ///     # CASE expression (not possible with 'cube query')
    ///     cube sql infrastructures_surface "SELECT CASE WHEN \"Type\"='Bureau' THEN 'Bureau' ELSE 'Autre' END AS cat, SUM(indicateur) FROM data GROUP BY cat"
    ///
    ///     # Read cube metadata
    ///     cube sql infrastructures_surface "SELECT * FROM metadata"
    ///
    ///     # Output as JSON
    ///     cube sql infrastructures_surface "SELECT * FROM data LIMIT 5" --format json
    #[command(verbatim_doc_comment)]
    Sql {
        /// Cube name or path to .sqlite file
        file: PathBuf,

        /// SQL query to execute
        query: String,

        /// Output format: table, csv, json
        #[arg(long, value_name = "FMT", default_value = "table")]
        format: String,
    },

    /// Manage the encryption key
    ///
    /// Without flags, shows key status. Use --refresh to fetch the latest
    /// key from GCS, or --delete to remove it from the keychain.
    ///
    /// EXAMPLES:
    ///     cube key                # Show key status
    ///     cube key --refresh      # Fetch key from GCS
    ///     cube key --delete       # Remove key from keychain
    #[command(verbatim_doc_comment)]
    Key {
        /// Fetch the latest key from GCS
        #[arg(long)]
        refresh: bool,

        /// Delete the key from the keychain
        #[arg(long)]
        delete: bool,
    },

    /// Send feedback about cube-cli to the UNISIS team
    ///
    /// Sends an email to unisis@unil.ch with your feedback message.
    /// The sender is automatically set to your GCP authenticated email.
    /// Requires GCP authentication (gcloud auth application-default login).
    ///
    /// Use this to report bugs, suggest features, or share comments about
    /// the CLI. The email includes your identity and the CLI version.
    ///
    /// EXAMPLES:
    ///     cube feedback "La commande query est très pratique !"
    ///     cube feedback "Bug : la commande schema plante avec le cube X"
    ///     echo "Mon feedback détaillé" | cube feedback
    #[command(verbatim_doc_comment)]
    Feedback {
        /// Feedback message (or pipe via stdin)
        message: Option<String>,
    },

    /// Sync cubes from Google Cloud Storage
    ///
    /// Downloads the most recent cube snapshot from the GCS bucket.
    /// Compares timestamps with the local cache and only downloads
    /// files that have been updated.
    ///
    /// Requires: gcloud auth application-default login
    Sync {
        /// Object prefix in the bucket
        #[arg(long, value_name = "PREFIX", default_value = "cubes/")]
        prefix: String,

        /// Override local cache directory
        #[arg(long, value_name = "DIR")]
        cache_dir: Option<PathBuf>,

        /// Force re-download of all files, even if the cache is up to date
        #[arg(long)]
        force: bool,
    },
}

/// Returns true if the local cache directory has no .sqlite files.
fn cache_is_empty(dev: bool) -> bool {
    let cache = match commands::schema::default_cache_dir(dev) {
        Ok(c) => c,
        Err(_) => return true,
    };
    if !cache.exists() {
        return true;
    }
    let Ok(entries) = std::fs::read_dir(&cache) else {
        return true;
    };
    !entries
        .flatten()
        .any(|e| e.path().extension().and_then(|x| x.to_str()) == Some("sqlite"))
}

/// Returns true if the command uses a direct file path rather than a cube name
/// (i.e. the argument contains a path separator or has a file extension).
fn uses_direct_path(command: &Commands) -> bool {
    let name = match command {
        Commands::Query { file, .. } => file.to_str().unwrap_or_default().to_string(),
        Commands::Sql { file, .. } => file.to_str().unwrap_or_default().to_string(),
        Commands::Schema { name: Some(n), .. } => n.clone(),
        _ => return false,
    };
    name.contains('/') || name.contains('\\') || std::path::Path::new(&name).extension().is_some()
}

/// Returns true if the command will open a cube from the cache (needs encryption key).
fn needs_encryption_key(command: &Commands) -> bool {
    match command {
        Commands::Query { .. } | Commands::Sql { .. } => !uses_direct_path(command),
        Commands::Schema { name: Some(_), .. } => !uses_direct_path(command),
        Commands::Schema { name: None, .. } => true, // catalogue opens all cached cubes
        _ => false,
    }
}

fn main() -> ExitCode {
    let cli = Cli::parse();

    let dev = cli.dev;

    let command = match cli.command {
        Some(cmd) => cmd,
        None => {
            // No subcommand: print help (same as `cube --help`)
            use clap::CommandFactory;
            Cli::command().print_help().ok();
            println!();
            return ExitCode::SUCCESS;
        }
    };

    let is_sync = matches!(command, Commands::Sync { .. });
    let is_key = matches!(command, Commands::Key { .. });
    let is_feedback = matches!(command, Commands::Feedback { .. });
    let is_data_command = !is_sync && !is_key && !is_feedback;

    // If the cache is empty and the command needs it, offer to sync first
    if is_data_command && !uses_direct_path(&command) && cache_is_empty(dev) {
        let sync_cmd = if dev { "cube --dev sync" } else { "cube sync" };
        if std::io::stderr().is_terminal() {
            eprintln!("Aucun cube en cache. Lancement de la synchronisation...\n");
            let bucket = commands::sync::bucket_for(dev);
            if let Err(e) = commands::sync::run(bucket, "cubes/", None, false) {
                error::print_json_error(&e);
                return ExitCode::FAILURE;
            }
            eprintln!();
        } else {
            error::print_json_error(&error::CubeError::not_found(format!(
                "Aucun cube en cache. Exécutez '{sync_cmd}' pour télécharger les cubes."
            )));
            return ExitCode::FAILURE;
        }
    }

    // Read encryption key from keychain only when opening a cube from the cache
    let encryption_key = if needs_encryption_key(&command) {
        commands::key::read_key().ok()
    } else {
        None
    };
    let key_ref = encryption_key.as_deref();

    let result = match command {
        Commands::Schema {
            name,
            dimension,
            search,
        } => commands::schema::run_with_key(
            name.as_deref(),
            dimension.as_deref(),
            search.as_deref(),
            dev,
            key_ref,
        ),
        Commands::Query {
            file,
            select,
            filter,
            exclude,
            group_by,
            arrange,
            limit,
            indicator,
            no_aggregate,
            format,
        } => commands::schema::resolve_cube(file.to_str().unwrap_or_default(), dev).and_then(
            |path| {
                commands::query::run_with_key(
                    &path,
                    &select,
                    &filter,
                    &exclude,
                    &group_by,
                    &arrange,
                    limit,
                    &indicator,
                    no_aggregate,
                    &format,
                    key_ref,
                )
            },
        ),
        Commands::Sql {
            file,
            query,
            format,
        } => commands::schema::resolve_cube(file.to_str().unwrap_or_default(), dev)
            .and_then(|path| commands::sql::run_with_key(&path, &query, &format, key_ref)),
        Commands::Key { refresh, delete } => commands::key::run(refresh, delete, dev),
        Commands::Feedback { message } => commands::feedback::run(message.as_deref(), dev),
        Commands::Sync {
            prefix,
            cache_dir,
            force,
        } => {
            let bucket = commands::sync::bucket_for(dev);
            commands::sync::run(bucket, &prefix, cache_dir.as_deref(), force)
        }
    };

    match result {
        Ok(()) => {
            // Emit current date/time so LLM agents have temporal context
            // (only for data commands, not sync/key)
            if is_data_command {
                let now = Local::now();
                eprintln!(
                    "cube: today is {} ({}), {} UTC{}",
                    now.format("%Y-%m-%d"),
                    now.format("%A"),
                    now.format("%H:%M"),
                    now.format("%:z"),
                );
            }
            if is_data_command {
                commands::sync::check_for_updates(dev);
            }
            version::check_for_new_version();
            ExitCode::SUCCESS
        }
        Err(e) => {
            error::print_json_error(&e);
            ExitCode::FAILURE
        }
    }
}
