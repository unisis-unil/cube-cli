mod commands;
mod db;
mod error;
mod formatter;

use chrono::Local;
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use std::process::ExitCode;

/// cube — UNISIS Data Warehouse CLI
///
/// Query SQLite cubes exported from the UNISIS data warehouse
/// at Université de Lausanne.
///
/// USAGE:
///     cube schema [--search TERM] [<cube> [<dimension>]]
///     cube query <cube> --group-by <dim> [flags]
///     cube sql <cube> "SELECT ..."
///     cube sync
///
/// EXAMPLES:
///     cube schema
///     cube schema --search "réussite"
///     cube schema infrastructures_surface
///     cube schema infrastructures_surface "Faculté"
///     cube query etudiants_nombre_d_etudiants_plan_principal --group-by Faculté --filter Semestre=2025A
///     cube query etudiants_nombre_d_etudiants_plan_principal --group-by Faculté --group-by Sexe --arrange indicateur:desc --limit 10 --format json
///     cube sql infrastructures_surface "SELECT Faculté, SUM(indicateur) FROM data GROUP BY Faculté"
///     cube sync
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
    /// EXAMPLES:
    ///     # Group by Faculté
    ///     cube query infrastructures_surface --group-by Faculté
    ///
    ///     # Two faculties (OR): Faculté IN ('FBM', 'SSP')
    ///     cube query infrastructures_surface --group-by Faculté --filter Faculté=FBM --filter Faculté=SSP
    ///
    ///     # Cross-dimension (AND): Faculté='FBM' AND Type='Bureau'
    ///     cube query infrastructures_surface --group-by Faculté --group-by Type --filter Faculté=FBM --filter Type=Bureau
    ///
    ///     # Exclude labs, sorted descending, top 5
    ///     cube query infrastructures_surface --group-by Faculté --exclude Type=Labo --arrange indicateur:desc --limit 5
    ///
    ///     # For complex boolean logic, use 'cube sql' instead
    #[command(verbatim_doc_comment)]
    Query {
        /// Cube name or path to .sqlite file
        file: PathBuf,

        /// Columns to display (repeatable, default: group-by columns + indicator)
        #[arg(long, value_name = "COL")]
        select: Vec<String>,

        /// Include filter: col=val (repeatable, same column → OR)
        #[arg(long, value_name = "EXPR")]
        filter: Vec<String>,

        /// Exclude filter: col=val (repeatable, same column → OR)
        #[arg(long, value_name = "EXPR")]
        exclude: Vec<String>,

        /// Group-by columns (repeatable, required unless --no-aggregate)
        #[arg(long, value_name = "COL")]
        group_by: Vec<String>,

        /// Sort order: col or col:asc or col:desc (repeatable)
        #[arg(long, value_name = "SPEC")]
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
    Sql {
        /// Cube name or path to .sqlite file
        file: PathBuf,

        /// SQL query to execute
        query: String,

        /// Output format: table, csv, json
        #[arg(long, value_name = "FMT", default_value = "table")]
        format: String,
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

    // Check for cube updates before schema/query/sql (not sync)
    if !is_sync {
        commands::sync::check_for_updates(dev);
    }

    let result = match command {
        Commands::Schema { name, dimension, search } => {
            commands::schema::run(name.as_deref(), dimension.as_deref(), search.as_deref(), dev)
        }
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
        } => commands::schema::resolve_cube(file.to_str().unwrap_or_default(), dev)
            .and_then(|path| commands::query::run(
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
            )),
        Commands::Sql {
            file,
            query,
            format,
        } => commands::schema::resolve_cube(file.to_str().unwrap_or_default(), dev)
            .and_then(|path| commands::sql::run(&path, &query, &format)),
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
            // (only for data commands, not sync)
            if !is_sync {
                let now = Local::now();
                eprintln!("cube: today is {} ({}), {} UTC{}",
                    now.format("%Y-%m-%d"),
                    now.format("%A"),
                    now.format("%H:%M"),
                    now.format("%:z"),
                );
            }
            ExitCode::SUCCESS
        }
        Err(e) => {
            error::print_json_error(&e);
            ExitCode::FAILURE
        }
    }
}
