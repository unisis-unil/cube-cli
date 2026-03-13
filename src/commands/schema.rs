use anyhow::Result;
use serde_json::{json, Value};
use std::path::{Path, PathBuf};

use crate::db;
use crate::error::CubeError;

const BOUNDARY_SIZE: usize = 10;

pub(crate) fn default_cache_dir(dev: bool) -> Result<PathBuf> {
    let home = dirs::home_dir()
        .ok_or_else(|| CubeError::not_found("Impossible de déterminer le répertoire home"))?;
    let subdir = if dev { "cache-dev" } else { "cache" };
    Ok(home.join(".unisis-cube").join(subdir))
}

/// Resolve a name or path to a .sqlite file path.
/// Accepts: a full path, a filename with extension, or a cube name (resolved from cache).
pub(crate) fn resolve_cube(name: &str, dev: bool) -> Result<PathBuf> {
    let as_path = Path::new(name);

    if as_path.extension().is_some() || name.contains('/') || name.contains('\\') {
        return Ok(as_path.to_path_buf());
    }

    let cache = default_cache_dir(dev)?;
    let candidate = cache.join(format!("{name}.sqlite"));
    if candidate.exists() {
        return Ok(candidate);
    }

    let sync_hint = if dev { "cube --dev sync" } else { "cube sync" };
    Err(CubeError::not_found(format!(
        "Cube '{name}' introuvable. Vérifiez le nom ou exécutez '{sync_hint}' pour mettre à jour le cache."
    )))
}

pub fn run(name: Option<&str>, dimension: Option<&str>, search: Option<&str>, dev: bool) -> Result<()> {
    match (name, dimension) {
        (None, _) => list_cubes(search, dev),
        (Some(n), None) => {
            let path = resolve_cube(n, dev)?;
            let schema = build_schema(&path)?;
            println!("{}", serde_json::to_string_pretty(&schema)?);
            Ok(())
        }
        (Some(n), Some(dim)) => {
            let path = resolve_cube(n, dev)?;
            let result = list_dimension_values(&path, dim)?;
            println!("{}", serde_json::to_string_pretty(&result)?);
            Ok(())
        }
    }
}

fn list_dimension_values(file: &Path, dimension: &str) -> Result<Value> {
    let conn = db::open(file)?;
    let schema = db::read_metadata_schema(&conn)?;
    let columns = db::get_table_columns(&conn, "data")?;

    let indicator_col = schema
        .get("indicator_column")
        .and_then(|v| v.as_str())
        .unwrap_or("indicateur");

    let col = columns.iter().find(|c| c.name == dimension);
    if col.is_none() {
        return Err(CubeError::not_found(format!(
            "Dimension '{dimension}' introuvable. Dimensions disponibles : {}",
            columns
                .iter()
                .filter(|c| c.name != indicator_col)
                .map(|c| c.name.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        )));
    }

    let values = db::get_all_distinct_values(&conn, dimension)?;
    let distinct_count = values.len();

    Ok(json!({
        "dimension": dimension,
        "distinct_count": distinct_count,
        "values": values,
    }))
}

/// Normalize a string for accent-insensitive matching (lowercase + strip combining marks).
fn normalize_for_search(s: &str) -> String {
    use unicode_normalization::UnicodeNormalization;
    s.nfd()
        .filter(|c| !unicode_normalization::char::is_combining_mark(*c))
        .collect::<String>()
        .to_lowercase()
}

/// Level 0: compact catalogue — name, description, measure (no dimensions).
fn list_cubes(search: Option<&str>, dev: bool) -> Result<()> {
    let cache = default_cache_dir(dev)?;
    if !cache.exists() {
        return Err(CubeError::not_found(
            "Aucun cache local. Exécutez 'cube sync' pour télécharger les cubes.",
        ));
    }

    let search_re = match search {
        Some(pat) => {
            let normalized_pat = normalize_for_search(pat);
            Some(regex::Regex::new(&normalized_pat).map_err(|e| {
                CubeError::validation(format!("Expression régulière invalide '{pat}': {e}"))
            })?)
        }
        None => None,
    };

    let mut cubes = Vec::new();
    for entry in std::fs::read_dir(&cache)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) == Some("sqlite") {
            let name = path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("?")
                .to_string();

            match build_catalogue_entry(&path) {
                Ok(entry_val) => {
                    if let Some(ref re) = search_re {
                        let haystack = format!(
                            "{} {} {}",
                            entry_val["name"].as_str().unwrap_or(""),
                            entry_val["cube"].as_str().unwrap_or(""),
                            entry_val["cube_description"].as_str().unwrap_or(""),
                        );
                        if !re.is_match(&normalize_for_search(&haystack)) {
                            continue;
                        }
                    }
                    cubes.push(entry_val);
                }
                Err(_) => {
                    cubes.push(json!({
                        "name": name,
                        "error": "Impossible de lire le schéma"
                    }));
                }
            }
        }
    }

    cubes.sort_by(|a, b| {
        let na = a["name"].as_str().unwrap_or("");
        let nb = b["name"].as_str().unwrap_or("");
        na.cmp(nb)
    });

    println!("{}", serde_json::to_string_pretty(&cubes)?);
    Ok(())
}

/// Build a compact catalogue entry for level 0 (no dimensions, no file path).
fn build_catalogue_entry(file: &Path) -> Result<Value> {
    let conn = db::open(file)?;
    let schema = db::read_metadata_schema(&conn)?;
    let row_count = db::get_row_count(&conn)?;
    let columns = db::get_table_columns(&conn, "data")?;

    let indicator_col = schema
        .get("indicator_column")
        .and_then(|v| v.as_str())
        .unwrap_or("indicateur");

    let dimension_count = columns
        .iter()
        .filter(|c| c.name != indicator_col)
        .count();

    let file_stem = file
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("?");

    Ok(json!({
        "name": file_stem,
        "cube": schema.get("cube").unwrap_or(&Value::Null),
        "cube_description": schema.get("cube_description").unwrap_or(&Value::Null),
        "measure": schema.get("measure").unwrap_or(&Value::Null),
        "measure_description": schema.get("measure_description").unwrap_or(&Value::Null),
        "aggregation": schema.get("aggregation").unwrap_or(&Value::Null),
        "row_count": row_count,
        "dimension_count": dimension_count,
    }))
}

/// Level 1: cube schema — dimensions with type, description and cardinality (no values).
pub(crate) fn build_schema(file: &Path) -> Result<Value> {
    let conn = db::open(file)?;
    let mut schema = db::read_metadata_schema(&conn)?;
    let columns = db::get_table_columns(&conn, "data")?;
    let row_count = db::get_row_count(&conn)?;

    let indicator_col = schema
        .get("indicator_column")
        .and_then(|v| v.as_str())
        .unwrap_or("indicateur")
        .to_string();

    // Build a lookup from the metadata dimensions array
    let meta_dims: Vec<Value> = schema
        .get("dimensions")
        .and_then(|d| d.as_array())
        .cloned()
        .unwrap_or_default();

    let mut dim_infos = Vec::new();
    for col in &columns {
        if col.name == indicator_col {
            continue;
        }

        let mut info = json!({
            "name": col.name,
            "type": col.col_type,
        });

        let col_type_upper = col.col_type.to_uppercase();
        if col_type_upper == "TEXT" {
            let values = db::get_all_distinct_values(&conn, &col.name)?;
            let distinct = values.len();
            info["distinct_count"] = json!(distinct);

            if distinct <= BOUNDARY_SIZE * 2 {
                info["values"] = json!(values);
            } else {
                info["first_values"] = json!(&values[..BOUNDARY_SIZE]);
                info["last_values"] = json!(&values[distinct - BOUNDARY_SIZE..]);
            }
        } else {
            let (min, max, distinct) = db::get_numeric_stats(&conn, &col.name)?;
            info["min"] = json!(min);
            info["max"] = json!(max);
            info["distinct_count"] = json!(distinct);
        }

        // Enrich with description and parent from metadata
        if let Some(meta) = meta_dims.iter().find(|d| d["name"] == col.name) {
            let desc = meta.get("description").unwrap_or(&Value::Null);
            if !desc.is_null() && desc.as_object().map_or(true, |o| !o.is_empty()) {
                info["description"] = desc.clone();
            }
            let parent = meta.get("parent").unwrap_or(&Value::Null);
            if !parent.is_null() && parent.as_object().map_or(true, |o| !o.is_empty()) {
                info["parent"] = parent.clone();
            }
        }

        dim_infos.push(info);
    }

    if let Value::Object(ref mut map) = schema {
        map.remove("dimensions");
        map.insert("row_count".to_string(), json!(row_count));
        map.insert("dimensions".to_string(), json!(dim_infos));
    }

    Ok(schema)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_db() -> tempfile::NamedTempFile {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let conn = rusqlite::Connection::open(tmp.path()).unwrap();
        conn.execute_batch(
            r#"CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT);
             INSERT INTO metadata VALUES ('schema', '{
                 "cube": "Infrastructures",
                 "measure": "Surface",
                 "measure_description": "surfaces en m²",
                 "cube_description": "Répertorie les surfaces.",
                 "indicator_column": "indicateur",
                 "aggregation": "SUM",
                 "dimensions": [
                     {"name": "Faculté", "description": null, "parent": null},
                     {"name": "Type", "description": "Type de surface", "parent": null}
                 ]
             }');
             CREATE TABLE data (
                 "Faculté" TEXT,
                 "Type" TEXT,
                 indicateur REAL
             );
             INSERT INTO data VALUES ('FBM', 'Bureau', 100.0);
             INSERT INTO data VALUES ('FBM', 'Labo', 200.0);
             INSERT INTO data VALUES ('SSP', 'Bureau', 80.0);"#,
        )
        .unwrap();
        tmp
    }

    #[test]
    fn test_build_schema_contains_metadata() {
        let tmp = create_test_db();
        let schema = build_schema(tmp.path()).unwrap();
        assert_eq!(schema["cube"], "Infrastructures");
        assert_eq!(schema["measure"], "Surface");
        assert_eq!(schema["aggregation"], "SUM");
    }

    #[test]
    fn test_build_schema_row_count() {
        let tmp = create_test_db();
        let schema = build_schema(tmp.path()).unwrap();
        assert_eq!(schema["row_count"], 3);
    }

    #[test]
    fn test_build_schema_dimensions_merged() {
        let tmp = create_test_db();
        let schema = build_schema(tmp.path()).unwrap();
        let dims = schema["dimensions"].as_array().unwrap();
        // 2 TEXT columns only (indicator excluded)
        assert_eq!(dims.len(), 2);

        // Low cardinality (≤ 20) → sorted "values" array
        let fac = &dims[0];
        assert_eq!(fac["name"], "Faculté");
        assert_eq!(fac["type"], "TEXT");
        assert_eq!(fac["distinct_count"], 2);
        let values = fac["values"].as_array().unwrap();
        assert_eq!(values, &[json!("FBM"), json!("SSP")]);
        assert!(fac.get("first_values").is_none() || fac["first_values"].is_null());

        // Type has a description from metadata
        let typ = &dims[1];
        assert_eq!(typ["name"], "Type");
        assert_eq!(typ["description"], "Type de surface");
        let type_values = typ["values"].as_array().unwrap();
        assert_eq!(type_values, &[json!("Bureau"), json!("Labo")]);
    }

    #[test]
    fn test_build_schema_no_separate_dimensions_key() {
        let tmp = create_test_db();
        let schema = build_schema(tmp.path()).unwrap();
        // dimensions should be the merged array, not the old metadata one
        let dims = schema["dimensions"].as_array().unwrap();
        assert_eq!(dims.len(), 2); // indicator excluded
        // Each entry has a "type" field (from columns), not just name/description/parent
        assert!(dims[0].get("type").is_some());
    }

    #[test]
    fn test_build_catalogue_entry() {
        let tmp = create_test_db();
        let entry = build_catalogue_entry(tmp.path()).unwrap();
        assert_eq!(entry["cube"], "Infrastructures");
        assert_eq!(entry["row_count"], 3);
        assert_eq!(entry["dimension_count"], 2);
        // Level 0: no dimensions list
        assert!(entry.get("dimensions").is_none());
    }

    #[test]
    fn test_list_dimension_values() {
        let tmp = create_test_db();
        let result = list_dimension_values(tmp.path(), "Faculté").unwrap();
        assert_eq!(result["dimension"], "Faculté");
        assert_eq!(result["distinct_count"], 2);
        let values = result["values"].as_array().unwrap();
        assert!(values.contains(&json!("FBM")));
        assert!(values.contains(&json!("SSP")));
    }

    #[test]
    fn test_list_dimension_values_not_found() {
        let tmp = create_test_db();
        let result = list_dimension_values(tmp.path(), "Nonexistent");
        assert!(result.is_err());
        let err = result.unwrap_err();
        let cube_err = err.downcast_ref::<CubeError>().unwrap();
        assert_eq!(cube_err.code, 404);
    }

    #[test]
    fn test_resolve_cube_with_extension() {
        let result = resolve_cube("foo.sqlite", false).unwrap();
        assert_eq!(result, PathBuf::from("foo.sqlite"));
    }

    #[test]
    fn test_resolve_cube_with_path() {
        let result = resolve_cube("./data/foo", false).unwrap();
        assert_eq!(result, PathBuf::from("./data/foo"));
    }

    #[test]
    fn test_resolve_cube_not_found() {
        let result = resolve_cube("nonexistent_cube_xyz", false);
        assert!(result.is_err());
    }

    #[test]
    fn test_run_with_file() {
        let tmp = create_test_db();
        assert!(run(Some(tmp.path().to_str().unwrap()), None, None, false).is_ok());
    }

    #[test]
    fn test_run_with_dimension() {
        let tmp = create_test_db();
        assert!(run(Some(tmp.path().to_str().unwrap()), Some("Faculté"), None, false).is_ok());
    }

    #[test]
    fn test_run_nonexistent_file() {
        let result = run(Some("/nonexistent.sqlite"), None, None, false);
        assert!(result.is_err());
    }

    #[test]
    fn test_run_nonexistent_dimension() {
        let tmp = create_test_db();
        let result = run(Some(tmp.path().to_str().unwrap()), Some("Nope"), None, false);
        assert!(result.is_err());
    }

    fn create_mixed_type_db() -> tempfile::NamedTempFile {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let conn = rusqlite::Connection::open(tmp.path()).unwrap();
        conn.execute_batch(
            r#"CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT);
             INSERT INTO metadata VALUES ('schema', '{
                 "cube": "Personnel",
                 "measure": "Effectif",
                 "measure_description": "nombre de personnes",
                 "cube_description": "Effectifs du personnel",
                 "indicator_column": "indicateur",
                 "aggregation": "SUM",
                 "dimensions": [
                     {"name": "Faculté", "description": null, "parent": null}
                 ]
             }');
             CREATE TABLE data (
                 "Faculté" TEXT,
                 "Age" INTEGER,
                 "Année civile" REAL,
                 "Taux de contrat" REAL,
                 indicateur REAL
             );
             INSERT INTO data VALUES ('FBM', 35, 2023.0, 0.8, 1.0);
             INSERT INTO data VALUES ('FBM', 42, 2023.0, 1.0, 1.0);
             INSERT INTO data VALUES ('SSP', 28, 2024.0, 0.5, 1.0);"#,
        )
        .unwrap();
        tmp
    }

    #[test]
    fn test_build_schema_includes_numeric_dimensions() {
        let tmp = create_mixed_type_db();
        let schema = build_schema(tmp.path()).unwrap();
        let dims = schema["dimensions"].as_array().unwrap();
        // 1 TEXT + 3 numeric (indicator excluded)
        assert_eq!(dims.len(), 4);

        let names: Vec<&str> = dims.iter().map(|d| d["name"].as_str().unwrap()).collect();
        assert!(names.contains(&"Age"));
        assert!(names.contains(&"Année civile"));
        assert!(names.contains(&"Taux de contrat"));
        assert!(!names.contains(&"indicateur"));

        // Numeric columns have min/max/distinct_count
        let age = dims.iter().find(|d| d["name"] == "Age").unwrap();
        assert_eq!(age["type"], "INTEGER");
        assert_eq!(age["min"], 28.0);
        assert_eq!(age["max"], 42.0);
        assert_eq!(age["distinct_count"], 3);
    }

    #[test]
    fn test_build_catalogue_entry_includes_numeric_dimensions_in_count() {
        let tmp = create_mixed_type_db();
        let entry = build_catalogue_entry(tmp.path()).unwrap();
        assert_eq!(entry["dimension_count"], 4);
        // Level 0: no dimensions list, only count
        assert!(entry.get("dimensions").is_none());
    }

    fn create_high_cardinality_db() -> tempfile::NamedTempFile {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let conn = rusqlite::Connection::open(tmp.path()).unwrap();
        conn.execute_batch(
            r#"CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT);
             INSERT INTO metadata VALUES ('schema', '{
                 "cube": "HighCard",
                 "measure": "Count",
                 "indicator_column": "indicateur",
                 "aggregation": "SUM",
                 "dimensions": []
             }');
             CREATE TABLE data (
                 "Code" TEXT,
                 indicateur REAL
             );"#,
        )
        .unwrap();
        // Insert 25 distinct sorted values: V01..V25
        for i in 1..=25 {
            conn.execute(
                "INSERT INTO data VALUES (?1, 1.0)",
                [format!("V{:02}", i)],
            )
            .unwrap();
        }
        tmp
    }

    #[test]
    fn test_build_schema_high_cardinality_first_last() {
        let tmp = create_high_cardinality_db();
        let schema = build_schema(tmp.path()).unwrap();
        let dims = schema["dimensions"].as_array().unwrap();
        assert_eq!(dims.len(), 1);

        let code = &dims[0];
        assert_eq!(code["name"], "Code");
        assert_eq!(code["distinct_count"], 25);
        // > 20 → no "values", instead "first_values" and "last_values"
        assert!(code.get("values").is_none() || code["values"].is_null());

        let first = code["first_values"].as_array().unwrap();
        assert_eq!(first.len(), 10);
        assert_eq!(first[0], "V01");
        assert_eq!(first[9], "V10");

        let last = code["last_values"].as_array().unwrap();
        assert_eq!(last.len(), 10);
        assert_eq!(last[0], "V16");
        assert_eq!(last[9], "V25");
    }

    #[test]
    fn test_build_schema_boundary_20_values_shows_all() {
        // Exactly 20 values → should show "values", not first/last
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let conn = rusqlite::Connection::open(tmp.path()).unwrap();
        conn.execute_batch(
            r#"CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT);
             INSERT INTO metadata VALUES ('schema', '{
                 "cube": "Boundary",
                 "measure": "Count",
                 "indicator_column": "indicateur",
                 "aggregation": "SUM",
                 "dimensions": []
             }');
             CREATE TABLE data ("Code" TEXT, indicateur REAL);"#,
        )
        .unwrap();
        for i in 1..=20 {
            conn.execute("INSERT INTO data VALUES (?1, 1.0)", [format!("V{:02}", i)])
                .unwrap();
        }

        let schema = build_schema(tmp.path()).unwrap();
        let code = &schema["dimensions"].as_array().unwrap()[0];
        assert_eq!(code["distinct_count"], 20);
        assert!(code["values"].as_array().is_some());
        assert!(code.get("first_values").is_none() || code["first_values"].is_null());
    }

    #[test]
    fn test_normalize_for_search() {
        assert_eq!(normalize_for_search("Réussite"), "reussite");
        assert_eq!(normalize_for_search("étudiants"), "etudiants");
        assert_eq!(normalize_for_search("BACHELOR"), "bachelor");
    }
}
