use anyhow::Result;
use serde_json::{json, Value};
use std::path::{Path, PathBuf};

use crate::db;
use crate::error::CubeError;

const VALUES_THRESHOLD: usize = 50;
const SAMPLE_SIZE: usize = 50;

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

pub fn run(name: Option<&str>, dimension: Option<&str>, dev: bool) -> Result<()> {
    match (name, dimension) {
        (None, _) => list_cubes(dev),
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
    let columns = db::get_table_columns(&conn, "data")?;

    let col = columns.iter().find(|c| c.name == dimension);
    if col.is_none() {
        return Err(CubeError::not_found(format!(
            "Dimension '{dimension}' introuvable. Dimensions disponibles : {}",
            columns
                .iter()
                .filter(|c| c.col_type.to_uppercase() == "TEXT")
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

fn list_cubes(dev: bool) -> Result<()> {
    let cache = default_cache_dir(dev)?;
    if !cache.exists() {
        return Err(CubeError::not_found(
            "Aucun cache local. Exécutez 'cube sync' pour télécharger les cubes.",
        ));
    }

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

            match build_schema_summary(&path) {
                Ok(summary) => cubes.push(summary),
                Err(_) => {
                    cubes.push(json!({
                        "name": name,
                        "file": path.to_string_lossy(),
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

fn build_schema_summary(file: &Path) -> Result<Value> {
    let conn = db::open(file)?;
    let schema = db::read_metadata_schema(&conn)?;
    let row_count = db::get_row_count(&conn)?;
    let columns = db::get_table_columns(&conn, "data")?;

    let dimensions: Vec<&str> = columns
        .iter()
        .filter(|c| c.col_type.to_uppercase() == "TEXT")
        .map(|c| c.name.as_str())
        .collect();

    let file_stem = file
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("?");

    Ok(json!({
        "name": file_stem,
        "cube": schema.get("cube").unwrap_or(&Value::Null),
        "measure": schema.get("measure").unwrap_or(&Value::Null),
        "measure_description": schema.get("measure_description").unwrap_or(&Value::Null),
        "cube_description": schema.get("cube_description").unwrap_or(&Value::Null),
        "aggregation": schema.get("aggregation").unwrap_or(&Value::Null),
        "row_count": row_count,
        "dimension_count": dimensions.len(),
        "dimensions": dimensions,
        "file": file.to_string_lossy(),
    }))
}

pub(crate) fn build_schema(file: &Path) -> Result<Value> {
    let conn = db::open(file)?;
    let mut schema = db::read_metadata_schema(&conn)?;
    let columns = db::get_table_columns(&conn, "data")?;
    let row_count = db::get_row_count(&conn)?;

    // Build a lookup from the metadata dimensions array
    let meta_dims: Vec<Value> = schema
        .get("dimensions")
        .and_then(|d| d.as_array())
        .cloned()
        .unwrap_or_default();

    let mut dim_infos = Vec::new();
    for col in &columns {
        let mut info = json!({
            "name": col.name,
            "type": col.col_type,
        });
        if col.col_type.to_uppercase() == "TEXT" {
            let distinct = db::get_distinct_count(&conn, &col.name)?;
            info["distinct_count"] = json!(distinct);

            if (distinct as usize) <= VALUES_THRESHOLD {
                let values = db::get_all_distinct_values(&conn, &col.name)?;
                info["values"] = json!(values);
            } else {
                let samples = db::get_sample_values(&conn, &col.name, SAMPLE_SIZE)?;
                info["sample_values"] = json!(samples);
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
        // 2 TEXT + 1 REAL = 3 entries
        assert_eq!(dims.len(), 3);

        // TEXT column with low cardinality → "values" + metadata enrichment
        let fac = &dims[0];
        assert_eq!(fac["name"], "Faculté");
        assert_eq!(fac["type"], "TEXT");
        assert_eq!(fac["distinct_count"], 2);
        let values = fac["values"].as_array().unwrap();
        assert_eq!(values.len(), 2);
        assert!(fac.get("sample_values").is_none() || fac["sample_values"].is_null());

        // Type has a description from metadata
        let typ = &dims[1];
        assert_eq!(typ["name"], "Type");
        assert_eq!(typ["description"], "Type de surface");

        // REAL column stays simple
        let ind = &dims[2];
        assert_eq!(ind["name"], "indicateur");
        assert_eq!(ind["type"], "REAL");
        assert!(ind.get("distinct_count").is_none() || ind["distinct_count"].is_null());
    }

    #[test]
    fn test_build_schema_no_separate_dimensions_key() {
        let tmp = create_test_db();
        let schema = build_schema(tmp.path()).unwrap();
        // dimensions should be the merged array, not the old metadata one
        let dims = schema["dimensions"].as_array().unwrap();
        // Each entry has a "type" field (from columns), not just name/description/parent
        assert!(dims[0].get("type").is_some());
    }

    #[test]
    fn test_build_schema_summary() {
        let tmp = create_test_db();
        let summary = build_schema_summary(tmp.path()).unwrap();
        assert_eq!(summary["cube"], "Infrastructures");
        assert_eq!(summary["row_count"], 3);
        assert_eq!(summary["dimension_count"], 2);
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
        assert!(run(Some(tmp.path().to_str().unwrap()), None, false).is_ok());
    }

    #[test]
    fn test_run_with_dimension() {
        let tmp = create_test_db();
        assert!(run(Some(tmp.path().to_str().unwrap()), Some("Faculté"), false).is_ok());
    }

    #[test]
    fn test_run_nonexistent_file() {
        let result = run(Some("/nonexistent.sqlite"), None, false);
        assert!(result.is_err());
    }

    #[test]
    fn test_run_nonexistent_dimension() {
        let tmp = create_test_db();
        let result = run(Some(tmp.path().to_str().unwrap()), Some("Nope"), false);
        assert!(result.is_err());
    }
}
