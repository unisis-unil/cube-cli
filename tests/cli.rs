use assert_cmd::Command;
use predicates::prelude::*;
use std::path::PathBuf;

fn test_db_path() -> PathBuf {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let path = tmp.path().to_path_buf();
    // Keep the file alive by leaking it (tests are short-lived)
    std::mem::forget(tmp);
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.execute_batch(
        r#"CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT);
         INSERT INTO metadata VALUES ('schema', '{
             "cube": "TestCube",
             "measure": "Surface",
             "measure_description": "surfaces en m²",
             "cube_description": "Cube de test.",
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
         INSERT INTO data VALUES ('FBM', 'Bureau', 120.5);
         INSERT INTO data VALUES ('FBM', 'Bureau', 85.0);
         INSERT INTO data VALUES ('FBM', 'Labo', 200.0);
         INSERT INTO data VALUES ('SSP', 'Bureau', 90.0);
         INSERT INTO data VALUES ('SSP', 'Salle', 150.0);
         INSERT INTO data VALUES ('HEC', 'Bureau', 110.0);
         INSERT INTO data VALUES ('HEC', 'Salle', 250.0);
         INSERT INTO data VALUES ('HEC', 'Labo', 75.0);"#,
    )
    .unwrap();
    path
}

// --- Help ---

#[test]
fn test_help() {
    Command::cargo_bin("cube")
        .unwrap()
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("UNISIS"))
        .stdout(predicate::str::contains("schema"))
        .stdout(predicate::str::contains("query"))
        .stdout(predicate::str::contains("sql"))
        .stdout(predicate::str::contains("sync"))
        .stdout(predicate::str::contains("EXAMPLES"))
        .stdout(predicate::str::contains("SOURCE"));
}

#[test]
fn test_no_args() {
    Command::cargo_bin("cube")
        .unwrap()
        .assert()
        .success()
        .stdout(predicate::str::contains("cube"));
}

// --- Schema ---

#[test]
fn test_schema_json_output() {
    let db = test_db_path();
    let output = Command::cargo_bin("cube")
        .unwrap()
        .args(["schema", db.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(output.status.success());
    let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(json["cube"], "TestCube");
    assert_eq!(json["row_count"], 8);
    let dims = json["dimensions"].as_array().unwrap();
    assert_eq!(dims.len(), 2); // indicator excluded
                               // Level 1: low cardinality (≤ 20) → sorted "values" array
    let fac = &dims[0];
    assert_eq!(fac["name"], "Faculté");
    assert_eq!(fac["distinct_count"], 3);
    let values = fac["values"].as_array().unwrap();
    assert_eq!(values.len(), 3);
}

#[test]
fn test_schema_dimension_values() {
    let db = test_db_path();
    let output = Command::cargo_bin("cube")
        .unwrap()
        .args(["schema", db.to_str().unwrap(), "Faculté"])
        .output()
        .unwrap();
    assert!(output.status.success());
    let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(json["dimension"], "Faculté");
    let values = json["values"].as_array().unwrap();
    assert_eq!(values.len(), 3);
    assert!(values.contains(&serde_json::json!("FBM")));
    assert!(values.contains(&serde_json::json!("SSP")));
    assert!(values.contains(&serde_json::json!("HEC")));
}

#[test]
fn test_schema_dimension_not_found() {
    let db = test_db_path();
    let output = Command::cargo_bin("cube")
        .unwrap()
        .args(["schema", db.to_str().unwrap(), "Nonexistent"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    let err: serde_json::Value = serde_json::from_str(&stderr).unwrap();
    assert_eq!(err["error"]["code"], 404);
}

#[test]
fn test_schema_nonexistent_file() {
    let output = Command::cargo_bin("cube")
        .unwrap()
        .args(["schema", "/nonexistent.sqlite"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    let err: serde_json::Value = serde_json::from_str(&stderr).unwrap();
    assert_eq!(err["error"]["code"], 404);
    assert_eq!(err["error"]["reason"], "notFound");
}

#[test]
fn test_schema_no_args_lists_cache() {
    // Without args, schema lists cubes from cache (may be empty or populated)
    let output = Command::cargo_bin("cube")
        .unwrap()
        .args(["schema"])
        .output()
        .unwrap();
    // Either succeeds with JSON array, or fails with notFound JSON error
    if output.status.success() {
        let json: Vec<serde_json::Value> = serde_json::from_slice(&output.stdout).unwrap();
        assert!(json.is_empty() || json[0].get("name").is_some());
    } else {
        let stderr = String::from_utf8(output.stderr).unwrap();
        let err: serde_json::Value = serde_json::from_str(&stderr).unwrap();
        assert!(err["error"]["code"].is_number());
    }
}

// --- Query ---

#[test]
fn test_query_group_by_table() {
    let db = test_db_path();
    Command::cargo_bin("cube")
        .unwrap()
        .args(["query", db.to_str().unwrap(), "--group-by", "Faculté"])
        .assert()
        .success()
        .stdout(predicate::str::contains("FBM"))
        .stdout(predicate::str::contains("SSP"))
        .stdout(predicate::str::contains("HEC"));
}

#[test]
fn test_query_group_by_json() {
    let db = test_db_path();
    let output = Command::cargo_bin("cube")
        .unwrap()
        .args([
            "query",
            db.to_str().unwrap(),
            "--group-by",
            "Faculté",
            "--format",
            "json",
        ])
        .output()
        .unwrap();
    assert!(output.status.success());
    let json: Vec<serde_json::Value> = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(json.len(), 3);
    for row in &json {
        assert!(row.get("Faculté").is_some());
        assert!(row.get("indicateur").is_some());
    }
}

#[test]
fn test_query_group_by_csv() {
    let db = test_db_path();
    let output = Command::cargo_bin("cube")
        .unwrap()
        .args([
            "query",
            db.to_str().unwrap(),
            "--group-by",
            "Faculté",
            "--format",
            "csv",
        ])
        .output()
        .unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    let lines: Vec<&str> = stdout.trim().lines().collect();
    assert_eq!(lines[0], "Faculté,indicateur");
    assert_eq!(lines.len(), 4); // header + 3 rows
}

#[test]
fn test_query_filter_include() {
    let db = test_db_path();
    let output = Command::cargo_bin("cube")
        .unwrap()
        .args([
            "query",
            db.to_str().unwrap(),
            "--group-by",
            "Faculté",
            "--filter",
            "Faculté=FBM",
            "--format",
            "json",
        ])
        .output()
        .unwrap();
    assert!(output.status.success());
    let json: Vec<serde_json::Value> = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(json.len(), 1);
    assert_eq!(json[0]["Faculté"], "FBM");
    assert_eq!(json[0]["indicateur"], 405.5);
}

#[test]
fn test_query_filter_multiple_values() {
    let db = test_db_path();
    let output = Command::cargo_bin("cube")
        .unwrap()
        .args([
            "query",
            db.to_str().unwrap(),
            "--group-by",
            "Faculté",
            "--filter",
            "Faculté=FBM",
            "--filter",
            "Faculté=SSP",
            "--format",
            "json",
        ])
        .output()
        .unwrap();
    assert!(output.status.success());
    let json: Vec<serde_json::Value> = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(json.len(), 2);
}

#[test]
fn test_query_exclude() {
    let db = test_db_path();
    let output = Command::cargo_bin("cube")
        .unwrap()
        .args([
            "query",
            db.to_str().unwrap(),
            "--group-by",
            "Faculté,Type",
            "--filter",
            "Faculté=FBM",
            "--exclude",
            "Type=Labo",
            "--format",
            "json",
        ])
        .output()
        .unwrap();
    assert!(output.status.success());
    let json: Vec<serde_json::Value> = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(json.len(), 1);
    assert_eq!(json[0]["Type"], "Bureau");
    assert_eq!(json[0]["indicateur"], 205.5);
}

#[test]
fn test_query_arrange_desc() {
    let db = test_db_path();
    let output = Command::cargo_bin("cube")
        .unwrap()
        .args([
            "query",
            db.to_str().unwrap(),
            "--group-by",
            "Faculté",
            "--arrange",
            "indicateur:desc",
            "--format",
            "json",
        ])
        .output()
        .unwrap();
    assert!(output.status.success());
    let json: Vec<serde_json::Value> = serde_json::from_slice(&output.stdout).unwrap();
    let values: Vec<f64> = json
        .iter()
        .map(|r| r["indicateur"].as_f64().unwrap())
        .collect();
    assert!(values[0] >= values[1] && values[1] >= values[2]);
}

#[test]
fn test_query_limit() {
    let db = test_db_path();
    let output = Command::cargo_bin("cube")
        .unwrap()
        .args([
            "query",
            db.to_str().unwrap(),
            "--group-by",
            "Faculté",
            "--arrange",
            "indicateur:desc",
            "--limit",
            "2",
            "--format",
            "json",
        ])
        .output()
        .unwrap();
    assert!(output.status.success());
    let json: Vec<serde_json::Value> = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(json.len(), 2);
}

#[test]
fn test_query_no_aggregate() {
    let db = test_db_path();
    let output = Command::cargo_bin("cube")
        .unwrap()
        .args([
            "query",
            db.to_str().unwrap(),
            "--no-aggregate",
            "--format",
            "json",
        ])
        .output()
        .unwrap();
    assert!(output.status.success());
    let json: Vec<serde_json::Value> = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(json.len(), 8); // all raw rows
}

#[test]
fn test_query_no_aggregate_with_select_and_limit() {
    let db = test_db_path();
    let output = Command::cargo_bin("cube")
        .unwrap()
        .args([
            "query",
            db.to_str().unwrap(),
            "--no-aggregate",
            "--select",
            "Faculté,indicateur",
            "--arrange",
            "indicateur:desc",
            "--limit",
            "3",
            "--format",
            "json",
        ])
        .output()
        .unwrap();
    assert!(output.status.success());
    let json: Vec<serde_json::Value> = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(json.len(), 3);
    // Should only have selected columns
    assert!(json[0].get("Faculté").is_some());
    assert!(json[0].get("indicateur").is_some());
    assert!(json[0].get("Type").is_none());
}

#[test]
fn test_query_missing_group_by() {
    let db = test_db_path();
    Command::cargo_bin("cube")
        .unwrap()
        .args(["query", db.to_str().unwrap()])
        .assert()
        .failure()
        .stderr(predicate::str::contains("--group-by"));
}

#[test]
fn test_query_select_subset() {
    let db = test_db_path();
    let output = Command::cargo_bin("cube")
        .unwrap()
        .args([
            "query",
            db.to_str().unwrap(),
            "--group-by",
            "Faculté,Type",
            "--select",
            "Faculté",
            "--limit",
            "1",
            "--format",
            "json",
        ])
        .output()
        .unwrap();
    assert!(output.status.success());
    let json: Vec<serde_json::Value> = serde_json::from_slice(&output.stdout).unwrap();
    assert!(json[0].get("Faculté").is_some());
    assert!(json[0].get("indicateur").is_some());
}

#[test]
fn test_query_multi_group_by() {
    let db = test_db_path();
    let output = Command::cargo_bin("cube")
        .unwrap()
        .args([
            "query",
            db.to_str().unwrap(),
            "--group-by",
            "Faculté,Type",
            "--format",
            "json",
        ])
        .output()
        .unwrap();
    assert!(output.status.success());
    let json: Vec<serde_json::Value> = serde_json::from_slice(&output.stdout).unwrap();
    // FBM/Bureau, FBM/Labo, SSP/Bureau, SSP/Salle, HEC/Bureau, HEC/Salle, HEC/Labo = 7
    assert_eq!(json.len(), 7);
}

// --- SQL ---

#[test]
fn test_sql_select() {
    let db = test_db_path();
    let output = Command::cargo_bin("cube")
        .unwrap()
        .args([
            "sql", db.to_str().unwrap(),
            "SELECT \"Faculté\", SUM(indicateur) AS total FROM data GROUP BY \"Faculté\" ORDER BY total DESC",
            "--format", "json",
        ])
        .output()
        .unwrap();
    assert!(output.status.success());
    let json: Vec<serde_json::Value> = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(json.len(), 3);
    assert_eq!(json[0]["Faculté"], "HEC");
}

#[test]
fn test_sql_table_format() {
    let db = test_db_path();
    Command::cargo_bin("cube")
        .unwrap()
        .args([
            "sql",
            db.to_str().unwrap(),
            "SELECT COUNT(*) AS n FROM data",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("8"));
}

#[test]
fn test_sql_csv_format() {
    let db = test_db_path();
    let output = Command::cargo_bin("cube")
        .unwrap()
        .args([
            "sql",
            db.to_str().unwrap(),
            "SELECT COUNT(*) AS n FROM data",
            "--format",
            "csv",
        ])
        .output()
        .unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("n\n8\n"));
}

#[test]
fn test_sql_invalid() {
    let db = test_db_path();
    Command::cargo_bin("cube")
        .unwrap()
        .args(["sql", db.to_str().unwrap(), "INVALID SQL"])
        .assert()
        .failure();
}

// --- Sync (just argument parsing, not actual GCS) ---

#[test]
fn test_sync_help() {
    Command::cargo_bin("cube")
        .unwrap()
        .args(["sync", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("bucket"))
        .stdout(predicate::str::contains("prefix"));
}
