use anyhow::Result;
use serde_json::Value;
use std::path::Path;

use crate::db;
use crate::formatter;

#[allow(dead_code)]
pub fn run(file: &Path, query: &str, format: &str) -> Result<()> {
    run_with_key(file, query, format, None)
}

pub fn run_with_key(file: &Path, query: &str, format: &str, key: Option<&str>) -> Result<()> {
    let conn = db::open_with_key(file, key)?;
    let mut stmt = conn.prepare(query)?;
    let col_count = stmt.column_count();
    let col_names: Vec<String> = (0..col_count)
        .map(|i| stmt.column_name(i).unwrap().to_string())
        .collect();

    let mut rows_data: Vec<Vec<Value>> = Vec::new();
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let mut row_values = Vec::new();
        for i in 0..col_count {
            let val: Value = match row.get_ref(i)? {
                rusqlite::types::ValueRef::Null => Value::Null,
                rusqlite::types::ValueRef::Integer(n) => Value::Number(n.into()),
                rusqlite::types::ValueRef::Real(f) => serde_json::Number::from_f64(f)
                    .map(Value::Number)
                    .unwrap_or(Value::Null),
                rusqlite::types::ValueRef::Text(t) => {
                    Value::String(String::from_utf8_lossy(t).to_string())
                }
                rusqlite::types::ValueRef::Blob(_) => Value::String("<blob>".to_string()),
            };
            row_values.push(val);
        }
        rows_data.push(row_values);
    }

    formatter::output(&col_names, &rows_data, format)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_db() -> tempfile::NamedTempFile {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let conn = rusqlite::Connection::open(tmp.path()).unwrap();
        conn.execute_batch(
            "CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT);
             INSERT INTO metadata VALUES ('schema', '{\"cube\": \"Test\"}');
             CREATE TABLE data (
                 \"Faculté\" TEXT,
                 indicateur REAL
             );
             INSERT INTO data VALUES ('FBM', 100.0);
             INSERT INTO data VALUES ('FBM', 50.0);
             INSERT INTO data VALUES ('SSP', 80.0);",
        )
        .unwrap();
        tmp
    }

    #[test]
    fn test_sql_select_all() {
        let tmp = create_test_db();
        let result = run(tmp.path(), "SELECT * FROM data", "json");
        assert!(result.is_ok());
    }

    #[test]
    fn test_sql_aggregate() {
        let tmp = create_test_db();
        let result = run(
            tmp.path(),
            "SELECT \"Faculté\", SUM(indicateur) AS total FROM data GROUP BY \"Faculté\"",
            "json",
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_sql_count() {
        let tmp = create_test_db();
        let result = run(tmp.path(), "SELECT COUNT(*) AS n FROM data", "csv");
        assert!(result.is_ok());
    }

    #[test]
    fn test_sql_invalid_query() {
        let tmp = create_test_db();
        let result = run(tmp.path(), "SELECT * FROM nonexistent", "json");
        assert!(result.is_err());
    }

    #[test]
    fn test_sql_table_format() {
        let tmp = create_test_db();
        let result = run(tmp.path(), "SELECT * FROM data LIMIT 1", "table");
        assert!(result.is_ok());
    }

    #[test]
    fn test_sql_metadata_table() {
        let tmp = create_test_db();
        let result = run(tmp.path(), "SELECT * FROM metadata", "json");
        assert!(result.is_ok());
    }

    #[test]
    fn test_sql_nonexistent_file() {
        let result = run(Path::new("/nonexistent.sqlite"), "SELECT 1", "json");
        assert!(result.is_err());
    }
}
