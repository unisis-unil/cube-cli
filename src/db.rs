use anyhow::{Context, Result};
use rusqlite::Connection;
use serde_json::Value;
use std::path::Path;

pub fn open(path: &Path) -> Result<Connection> {
    if !path.exists() {
        return Err(crate::error::CubeError::not_found(format!(
            "Fichier introuvable : {}",
            path.display()
        )));
    }
    let conn = Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
        .with_context(|| format!("Impossible d'ouvrir {}", path.display()))?;
    Ok(conn)
}

pub fn read_metadata_schema(conn: &Connection) -> Result<Value> {
    let json_str: String = conn.query_row(
        "SELECT value FROM metadata WHERE key = 'schema'",
        [],
        |row| row.get(0),
    )?;
    let schema: Value = serde_json::from_str(&json_str)?;
    Ok(schema)
}

pub struct ColumnInfo {
    pub name: String,
    pub col_type: String,
}

pub fn get_table_columns(conn: &Connection, table: &str) -> Result<Vec<ColumnInfo>> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info(\"{}\")", table))?;
    let cols = stmt
        .query_map([], |row| {
            Ok(ColumnInfo {
                name: row.get(1)?,
                col_type: row.get(2)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(cols)
}

pub fn get_row_count(conn: &Connection) -> Result<i64> {
    let count: i64 = conn.query_row("SELECT COUNT(*) FROM data", [], |row| row.get(0))?;
    Ok(count)
}

pub fn get_distinct_count(conn: &Connection, column: &str) -> Result<i64> {
    let count: i64 = conn.query_row(
        &format!("SELECT COUNT(DISTINCT \"{}\") FROM data", column),
        [],
        |row| row.get(0),
    )?;
    Ok(count)
}

pub fn get_sample_values(conn: &Connection, column: &str, limit: usize) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(&format!(
        "SELECT DISTINCT \"{}\" FROM data WHERE \"{}\" IS NOT NULL LIMIT {}",
        column, column, limit
    ))?;
    let values = stmt
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(values)
}

pub fn get_numeric_stats(conn: &Connection, column: &str) -> Result<(f64, f64, i64)> {
    let (min, max, distinct): (f64, f64, i64) = conn.query_row(
        &format!(
            "SELECT MIN(\"{0}\"), MAX(\"{0}\"), COUNT(DISTINCT \"{0}\") FROM data WHERE \"{0}\" IS NOT NULL",
            column
        ),
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    Ok((min, max, distinct))
}

pub fn get_all_distinct_values(conn: &Connection, column: &str) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(&format!(
        "SELECT DISTINCT \"{}\" FROM data WHERE \"{}\" IS NOT NULL ORDER BY \"{}\"",
        column, column, column
    ))?;
    let values = stmt
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    fn setup_db() -> (tempfile::NamedTempFile, Connection) {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let conn = Connection::open(tmp.path()).unwrap();
        conn.execute_batch(
            "CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT);
             INSERT INTO metadata VALUES ('schema', '{\"cube\": \"Test\", \"measure\": \"Count\"}');
             CREATE TABLE data (
                 \"Faculté\" TEXT,
                 \"Année\" TEXT,
                 indicateur REAL
             );
             INSERT INTO data VALUES ('FBM', '2022', 10.0);
             INSERT INTO data VALUES ('FBM', '2023', 20.0);
             INSERT INTO data VALUES ('SSP', '2022', 30.0);
             INSERT INTO data VALUES ('SSP', '2023', 40.0);
             INSERT INTO data VALUES ('HEC', '2023', 50.0);",
        )
        .unwrap();
        (tmp, conn)
    }

    #[test]
    fn test_open_valid_file() {
        let (tmp, _conn) = setup_db();
        let result = open(tmp.path());
        assert!(result.is_ok());
    }

    #[test]
    fn test_open_nonexistent_file() {
        let result = open(Path::new("/nonexistent/path.sqlite"));
        assert!(result.is_err());
    }

    #[test]
    fn test_read_metadata_schema() {
        let (_tmp, conn) = setup_db();
        let schema = read_metadata_schema(&conn).unwrap();
        assert_eq!(schema["cube"], "Test");
        assert_eq!(schema["measure"], "Count");
    }

    #[test]
    fn test_get_table_columns() {
        let (_tmp, conn) = setup_db();
        let cols = get_table_columns(&conn, "data").unwrap();
        assert_eq!(cols.len(), 3);
        assert_eq!(cols[0].name, "Faculté");
        assert_eq!(cols[0].col_type, "TEXT");
        assert_eq!(cols[2].name, "indicateur");
        assert_eq!(cols[2].col_type, "REAL");
    }

    #[test]
    fn test_get_row_count() {
        let (_tmp, conn) = setup_db();
        assert_eq!(get_row_count(&conn).unwrap(), 5);
    }

    #[test]
    fn test_get_distinct_count() {
        let (_tmp, conn) = setup_db();
        assert_eq!(get_distinct_count(&conn, "Faculté").unwrap(), 3);
        assert_eq!(get_distinct_count(&conn, "Année").unwrap(), 2);
    }

    #[test]
    fn test_get_sample_values() {
        let (_tmp, conn) = setup_db();
        let samples = get_sample_values(&conn, "Faculté", 5).unwrap();
        assert_eq!(samples.len(), 3);
        assert!(samples.contains(&"FBM".to_string()));
        assert!(samples.contains(&"SSP".to_string()));
        assert!(samples.contains(&"HEC".to_string()));
    }

    #[test]
    fn test_get_sample_values_with_limit() {
        let (_tmp, conn) = setup_db();
        let samples = get_sample_values(&conn, "Faculté", 2).unwrap();
        assert_eq!(samples.len(), 2);
    }

    #[test]
    fn test_get_table_columns_metadata() {
        let (_tmp, conn) = setup_db();
        let cols = get_table_columns(&conn, "metadata").unwrap();
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].name, "key");
        assert_eq!(cols[1].name, "value");
    }

    #[test]
    fn test_get_numeric_stats() {
        let (_tmp, conn) = setup_db();
        let (min, max, distinct) = get_numeric_stats(&conn, "indicateur").unwrap();
        assert_eq!(min, 10.0);
        assert_eq!(max, 50.0);
        assert_eq!(distinct, 5);
    }
}
