use anyhow::Result;
use comfy_table::{ContentArrangement, Table};
use serde_json::{Map, Value};
use std::io::Write;

pub fn output(columns: &[String], rows: &[Vec<Value>], format: &str) -> Result<()> {
    output_to(columns, rows, format, &mut std::io::stdout())
}

pub fn output_to(
    columns: &[String],
    rows: &[Vec<Value>],
    format: &str,
    writer: &mut dyn Write,
) -> Result<()> {
    match format {
        "table" => print_table(columns, rows, writer),
        "csv" => print_csv(columns, rows, writer),
        "json" => print_json(columns, rows, writer),
        _ => Err(crate::error::CubeError::validation(format!(
            "Format inconnu : '{format}'. Formats supportés : table, csv, json"
        ))),
    }
}

fn print_table(columns: &[String], rows: &[Vec<Value>], writer: &mut dyn Write) -> Result<()> {
    let mut table = Table::new();
    table.set_content_arrangement(ContentArrangement::Dynamic);
    table.set_header(columns);

    for row in rows {
        let cells: Vec<String> = row.iter().map(value_to_string).collect();
        table.add_row(cells);
    }

    writeln!(writer, "{table}")?;
    Ok(())
}

fn print_csv(columns: &[String], rows: &[Vec<Value>], writer: &mut dyn Write) -> Result<()> {
    let mut wtr = csv::Writer::from_writer(writer);
    wtr.write_record(columns)?;
    for row in rows {
        let record: Vec<String> = row.iter().map(value_to_string).collect();
        wtr.write_record(&record)?;
    }
    wtr.flush()?;
    Ok(())
}

fn print_json(columns: &[String], rows: &[Vec<Value>], writer: &mut dyn Write) -> Result<()> {
    let json_rows: Vec<Value> = rows
        .iter()
        .map(|row| {
            let mut map = Map::new();
            for (i, col) in columns.iter().enumerate() {
                map.insert(col.clone(), row[i].clone());
            }
            Value::Object(map)
        })
        .collect();
    writeln!(writer, "{}", serde_json::to_string_pretty(&json_rows)?)?;
    Ok(())
}

pub fn value_to_string(v: &Value) -> String {
    match v {
        Value::Null => String::new(),
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        _ => v.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn cols(names: &[&str]) -> Vec<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn test_value_to_string_null() {
        assert_eq!(value_to_string(&Value::Null), "");
    }

    #[test]
    fn test_value_to_string_string() {
        assert_eq!(value_to_string(&json!("hello")), "hello");
    }

    #[test]
    fn test_value_to_string_number() {
        assert_eq!(value_to_string(&json!(42)), "42");
        assert_eq!(value_to_string(&json!(3.14)), "3.14");
    }

    #[test]
    fn test_value_to_string_bool() {
        assert_eq!(value_to_string(&json!(true)), "true");
    }

    #[test]
    fn test_output_json_format() {
        let columns = cols(&["name", "value"]);
        let rows = vec![vec![json!("A"), json!(10)], vec![json!("B"), json!(20)]];
        let mut buf = Vec::new();
        output_to(&columns, &rows, "json", &mut buf).unwrap();
        let output: Vec<Value> = serde_json::from_slice(&buf).unwrap();
        assert_eq!(output.len(), 2);
        assert_eq!(output[0]["name"], "A");
        assert_eq!(output[0]["value"], 10);
        assert_eq!(output[1]["name"], "B");
    }

    #[test]
    fn test_output_csv_format() {
        let columns = cols(&["col1", "col2"]);
        let rows = vec![vec![json!("x"), json!(1)]];
        let mut buf = Vec::new();
        output_to(&columns, &rows, "csv", &mut buf).unwrap();
        let s = String::from_utf8(buf).unwrap();
        let lines: Vec<&str> = s.trim().lines().collect();
        assert_eq!(lines[0], "col1,col2");
        assert_eq!(lines[1], "x,1");
    }

    #[test]
    fn test_output_table_format() {
        let columns = cols(&["a", "b"]);
        let rows = vec![vec![json!("x"), json!(1)]];
        let mut buf = Vec::new();
        output_to(&columns, &rows, "table", &mut buf).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("a"));
        assert!(s.contains("b"));
        assert!(s.contains("x"));
        assert!(s.contains("1"));
    }

    #[test]
    fn test_output_unknown_format() {
        let columns = cols(&["a"]);
        let rows = vec![];
        let mut buf = Vec::new();
        let result = output_to(&columns, &rows, "xml", &mut buf);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Format inconnu"));
    }

    #[test]
    fn test_output_empty_rows() {
        let columns = cols(&["a", "b"]);
        let rows: Vec<Vec<Value>> = vec![];
        let mut buf = Vec::new();
        output_to(&columns, &rows, "json", &mut buf).unwrap();
        let output: Vec<Value> = serde_json::from_slice(&buf).unwrap();
        assert!(output.is_empty());
    }
}
