use anyhow::Result;
use serde_json::Value;
use std::path::Path;

use crate::db;
use crate::error::CubeError;
use crate::formatter;

#[derive(Debug)]
pub(crate) struct Filter {
    pub column: String,
    pub values: Vec<String>,
}

pub(crate) fn parse_filters(raw: &[String]) -> Result<Vec<Filter>> {
    // Use an IndexMap-style approach to merge repeated columns while preserving order
    let mut order: Vec<String> = Vec::new();
    let mut map: std::collections::HashMap<String, Vec<String>> = std::collections::HashMap::new();

    for f in raw {
        let parts: Vec<&str> = f.splitn(2, '=').collect();
        if parts.len() != 2 {
            return Err(CubeError::validation(format!(
                "Filtre invalide : '{f}'. Format attendu : col=val (répétable pour plusieurs valeurs)"
            )));
        }
        let column = parts[0].to_string();
        let value = parts[1].to_string();

        if !map.contains_key(&column) {
            order.push(column.clone());
        }
        map.entry(column).or_default().push(value);
    }

    Ok(order
        .into_iter()
        .map(|col| {
            let values = map.remove(&col).unwrap();
            Filter {
                column: col,
                values,
            }
        })
        .collect())
}

pub(crate) fn build_where_clause(
    includes: &[Filter],
    excludes: &[Filter],
    params: &mut Vec<String>,
) -> String {
    let mut clauses = Vec::new();

    for f in includes {
        let placeholders: Vec<String> = f
            .values
            .iter()
            .map(|v| {
                params.push(v.clone());
                "?".to_string()
            })
            .collect();
        clauses.push(format!("\"{}\" IN ({})", f.column, placeholders.join(", ")));
    }

    for f in excludes {
        let placeholders: Vec<String> = f
            .values
            .iter()
            .map(|v| {
                params.push(v.clone());
                "?".to_string()
            })
            .collect();
        clauses.push(format!(
            "\"{}\" NOT IN ({})",
            f.column,
            placeholders.join(", ")
        ));
    }

    if clauses.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", clauses.join(" AND "))
    }
}

/// Flatten repeatable, comma-separated arguments into an ordered list.
/// e.g. ["Faculté,Sexe", "Type"] → ["Faculté", "Sexe", "Type"]
fn flatten_args(args: &[String]) -> Vec<String> {
    args.iter()
        .flat_map(|s| s.split(','))
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

/// Flatten repeatable arrange specs, preserving order.
/// Each entry can be "col", "col:asc", "col:desc", or comma-separated combinations.
fn flatten_arrange(args: &[String]) -> Vec<String> {
    args.iter()
        .flat_map(|s| s.split(','))
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

pub(crate) fn build_order_by(specs: &[String]) -> String {
    let parts: Vec<String> = specs
        .iter()
        .map(|part| {
            let trimmed = part.trim();
            // Split on the last ':' only if the suffix is asc/desc,
            // so column names containing ':' are preserved.
            if let Some(pos) = trimmed.rfind(':') {
                let suffix = trimmed[pos + 1..].trim().to_uppercase();
                if suffix == "ASC" || suffix == "DESC" {
                    let col = trimmed[..pos].trim();
                    return format!("\"{}\" {}", col, suffix);
                }
            }
            format!("\"{}\" ASC", trimmed)
        })
        .collect();
    format!(" ORDER BY {}", parts.join(", "))
}

#[allow(clippy::too_many_arguments)]
pub fn run(
    file: &Path,
    select: &[String],
    filter: &[String],
    exclude: &[String],
    group_by: &[String],
    arrange: &[String],
    limit: Option<usize>,
    indicator: &str,
    no_aggregate: bool,
    format: &str,
) -> Result<()> {
    let conn = db::open(file)?;
    let includes = parse_filters(filter)?;
    let excludes = parse_filters(exclude)?;
    let select_flat = flatten_args(select);
    let group_flat = flatten_args(group_by);
    let arrange_flat = flatten_arrange(arrange);

    let mut params: Vec<String> = Vec::new();
    let where_clause = build_where_clause(&includes, &excludes, &mut params);

    let sql = if no_aggregate {
        let select_cols = if select_flat.is_empty() {
            // Exclude the indicator column by default in no-aggregate mode
            let cols = db::get_table_columns(&conn, "data")?;
            let dim_cols: Vec<String> = cols
                .into_iter()
                .filter(|c| c.name != indicator)
                .map(|c| format!("\"{}\"", c.name))
                .collect();
            if dim_cols.is_empty() {
                "*".to_string()
            } else {
                dim_cols.join(", ")
            }
        } else {
            select_flat
                .iter()
                .map(|c| format!("\"{}\"", c))
                .collect::<Vec<_>>()
                .join(", ")
        };
        let mut q = format!("SELECT {select_cols} FROM data{where_clause}");
        if !arrange_flat.is_empty() {
            q.push_str(&build_order_by(&arrange_flat));
        }
        if let Some(lim) = limit {
            q.push_str(&format!(" LIMIT {lim}"));
        }
        q
    } else {
        if group_flat.is_empty() {
            return Err(CubeError::validation(
                "Usage: cube query <file> --group-by col [flags]. --group-by est obligatoire sauf avec --no-aggregate"
            ));
        }
        let select_cols = if select_flat.is_empty() {
            group_flat.clone()
        } else {
            select_flat
        };

        let select_parts: Vec<String> = select_cols
            .iter()
            .map(|c| format!("\"{}\"", c))
            .chain(std::iter::once(format!(
                "SUM(\"{}\") AS \"{}\"",
                indicator, indicator
            )))
            .collect();

        let group_parts: Vec<String> = group_flat.iter().map(|c| format!("\"{}\"", c)).collect();

        let mut q = format!(
            "SELECT {} FROM data{} GROUP BY {}",
            select_parts.join(", "),
            where_clause,
            group_parts.join(", ")
        );

        if !arrange_flat.is_empty() {
            q.push_str(&build_order_by(&arrange_flat));
        }
        if let Some(lim) = limit {
            q.push_str(&format!(" LIMIT {lim}"));
        }
        q
    };

    let mut stmt = conn.prepare(&sql)?;
    let col_count = stmt.column_count();
    let col_names: Vec<String> = (0..col_count)
        .map(|i| stmt.column_name(i).unwrap().to_string())
        .collect();

    let param_refs: Vec<&dyn rusqlite::types::ToSql> = params
        .iter()
        .map(|s| s as &dyn rusqlite::types::ToSql)
        .collect();

    let mut rows_data: Vec<Vec<Value>> = Vec::new();
    let mut rows = stmt.query(param_refs.as_slice())?;
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

    // --- parse_filters ---

    #[test]
    fn test_parse_filters_single_value() {
        let raw = vec!["Faculté=FBM".to_string()];
        let filters = parse_filters(&raw).unwrap();
        assert_eq!(filters.len(), 1);
        assert_eq!(filters[0].column, "Faculté");
        assert_eq!(filters[0].values, vec!["FBM"]);
    }

    #[test]
    fn test_parse_filters_repeated_column_merges() {
        let raw = vec![
            "Faculté=SSP".to_string(),
            "Faculté=HEC".to_string(),
            "Faculté=FBM".to_string(),
        ];
        let filters = parse_filters(&raw).unwrap();
        assert_eq!(filters.len(), 1);
        assert_eq!(filters[0].column, "Faculté");
        assert_eq!(filters[0].values, vec!["SSP", "HEC", "FBM"]);
    }

    #[test]
    fn test_parse_filters_multiple_columns() {
        let raw = vec!["Faculté=FBM".to_string(), "Année civile=2023".to_string()];
        let filters = parse_filters(&raw).unwrap();
        assert_eq!(filters.len(), 2);
        assert_eq!(filters[0].column, "Faculté");
        assert_eq!(filters[1].column, "Année civile");
    }

    #[test]
    fn test_parse_filters_preserves_order() {
        let raw = vec![
            "Type=Bureau".to_string(),
            "Faculté=FBM".to_string(),
            "Type=Labo".to_string(),
        ];
        let filters = parse_filters(&raw).unwrap();
        assert_eq!(filters.len(), 2);
        assert_eq!(filters[0].column, "Type");
        assert_eq!(filters[0].values, vec!["Bureau", "Labo"]);
        assert_eq!(filters[1].column, "Faculté");
    }

    #[test]
    fn test_parse_filters_value_with_comma() {
        let raw = vec!["col=hello, world".to_string()];
        let filters = parse_filters(&raw).unwrap();
        assert_eq!(filters[0].values, vec!["hello, world"]);
    }

    #[test]
    fn test_parse_filters_invalid() {
        let raw = vec!["invalid-filter".to_string()];
        let result = parse_filters(&raw);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Filtre invalide"));
    }

    #[test]
    fn test_parse_filters_empty() {
        let raw: Vec<String> = vec![];
        let filters = parse_filters(&raw).unwrap();
        assert!(filters.is_empty());
    }

    #[test]
    fn test_parse_filters_value_with_equals() {
        let raw = vec!["col=a=b".to_string()];
        let filters = parse_filters(&raw).unwrap();
        assert_eq!(filters[0].column, "col");
        assert_eq!(filters[0].values, vec!["a=b"]);
    }

    // --- build_where_clause ---

    #[test]
    fn test_build_where_clause_empty() {
        let mut params = Vec::new();
        let clause = build_where_clause(&[], &[], &mut params);
        assert_eq!(clause, "");
        assert!(params.is_empty());
    }

    #[test]
    fn test_build_where_clause_single_include() {
        let includes = vec![Filter {
            column: "Faculté".to_string(),
            values: vec!["FBM".to_string()],
        }];
        let mut params = Vec::new();
        let clause = build_where_clause(&includes, &[], &mut params);
        assert_eq!(clause, " WHERE \"Faculté\" IN (?)");
        assert_eq!(params, vec!["FBM"]);
    }

    #[test]
    fn test_build_where_clause_multi_value_include() {
        let includes = vec![Filter {
            column: "Faculté".to_string(),
            values: vec!["FBM".to_string(), "SSP".to_string()],
        }];
        let mut params = Vec::new();
        let clause = build_where_clause(&includes, &[], &mut params);
        assert_eq!(clause, " WHERE \"Faculté\" IN (?, ?)");
        assert_eq!(params, vec!["FBM", "SSP"]);
    }

    #[test]
    fn test_build_where_clause_exclude() {
        let excludes = vec![Filter {
            column: "Type".to_string(),
            values: vec!["Labo".to_string()],
        }];
        let mut params = Vec::new();
        let clause = build_where_clause(&[], &excludes, &mut params);
        assert_eq!(clause, " WHERE \"Type\" NOT IN (?)");
        assert_eq!(params, vec!["Labo"]);
    }

    #[test]
    fn test_build_where_clause_include_and_exclude() {
        let includes = vec![Filter {
            column: "Faculté".to_string(),
            values: vec!["FBM".to_string()],
        }];
        let excludes = vec![Filter {
            column: "Type".to_string(),
            values: vec!["Labo".to_string()],
        }];
        let mut params = Vec::new();
        let clause = build_where_clause(&includes, &excludes, &mut params);
        assert!(clause.contains("\"Faculté\" IN (?)"));
        assert!(clause.contains("\"Type\" NOT IN (?)"));
        assert!(clause.contains(" AND "));
        assert_eq!(params, vec!["FBM", "Labo"]);
    }

    // --- flatten_args ---

    #[test]
    fn test_flatten_args_single() {
        let args = vec!["Faculté".to_string()];
        assert_eq!(flatten_args(&args), vec!["Faculté"]);
    }

    #[test]
    fn test_flatten_args_comma_separated() {
        let args = vec!["Faculté,Sexe".to_string()];
        assert_eq!(flatten_args(&args), vec!["Faculté", "Sexe"]);
    }

    #[test]
    fn test_flatten_args_repeated() {
        let args = vec!["Faculté".to_string(), "Sexe".to_string()];
        assert_eq!(flatten_args(&args), vec!["Faculté", "Sexe"]);
    }

    #[test]
    fn test_flatten_args_mixed() {
        let args = vec!["Faculté,Sexe".to_string(), "Type".to_string()];
        assert_eq!(flatten_args(&args), vec!["Faculté", "Sexe", "Type"]);
    }

    // --- build_order_by ---

    #[test]
    fn test_build_order_by_desc() {
        assert_eq!(
            build_order_by(&["indicateur:desc".to_string()]),
            " ORDER BY \"indicateur\" DESC"
        );
    }

    #[test]
    fn test_build_order_by_asc() {
        assert_eq!(
            build_order_by(&["col:asc".to_string()]),
            " ORDER BY \"col\" ASC"
        );
    }

    #[test]
    fn test_build_order_by_default_asc() {
        assert_eq!(
            build_order_by(&["col".to_string()]),
            " ORDER BY \"col\" ASC"
        );
    }

    #[test]
    fn test_build_order_by_multiple() {
        assert_eq!(
            build_order_by(&["Faculté:asc".to_string(), "indicateur:desc".to_string()]),
            " ORDER BY \"Faculté\" ASC, \"indicateur\" DESC"
        );
    }

    #[test]
    fn test_build_order_by_column_with_colon() {
        // Column names containing ':' must not be split
        assert_eq!(
            build_order_by(&[
                "Cohorte (périmètre: Niveau d'étude par division facultaire):desc".to_string()
            ]),
            " ORDER BY \"Cohorte (périmètre: Niveau d'étude par division facultaire)\" DESC"
        );
    }

    #[test]
    fn test_build_order_by_column_with_colon_no_direction() {
        assert_eq!(
            build_order_by(&["Cohorte (périmètre: Niveau)".to_string()]),
            " ORDER BY \"Cohorte (périmètre: Niveau)\" ASC"
        );
    }

    // --- run (integration with temp DB) ---

    fn create_test_db() -> tempfile::NamedTempFile {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let conn = rusqlite::Connection::open(tmp.path()).unwrap();
        conn.execute_batch(
            "CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT);
             INSERT INTO metadata VALUES ('schema', '{\"cube\": \"Test\"}');
             CREATE TABLE data (
                 \"Faculté\" TEXT,
                 \"Type\" TEXT,
                 indicateur REAL
             );
             INSERT INTO data VALUES ('FBM', 'Bureau', 100.0);
             INSERT INTO data VALUES ('FBM', 'Bureau', 50.0);
             INSERT INTO data VALUES ('FBM', 'Labo', 200.0);
             INSERT INTO data VALUES ('SSP', 'Bureau', 80.0);
             INSERT INTO data VALUES ('HEC', 'Bureau', 120.0);
             INSERT INTO data VALUES ('HEC', 'Labo', 60.0);",
        )
        .unwrap();
        tmp
    }

    #[test]
    fn test_run_group_by() {
        let tmp = create_test_db();
        let result = run(
            tmp.path(),
            &[],
            &[],
            &[],
            &["Faculté".into()],
            &[],
            None,
            "indicateur",
            false,
            "json",
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_run_group_by_repeated() {
        let tmp = create_test_db();
        let result = run(
            tmp.path(),
            &[],
            &[],
            &[],
            &["Faculté".into(), "Type".into()],
            &[],
            None,
            "indicateur",
            false,
            "json",
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_run_group_by_required() {
        let tmp = create_test_db();
        let result = run(
            tmp.path(),
            &[],
            &[],
            &[],
            &[],
            &[],
            None,
            "indicateur",
            false,
            "json",
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("--group-by"));
    }

    #[test]
    fn test_run_no_aggregate() {
        let tmp = create_test_db();
        let result = run(
            tmp.path(),
            &[],
            &[],
            &[],
            &[],
            &[],
            None,
            "indicateur",
            true,
            "json",
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_run_with_filter() {
        let tmp = create_test_db();
        let result = run(
            tmp.path(),
            &[],
            &["Faculté=FBM".into()],
            &[],
            &["Faculté".into()],
            &[],
            None,
            "indicateur",
            false,
            "json",
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_run_with_exclude() {
        let tmp = create_test_db();
        let result = run(
            tmp.path(),
            &[],
            &[],
            &["Type=Labo".into()],
            &["Faculté".into()],
            &[],
            None,
            "indicateur",
            false,
            "json",
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_run_with_limit() {
        let tmp = create_test_db();
        let result = run(
            tmp.path(),
            &[],
            &[],
            &[],
            &["Faculté".into()],
            &[],
            Some(1),
            "indicateur",
            false,
            "json",
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_run_with_arrange() {
        let tmp = create_test_db();
        let result = run(
            tmp.path(),
            &[],
            &[],
            &[],
            &["Faculté".into()],
            &["indicateur:desc".into()],
            None,
            "indicateur",
            false,
            "json",
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_run_with_select() {
        let tmp = create_test_db();
        let result = run(
            tmp.path(),
            &["Faculté".into()],
            &[],
            &[],
            &["Faculté".into()],
            &[],
            None,
            "indicateur",
            false,
            "json",
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_run_no_aggregate_with_select() {
        let tmp = create_test_db();
        let result = run(
            tmp.path(),
            &["Faculté".into(), "indicateur".into()],
            &[],
            &[],
            &[],
            &["indicateur:desc".into()],
            Some(2),
            "indicateur",
            true,
            "csv",
        );
        assert!(result.is_ok());
    }
}
