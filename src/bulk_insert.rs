//! Bulk insert operations using ATTACH DATABASE
//!
//! This module provides high-performance bulk insert functionality by using SQLite's
//! ATTACH DATABASE command to copy data directly between databases without JavaScript/IPC
//! overhead.

use std::collections::HashSet;
use std::time::Instant;

use serde::{Deserialize, Serialize};
use tauri::State;

use crate::{DbInstances, Error, Result};

/// Describes how to derive a target column value from the source database.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type")]
pub enum ColumnSource {
   /// Use a constant string value (e.g., lank, languageCode)
   Constant { value: String },

   /// Map directly from a source column by name
   Column { name: String },

   /// Apply a SQL expression (e.g., "'doc-' || DocumentId")
   Expression { sql: String },
}

/// Mapping configuration for a single column.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ColumnMapping {
   /// Target column name in the destination database
   pub target_column: String,

   /// How to derive the value for this column
   pub source: ColumnSource,
}

/// Complete mapping configuration for copying a table.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableMapping {
   /// Source table name in the attached database (e.g., "BibleCitation")
   pub source_table: String,

   /// Optional alias for the source table (e.g., "dp" for DocumentParagraph)
   /// If not provided, no alias is used.
   #[serde(default)]
   pub source_alias: Option<String>,

   /// Optional JOIN clauses to add to the FROM clause.
   /// Each string should be a complete JOIN clause, e.g.:
   /// "JOIN Document d ON dp.DocumentId = d.DocumentId"
   /// The attached_db prefix will be added automatically.
   #[serde(default)]
   pub source_joins: Vec<String>,

   /// Target table name in the main database (e.g., "JwpubBibleCitation")
   pub target_table: String,

   /// Column mappings defining how to transform data
   pub columns: Vec<ColumnMapping>,

   /// Use INSERT OR REPLACE instead of plain INSERT (default: true)
   #[serde(default = "default_true")]
   pub replace_on_conflict: bool,
}

fn default_true() -> bool {
   true
}

/// Result of inserting data into a single table.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TableInsertResult {
   /// Target table name
   pub table_name: String,

   /// Number of rows inserted
   pub rows_inserted: u64,

   /// Duration in milliseconds for this table
   pub duration_ms: u64,

   /// Whether this table was skipped
   pub skipped: bool,

   /// Reason for skipping (if skipped is true)
   #[serde(skip_serializing_if = "Option::is_none")]
   pub skip_reason: Option<String>,
}

/// Result of the entire bulk insert operation.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BulkInsertResult {
   /// Number of tables successfully processed (not skipped)
   pub tables_processed: usize,

   /// Total rows inserted across all tables
   pub total_rows_inserted: u64,

   /// Per-table results
   pub table_results: Vec<TableInsertResult>,

   /// Total duration in milliseconds
   pub duration_ms: u64,
}

/// Bulk insert data from an attached database into the main database.
///
/// This command uses SQLite's ATTACH DATABASE to copy data directly between databases
/// without JavaScript/IPC overhead. All data stays in SQLite's native binary format.
///
/// # Arguments
///
/// * `db` - Path to the main database (must already be loaded)
/// * `attach_path` - Absolute path to the database file to attach
/// * `table_mappings` - Configuration for each table to copy
///
/// # Transaction Behavior
///
/// All inserts are wrapped in a single transaction. If any table insert fails,
/// the entire operation is rolled back.
#[tauri::command]
pub async fn bulk_insert_from_attached(
   db_instances: State<'_, DbInstances>,
   db: String,
   attach_path: String,
   table_mappings: Vec<TableMapping>,
) -> Result<BulkInsertResult> {
   let start = Instant::now();

   let instances = db_instances.0.read().await;
   let wrapper = instances
      .get(&db)
      .ok_or_else(|| Error::DatabaseNotLoaded(db.clone()))?;

   // Acquire writer for the entire operation
   let mut writer = wrapper.acquire_writer().await?;

   // Attach the source database
   let attach_sql = format!("ATTACH DATABASE '{}' AS attached_db", attach_path);
   sqlx::query(&attach_sql).execute(&mut *writer).await?;

   // Get list of tables in the attached database for existence checks
   let table_rows: Vec<(String,)> =
      sqlx::query_as("SELECT name FROM attached_db.sqlite_master WHERE type = 'table'")
         .fetch_all(&mut *writer)
         .await?;

   let attached_tables: HashSet<String> = table_rows.into_iter().map(|(name,)| name).collect();

   // Build a map of table -> columns for column existence checks
   let mut table_columns: std::collections::HashMap<String, HashSet<String>> =
      std::collections::HashMap::new();

   for table_name in &attached_tables {
      let pragma_sql = format!("PRAGMA attached_db.table_info('{}')", table_name);
      let column_rows: Vec<(i32, String, String, i32, Option<String>, i32)> =
         sqlx::query_as(&pragma_sql).fetch_all(&mut *writer).await?;

      let columns: HashSet<String> = column_rows
         .into_iter()
         .map(|(_, name, _, _, _, _)| name)
         .collect();
      table_columns.insert(table_name.clone(), columns);
   }

   // Begin transaction
   sqlx::query("BEGIN IMMEDIATE").execute(&mut *writer).await?;

   let mut table_results = Vec::new();
   let mut total_rows = 0u64;

   // Process each table mapping
   let result: Result<()> = async {
      for mapping in &table_mappings {
         let table_start = Instant::now();

         // Check if source table exists in attached database
         if !attached_tables.contains(&mapping.source_table) {
            table_results.push(TableInsertResult {
               table_name: mapping.target_table.clone(),
               rows_inserted: 0,
               duration_ms: 0,
               skipped: true,
               skip_reason: Some(format!(
                  "Source table '{}' not found in attached database",
                  mapping.source_table
               )),
            });
            continue;
         }

         // Get available columns for this source table AND any joined tables
         let mut available_columns = table_columns
            .get(&mapping.source_table)
            .cloned()
            .unwrap_or_default();

         // Also include columns from joined tables
         for join_clause in &mapping.source_joins {
            // Extract table name from JOIN clause (e.g., "JOIN Document d ON ..." -> "Document")
            let join_table = extract_table_from_join(join_clause);
            if let Some(table_name) = join_table {
               if let Some(join_columns) = table_columns.get(&table_name) {
                  available_columns.extend(join_columns.iter().cloned());
               }
            }
         }

         // Build and execute INSERT...SELECT SQL, filtering out missing columns
         let sql = match build_insert_select_sql(mapping, &available_columns) {
            Some(sql) => sql,
            None => {
               // All columns were filtered out - skip this table
               table_results.push(TableInsertResult {
                  table_name: mapping.target_table.clone(),
                  rows_inserted: 0,
                  duration_ms: 0,
                  skipped: true,
                  skip_reason: Some(format!(
                     "No valid columns found for table '{}'",
                     mapping.source_table
                  )),
               });
               continue;
            }
         };

         let result = sqlx::query(&sql).execute(&mut *writer).await?;
         let rows = result.rows_affected();
         total_rows += rows;

         table_results.push(TableInsertResult {
            table_name: mapping.target_table.clone(),
            rows_inserted: rows,
            duration_ms: table_start.elapsed().as_millis() as u64,
            skipped: false,
            skip_reason: None,
         });
      }

      Ok(())
   }
   .await;

   // Commit or rollback based on result
   match result {
      Ok(()) => {
         sqlx::query("COMMIT").execute(&mut *writer).await?;
      }
      Err(e) => {
         let _ = sqlx::query("ROLLBACK").execute(&mut *writer).await;
         // Detach before returning error
         let _ = sqlx::query("DETACH DATABASE attached_db")
            .execute(&mut *writer)
            .await;
         return Err(e);
      }
   }

   // Detach the source database
   sqlx::query("DETACH DATABASE attached_db")
      .execute(&mut *writer)
      .await?;

   let tables_processed = table_results.iter().filter(|r| !r.skipped).count();

   Ok(BulkInsertResult {
      tables_processed,
      total_rows_inserted: total_rows,
      table_results,
      duration_ms: start.elapsed().as_millis() as u64,
   })
}

/// Build the INSERT...SELECT SQL statement from a table mapping.
/// Returns None if no valid columns remain after filtering.
fn build_insert_select_sql(
   mapping: &TableMapping,
   available_columns: &HashSet<String>,
) -> Option<String> {
   // Filter columns to only include those that reference existing source columns
   let valid_columns: Vec<(&ColumnMapping, String)> = mapping
      .columns
      .iter()
      .filter_map(|c| {
         let expr = match &c.source {
            ColumnSource::Constant { value } => {
               // Constants are always valid
               Some(format!("'{}'", value.replace('\'', "''")))
            }
            ColumnSource::Column { name } => {
               // Only include if column exists in source table
               if available_columns.contains(name) {
                  Some(name.clone())
               } else {
                  None
               }
            }
            ColumnSource::Expression { sql } => {
               // For expressions, check if all referenced columns exist.
               // We parse the SQL to extract potential column names and verify they exist.
               if expression_uses_only_available_columns(sql, available_columns) {
                  Some(sql.clone())
               } else {
                  None
               }
            }
         };
         expr.map(|e| (c, e))
      })
      .collect();

   if valid_columns.is_empty() {
      return None;
   }

   let target_columns: Vec<&str> = valid_columns
      .iter()
      .map(|(c, _)| c.target_column.as_str())
      .collect();

   let select_expressions: Vec<&str> = valid_columns
      .iter()
      .map(|(_, expr)| expr.as_str())
      .collect();

   let insert_type = if mapping.replace_on_conflict {
      "INSERT OR REPLACE"
   } else {
      "INSERT"
   };

   // Build FROM clause with optional alias and JOINs
   let from_clause = if let Some(alias) = &mapping.source_alias {
      format!("attached_db.{} {}", mapping.source_table, alias)
   } else {
      format!("attached_db.{}", mapping.source_table)
   };

   // Add JOIN clauses, prefixing table names with attached_db
   let joins = if mapping.source_joins.is_empty() {
      String::new()
   } else {
      let join_clauses: Vec<String> = mapping
         .source_joins
         .iter()
         .map(|join| {
            // Add attached_db prefix to JOIN table references
            // e.g., "JOIN Document d ON ..." -> "JOIN attached_db.Document d ON ..."
            if let Some(rest) = join.strip_prefix("JOIN ") {
               format!("JOIN attached_db.{}", rest)
            } else if let Some(rest) = join.strip_prefix("LEFT JOIN ") {
               format!("LEFT JOIN attached_db.{}", rest)
            } else if let Some(rest) = join.strip_prefix("INNER JOIN ") {
               format!("INNER JOIN attached_db.{}", rest)
            } else {
               // Assume it's already properly formatted
               join.clone()
            }
         })
         .collect();
      format!(" {}", join_clauses.join(" "))
   };

   Some(format!(
      "{} INTO {} ({}) SELECT {} FROM {}{}",
      insert_type,
      mapping.target_table,
      target_columns.join(", "),
      select_expressions.join(", "),
      from_clause,
      joins
   ))
}

/// Check if a SQL expression only uses columns that are available.
/// This is a simple heuristic that checks for common column name patterns.
fn expression_uses_only_available_columns(sql: &str, available_columns: &HashSet<String>) -> bool {
   // Extract potential column references from the SQL
   // This is a simplified check - it looks for identifiers that might be column names
   let mut in_string = false;
   let mut current_word = String::new();
   let mut potential_columns: Vec<String> = Vec::new();

   for c in sql.chars() {
      if c == '\'' {
         in_string = !in_string;
         continue;
      }

      if in_string {
         continue;
      }

      if c.is_alphanumeric() || c == '_' {
         current_word.push(c);
      } else if !current_word.is_empty() {
         // Check if this looks like a column name (starts with letter, not a SQL keyword)
         let word = current_word.clone();
         let upper = word.to_uppercase();
         let sql_keywords = [
            "SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "NULL", "AS", "CASE", "WHEN", "THEN",
            "ELSE", "END", "COALESCE", "CAST", "TEXT", "INTEGER", "REAL", "BLOB", "TRUE", "FALSE",
            "IS", "IN", "LIKE", "BETWEEN", "EXISTS", "DISTINCT",
         ];

         if !sql_keywords.contains(&upper.as_str())
            && word
               .chars()
               .next()
               .map(|c| c.is_alphabetic())
               .unwrap_or(false)
            && !word.chars().all(|c| c.is_numeric())
         {
            potential_columns.push(word);
         }
         current_word.clear();
      }
   }

   // Check the last word
   if !current_word.is_empty() {
      let word = current_word;
      let upper = word.to_uppercase();
      let sql_keywords = [
         "SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "NULL", "AS", "CASE", "WHEN", "THEN",
         "ELSE", "END", "COALESCE", "CAST", "TEXT", "INTEGER", "REAL", "BLOB", "TRUE", "FALSE",
         "IS", "IN", "LIKE", "BETWEEN", "EXISTS", "DISTINCT",
      ];

      if !sql_keywords.contains(&upper.as_str())
         && word
            .chars()
            .next()
            .map(|c| c.is_alphabetic())
            .unwrap_or(false)
         && !word.chars().all(|c| c.is_numeric())
      {
         potential_columns.push(word);
      }
   }

   // All potential column references must exist in available_columns
   potential_columns
      .iter()
      .all(|col| available_columns.contains(col))
}

/// Extract the table name from a JOIN clause.
/// e.g., "JOIN Document d ON dp.DocumentId = d.DocumentId" -> Some("Document")
/// e.g., "LEFT JOIN Document d ON ..." -> Some("Document")
fn extract_table_from_join(join_clause: &str) -> Option<String> {
   // Remove the JOIN prefix to get to the table name
   let rest = if let Some(r) = join_clause.strip_prefix("JOIN ") {
      r
   } else if let Some(r) = join_clause.strip_prefix("LEFT JOIN ") {
      r
   } else if let Some(r) = join_clause.strip_prefix("INNER JOIN ") {
      r
   } else {
      return None;
   };

   // The table name is the first word (before space or alias)
   rest.split_whitespace().next().map(|s| s.to_string())
}

#[cfg(test)]
mod tests {
   use super::*;

   fn make_columns(names: &[&str]) -> HashSet<String> {
      names.iter().map(|s| s.to_string()).collect()
   }

   #[test]
   fn test_build_insert_select_sql_with_constants() {
      let mapping = TableMapping {
         source_table: "BibleCitation".to_string(),
         source_alias: None,
         source_joins: vec![],
         target_table: "JwpubBibleCitation".to_string(),
         columns: vec![
            ColumnMapping {
               target_column: "lank".to_string(),
               source: ColumnSource::Constant {
                  value: "pub-it".to_string(),
               },
            },
            ColumnMapping {
               target_column: "citationId".to_string(),
               source: ColumnSource::Column {
                  name: "BibleCitationId".to_string(),
               },
            },
         ],
         replace_on_conflict: true,
      };

      let available = make_columns(&["BibleCitationId", "OtherColumn"]);
      let sql = build_insert_select_sql(&mapping, &available);
      assert_eq!(
         sql,
         Some(
            "INSERT OR REPLACE INTO JwpubBibleCitation (lank, citationId) \
             SELECT 'pub-it', BibleCitationId FROM attached_db.BibleCitation"
               .to_string()
         )
      );
   }

   #[test]
   fn test_build_insert_select_sql_with_expression() {
      let mapping = TableMapping {
         source_table: "Document".to_string(),
         source_alias: None,
         source_joins: vec![],
         target_table: "JwpubDocument".to_string(),
         columns: vec![ColumnMapping {
            target_column: "documentLank".to_string(),
            source: ColumnSource::Expression {
               sql: "'doc-' || MepsDocumentId".to_string(),
            },
         }],
         replace_on_conflict: false,
      };

      let available = make_columns(&["MepsDocumentId"]);
      let sql = build_insert_select_sql(&mapping, &available);
      assert_eq!(
         sql,
         Some(
            "INSERT INTO JwpubDocument (documentLank) \
             SELECT 'doc-' || MepsDocumentId FROM attached_db.Document"
               .to_string()
         )
      );
   }

   #[test]
   fn test_build_insert_select_sql_escapes_quotes() {
      let mapping = TableMapping {
         source_table: "Test".to_string(),
         source_alias: None,
         source_joins: vec![],
         target_table: "TestTarget".to_string(),
         columns: vec![ColumnMapping {
            target_column: "value".to_string(),
            source: ColumnSource::Constant {
               value: "it's a test".to_string(),
            },
         }],
         replace_on_conflict: true,
      };

      let available = make_columns(&[]);
      let sql = build_insert_select_sql(&mapping, &available);
      assert!(sql.is_some());
      assert!(sql.unwrap().contains("'it''s a test'"));
   }

   #[test]
   fn test_build_insert_select_sql_filters_missing_columns() {
      let mapping = TableMapping {
         source_table: "Test".to_string(),
         source_alias: None,
         source_joins: vec![],
         target_table: "TestTarget".to_string(),
         columns: vec![
            ColumnMapping {
               target_column: "existingCol".to_string(),
               source: ColumnSource::Column {
                  name: "ExistingColumn".to_string(),
               },
            },
            ColumnMapping {
               target_column: "missingCol".to_string(),
               source: ColumnSource::Column {
                  name: "MissingColumn".to_string(),
               },
            },
         ],
         replace_on_conflict: true,
      };

      let available = make_columns(&["ExistingColumn"]);
      let sql = build_insert_select_sql(&mapping, &available);
      assert!(sql.is_some());
      let sql_str = sql.unwrap();
      assert!(sql_str.contains("existingCol"));
      assert!(!sql_str.contains("missingCol"));
   }

   #[test]
   fn test_build_insert_select_sql_returns_none_when_all_columns_missing() {
      let mapping = TableMapping {
         source_table: "Test".to_string(),
         source_alias: None,
         source_joins: vec![],
         target_table: "TestTarget".to_string(),
         columns: vec![ColumnMapping {
            target_column: "col".to_string(),
            source: ColumnSource::Column {
               name: "MissingColumn".to_string(),
            },
         }],
         replace_on_conflict: true,
      };

      let available = make_columns(&["OtherColumn"]);
      let sql = build_insert_select_sql(&mapping, &available);
      assert!(sql.is_none());
   }

   #[test]
   fn test_build_insert_select_sql_with_join() {
      let mapping = TableMapping {
         source_table: "DocumentParagraph".to_string(),
         source_alias: Some("dp".to_string()),
         source_joins: vec!["JOIN Document d ON dp.DocumentId = d.DocumentId".to_string()],
         target_table: "JwpubDocumentParagraph".to_string(),
         columns: vec![
            ColumnMapping {
               target_column: "documentLank".to_string(),
               source: ColumnSource::Expression {
                  sql: "'doc-' || d.MepsDocumentId".to_string(),
               },
            },
            ColumnMapping {
               target_column: "paragraphIndex".to_string(),
               source: ColumnSource::Column {
                  name: "ParagraphIndex".to_string(),
               },
            },
         ],
         replace_on_conflict: false,
      };

      let available = make_columns(&["ParagraphIndex", "MepsDocumentId"]);
      let sql = build_insert_select_sql(&mapping, &available);
      assert!(sql.is_some());
      let sql_str = sql.unwrap();
      assert!(sql_str.contains("FROM attached_db.DocumentParagraph dp"));
      assert!(sql_str.contains("JOIN attached_db.Document d ON dp.DocumentId = d.DocumentId"));
   }

   #[test]
   fn test_expression_uses_only_available_columns() {
      let available = make_columns(&["DocumentId", "Title"]);

      // Expression with available column
      assert!(expression_uses_only_available_columns(
         "'doc-' || DocumentId",
         &available
      ));

      // Expression with missing column
      assert!(!expression_uses_only_available_columns(
         "'doc-' || MepsDocumentId",
         &available
      ));

      // Expression with COALESCE using available columns
      assert!(expression_uses_only_available_columns(
         "COALESCE(DocumentId, 0)",
         &available
      ));

      // Pure constant expression (no columns)
      assert!(expression_uses_only_available_columns(
         "'constant'",
         &available
      ));
   }
}
