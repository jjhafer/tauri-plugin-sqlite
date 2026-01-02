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

         // Build and execute INSERT...SELECT SQL
         let sql = build_insert_select_sql(mapping);
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
fn build_insert_select_sql(mapping: &TableMapping) -> String {
   let target_columns: Vec<&str> = mapping
      .columns
      .iter()
      .map(|c| c.target_column.as_str())
      .collect();

   let select_expressions: Vec<String> = mapping
      .columns
      .iter()
      .map(|c| match &c.source {
         ColumnSource::Constant { value } => {
            // Quote string constants and escape single quotes
            format!("'{}'", value.replace('\'', "''"))
         }
         ColumnSource::Column { name } => name.clone(),
         ColumnSource::Expression { sql } => sql.clone(),
      })
      .collect();

   let insert_type = if mapping.replace_on_conflict {
      "INSERT OR REPLACE"
   } else {
      "INSERT"
   };

   format!(
      "{} INTO {} ({}) SELECT {} FROM attached_db.{}",
      insert_type,
      mapping.target_table,
      target_columns.join(", "),
      select_expressions.join(", "),
      mapping.source_table
   )
}

#[cfg(test)]
mod tests {
   use super::*;

   #[test]
   fn test_build_insert_select_sql_with_constants() {
      let mapping = TableMapping {
         source_table: "BibleCitation".to_string(),
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

      let sql = build_insert_select_sql(&mapping);
      assert_eq!(
         sql,
         "INSERT OR REPLACE INTO JwpubBibleCitation (lank, citationId) \
          SELECT 'pub-it', BibleCitationId FROM attached_db.BibleCitation"
      );
   }

   #[test]
   fn test_build_insert_select_sql_with_expression() {
      let mapping = TableMapping {
         source_table: "Document".to_string(),
         target_table: "JwpubDocument".to_string(),
         columns: vec![ColumnMapping {
            target_column: "documentLank".to_string(),
            source: ColumnSource::Expression {
               sql: "'doc-' || MepsDocumentId".to_string(),
            },
         }],
         replace_on_conflict: false,
      };

      let sql = build_insert_select_sql(&mapping);
      assert_eq!(
         sql,
         "INSERT INTO JwpubDocument (documentLank) \
          SELECT 'doc-' || MepsDocumentId FROM attached_db.Document"
      );
   }

   #[test]
   fn test_build_insert_select_sql_escapes_quotes() {
      let mapping = TableMapping {
         source_table: "Test".to_string(),
         target_table: "TestTarget".to_string(),
         columns: vec![ColumnMapping {
            target_column: "value".to_string(),
            source: ColumnSource::Constant {
               value: "it's a test".to_string(),
            },
         }],
         replace_on_conflict: true,
      };

      let sql = build_insert_select_sql(&mapping);
      assert!(sql.contains("'it''s a test'"));
   }
}
