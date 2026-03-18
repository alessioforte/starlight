//! CSV Writer Task
//!
//! Sink task that writes incoming JSON messages to a CSV file.
//! Supports append mode, configurable delimiter, and auto-header detection.

use crate::ctx::TaskContext;
use crate::err::Result;
use crate::task::{BaseTask, Task, TaskInfo};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::io::AsyncWriteExt;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// How to handle the header row.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HeaderMode {
    /// Derive columns from the first message's keys (default).
    Auto,
    /// Use these exact columns in this order. Keys not listed are ignored;
    /// missing keys produce an empty cell.
    Explicit(Vec<String>),
}

impl Default for HeaderMode {
    fn default() -> Self {
        Self::Auto
    }
}

/// What to do when the output file already exists.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WriteMode {
    /// Overwrite the file (default).
    Overwrite,
    /// Append to the file. Header is only written if the file is empty/new.
    Append,
}

impl Default for WriteMode {
    fn default() -> Self {
        Self::Overwrite
    }
}

/// CSV Writer task parameters.
#[derive(Debug, Serialize, Deserialize)]
pub struct Params {
    /// Output file path.
    pub filename: String,
    /// CSV delimiter (default: `,`).
    #[serde(default = "default_delimiter")]
    pub delimiter: char,
    /// Header handling (default: auto).
    #[serde(default)]
    pub header: HeaderMode,
    /// Write mode (default: overwrite).
    #[serde(default)]
    pub write_mode: WriteMode,
    /// Flush to disk every N rows (default: 1 — every row).
    #[serde(default = "default_flush_every")]
    pub flush_every: u64,
}

fn default_delimiter() -> char {
    ','
}

fn default_flush_every() -> u64 {
    1
}

/// CSV Writer state.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct State {
    rows_written: AtomicU64,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Format a JSON value as a CSV cell.
fn value_to_cell(v: &Value) -> String {
    match v {
        Value::Null => String::new(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        Value::String(s) => {
            // Quote if the value contains the delimiter, quotes, or newlines
            if s.contains(',') || s.contains('"') || s.contains('\n') || s.contains('\r') {
                format!("\"{}\"", s.replace('"', "\"\""))
            } else {
                s.clone()
            }
        }
        // Arrays / objects → compact JSON string, quoted
        other => {
            let raw = other.to_string();
            format!("\"{}\"", raw.replace('"', "\"\""))
        }
    }
}

/// Build a CSV line from ordered column values.
fn build_line(columns: &[String], msg: &Value, delimiter: char) -> String {
    columns
        .iter()
        .map(|col| {
            msg.get(col)
                .map(value_to_cell)
                .unwrap_or_default()
        })
        .collect::<Vec<_>>()
        .join(&delimiter.to_string())
}

// ---------------------------------------------------------------------------
// Task implementation
// ---------------------------------------------------------------------------

/// CSV Writer task
///
/// Sink task — receives JSON messages and writes them as CSV rows.
///
/// # Inputs
///
/// Any number of input channels (merged automatically).
///
/// # Outputs
///
/// None (sink task). Optionally wire `"out"` to forward each message
/// after writing (passthrough).
///
/// # Example Configuration
///
/// ```json
/// {
///   "filename": "output.csv",
///   "delimiter": ",",
///   "header": "auto",
///   "write_mode": "overwrite",
///   "flush_every": 100
/// }
/// ```
pub struct CsvWriter {
    base: BaseTask<Params, State>,
}

impl CsvWriter {
    pub fn create(id: String, params: Value) -> Result<Box<dyn Task>> {
        Ok(Box::new(Self {
            base: BaseTask::new(id, params)?,
        }))
    }
}

#[async_trait]
impl Task for CsvWriter {
    fn name(&self) -> &str {
        "CsvWriter"
    }

    fn set_status_handle(&mut self, status: Arc<tokio::sync::RwLock<crate::task::TaskStatus>>) {
        self.base.status = Some(status);
    }

    fn get_info(&self) -> TaskInfo {
        let current_status = if let Some(status_lock) = &self.base.status {
            status_lock.try_read().ok().map(|s| format!("{:?}", *s))
        } else {
            None
        };

        TaskInfo {
            id: self.base.id.clone(),
            params: serde_json::to_value(&self.base.params).unwrap_or(json!({})),
            state: serde_json::to_value(&self.base.state).unwrap_or(json!({})),
            status: current_status,
            metrics: None,
        }
    }

    async fn execute(&self, ctx: Arc<TaskContext>) -> Result<()> {
        let params = &self.base.params;
        let state = &self.base.state;
        let mut input = ctx.merged_input().await?;
        let passthrough = ctx.output("out").ok();

        // Open file
        let mut file = match params.write_mode {
            WriteMode::Overwrite => {
                tokio::fs::File::create(&params.filename).await.map_err(|e| {
                    crate::err::EngineError::task_execution(
                        &self.base.id,
                        format!("Failed to create '{}': {}", params.filename, e),
                    )
                })?
            }
            WriteMode::Append => {
                tokio::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&params.filename)
                    .await
                    .map_err(|e| {
                        crate::err::EngineError::task_execution(
                            &self.base.id,
                            format!("Failed to open '{}': {}", params.filename, e),
                        )
                    })?
            }
        };

        // Check if we need to write a header (append mode: only if file is empty)
        let need_header = match params.write_mode {
            WriteMode::Overwrite => true,
            WriteMode::Append => {
                let meta = file.metadata().await.map_err(|e| {
                    crate::err::EngineError::task_execution(
                        &self.base.id,
                        format!("Failed to read metadata: {}", e),
                    )
                })?;
                meta.len() == 0
            }
        };

        // Determine columns — may be deferred until first message if auto
        let mut columns: Option<Vec<String>> = match &params.header {
            HeaderMode::Explicit(cols) => Some(cols.clone()),
            HeaderMode::Auto => None,
        };

        // If we already know columns and need a header, write it now
        if need_header {
            if let Some(ref cols) = columns {
                let header_line = cols.join(&params.delimiter.to_string());
                file.write_all(header_line.as_bytes()).await?;
                file.write_all(b"\n").await?;
            }
        }

        tracing::info!(
            "CsvWriter [{}]: Writing to '{}' (mode={:?}, delimiter='{}')",
            self.base.id,
            params.filename,
            params.write_mode,
            params.delimiter,
        );

        let mut rows_since_flush = 0u64;

        while ctx.running().await {
            match input.recv().await {
                Ok(msg) => {
                    // Auto-detect columns from first message
                    if columns.is_none() {
                        if let Value::Object(map) = &msg {
                            let cols: Vec<String> = map.keys().cloned().collect();
                            if need_header {
                                let header_line = cols.join(&params.delimiter.to_string());
                                file.write_all(header_line.as_bytes()).await?;
                                file.write_all(b"\n").await?;
                            }
                            columns = Some(cols);
                        } else {
                            tracing::warn!(
                                "CsvWriter [{}]: First message is not a JSON object, skipping",
                                self.base.id
                            );
                            continue;
                        }
                    }

                    let cols = columns.as_ref().unwrap();
                    let line = build_line(cols, &msg, params.delimiter);
                    file.write_all(line.as_bytes()).await?;
                    file.write_all(b"\n").await?;

                    state.rows_written.fetch_add(1, Ordering::Relaxed);
                    rows_since_flush += 1;

                    // Periodic flush
                    if rows_since_flush >= params.flush_every {
                        file.flush().await?;
                        rows_since_flush = 0;
                    }

                    // Passthrough
                    if let Some(ref out) = passthrough {
                        out.send(msg).await?;
                    }

                    let total = state.rows_written.load(Ordering::Relaxed);
                    if total > 0 && total % 10_000 == 0 {
                        tracing::debug!(
                            "CsvWriter [{}]: {} rows written",
                            self.base.id,
                            total,
                        );
                    }
                }
                Err(_) => {
                    tracing::debug!("CsvWriter [{}]: Input channel closed", self.base.id);
                    break;
                }
            }
        }

        // Final flush
        file.flush().await?;

        let total = state.rows_written.load(Ordering::Relaxed);
        tracing::info!(
            "CsvWriter [{}]: Finished — {} rows written to '{}'",
            self.base.id,
            total,
            params.filename,
        );

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // --- value_to_cell ---

    #[test]
    fn test_cell_string() {
        assert_eq!(value_to_cell(&json!("hello")), "hello");
    }

    #[test]
    fn test_cell_string_with_comma() {
        assert_eq!(value_to_cell(&json!("a,b")), "\"a,b\"");
    }

    #[test]
    fn test_cell_string_with_quotes() {
        assert_eq!(value_to_cell(&json!("say \"hi\"")), "\"say \"\"hi\"\"\"");
    }

    #[test]
    fn test_cell_number() {
        assert_eq!(value_to_cell(&json!(42)), "42");
        assert_eq!(value_to_cell(&json!(3.14)), "3.14");
    }

    #[test]
    fn test_cell_bool() {
        assert_eq!(value_to_cell(&json!(true)), "true");
        assert_eq!(value_to_cell(&json!(false)), "false");
    }

    #[test]
    fn test_cell_null() {
        assert_eq!(value_to_cell(&json!(null)), "");
    }

    #[test]
    fn test_cell_array() {
        let cell = value_to_cell(&json!([1, 2]));
        assert!(cell.starts_with('"'));
        assert!(cell.contains("[1,2]"));
    }

    // --- build_line ---

    #[test]
    fn test_build_line_basic() {
        let cols = vec!["name".into(), "age".into(), "city".into()];
        let msg = json!({"name": "Alice", "age": 30, "city": "NYC"});
        let line = build_line(&cols, &msg, ',');
        assert_eq!(line, "Alice,30,NYC");
    }

    #[test]
    fn test_build_line_missing_field() {
        let cols = vec!["a".into(), "b".into(), "c".into()];
        let msg = json!({"a": 1, "c": 3});
        let line = build_line(&cols, &msg, ',');
        assert_eq!(line, "1,,3");
    }

    #[test]
    fn test_build_line_custom_delimiter() {
        let cols = vec!["x".into(), "y".into()];
        let msg = json!({"x": "foo", "y": "bar"});
        let line = build_line(&cols, &msg, ';');
        assert_eq!(line, "foo;bar");
    }

    #[test]
    fn test_build_line_with_null() {
        let cols = vec!["a".into(), "b".into()];
        let msg = json!({"a": 1, "b": null});
        let line = build_line(&cols, &msg, ',');
        assert_eq!(line, "1,");
    }

    // --- Factory ---

    #[test]
    fn test_csv_writer_create_minimal() {
        let params = json!({ "filename": "/tmp/out.csv" });
        assert!(CsvWriter::create("test".into(), params).is_ok());
    }

    #[test]
    fn test_csv_writer_create_full() {
        let params = json!({
            "filename": "/tmp/out.csv",
            "delimiter": ";",
            "header": { "explicit": ["name", "age", "city"] },
            "write_mode": "append",
            "flush_every": 100
        });
        assert!(CsvWriter::create("test".into(), params).is_ok());
    }

    #[test]
    fn test_csv_writer_create_invalid() {
        assert!(CsvWriter::create("test".into(), json!({"wrong": true})).is_err());
    }

    // --- HeaderMode serde ---

    #[test]
    fn test_header_mode_auto() {
        let mode: HeaderMode = serde_json::from_value(json!("auto")).unwrap();
        assert!(matches!(mode, HeaderMode::Auto));
    }

    #[test]
    fn test_header_mode_explicit() {
        let mode: HeaderMode =
            serde_json::from_value(json!({"explicit": ["a", "b", "c"]})).unwrap();
        match mode {
            HeaderMode::Explicit(cols) => assert_eq!(cols, vec!["a", "b", "c"]),
            _ => panic!("expected Explicit"),
        }
    }

    // --- WriteMode serde ---

    #[test]
    fn test_write_mode_overwrite() {
        let mode: WriteMode = serde_json::from_value(json!("overwrite")).unwrap();
        assert!(matches!(mode, WriteMode::Overwrite));
    }

    #[test]
    fn test_write_mode_append() {
        let mode: WriteMode = serde_json::from_value(json!("append")).unwrap();
        assert!(matches!(mode, WriteMode::Append));
    }

    // --- Integration: write + read back ---

    #[tokio::test]
    async fn test_csv_writer_integration() {
        use std::collections::HashMap;
        use tokio::sync::mpsc;

        let tmp = tempfile::NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_string();

        // Set up context with one input channel
        let (tx, rx) = mpsc::channel(10);
        let mut inputs = HashMap::new();
        inputs.insert("data".to_string(), rx);

        let ctx = Arc::new(TaskContext::new("csv_test".into(), inputs, HashMap::new()));
        ctx.resume();

        let params = json!({
            "filename": path,
            "flush_every": 1
        });
        let task = CsvWriter::create("writer".into(), params).unwrap();

        // Send some messages then close
        tx.send(json!({"name": "Alice", "age": 30})).await.unwrap();
        tx.send(json!({"name": "Bob", "age": 25})).await.unwrap();
        drop(tx);

        // Run task
        task.execute(ctx).await.unwrap();

        // Read back
        let content = tokio::fs::read_to_string(&path).await.unwrap();
        let lines: Vec<&str> = content.trim().lines().collect();

        assert_eq!(lines.len(), 3); // header + 2 rows
        // Header should contain both column names
        assert!(lines[0].contains("name"));
        assert!(lines[0].contains("age"));
        // Data rows
        assert!(lines[1].contains("Alice"));
        assert!(lines[2].contains("Bob"));
    }
}
