//! CSV Writer Task
//!
//! Receives JSON objects and writes them to a CSV file.

use crate::context::TaskContext;
use crate::error::Result;
use crate::task::{BaseTask, Task};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

/// CSV Writer parameters
#[derive(Debug, Deserialize)]
pub struct Params {
    /// Path to the output CSV file
    pub filename: String,
    /// CSV delimiter (default: ',')
    #[serde(default = "default_delimiter")]
    pub delimiter: char,
    /// Whether to append to existing file (default: true)
    #[serde(default = "default_append")]
    pub append: bool,
}

fn default_delimiter() -> char {
    ','
}

fn default_append() -> bool {
    true
}

/// CSV Writer state
#[derive(Debug)]
pub struct State {
    /// File handle (initialized on first write)
    file: Mutex<Option<tokio::fs::File>>,
    /// Whether header has been written
    header_written: Mutex<bool>,
}

impl Default for State {
    fn default() -> Self {
        Self {
            file: Mutex::new(None),
            header_written: Mutex::new(false),
        }
    }
}

/// CSV Writer task
///
/// Receives JSON objects and writes them as CSV rows.
/// Creates the file on first write and keeps it open for performance.
///
/// # Input
///
/// Expects JSON objects from the "in" channel.
/// All objects should have consistent fields.
///
/// # Example
///
/// ```json
/// {
///   "filename": "output.csv",
///   "delimiter": ",",
///   "append": true
/// }
/// ```
pub struct CsvWriter {
    base: BaseTask<Params, State>,
}

impl CsvWriter {
    /// Create a new CSV writer task
    pub fn create(id: String, params: Value) -> Result<Box<dyn Task>> {
        Ok(Box::new(Self {
            base: BaseTask::new(id, params)?,
        }))
    }

    /// Initialize the file if not already open
    async fn init_file(&self) -> Result<()> {
        let mut file_guard = self.base.state.file.lock().await;

        if file_guard.is_none() {
            let params = &self.base.params;

            // Create parent directory if needed
            if let Some(parent) = std::path::Path::new(&params.filename).parent() {
                tokio::fs::create_dir_all(parent).await.map_err(|e| {
                    crate::error::EngineError::task_execution(
                        &self.base.id,
                        format!("Failed to create directory: {}", e),
                    )
                })?;
            }

            // Check if file exists to determine if header should be written
            let file_exists = tokio::fs::metadata(&params.filename).await.is_ok();

            // Open file
            let file = tokio::fs::OpenOptions::new()
                .create(true)
                .append(params.append)
                .write(!params.append)
                .truncate(!params.append)
                .open(&params.filename)
                .await
                .map_err(|e| {
                    crate::error::EngineError::task_execution(
                        &self.base.id,
                        format!("Failed to open file '{}': {}", params.filename, e),
                    )
                })?;

            *file_guard = Some(file);

            // If appending to existing file, header is already written
            if params.append && file_exists {
                let mut header_guard = self.base.state.header_written.lock().await;
                *header_guard = true;
            }

            log::info!(
                "CSV Writer [{}]: Opened file '{}' (append: {})",
                self.base.id,
                params.filename,
                params.append
            );
        }

        Ok(())
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

    fn get_state(&self) -> Value {
        use serde_json::json;

        // Safely access state without blocking
        let file_open = self
            .base
            .state
            .file
            .try_lock()
            .map(|guard| guard.is_some())
            .unwrap_or(false);

        let header_written = self
            .base
            .state
            .header_written
            .try_lock()
            .map(|guard| *guard)
            .unwrap_or(false);

        // Include current status if available
        let current_status = if let Some(status_lock) = &self.base.status {
            status_lock.try_read().ok().map(|s| format!("{:?}", *s))
        } else {
            None
        };

        json!({
            "filename": self.base.params.filename,
            "delimiter": self.base.params.delimiter.to_string(),
            "append": self.base.params.append,
            "file_open": file_open,
            "header_written": header_written,
            "status": current_status,
        })
    }

    async fn execute(&self, ctx: Arc<TaskContext>) -> Result<()> {
        // Get merged input from all channels (receives from all dependencies)
        let mut input = ctx.merged_input()?;

        // Initialize file
        self.init_file().await?;

        let mut records_written = 0;

        // Process incoming messages
        while ctx.is_running() {
            match input.recv().await {
                Ok(Value::Object(map)) => {
                    if map.is_empty() {
                        continue;
                    }

                    let mut file_guard = self.base.state.file.lock().await;
                    let file = file_guard.as_mut().ok_or_else(|| {
                        crate::error::EngineError::task_execution(
                            &self.base.id,
                            "File not initialized",
                        )
                    })?;

                    let mut header_guard = self.base.state.header_written.lock().await;

                    // Write header if needed
                    if !*header_guard {
                        let keys: Vec<String> = map.keys().cloned().collect();
                        let header = keys.join(&self.base.params.delimiter.to_string());
                        file.write_all(header.as_bytes()).await?;
                        file.write_all(b"\n").await?;
                        *header_guard = true;

                        log::debug!(
                            "CSV Writer [{}]: Wrote header with {} columns",
                            self.base.id,
                            keys.len()
                        );
                    }

                    // Write values in consistent order (sorted by key)
                    let mut keys: Vec<_> = map.keys().collect();
                    keys.sort();

                    let values: Vec<String> = keys
                        .iter()
                        .filter_map(|k| map.get(*k))
                        .map(|v| match v {
                            Value::String(s) => {
                                // Escape quotes and wrap in quotes if contains delimiter
                                if s.contains(self.base.params.delimiter) || s.contains('"') {
                                    format!("\"{}\"", s.replace('"', "\"\""))
                                } else {
                                    s.clone()
                                }
                            }
                            Value::Number(n) => n.to_string(),
                            Value::Bool(b) => b.to_string(),
                            Value::Null => String::new(),
                            _ => v.to_string(),
                        })
                        .collect();

                    let line = values.join(&self.base.params.delimiter.to_string());
                    file.write_all(line.as_bytes()).await?;
                    file.write_all(b"\n").await?;
                    file.flush().await?;

                    records_written += 1;

                    if records_written % 100 == 0 {
                        log::debug!(
                            "CSV Writer [{}]: Written {} records",
                            self.base.id,
                            records_written
                        );
                    }
                }
                Ok(_) => {
                    log::warn!("CSV Writer [{}]: Received non-object value", self.base.id);
                }
                Err(e) => {
                    // Check if it's just because we're not running
                    if !ctx.is_running() {
                        break;
                    }
                    log::error!("CSV Writer [{}]: Receive error: {}", self.base.id, e);
                    break;
                }
            }
        }

        log::info!(
            "CSV Writer [{}]: Finished, wrote {} records",
            self.base.id,
            records_written
        );

        Ok(())
    }

    async fn on_stop(&self, _ctx: Arc<TaskContext>) -> Result<()> {
        // Ensure file is flushed and closed
        let mut file_guard = self.base.state.file.lock().await;
        if let Some(mut file) = file_guard.take() {
            file.flush().await.ok();
            log::info!("CSV Writer [{}]: File closed", self.base.id);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn test_csv_writer_creation() {
        let file = NamedTempFile::new().unwrap();
        let params = json!({
            "filename": file.path().to_str().unwrap(),
            "delimiter": ",",
            "append": false
        });

        let result = CsvWriter::create("test".to_string(), params);
        assert!(result.is_ok());
    }
}
