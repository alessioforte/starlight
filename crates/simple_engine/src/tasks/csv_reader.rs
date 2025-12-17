//! CSV Reader Task
//!
//! Reads CSV files and streams records as JSON objects.

use crate::context::TaskContext;
use crate::error::Result;
use crate::task::{BaseTask, Task};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Value, json};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::io::AsyncBufReadExt;

/// CSV Reader parameters
#[derive(Debug, Deserialize)]
pub struct Params {
    /// Path to the CSV file
    pub filename: String,
    /// CSV delimiter (default: ',')
    #[serde(default = "default_delimiter")]
    pub delimiter: char,
    /// Interval between records in milliseconds (default: 0)
    #[serde(default)]
    pub interval_ms: u64,
    /// Optional: Start reading from this line (0-indexed)
    #[serde(default)]
    pub start_line: usize,
}

fn default_delimiter() -> char {
    ','
}

/// CSV Reader state (for pause/resume support)
#[derive(Debug, Default)]
pub struct State {
    /// Current line being processed
    current_line: AtomicUsize,
}

/// CSV Reader task
///
/// Reads a CSV file and emits each row as a JSON object.
/// Supports pause/resume by tracking the current line.
///
/// # Output
///
/// Sends JSON objects with column names as keys to the "out" channel.
///
/// # Example
///
/// ```json
/// {
///   "filename": "data.csv",
///   "delimiter": ",",
///   "interval_ms": 100
/// }
/// ```
pub struct CsvReader {
    base: BaseTask<Params, State>,
}

impl CsvReader {
    /// Create a new CSV reader task
    pub fn create(id: String, params: Value) -> Result<Box<dyn Task>> {
        Ok(Box::new(Self {
            base: BaseTask::new(id, params)?,
        }))
    }
}

#[async_trait]
impl Task for CsvReader {
    fn name(&self) -> &str {
        "CsvReader"
    }

    fn set_status_handle(&mut self, status: Arc<tokio::sync::RwLock<crate::task::TaskStatus>>) {
        self.base.status = Some(status);
    }

    fn get_state(&self) -> Value {
        // Include current status if available
        let current_status = if let Some(status_lock) = &self.base.status {
            status_lock.try_read().ok().map(|s| format!("{:?}", *s))
        } else {
            None
        };

        json!({
            "filename": self.base.params.filename,
            "delimiter": self.base.params.delimiter.to_string(),
            "interval_ms": self.base.params.interval_ms,
            "start_line": self.base.params.start_line,
            "current_line": self.base.state.current_line.load(Ordering::Relaxed),
            "status": current_status,
        })
    }

    async fn execute(&self, ctx: Arc<TaskContext>) -> Result<()> {
        let params = &self.base.params;
        let state = &self.base.state;

        // Get output channel
        let output = ctx.output("out")?;

        // Open file for streaming
        let file = tokio::fs::File::open(&params.filename).await.map_err(|e| {
            crate::error::EngineError::task_execution(
                &self.base.id,
                format!("Failed to open file '{}': {}", params.filename, e),
            )
        })?;

        let reader = tokio::io::BufReader::new(file);
        let mut lines = reader.lines();

        // Read header
        let header_line = lines.next_line().await?.ok_or_else(|| {
            crate::error::EngineError::task_execution(&self.base.id, "Empty CSV file")
        })?;

        let headers: Vec<String> = header_line
            .split(params.delimiter)
            .map(|s| s.trim().to_string())
            .collect();

        log::info!(
            "CSV Reader [{}]: Loaded {} columns from '{}'",
            self.base.id,
            headers.len(),
            params.filename
        );

        // Skip to start line if needed
        let start_line = params
            .start_line
            .max(state.current_line.load(Ordering::Relaxed));
        for _ in 0..start_line {
            if lines.next_line().await?.is_none() {
                log::info!("CSV Reader [{}]: Reached end of file", self.base.id);
                return Ok(());
            }
        }

        state.current_line.store(start_line, Ordering::Relaxed);

        // Set up rate limiting
        let interval = if params.interval_ms > 0 {
            Some(tokio::time::Duration::from_millis(params.interval_ms))
        } else {
            None
        };

        // Process records
        let mut records_sent = 0;
        while let Some(line) = lines.next_line().await? {
            // Check if we should continue running
            if !ctx.is_running() {
                log::debug!(
                    "CSV Reader [{}]: Paused at line {}",
                    self.base.id,
                    state.current_line.load(Ordering::Relaxed)
                );
                break;
            }

            // Skip empty lines
            if line.trim().is_empty() {
                continue;
            }

            // Parse line into JSON
            let values: Vec<&str> = line.split(params.delimiter).map(|s| s.trim()).collect();

            if values.len() != headers.len() {
                log::warn!(
                    "CSV Reader [{}]: Line {} has {} values but expected {} columns",
                    self.base.id,
                    state.current_line.load(Ordering::Relaxed) + 1,
                    values.len(),
                    headers.len()
                );
                state.current_line.fetch_add(1, Ordering::Relaxed);
                continue;
            }

            let mut record = serde_json::Map::new();
            for (header, value) in headers.iter().zip(values.iter()) {
                // Try to parse as number, boolean, or keep as string
                let json_value = if let Ok(num) = value.parse::<i64>() {
                    json!(num)
                } else if let Ok(num) = value.parse::<f64>() {
                    json!(num)
                } else if let Ok(b) = value.parse::<bool>() {
                    json!(b)
                } else {
                    json!(value)
                };
                record.insert(header.clone(), json_value);
            }

            // Send to output
            output.send(Value::Object(record))?;
            records_sent += 1;

            // Update state
            state.current_line.fetch_add(1, Ordering::Relaxed);

            // Rate limiting
            if let Some(duration) = interval {
                tokio::time::sleep(duration).await;
            }
        }

        log::info!(
            "CSV Reader [{}]: Finished reading file, sent {} records",
            self.base.id,
            records_sent
        );

        Ok(())
    }

    async fn on_start(&self, _ctx: Arc<TaskContext>) -> Result<()> {
        log::info!(
            "CSV Reader [{}]: Starting from line {}",
            self.base.id,
            self.base.state.current_line.load(Ordering::Relaxed)
        );
        Ok(())
    }

    async fn on_pause(&self, _ctx: Arc<TaskContext>) -> Result<()> {
        log::info!(
            "CSV Reader [{}]: Paused at line {}",
            self.base.id,
            self.base.state.current_line.load(Ordering::Relaxed)
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn test_csv_reader_basic() {
        // Create a temporary CSV file
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "name,age,city").unwrap();
        writeln!(file, "Alice,30,NYC").unwrap();
        writeln!(file, "Bob,25,LA").unwrap();
        file.flush().unwrap();

        let params = json!({
            "filename": file.path().to_str().unwrap(),
            "delimiter": ",",
            "interval_ms": 0
        });

        let result = CsvReader::create("test".to_string(), params);
        assert!(result.is_ok());
    }
}
