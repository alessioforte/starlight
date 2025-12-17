//! Logger Task
//!
//! Logs incoming messages to stdout/stderr (sink task example).

use crate::ctx::TaskContext;
use crate::err::Result;
use crate::task::{BaseTask, Task, TaskInfo};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Logger parameters
#[derive(Debug, Serialize, Deserialize)]
pub struct Params {
    /// Log level: "info", "debug", "warn", "error" (default: "info")
    #[serde(default = "default_level")]
    pub level: String,
    /// Optional prefix for log messages
    #[serde(default)]
    pub prefix: Option<String>,
    /// Whether to pretty-print JSON (default: false)
    #[serde(default)]
    pub pretty: bool,
    /// Whether to log to stderr instead of stdout (default: false)
    #[serde(default)]
    pub use_stderr: bool,
}

fn default_level() -> String {
    "info".to_string()
}

/// Logger state
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct State {
    /// Number of messages logged
    logged: AtomicUsize,
}

/// Logger task
///
/// Receives messages and logs them to stdout or stderr.
/// This is an example of a sink task (no outputs).
///
/// # Input
///
/// Accepts any JSON value from the "in" channel.
///
/// # Example Configuration
///
/// ```json
/// {
///   "level": "info",
///   "prefix": "[DATA]",
///   "pretty": true,
///   "use_stderr": false
/// }
/// ```
pub struct Logger {
    base: BaseTask<Params, State>,
}

impl Logger {
    /// Create a new logger task
    pub fn create(id: String, params: Value) -> Result<Box<dyn Task>> {
        Ok(Box::new(Self {
            base: BaseTask::new(id, params)?,
        }))
    }

    /// Format a message for logging
    fn format_message(&self, value: &Value) -> String {
        let params = &self.base.params;

        let json_str = if params.pretty {
            serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string())
        } else {
            value.to_string()
        };

        if let Some(prefix) = &params.prefix {
            format!("{} {}", prefix, json_str)
        } else {
            json_str
        }
    }

    /// Log a message according to the configured level
    fn log_message(&self, message: &str) {
        let params = &self.base.params;
        let task_id = &self.base.id;

        match params.level.as_str() {
            "error" => {
                if params.use_stderr {
                    eprintln!("{}", message);
                } else {
                    tracing::error!("[{}] {}", task_id, message);
                }
            }
            "warn" => {
                if params.use_stderr {
                    eprintln!("{}", message);
                } else {
                    tracing::warn!("[{}] {}", task_id, message);
                }
            }
            "debug" => {
                if params.use_stderr {
                    eprintln!("{}", message);
                } else {
                    tracing::debug!("[{}] {}", task_id, message);
                }
            }
            _ => {
                // Default to info
                if params.use_stderr {
                    eprintln!("{}", message);
                } else {
                    tracing::info!("[{}] {}", task_id, message);
                }
            }
        }
    }
}

#[async_trait]
impl Task for Logger {
    fn name(&self) -> &str {
        "Logger"
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
        }
    }

    async fn execute(&self, ctx: Arc<TaskContext>) -> Result<()> {
        // Get merged input from all channels (receives from all dependencies)
        let mut input = ctx.merged_input()?;

        tracing::info!(
            "Logger [{}]: Started (level: {}, pretty: {})",
            self.base.id,
            self.base.params.level,
            self.base.params.pretty
        );

        // Process incoming messages
        while ctx.is_running() {
            match input.recv().await {
                Ok(value) => {
                    let message = self.format_message(&value);
                    self.log_message(&message);

                    let count = self.base.state.logged.fetch_add(1, Ordering::Relaxed);

                    // Log progress every 1000 messages
                    if count > 0 && count % 1000 == 0 {
                        tracing::debug!("Logger [{}]: Logged {} messages", self.base.id, count);
                    }
                }
                Err(e) => {
                    if !ctx.is_running() {
                        break;
                    }
                    tracing::error!("Logger [{}]: Receive error: {}", self.base.id, e);
                    break;
                }
            }
        }

        tracing::info!(
            "Logger [{}]: Finished, logged {} messages",
            self.base.id,
            self.base.state.logged.load(Ordering::Relaxed)
        );

        Ok(())
    }

    async fn on_start(&self, _ctx: Arc<TaskContext>) -> Result<()> {
        tracing::info!("Logger [{}]: Ready to log messages", self.base.id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_logger_creation() {
        let params = json!({
            "level": "info",
            "prefix": "[TEST]",
            "pretty": true
        });

        let result = Logger::create("test".to_string(), params);
        assert!(result.is_ok());
    }

    #[test]
    fn test_logger_default_params() {
        let params = json!({});
        let result = Logger::create("test".to_string(), params);
        assert!(result.is_ok());
    }

    #[test]
    fn test_format_message() {
        let params = json!({
            "level": "info",
            "prefix": "[DATA]",
            "pretty": false
        });

        let logger = Logger {
            base: BaseTask::new("test".to_string(), params).unwrap(),
        };

        let value = json!({"key": "value"});
        let formatted = logger.format_message(&value);

        assert!(formatted.starts_with("[DATA]"));
        assert!(formatted.contains("key"));
    }
}
