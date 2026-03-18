//! JSON Mapper Task
//!
//! Maps and transforms JSON fields from input to output.

use crate::ctx::TaskContext;
use crate::err::Result;
use crate::task::{BaseTask, Task, TaskInfo};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::Arc;

/// JSON Mapper parameters
#[derive(Debug, Serialize, Deserialize)]
pub struct Params {
    /// Field mappings: "output_field" -> "input.nested.field"
    pub mappings: HashMap<String, String>,
    /// Whether to pass through unmapped fields (default: false)
    #[serde(default)]
    pub pass_through: bool,
}

/// JSON Mapper state
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct State {}

/// JSON Mapper task
///
/// Transforms JSON objects by mapping fields according to configuration.
/// This is an example of a processing task (has both inputs and outputs).
///
/// # Input
///
/// Expects JSON objects from the "in" channel.
///
/// # Output
///
/// Sends transformed JSON objects to the "out" channel.
///
/// # Example Configuration
///
/// ```json
/// {
///   "mappings": {
///     "id": "user.user_id",
///     "name": "user.full_name",
///     "email": "contact.email"
///   },
///   "pass_through": false
/// }
/// ```
///
/// This would transform:
/// ```json
/// {
///   "user": {
///     "user_id": 123,
///     "full_name": "John Doe"
///   },
///   "contact": {
///     "email": "john@example.com"
///   }
/// }
/// ```
///
/// Into:
/// ```json
/// {
///   "id": 123,
///   "name": "John Doe",
///   "email": "john@example.com"
/// }
/// ```
pub struct JsonMapper {
    base: BaseTask<Params, State>,
}

impl JsonMapper {
    /// Create a new JSON mapper task
    pub fn create(id: String, params: Value) -> Result<Box<dyn Task>> {
        Ok(Box::new(Self {
            base: BaseTask::new(id, params)?,
        }))
    }

    /// Get a nested value from a JSON object using dot notation
    fn get_nested_value<'a>(value: &'a Value, path: &str) -> Option<&'a Value> {
        path.split('.').fold(Some(value), |acc, key| {
            acc.and_then(|v| match v {
                Value::Object(map) => map.get(key),
                Value::Array(arr) => {
                    // Support array indexing like "items.0.name"
                    key.parse::<usize>().ok().and_then(|idx| arr.get(idx))
                }
                _ => None,
            })
        })
    }
}

#[async_trait]
impl Task for JsonMapper {
    fn name(&self) -> &str {
        "JsonMapper"
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

        // Get merged input from all channels (receives from all dependencies)
        let mut input = ctx.merged_input().await?;
        let output = ctx.output("out")?;

        tracing::info!(
            "JSON Mapper [{}]: Started with {} mappings",
            self.base.id,
            params.mappings.len()
        );

        let mut processed = 0;

        // Process messages
        while ctx.running().await {
            match input.recv().await {
                Ok(value) => {
                    let mut result = if params.pass_through {
                        // Start with a copy of the original
                        value.clone()
                    } else {
                        // Start with an empty object
                        Value::Object(serde_json::Map::new())
                    };

                    // Apply mappings
                    if let Value::Object(ref mut result_map) = result {
                        for (output_field, input_path) in &params.mappings {
                            if let Some(val) = Self::get_nested_value(&value, input_path) {
                                result_map.insert(output_field.clone(), val.clone());
                            } else {
                                tracing::trace!(
                                    "JSON Mapper [{}]: Path '{}' not found in input",
                                    self.base.id,
                                    input_path
                                );
                            }
                        }
                    }

                    // Send to output (async — applies backpressure)
                    output.send(result).await?;
                    processed += 1;

                    if processed % 1000 == 0 {
                        tracing::debug!(
                            "JSON Mapper [{}]: Processed {} records",
                            self.base.id,
                            processed
                        );
                    }
                }
                Err(_) => {
                    // Channel closed — all upstream producers finished
                    tracing::debug!("JSON Mapper [{}]: Input channel closed", self.base.id);
                    break;
                }
            }
        }

        tracing::info!(
            "JSON Mapper [{}]: Finished, processed {} records",
            self.base.id,
            processed
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_json_mapper_creation() {
        let mut mappings = HashMap::new();
        mappings.insert("out_field".to_string(), "in_field".to_string());

        let params = json!({
            "mappings": mappings,
            "pass_through": false
        });

        let result = JsonMapper::create("test".to_string(), params);
        assert!(result.is_ok());
    }

    #[test]
    fn test_get_nested_value() {
        let data = json!({
            "user": {
                "name": "John",
                "age": 30
            },
            "items": [
                {"id": 1},
                {"id": 2}
            ]
        });

        // Test simple nested path
        let val = JsonMapper::get_nested_value(&data, "user.name");
        assert_eq!(val, Some(&json!("John")));

        // Test array indexing
        let val = JsonMapper::get_nested_value(&data, "items.0.id");
        assert_eq!(val, Some(&json!(1)));

        // Test non-existent path
        let val = JsonMapper::get_nested_value(&data, "user.email");
        assert_eq!(val, None);
    }
}
