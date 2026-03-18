//! Dummy Task
//!
//! A simple task that does nothing, used for testing and placeholders.

use crate::ctx::TaskContext;
use crate::err::Result;
use crate::task::{BaseTask, Task, TaskInfo};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::Arc;

/// Dummy Task parameters
#[derive(Debug, Serialize, Deserialize)]
pub struct Params {}

/// Dummy Task state
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct State;

/// Dummy Task
/// /// A no-op task that can be used as a placeholder in workflows.
/// /// # Output
/// /// Sends a single "done" message to the "out" channel when started.
/// /// # Example
/// /// ```json
/// /// {
/// ///   "type": "dummy",
/// ///   "params": {}
/// /// }
/// ```
pub struct Dummy {
    base: BaseTask<Params, State>,
}

impl Dummy {
    /// Create a new Dummy task
    pub fn create(id: String, params: Value) -> Result<Box<dyn Task>> {
        Ok(Box::new(Self {
            base: BaseTask::new(id, params)?,
        }))
    }
}

#[async_trait]
impl Task for Dummy {
    fn name(&self) -> &str {
        "Dummy"
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
        tracing::info!("Starting Dummy task {}", self.base.id);

        // Send a single "done" message if an output is configured
        if let Ok(output) = ctx.output("out") {
            output.send(json!({"status": "done"})).await?;
        }

        tracing::info!("Dummy task {} completed", self.base.id);

        Ok(())
    }
}
