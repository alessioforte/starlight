use crate::tasks::Tasks;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

/// Configuration for a workflow
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    pub id: String,
    pub name: String,
    pub description: Option<String>,
    pub channel_buffer_size: Option<usize>,
    pub tasks: Vec<TaskConfig>,
}

/// Configuration for a single task in the workflow
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TaskConfig {
    /// Unique task identifier
    pub id: String,

    /// Task type
    #[serde(rename = "type")]
    pub kind: Tasks,

    /// Task parameters as JSON
    pub params: Value,

    /// List of upstream task IDs this task depends on
    pub dependencies: Vec<String>,

    /// Map of output labels to downstream task IDs
    /// Key: output label (e.g., "out", "error")
    /// Value: list of downstream task IDs that receive from this output
    pub outputs: HashMap<String, Vec<String>>,
}

impl TaskConfig {
    /// Create a new task configuration
    pub fn new(id: impl Into<String>, kind: Tasks, params: Value) -> Self {
        Self {
            id: id.into(),
            kind,
            params,
            dependencies: Vec::new(),
            outputs: HashMap::new(),
        }
    }

    /// Add a dependency (input from another task)
    pub fn with_dependency(mut self, task_id: impl Into<String>) -> Self {
        self.dependencies.push(task_id.into());
        self
    }

    /// Add multiple dependencies
    pub fn with_dependencies(mut self, task_ids: Vec<String>) -> Self {
        self.dependencies.extend(task_ids);
        self
    }

    /// Add an output mapping
    pub fn with_output(mut self, label: impl Into<String>, targets: Vec<String>) -> Self {
        self.outputs.insert(label.into(), targets);
        self
    }

    /// Add multiple outputs
    pub fn with_outputs(mut self, outputs: HashMap<String, Vec<String>>) -> Self {
        self.outputs.extend(outputs);
        self
    }
}
