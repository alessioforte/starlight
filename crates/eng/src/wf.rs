//! Workflow orchestration and management
//!
//! This module provides the workflow builder and runtime for orchestrating tasks.

use crate::cfg::TaskConfig;
use crate::ctx::TaskContext;
use crate::err::{EngineError, Result, WorkflowError};
use crate::task::{Command, Task, TaskInfo, TaskRunner};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{broadcast, watch};
use tokio::task::JoinHandle;

/// Workflow status
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowStatus {
    /// Workflow is constructed but not started
    Idle,
    /// Workflow is running
    Running,
    /// Workflow is paused
    Paused,
    /// Workflow has stopped
    Stopped,
    /// Workflow encountered an error
    Failed(String),
}

/// Workflow metadata and information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowInfo {
    pub id: String,
    pub name: Option<String>,
    pub description: Option<String>,
    pub status: WorkflowStatus,
    pub task_count: usize,
}

/// Main workflow structure
///
/// A workflow orchestrates multiple tasks, managing their execution
/// and the data flow between them.
pub struct Workflow {
    /// Unique workflow identifier
    pub id: String,

    /// Optional workflow name
    pub name: Option<String>,

    /// Optional workflow description
    pub description: Option<String>,

    /// Control channel for all tasks
    cmd_tx: watch::Sender<Command>,

    /// Task handles for cleanup
    handles: Vec<JoinHandle<()>>,

    /// Task handles for accessing state (task_id -> Arc<Box<dyn Task>>)
    task_handles: HashMap<String, Arc<Box<dyn Task>>>,

    /// Current workflow status
    status: Arc<tokio::sync::RwLock<WorkflowStatus>>,
}

impl Workflow {
    /// Get workflow information
    pub async fn info(&self) -> WorkflowInfo {
        WorkflowInfo {
            id: self.id.clone(),
            name: self.name.clone(),
            description: self.description.clone(),
            status: self.status.read().await.clone(),
            task_count: self.handles.len(),
        }
    }

    // /// Get current workflow status
    // pub async fn status(&self) -> WorkflowStatus {
    //     self.status.read().await.clone()
    // }

    /// Get information about all tasks in the workflow
    pub fn state(&self) -> Vec<TaskInfo> {
        let mut infos = Vec::new();
        for (_, task) in &self.task_handles {
            let info = task.get_info();
            infos.push(info);
        }
        infos
    }

    /// Start the workflow
    pub async fn start(&self) -> Result<()> {
        self.cmd_tx.send(Command::Start).map_err(|e| {
            EngineError::Workflow(WorkflowError::StartFailed(self.id.clone(), e.to_string()))
        })?;

        let mut status = self.status.write().await;
        *status = WorkflowStatus::Running;
        Ok(())
    }

    /// Pause the workflow
    pub async fn pause(&self) -> Result<()> {
        self.cmd_tx
            .send(Command::Pause)
            .map_err(|e| EngineError::config(format!("Failed to pause: {}", e)))?;

        let mut status = self.status.write().await;
        *status = WorkflowStatus::Paused;

        Ok(())
    }

    /// Stop the workflow
    pub async fn stop(&self) -> Result<()> {
        self.cmd_tx.send(Command::Stop).map_err(|e| {
            EngineError::Workflow(WorkflowError::StopFailed(self.id.clone(), e.to_string()))
        })?;

        let mut status = self.status.write().await;
        *status = WorkflowStatus::Stopped;

        Ok(())
    }

    /// Wait for all tasks to complete
    ///
    /// This consumes the workflow and blocks until all tasks have finished.
    pub async fn wait(mut self) -> Result<()> {
        for handle in self.handles.drain(..) {
            handle.await?;
        }
        Ok(())
    }

    /// Abort all tasks immediately
    pub fn abort(&mut self) {
        for handle in &self.handles {
            handle.abort();
        }
    }
}

impl Drop for Workflow {
    fn drop(&mut self) {
        // Abort all tasks on drop
        for handle in &self.handles {
            handle.abort();
        }
    }
}

/// Builder for constructing workflows
///
/// Provides a fluent API for building complex workflows.
pub struct WorkflowBuilder {
    id: String,
    name: Option<String>,
    description: Option<String>,
    tasks: Vec<TaskConfig>,
    channel_capacity: usize,
}

impl WorkflowBuilder {
    /// Create a new workflow builder
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            name: None,
            description: None,
            tasks: Vec::new(),
            channel_capacity: 1000,
        }
    }

    /// Set the workflow name
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Set the workflow description
    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Add a task to the workflow
    pub fn add_task(mut self, config: TaskConfig) -> Self {
        self.tasks.push(config);
        self
    }

    /// Set the channel capacity for inter-task communication
    ///
    /// Default is 1000. Higher values allow more buffering but use more memory.
    pub fn channel_capacity(mut self, capacity: usize) -> Self {
        self.channel_capacity = capacity;
        self
    }

    /// Build the workflow
    ///
    /// This validates the configuration, creates all channels,
    /// instantiates tasks, and returns a ready-to-run workflow.
    pub fn build(self) -> Result<Workflow> {
        // Validate workflow
        self.validate()?;

        let (cmd_tx, _) = watch::channel(Command::Pause);
        let status = Arc::new(tokio::sync::RwLock::new(WorkflowStatus::Idle));

        // Create broadcast channels for inter-task communication
        let mut channels: HashMap<String, broadcast::Sender<Value>> = HashMap::new();

        let mut task_handles: HashMap<String, Arc<Box<dyn Task>>> = HashMap::new();

        // Create channels for all output channel IDs
        // Each output channel ID from the task configs needs a broadcast channel
        for task in &self.tasks {
            for (_label, channel_ids) in &task.outputs {
                for channel_id in channel_ids {
                    channels
                        .entry(channel_id.clone())
                        .or_insert_with(|| broadcast::channel(self.channel_capacity).0);
                }
            }
        }

        let mut handles = Vec::new();

        // Spawn each task
        for task_config in self.tasks {
            let factory = task_config.kind.to_factory();
            let task_instance =
                (factory)(task_config.id.clone(), task_config.params).map_err(|e| {
                    EngineError::Workflow(WorkflowError::InvalidConfig(
                        self.id.clone(),
                        format!("Failed to create task '{}': {}", task_config.id, e),
                    ))
                })?;

            // Build input map (channels this task reads from)
            // Key: channel ID from dependencies
            // Value: broadcast sender for that channel
            let mut inputs = HashMap::new();
            for channel_id in &task_config.dependencies {
                let sender = channels.get(channel_id).ok_or_else(|| {
                    EngineError::Workflow(WorkflowError::InvalidConfig(
                        self.id.clone(),
                        format!(
                            "Input channel '{}' not found for task '{}'. Make sure an upstream task outputs to this channel.",
                            channel_id, task_config.id
                        ),
                    ))
                })?;
                inputs.insert(channel_id.clone(), sender.clone());
            }

            // Build output map (channels this task writes to)
            // Key: output label (e.g., "out")
            // Value: list of broadcast senders for each channel ID
            let mut outputs = HashMap::new();
            for (label, channel_ids) in &task_config.outputs {
                let mut senders = Vec::new();
                for channel_id in channel_ids {
                    let sender = channels.get(channel_id).ok_or_else(|| {
                        EngineError::Workflow(WorkflowError::InvalidConfig(
                            self.id.clone(),
                            format!(
                                "Output channel '{}' not found for task '{}' output label '{}'",
                                channel_id, task_config.id, label
                            ),
                        ))
                    })?;
                    senders.push(sender.clone());
                }
                outputs.insert(label.clone(), senders);
            }

            // Create task context
            let context = TaskContext::new(task_config.id.clone(), inputs, outputs);

            // Create task runner
            let runner = TaskRunner::new(task_instance, context, cmd_tx.subscribe());

            let task_handle = runner.task_handle();
            task_handles.insert(task_config.id.clone(), task_handle);

            // Spawn the task
            handles.push(tokio::spawn(runner.run()));

            tracing::debug!("Task '{}' spawned", task_config.id);
        }

        tracing::info!("Workflow '{}' built with {} tasks", self.id, handles.len());

        Ok(Workflow {
            id: self.id,
            name: self.name,
            description: self.description,
            cmd_tx,
            task_handles,
            handles,
            status,
        })
    }

    /// Validate the workflow configuration
    fn validate(&self) -> Result<()> {
        // Check for duplicate task IDs
        let mut task_ids = std::collections::HashSet::new();
        for task in &self.tasks {
            if !task_ids.insert(&task.id) {
                return Err(EngineError::Workflow(WorkflowError::InvalidConfig(
                    self.id.clone(),
                    format!("Duplicate task ID: '{}'", task.id),
                )));
            }
        }

        // Collect all output channel IDs
        let mut available_channels = std::collections::HashSet::new();
        for task in &self.tasks {
            for (_label, channel_ids) in &task.outputs {
                for channel_id in channel_ids {
                    available_channels.insert(channel_id.as_str());
                }
            }
        }

        // Check that all dependencies reference existing output channels
        for task in &self.tasks {
            for dep_channel_id in &task.dependencies {
                if !available_channels.contains(dep_channel_id.as_str()) {
                    return Err(EngineError::Workflow(WorkflowError::InvalidConfig(
                        self.id.clone(),
                        format!(
                            "Task '{}' depends on channel '{}' which is not produced by any upstream task output",
                            task.id, dep_channel_id
                        ),
                    )));
                }
            }
        }

        // Check for circular dependencies using task-to-task relationships
        self.check_cycles()?;

        Ok(())
    }

    /// Check for circular dependencies using DFS
    fn check_cycles(&self) -> Result<()> {
        // Build a map from channel ID to the task that produces it
        let mut channel_to_task: HashMap<&str, &str> = HashMap::new();
        for task in &self.tasks {
            for (_label, channel_ids) in &task.outputs {
                for channel_id in channel_ids {
                    channel_to_task.insert(channel_id.as_str(), task.id.as_str());
                }
            }
        }

        // Build task dependency graph (task -> upstream tasks)
        let mut graph: HashMap<&str, Vec<&str>> = HashMap::new();
        for task in &self.tasks {
            let mut upstream_tasks = Vec::new();
            for dep_channel_id in &task.dependencies {
                if let Some(&upstream_task_id) = channel_to_task.get(dep_channel_id.as_str()) {
                    upstream_tasks.push(upstream_task_id);
                }
            }
            graph.insert(&task.id, upstream_tasks);
        }

        // Check each task for cycles
        let mut visited = std::collections::HashSet::new();
        let mut rec_stack = std::collections::HashSet::new();

        for task in &self.tasks {
            if !visited.contains(task.id.as_str()) {
                if self.has_cycle(&graph, &task.id, &mut visited, &mut rec_stack) {
                    return Err(EngineError::Workflow(WorkflowError::CircularDependency(
                        self.id.clone(),
                    )));
                }
            }
        }

        Ok(())
    }

    /// DFS helper for cycle detection
    fn has_cycle(
        &self,
        graph: &HashMap<&str, Vec<&str>>,
        task_id: &str,
        visited: &mut std::collections::HashSet<String>,
        rec_stack: &mut std::collections::HashSet<String>,
    ) -> bool {
        visited.insert(task_id.to_string());
        rec_stack.insert(task_id.to_string());

        if let Some(deps) = graph.get(task_id) {
            for dep in deps {
                if !visited.contains(*dep) {
                    if self.has_cycle(graph, dep, visited, rec_stack) {
                        return true;
                    }
                } else if rec_stack.contains(*dep) {
                    return true;
                }
            }
        }

        rec_stack.remove(task_id);
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tasks::Tasks;

    #[tokio::test]
    async fn test_workflow_builder() {
        let result = WorkflowBuilder::new("test")
            .name("Test Workflow")
            .add_task(TaskConfig::new(
                "task1",
                Tasks::Dummy,
                Value::Object(Default::default()),
            ))
            .build();

        assert!(result.is_ok());
    }

    #[test]
    fn test_duplicate_task_ids() {
        let result = WorkflowBuilder::new("test")
            .add_task(TaskConfig::new(
                "task1",
                Tasks::Dummy,
                Value::Object(Default::default()),
            ))
            .add_task(TaskConfig::new(
                "task1",
                Tasks::Dummy,
                Value::Object(Default::default()),
            ))
            .build();

        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_dependency() {
        let result = WorkflowBuilder::new("test")
            .add_task(
                TaskConfig::new("task1", Tasks::Dummy, Value::Object(Default::default()))
                    .with_dependency("nonexistent_channel"),
            )
            .build();

        assert!(result.is_err());
    }

    #[test]
    fn test_circular_dependency() {
        let result = WorkflowBuilder::new("test")
            .add_task(
                TaskConfig::new("task1", Tasks::Dummy, Value::Object(Default::default()))
                    .with_dependency("task2_out")
                    .with_output("out", vec!["task1_out".to_string()]),
            )
            .add_task(
                TaskConfig::new("task2", Tasks::Dummy, Value::Object(Default::default()))
                    .with_dependency("task1_out")
                    .with_output("out", vec!["task2_out".to_string()]),
            )
            .build();

        assert!(result.is_err());
    }
}
