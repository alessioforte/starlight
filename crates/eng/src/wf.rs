//! Workflow orchestration and management
//!
//! This module provides the workflow builder and runtime for orchestrating tasks.

use crate::cfg::TaskConfig;
use crate::ctx::TaskContext;
use crate::err::{EngineError, Result, WorkflowError};
use crate::metrics::CoarseClock;
use crate::task::{Command, Task, TaskInfo, TaskRunner};
use crate::tasks::TaskRegistry;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, watch};
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

    /// Task contexts for metrics access (task_id -> TaskContext)
    contexts: HashMap<String, Arc<TaskContext>>,

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

    /// Get information about all tasks in the workflow, including live metrics
    pub fn state(&self) -> Vec<TaskInfo> {
        let mut infos = Vec::new();
        for (task_id, task) in &self.task_handles {
            let mut info = task.get_info();
            // Inject live metrics from the task's context
            if let Some(ctx) = self.contexts.get(task_id) {
                info.metrics = Some(ctx.metrics());
            }
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
pub struct WorkflowBuilder<'r> {
    id: String,
    name: Option<String>,
    description: Option<String>,
    tasks: Vec<TaskConfig>,
    channel_capacity: usize,
    /// Update interval for the shared coarse clock (default: 100ms)
    clock_interval: Duration,
    /// Reference to the task registry for resolving task types
    registry: &'r TaskRegistry,
}

impl<'r> WorkflowBuilder<'r> {
    /// Create a new workflow builder with a reference to the task registry.
    pub fn new(id: impl Into<String>, registry: &'r TaskRegistry) -> Self {
        Self {
            id: id.into(),
            name: None,
            description: None,
            tasks: Vec::new(),
            channel_capacity: 1000,
            clock_interval: Duration::from_millis(100),
            registry,
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

    /// Set the update interval for the shared coarse clock
    ///
    /// Default is 100 ms. Lower values give finer timestamp resolution
    /// but cost one syscall per tick.
    #[allow(dead_code)]
    pub fn clock_interval(mut self, interval: Duration) -> Self {
        self.clock_interval = interval;
        self
    }

    /// Build the workflow
    ///
    /// This validates the configuration, creates all channels,
    /// instantiates tasks via the registry, and returns a ready-to-run workflow.
    pub fn build(self) -> Result<Workflow> {
        // Validate workflow
        self.validate()?;

        let (cmd_tx, _) = watch::channel(Command::Pause);
        let status = Arc::new(tokio::sync::RwLock::new(WorkflowStatus::Idle));
        let mut task_handles: HashMap<String, Arc<Box<dyn Task>>> = HashMap::new();

        // Start a shared coarse clock for all tasks in this workflow
        let clock = CoarseClock::start(self.clock_interval);

        // ---------------------------------------------------------
        // 1. Map channel_id → list of consuming task IDs
        // ---------------------------------------------------------
        let mut channel_consumers: HashMap<String, Vec<String>> = HashMap::new();
        for task in &self.tasks {
            for (_port, channel_ids) in &task.dependencies {
                for dep_channel_id in channel_ids {
                    channel_consumers
                        .entry(dep_channel_id.clone())
                        .or_default()
                        .push(task.id.clone());
                }
            }
        }

        // ---------------------------------------------------------
        // 1b. Build reverse map: (consumer_task_id, channel_id) → port_name
        // ---------------------------------------------------------
        let mut channel_to_port: HashMap<(String, String), String> = HashMap::new();
        for task in &self.tasks {
            for (port_name, channel_ids) in &task.dependencies {
                for channel_id in channel_ids {
                    channel_to_port
                        .insert((task.id.clone(), channel_id.clone()), port_name.clone());
                }
            }
        }

        // ---------------------------------------------------------
        // 2. For each (channel_id, consumer) pair create one mpsc channel.
        //    Collect senders for producers, receivers for consumers.
        // ---------------------------------------------------------
        // producer task id → output label → Vec<mpsc::Sender>
        let mut producer_senders: HashMap<String, HashMap<String, Vec<mpsc::Sender<Value>>>> =
            HashMap::new();
        // consumer task id → port_name → Vec<(channel_id, mpsc::Receiver)>
        let mut consumer_receivers: HashMap<
            String,
            HashMap<String, Vec<(String, mpsc::Receiver<Value>)>>,
        > = HashMap::new();

        for task in &self.tasks {
            for (label, channel_ids) in &task.outputs {
                for channel_id in channel_ids {
                    let consumers = channel_consumers
                        .get(channel_id)
                        .cloned()
                        .unwrap_or_default();
                    for consumer_task_id in consumers {
                        let (tx, rx) = mpsc::channel(self.channel_capacity);
                        producer_senders
                            .entry(task.id.clone())
                            .or_default()
                            .entry(label.clone())
                            .or_default()
                            .push(tx);

                        let port_name = channel_to_port
                            .get(&(consumer_task_id.clone(), channel_id.clone()))
                            .cloned()
                            .unwrap_or_else(|| "in".to_string());

                        consumer_receivers
                            .entry(consumer_task_id)
                            .or_default()
                            .entry(port_name)
                            .or_default()
                            .push((channel_id.clone(), rx));
                    }
                }
            }
        }

        // ---------------------------------------------------------
        // 3. Instantiate and spawn each task
        // ---------------------------------------------------------
        let mut handles = Vec::new();
        let mut contexts: HashMap<String, Arc<TaskContext>> = HashMap::new();

        for task_config in self.tasks {
            // Use the registry to create the task instance
            let task_instance = self
                .registry
                .create(
                    &task_config.kind,
                    task_config.id.clone(),
                    task_config.params,
                )
                .map_err(|e| {
                    EngineError::Workflow(WorkflowError::InvalidConfig(
                        self.id.clone(),
                        format!("Failed to create task '{}': {}", task_config.id, e),
                    ))
                })?;

            // Inputs: port-keyed receivers for this task
            let inputs = consumer_receivers
                .remove(&task_config.id)
                .unwrap_or_default();

            // Outputs: senders from this task
            let outputs = producer_senders.remove(&task_config.id).unwrap_or_default();

            // Create task context with the configured channel capacity and shared clock
            let context = TaskContext::with_capacity(
                task_config.id.clone(),
                inputs,
                outputs,
                self.channel_capacity,
                Some(clock.clone()),
            );

            // Create task runner
            let runner = TaskRunner::new(task_instance, context, cmd_tx.subscribe());

            let task_handle = runner.task_handle();
            let ctx_handle = runner.context_handle();
            task_handles.insert(task_config.id.clone(), task_handle);
            contexts.insert(task_config.id.clone(), ctx_handle);

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
            contexts,
            handles,
            status,
        })
    }

    /// Validate the workflow configuration
    fn validate(&self) -> Result<()> {
        // Check that all task types exist in the registry
        for task in &self.tasks {
            if !self.registry.contains(&task.kind) {
                return Err(EngineError::Workflow(WorkflowError::InvalidConfig(
                    self.id.clone(),
                    format!(
                        "Task '{}' has unknown type '{}'. Available: {:?}",
                        task.id,
                        task.kind,
                        self.registry.list()
                    ),
                )));
            }
        }

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
            for (port_name, channel_ids) in &task.dependencies {
                for dep_channel_id in channel_ids {
                    if !available_channels.contains(dep_channel_id.as_str()) {
                        return Err(EngineError::Workflow(WorkflowError::InvalidConfig(
                            self.id.clone(),
                            format!(
                                "Task '{}' port '{}' depends on channel '{}' which is not produced by any upstream task output",
                                task.id, port_name, dep_channel_id
                            ),
                        )));
                    }
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
            for (_port, channel_ids) in &task.dependencies {
                for dep_channel_id in channel_ids {
                    if let Some(&upstream_task_id) = channel_to_task.get(dep_channel_id.as_str()) {
                        upstream_tasks.push(upstream_task_id);
                    }
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
    use crate::cfg::TaskConfig;

    fn registry() -> TaskRegistry {
        TaskRegistry::with_builtins()
    }

    #[tokio::test]
    async fn test_workflow_builder() {
        let r = registry();
        let result = WorkflowBuilder::new("test", &r)
            .name("Test Workflow")
            .add_task(TaskConfig::new(
                "task1",
                "dummy",
                Value::Object(Default::default()),
            ))
            .build();

        assert!(result.is_ok());
    }

    #[test]
    fn test_duplicate_task_ids() {
        let r = registry();
        let result = WorkflowBuilder::new("test", &r)
            .add_task(TaskConfig::new(
                "task1",
                "dummy",
                Value::Object(Default::default()),
            ))
            .add_task(TaskConfig::new(
                "task1",
                "dummy",
                Value::Object(Default::default()),
            ))
            .build();

        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_dependency() {
        let r = registry();
        let result = WorkflowBuilder::new("test", &r)
            .add_task(
                TaskConfig::new("task1", "dummy", Value::Object(Default::default()))
                    .with_dependency("nonexistent_channel"),
            )
            .build();

        assert!(result.is_err());
    }

    #[test]
    fn test_circular_dependency() {
        let r = registry();
        let result = WorkflowBuilder::new("test", &r)
            .add_task(
                TaskConfig::new("task1", "dummy", Value::Object(Default::default()))
                    .with_dependency("task2_out")
                    .with_output("out", vec!["task1_out".to_string()]),
            )
            .add_task(
                TaskConfig::new("task2", "dummy", Value::Object(Default::default()))
                    .with_dependency("task1_out")
                    .with_output("out", vec!["task2_out".to_string()]),
            )
            .build();

        assert!(result.is_err());
    }

    #[test]
    fn test_unknown_task_type() {
        let r = registry();
        let result = WorkflowBuilder::new("test", &r)
            .add_task(TaskConfig::new(
                "task1",
                "nonexistent_type",
                Value::Object(Default::default()),
            ))
            .build();

        assert!(result.is_err());
    }
}
