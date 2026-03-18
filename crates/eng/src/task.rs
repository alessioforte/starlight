//! Task trait and execution runtime
//!
//! This module defines the `Task` trait that all tasks must implement,
//! along with the runtime machinery for executing tasks with lifecycle management.

use crate::ctx::TaskContext;
use crate::err::{EngineError, Result};
use crate::metrics::MetricsSnapshot;
use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::watch;

/// Command for controlling task execution
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// Start or resume task execution
    Start,
    /// Pause task execution (task should stop processing but remain ready)
    Pause,
    /// Stop task execution permanently
    Stop,
}

/// Task status for monitoring
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    /// Task is initialized but not running
    Idle,
    /// Task is actively processing
    Running,
    /// Task is paused
    Paused,
    /// Task has stopped
    Stopped,
    /// Task encountered an error
    Failed(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskInfo {
    pub id: String,
    pub state: Value,
    pub params: Value,
    pub status: Option<String>,
    pub metrics: Option<MetricsSnapshot>,
}

/// Main task trait that all tasks must implement
///
/// Tasks only need to implement two methods:
/// - `execute`: The main task logic
/// - `name`: The task name for logging
///
/// Everything else (lifecycle, channels, error handling) is managed by the framework.
#[async_trait]
pub trait Task: Send + Sync + 'static {
    /// Execute the task
    ///
    /// This is the only method that task developers need to implement.
    /// It receives a `TaskContext` which provides access to input/output channels
    /// and the running state.
    ///
    /// # Typical Patterns
    ///
    /// ## Source Task (no inputs)
    /// ```ignore
    /// async fn execute(&self, ctx: Arc<TaskContext>) -> Result<()> {
    ///     let output = ctx.output("out")?;
    ///     while ctx.running().await {
    ///         let data = json!({"generated": "data"});
    ///         output.send(data).await?;
    ///         tokio::time::sleep(Duration::from_secs(1)).await;
    ///     }
    ///     Ok(())
    /// }
    /// ```
    ///
    /// ## Processing Task (inputs and outputs)
    /// ```ignore
    /// async fn execute(&self, ctx: Arc<TaskContext>) -> Result<()> {
    ///     let mut input = ctx.merged_input().await?;
    ///     let output = ctx.output("out")?;
    ///     while ctx.running().await {
    ///         let data = input.recv().await?;
    ///         output.send(data).await?;
    ///     }
    ///     Ok(())
    /// }
    /// ```
    ///
    /// ## Sink Task (no outputs)
    /// ```ignore
    /// async fn execute(&self, ctx: Arc<TaskContext>) -> Result<()> {
    ///     let mut input = ctx.merged_input().await?;
    ///     while ctx.running().await {
    ///         let data = input.recv().await?;
    ///         // Store or log data...
    ///     }
    ///     Ok(())
    /// }
    /// ```
    async fn execute(&self, ctx: Arc<TaskContext>) -> Result<()>;

    /// Get the task name for logging and identification
    fn name(&self) -> &str;

    /// Optional: Called before execute() starts
    ///
    /// Use this for initialization that should happen once per task start.
    async fn on_start(&self, _ctx: Arc<TaskContext>) -> Result<()> {
        Ok(())
    }

    /// Optional: Called when task is paused
    ///
    /// Use this for cleanup that should happen on pause.
    async fn on_pause(&self, _ctx: Arc<TaskContext>) -> Result<()> {
        Ok(())
    }

    /// Optional: Called when task is stopped
    ///
    /// Use this for final cleanup.
    async fn on_stop(&self, _ctx: Arc<TaskContext>) -> Result<()> {
        Ok(())
    }

    fn get_info(&self) -> TaskInfo {
        TaskInfo {
            id: "".to_string(),
            state: serde_json::json!({}),
            params: serde_json::json!({}),
            status: None,
            metrics: None,
        }
    }

    /// Optional: Set the status handle for this task
    ///
    /// Called by TaskRunner during initialization to provide tasks
    /// with access to their own status. Tasks that want to read/update
    /// their status should store this handle.
    ///
    /// Default implementation does nothing.
    fn set_status_handle(&mut self, _status: Arc<tokio::sync::RwLock<TaskStatus>>) {}
}

/// Base task structure with typed parameters and state
///
/// This is a convenience type for tasks that need to deserialize parameters.
pub struct BaseTask<P, S> {
    pub id: String,
    pub params: P,
    pub state: S,
    pub status: Option<Arc<tokio::sync::RwLock<TaskStatus>>>,
}

impl<P, S> BaseTask<P, S>
where
    P: DeserializeOwned,
    S: Default,
{
    /// Create a new base task from JSON parameters
    pub fn new(id: String, params: serde_json::Value) -> Result<Self> {
        let params: P = serde_json::from_value(params)
            .map_err(|e| EngineError::invalid_params(&id, e.to_string()))?;

        Ok(Self {
            id,
            params,
            state: S::default(),
            status: None,
        })
    }
}

/// Task runner that manages task lifecycle
///
/// This wraps a task and handles:
/// - Command processing (start/pause/stop)
/// - Error handling
/// - Logging
/// - Lifecycle hooks
pub struct TaskRunner {
    task: Arc<Box<dyn Task>>,
    context: Arc<TaskContext>,
    cmd_rx: watch::Receiver<Command>,
    status: Arc<tokio::sync::RwLock<TaskStatus>>,
}

impl TaskRunner {
    /// Create a new task runner
    pub fn new(
        mut task: Box<dyn Task>,
        context: TaskContext,
        cmd_rx: watch::Receiver<Command>,
    ) -> Self {
        let status = Arc::new(tokio::sync::RwLock::new(TaskStatus::Idle));

        // Inject status handle into the task
        task.set_status_handle(Arc::clone(&status));

        Self {
            task: Arc::new(task),
            context: Arc::new(context),
            cmd_rx,
            status,
        }
    }

    pub fn task_handle(&self) -> Arc<Box<dyn Task>> {
        Arc::clone(&self.task)
    }

    pub fn context_handle(&self) -> Arc<TaskContext> {
        Arc::clone(&self.context)
    }

    // /// Get current task status
    // pub async fn status(&self) -> TaskStatus {
    //     self.status.read().await.clone()
    // }

    /// Update task status
    async fn set_status(&self, status: TaskStatus) {
        let mut s = self.status.write().await;
        *s = status;
    }

    /// Run the task with full lifecycle management
    ///
    /// This is the main entry point for task execution.
    /// It handles all lifecycle events and error recovery.
    pub async fn run(mut self) {
        let task_name = self.task.name();
        let task_id = &self.context.id;

        tracing::info!("Task [{task_name}]-{task_id} initialized");
        self.set_status(TaskStatus::Idle).await;

        // Wait for the first Start command before doing anything
        loop {
            if let Err(e) = self.cmd_rx.changed().await {
                tracing::error!("Task [{task_name}]-{task_id} command channel closed: {}", e);
                return;
            }

            let cmd = self.cmd_rx.borrow().clone();
            match cmd {
                Command::Start => break,
                Command::Stop => {
                    tracing::info!("Task [{task_name}]-{task_id} stopped before starting");
                    self.context.stop();
                    self.set_status(TaskStatus::Stopped).await;
                    if let Err(e) = self.task.on_stop(Arc::clone(&self.context)).await {
                        tracing::error!("Task [{task_name}]-{task_id} on_stop failed: {}", e);
                    }
                    return;
                }
                Command::Pause => {
                    // Not started yet, ignore pause
                    tracing::debug!("Task [{task_name}]-{task_id} received pause while idle");
                }
            }
        }

        // --- First Start: launch execute() ---
        tracing::info!("Task [{task_name}]-{task_id} starting");
        self.set_status(TaskStatus::Running).await;
        self.context.resume();

        if let Err(e) = self.task.on_start(Arc::clone(&self.context)).await {
            tracing::error!("Task [{task_name}]-{task_id} on_start failed: {}", e);
            self.set_status(TaskStatus::Failed(e.to_string())).await;
            return;
        }

        let task_ref = &self.task;
        let ctx_clone = Arc::clone(&self.context);
        let execute_future = task_ref.execute(ctx_clone);
        tokio::pin!(execute_future);

        // --- Main select loop: keep execute_future alive across pause/resume ---
        loop {
            tokio::select! {
                result = &mut execute_future => {
                    // Task execution completed (naturally or because ctx.running() returned false)
                    match result {
                        Ok(_) => {
                            tracing::info!("Task [{task_name}]-{task_id} completed successfully");
                            self.set_status(TaskStatus::Stopped).await;
                        }
                        Err(e) => {
                            tracing::error!("Task [{task_name}]-{task_id} execution failed: {}", e);
                            self.set_status(TaskStatus::Failed(e.to_string())).await;
                        }
                    }
                    self.context.stop();
                    break;
                }

                cmd_result = self.cmd_rx.changed() => {
                    if cmd_result.is_err() {
                        tracing::error!("Task [{task_name}]-{task_id} command channel closed");
                        self.context.stop();
                        break;
                    }

                    let new_cmd = self.cmd_rx.borrow().clone();
                    match new_cmd {
                        Command::Pause => {
                            tracing::info!("Task [{task_name}]-{task_id} pausing");
                            self.context.pause();
                            self.set_status(TaskStatus::Paused).await;

                            if let Err(e) = self.task.on_pause(Arc::clone(&self.context)).await {
                                tracing::error!("Task [{task_name}]-{task_id} on_pause failed: {}", e);
                            }
                            // Do NOT break — the execute_future stays alive,
                            // blocked inside ctx.running().await
                        }
                        Command::Start => {
                            tracing::info!("Task [{task_name}]-{task_id} resuming");
                            self.context.resume();
                            self.set_status(TaskStatus::Running).await;
                            // The task unblocks from ctx.running().await automatically
                        }
                        Command::Stop => {
                            tracing::info!("Task [{task_name}]-{task_id} stopping");
                            self.context.stop();
                            self.set_status(TaskStatus::Stopped).await;

                            if let Err(e) = self.task.on_stop(Arc::clone(&self.context)).await {
                                tracing::error!("Task [{task_name}]-{task_id} on_stop failed: {}", e);
                            }
                            break;
                        }
                    }
                }
            }
        }

        tracing::info!("Task [{task_name}]-{task_id} shutdown complete");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ctx::TaskContext;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::watch;

    struct TestTask {
        counter: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl Task for TestTask {
        async fn execute(&self, ctx: Arc<TaskContext>) -> Result<()> {
            while ctx.running().await {
                self.counter.fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            }
            Ok(())
        }

        fn name(&self) -> &str {
            "TestTask"
        }
    }

    #[tokio::test]
    async fn test_task_execution() {
        let counter = Arc::new(AtomicUsize::new(0));
        let task = TestTask {
            counter: Arc::clone(&counter),
        };

        let ctx = TaskContext::new("test".to_string(), HashMap::new(), HashMap::new());
        let (cmd_tx, cmd_rx) = watch::channel(Command::Pause);

        let runner = TaskRunner::new(Box::new(task), ctx, cmd_rx);
        let handle = tokio::spawn(runner.run());

        // Start the task
        cmd_tx.send(Command::Start).unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // Stop the task
        cmd_tx.send(Command::Stop).unwrap();
        handle.await.unwrap();

        // Should have incremented at least a few times
        assert!(counter.load(Ordering::Relaxed) > 0);
    }

    #[tokio::test]
    async fn test_task_pause_resume() {
        let counter = Arc::new(AtomicUsize::new(0));
        let task = TestTask {
            counter: Arc::clone(&counter),
        };

        let ctx = TaskContext::new("test".to_string(), HashMap::new(), HashMap::new());
        let (cmd_tx, cmd_rx) = watch::channel(Command::Pause);

        let runner = TaskRunner::new(Box::new(task), ctx, cmd_rx);
        let handle = tokio::spawn(runner.run());

        // Start
        cmd_tx.send(Command::Start).unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(30)).await;

        let count_1 = counter.load(Ordering::Relaxed);

        // Pause
        cmd_tx.send(Command::Pause).unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(30)).await;

        let count_2 = counter.load(Ordering::Relaxed);

        // Should not have incremented while paused
        assert_eq!(count_1, count_2);

        // Resume
        cmd_tx.send(Command::Start).unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(30)).await;

        let count_3 = counter.load(Ordering::Relaxed);

        // Should have incremented after resume
        assert!(count_3 > count_2);

        // Stop
        cmd_tx.send(Command::Stop).unwrap();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_stop_command_during_execution() {
        let counter = Arc::new(AtomicUsize::new(0));
        let task = TestTask {
            counter: Arc::clone(&counter),
        };

        let ctx = TaskContext::new("test".to_string(), HashMap::new(), HashMap::new());
        let (cmd_tx, cmd_rx) = watch::channel(Command::Pause);

        let runner = TaskRunner::new(Box::new(task), ctx, cmd_rx);
        let handle = tokio::spawn(runner.run());

        // Start the task
        cmd_tx.send(Command::Start).unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;

        let count_before_stop = counter.load(Ordering::Relaxed);
        assert!(count_before_stop > 0, "Task should have started executing");

        // Send stop command while task is running
        cmd_tx.send(Command::Stop).unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;

        let count_after_stop = counter.load(Ordering::Relaxed);

        // Task should stop incrementing shortly after stop command
        tokio::time::sleep(tokio::time::Duration::from_millis(30)).await;
        let count_final = counter.load(Ordering::Relaxed);

        // Count should not increase significantly after stop
        assert!(
            count_final <= count_after_stop + 5,
            "Task should have stopped, but counter kept incrementing"
        );

        handle.await.unwrap();
    }
}
