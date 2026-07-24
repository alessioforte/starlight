//! Workflow orchestration and management
//!
//! This module provides the workflow builder and runtime for orchestrating tasks.

use crate::cfg::{JobConfig, JobErrorPolicy, ResourceConfig, ResourceScope, TaskConfig};
use crate::checkpoint::{CheckpointStore, WorkflowCheckpoint};
use crate::ctx::TaskContext;
use crate::err::{EngineError, Result, WorkflowError};
use crate::metrics::CoarseClock;
use crate::msg::Msg;
use crate::resource::{ArtifactMap, ResourceMap, load_resources, load_resources_with_artifacts};
use crate::task::{Command, Task, TaskInfo, TaskRunner, TaskStatus};
use crate::tasks::TaskRegistry;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tokio::task::{JoinHandle, JoinSet};

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
    /// Workflow completed all jobs
    Completed,
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
    pub current_job: Option<String>,
    pub completed_jobs: Vec<String>,
    pub failed_jobs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobOutcome {
    Completed,
    Failed { task_id: String, error: String },
    Stopped,
}

#[derive(Clone)]
struct ActiveJobView {
    job_id: String,
    cmd_tx: watch::Sender<Command>,
    stop_requested: Arc<AtomicBool>,
    task_count: usize,
    task_handles: HashMap<String, Arc<Box<dyn Task>>>,
    contexts: HashMap<String, Arc<TaskContext>>,
}

/// Runtime for one job DAG.
///
/// This owns the task runners, task contexts, command channel, and channel
/// wiring for a single job. Workflow-level code decides which job runtime is
/// active.
struct JobRuntime {
    workflow_id: String,
    job_id: String,
    task_configs: Vec<TaskConfig>,
    channel_capacity: usize,
    clock_interval: Duration,
    resources: Arc<ResourceMap>,
    stop_requested: Arc<AtomicBool>,
    cmd_tx: watch::Sender<Command>,
    handles: JoinSet<(String, TaskStatus)>,
    task_handles: HashMap<String, Arc<Box<dyn Task>>>,
    contexts: HashMap<String, Arc<TaskContext>>,
}

impl JobRuntime {
    fn spawn(
        workflow_id: String,
        job: &JobConfig,
        registry: &TaskRegistry,
        resources: Arc<ResourceMap>,
        channel_capacity: usize,
        clock_interval: Duration,
    ) -> Result<Self> {
        let (cmd_tx, _) = watch::channel(Command::Pause);
        let mut runtime = Self {
            workflow_id,
            job_id: job.id.clone(),
            task_configs: job.tasks.clone(),
            channel_capacity,
            clock_interval,
            resources,
            stop_requested: Arc::new(AtomicBool::new(false)),
            cmd_tx,
            handles: JoinSet::new(),
            task_handles: HashMap::new(),
            contexts: HashMap::new(),
        };

        runtime.rebuild(registry)?;
        Ok(runtime)
    }

    /// Recreate channels, task instances, contexts, and runners.
    fn rebuild(&mut self, registry: &TaskRegistry) -> Result<()> {
        self.abort();
        self.handles = JoinSet::new();
        self.task_handles.clear();
        self.contexts.clear();
        self.stop_requested.store(false, Ordering::Release);

        let (cmd_tx, _) = watch::channel(Command::Pause);
        let clock = CoarseClock::start(self.clock_interval);

        // 1. Map channel_id → list of consuming task IDs
        let mut channel_consumers: HashMap<String, Vec<String>> = HashMap::new();
        for task in &self.task_configs {
            for (_port, channel_ids) in &task.dependencies {
                for dep_channel_id in channel_ids {
                    channel_consumers
                        .entry(dep_channel_id.clone())
                        .or_default()
                        .push(task.id.clone());
                }
            }
        }

        // 1b. Build reverse map: (consumer_task_id, channel_id) → port_name
        let mut channel_to_port: HashMap<(String, String), String> = HashMap::new();
        for task in &self.task_configs {
            for (port_name, channel_ids) in &task.dependencies {
                for channel_id in channel_ids {
                    channel_to_port
                        .insert((task.id.clone(), channel_id.clone()), port_name.clone());
                }
            }
        }

        // 2. Create mpsc channels for each (channel_id, consumer) pair
        let mut producer_senders: HashMap<String, HashMap<String, Vec<mpsc::Sender<Msg>>>> =
            HashMap::new();
        let mut consumer_receivers: HashMap<
            String,
            HashMap<String, Vec<(String, mpsc::Receiver<Msg>)>>,
        > = HashMap::new();

        for task in &self.task_configs {
            for (label, channel_ids) in &task.outputs {
                let output_senders = producer_senders
                    .entry(task.id.clone())
                    .or_default()
                    .entry(label.clone())
                    .or_default();

                for channel_id in channel_ids {
                    let consumers = channel_consumers
                        .get(channel_id)
                        .cloned()
                        .unwrap_or_default();
                    for consumer_task_id in consumers {
                        let (tx, rx) = mpsc::channel(self.channel_capacity);
                        output_senders.push(tx);

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

        // 3. Instantiate and spawn each task
        let mut handles = JoinSet::new();
        let mut task_handles: HashMap<String, Arc<Box<dyn Task>>> = HashMap::new();
        let mut contexts: HashMap<String, Arc<TaskContext>> = HashMap::new();

        for task_config in &self.task_configs {
            let task_instance = registry
                .create(
                    &task_config.kind,
                    task_config.id.clone(),
                    task_config.params.clone(),
                )
                .map_err(|e| {
                    EngineError::Workflow(WorkflowError::InvalidConfig(
                        self.workflow_id.clone(),
                        format!("Failed to create task '{}': {}", task_config.id, e),
                    ))
                })?;

            let inputs = consumer_receivers
                .remove(&task_config.id)
                .unwrap_or_default();

            let outputs = producer_senders.remove(&task_config.id).unwrap_or_default();

            let context = TaskContext::with_capacity_and_resources(
                task_config.id.clone(),
                inputs,
                outputs,
                Arc::clone(&self.resources),
                self.channel_capacity,
                Some(clock.clone()),
            );

            let runner = TaskRunner::new(task_instance, context, cmd_tx.subscribe());

            let task_handle = runner.task_handle();
            let ctx_handle = runner.context_handle();
            task_handles.insert(task_config.id.clone(), task_handle);
            contexts.insert(task_config.id.clone(), ctx_handle);

            let task_id = task_config.id.clone();
            handles.spawn(async move {
                let status = runner.run().await;
                (task_id, status)
            });

            tracing::debug!(
                "Task '{}' spawned for job '{}' in workflow '{}'",
                task_config.id,
                self.job_id,
                self.workflow_id
            );
        }

        tracing::info!(
            "Workflow '{}' job '{}' spawned with {} tasks",
            self.workflow_id,
            self.job_id,
            handles.len()
        );

        self.cmd_tx = cmd_tx;
        self.handles = handles;
        self.task_handles = task_handles;
        self.contexts = contexts;

        Ok(())
    }

    fn start(&self) -> Result<()> {
        if self.handles.is_empty() {
            return Ok(());
        }

        self.cmd_tx.send(Command::Start).map_err(|e| {
            EngineError::Workflow(WorkflowError::StartFailed(
                self.workflow_id.clone(),
                e.to_string(),
            ))
        })
    }

    fn task_count(&self) -> usize {
        self.handles.len()
    }

    fn view(&self) -> ActiveJobView {
        ActiveJobView {
            job_id: self.job_id.clone(),
            cmd_tx: self.cmd_tx.clone(),
            stop_requested: Arc::clone(&self.stop_requested),
            task_count: self.task_count(),
            task_handles: self.task_handles.clone(),
            contexts: self.contexts.clone(),
        }
    }

    async fn wait(&mut self) -> Result<JobOutcome> {
        while let Some(result) = self.handles.join_next().await {
            match result {
                Ok((task_id, TaskStatus::Failed(error))) => {
                    self.abort();
                    return Ok(JobOutcome::Failed { task_id, error });
                }
                Ok((_task_id, _status)) => {}
                Err(err) if self.stop_requested.load(Ordering::Acquire) || err.is_cancelled() => {
                    return Ok(JobOutcome::Stopped);
                }
                Err(err) => {
                    return Ok(JobOutcome::Failed {
                        task_id: "<join>".to_string(),
                        error: err.to_string(),
                    });
                }
            }
        }

        if self.stop_requested.load(Ordering::Acquire) {
            Ok(JobOutcome::Stopped)
        } else {
            Ok(JobOutcome::Completed)
        }
    }

    fn abort(&mut self) {
        self.handles.abort_all();
    }
}

impl Drop for JobRuntime {
    fn drop(&mut self) {
        self.abort();
    }
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

    /// Current workflow status
    status: Arc<RwLock<WorkflowStatus>>,

    // -- Build info (stored for restart) --
    /// Job configurations used to rebuild active job runtimes
    jobs: Vec<JobConfig>,

    /// Active job index in `jobs`
    current_job_index: Arc<std::sync::Mutex<Option<usize>>>,

    /// Completed job IDs in execution order
    completed_jobs: Arc<std::sync::Mutex<Vec<String>>>,

    /// Failed job IDs in execution order
    failed_jobs: Arc<std::sync::Mutex<Vec<String>>>,

    /// Runtime for the active job DAG
    active_job_runtime: Option<JobRuntime>,

    /// Shared active job view used by status/state APIs and lifecycle commands
    active_job: Arc<std::sync::Mutex<Option<ActiveJobView>>>,

    /// Workflow-scope resources loaded once for this workflow
    workflow_resources: Arc<ResourceMap>,

    /// Task registry for creating task instances
    registry: TaskRegistry,

    /// Channel capacity for inter-task communication
    channel_capacity: usize,

    /// Update interval for the shared coarse clock
    clock_interval: Duration,

    /// Background sequential job driver
    driver: Option<JoinHandle<()>>,

    /// Store for workflow/job checkpoint metadata
    checkpoint_store: Option<CheckpointStore>,
}

impl Workflow {
    /// Get workflow information
    pub fn info(&self) -> WorkflowInfo {
        let active_job = self.active_job.lock().expect("active job mutex poisoned");
        let task_count = active_job
            .as_ref()
            .map(|job| job.task_count)
            .unwrap_or_default();
        drop(active_job);

        let current_job_index = *self
            .current_job_index
            .lock()
            .expect("current job mutex poisoned");
        let current_job =
            current_job_index.and_then(|index| self.jobs.get(index).map(|job| job.id.clone()));

        WorkflowInfo {
            id: self.id.clone(),
            name: self.name.clone(),
            description: self.description.clone(),
            status: self
                .status
                .read()
                .expect("workflow status lock poisoned")
                .clone(),
            task_count,
            current_job,
            completed_jobs: self
                .completed_jobs
                .lock()
                .expect("completed jobs mutex poisoned")
                .clone(),
            failed_jobs: self
                .failed_jobs
                .lock()
                .expect("failed jobs mutex poisoned")
                .clone(),
        }
    }

    fn checkpoint_snapshot(&self) -> WorkflowCheckpoint {
        checkpoint_from_state(
            &self.id,
            &self.jobs,
            &self.status,
            &self.current_job_index,
            &self.completed_jobs,
            &self.failed_jobs,
        )
    }

    fn persist_checkpoint(&self) -> Result<()> {
        if let Some(store) = &self.checkpoint_store {
            store.save_workflow(&self.checkpoint_snapshot())?;
        }

        Ok(())
    }

    /// Get information about all tasks in the workflow, including live metrics
    pub fn state(&self) -> Vec<TaskInfo> {
        let active_job = self.active_job.lock().expect("active job mutex poisoned");
        let Some(active_job) = active_job.as_ref() else {
            return Vec::new();
        };

        let mut infos = Vec::new();
        for (task_id, task) in &active_job.task_handles {
            let mut info = task.get_info();
            info.id = task_id.clone();
            if let Some(ctx) = active_job.contexts.get(task_id) {
                info.metrics = Some(ctx.metrics());
            }
            infos.push(info);
        }
        infos
    }

    /// Start the workflow
    ///
    /// If the workflow was previously stopped or failed, it is fully rebuilt
    /// (new channels, task instances, contexts, and runners) before starting.
    pub async fn start(&mut self) -> Result<()> {
        let current = self
            .status
            .read()
            .expect("workflow status lock poisoned")
            .clone();
        match current {
            WorkflowStatus::Running => return Ok(()),
            WorkflowStatus::Paused => {
                self.send_active_command(Command::Start)?;
                {
                    let mut status = self.status.write().expect("workflow status lock poisoned");
                    *status = WorkflowStatus::Running;
                }
                self.persist_checkpoint()?;
                return Ok(());
            }
            WorkflowStatus::Completed => {
                self.reset_progress();
                self.spawn_active_job()?;
            }
            WorkflowStatus::Stopped | WorkflowStatus::Failed(_) => {
                if self.driver_is_running() {
                    return Ok(());
                }
                if self.active_job_runtime.is_none() {
                    self.spawn_active_job()?;
                }
            }
            WorkflowStatus::Idle => {}
        }

        if self.driver_is_running() {
            return Ok(());
        }

        let first_runtime = self.active_job_runtime.take();
        self.start_driver(first_runtime);
        {
            let mut status = self.status.write().expect("workflow status lock poisoned");
            *status = WorkflowStatus::Running;
        }
        self.persist_checkpoint()?;
        Ok(())
    }

    /// Internal: (re)create the active job runtime.
    ///
    /// Called by `WorkflowBuilder::build()` for the initial setup and by
    /// `start()` when restarting a stopped/failed workflow.
    fn spawn_active_job(&mut self) -> Result<()> {
        if let Some(runtime) = &mut self.active_job_runtime {
            runtime.abort();
        }

        let Some(job_index) = *self
            .current_job_index
            .lock()
            .expect("current job mutex poisoned")
        else {
            self.active_job_runtime = None;
            *self.active_job.lock().expect("active job mutex poisoned") = None;
            return Ok(());
        };

        let job = self.jobs.get(job_index).ok_or_else(|| {
            EngineError::Workflow(WorkflowError::InvalidConfig(
                self.id.clone(),
                format!("current job index {} is out of bounds", job_index),
            ))
        })?;

        let resources = self.load_job_resources(job_index, job)?;

        let runtime = JobRuntime::spawn(
            self.id.clone(),
            job,
            &self.registry,
            resources,
            self.channel_capacity,
            self.clock_interval,
        )?;

        *self.active_job.lock().expect("active job mutex poisoned") = Some(runtime.view());
        self.active_job_runtime = Some(runtime);

        Ok(())
    }

    fn reset_progress(&mut self) {
        if let Some(handle) = self.driver.take() {
            handle.abort();
        }
        if let Some(runtime) = &mut self.active_job_runtime {
            runtime.abort();
        }
        *self
            .current_job_index
            .lock()
            .expect("current job mutex poisoned") =
            if self.jobs.is_empty() { None } else { Some(0) };
        self.completed_jobs
            .lock()
            .expect("completed jobs mutex poisoned")
            .clear();
        self.failed_jobs
            .lock()
            .expect("failed jobs mutex poisoned")
            .clear();
        *self.active_job.lock().expect("active job mutex poisoned") = None;
        self.active_job_runtime = None;
    }

    fn driver_is_running(&self) -> bool {
        self.driver
            .as_ref()
            .map(|handle| !handle.is_finished())
            .unwrap_or(false)
    }

    fn start_driver(&mut self, first_runtime: Option<JobRuntime>) {
        let args = WorkflowDriverArgs {
            workflow_id: self.id.clone(),
            jobs: self.jobs.clone(),
            registry: self.registry.clone(),
            workflow_resources: Arc::clone(&self.workflow_resources),
            channel_capacity: self.channel_capacity,
            clock_interval: self.clock_interval,
            status: Arc::clone(&self.status),
            current_job_index: Arc::clone(&self.current_job_index),
            completed_jobs: Arc::clone(&self.completed_jobs),
            failed_jobs: Arc::clone(&self.failed_jobs),
            active_job: Arc::clone(&self.active_job),
            first_runtime,
            checkpoint_store: self.checkpoint_store.clone(),
        };

        self.driver = Some(tokio::spawn(run_workflow_jobs(args)));
    }

    fn load_job_resources(&self, job_index: usize, job: &JobConfig) -> Result<Arc<ResourceMap>> {
        load_job_resource_map(&self.jobs, job_index, &self.workflow_resources, job)
    }

    fn send_active_command(&self, command: Command) -> Result<()> {
        let active_job = self.active_job.lock().expect("active job mutex poisoned");
        let Some(active_job) = active_job.as_ref() else {
            return Ok(());
        };

        if command == Command::Stop {
            active_job.stop_requested.store(true, Ordering::Release);
        }

        active_job.cmd_tx.send(command).map_err(|e| {
            EngineError::Workflow(WorkflowError::StartFailed(
                self.id.clone(),
                format!(
                    "failed to send command to active job '{}': {}",
                    active_job.job_id, e
                ),
            ))
        })
    }

    /// Pause the workflow
    pub async fn pause(&self) -> Result<()> {
        let current = self
            .status
            .read()
            .expect("workflow status lock poisoned")
            .clone();
        if current != WorkflowStatus::Running {
            return Err(EngineError::Workflow(WorkflowError::InvalidTransition(
                self.id.clone(),
                format!("cannot pause workflow in state {current:?}"),
            )));
        }

        self.send_active_command(Command::Pause)?;

        {
            let mut status = self.status.write().expect("workflow status lock poisoned");
            *status = WorkflowStatus::Paused;
        }
        self.persist_checkpoint()?;

        Ok(())
    }

    /// Stop the workflow
    pub async fn stop(&self) -> Result<()> {
        self.send_active_command(Command::Stop)?;

        {
            let mut status = self.status.write().expect("workflow status lock poisoned");
            *status = WorkflowStatus::Stopped;
        }
        self.persist_checkpoint()?;

        Ok(())
    }

    /// Wait for all tasks to complete
    ///
    /// This consumes the workflow and blocks until all tasks have finished.
    pub async fn wait(mut self) -> Result<()> {
        if let Some(driver) = self.driver.take() {
            driver.await?;
        } else if let Some(runtime) = &mut self.active_job_runtime {
            let _ = runtime.wait().await?;
        }
        Ok(())
    }

    /// Abort all tasks immediately
    pub fn abort(&mut self) {
        if let Some(driver) = self.driver.take() {
            driver.abort();
        }
        if let Some(runtime) = &mut self.active_job_runtime {
            runtime.abort();
        }
        *self.active_job.lock().expect("active job mutex poisoned") = None;
    }
}

impl Drop for Workflow {
    fn drop(&mut self) {
        self.abort();
    }
}

struct WorkflowDriverArgs {
    workflow_id: String,
    jobs: Vec<JobConfig>,
    registry: TaskRegistry,
    workflow_resources: Arc<ResourceMap>,
    channel_capacity: usize,
    clock_interval: Duration,
    status: Arc<RwLock<WorkflowStatus>>,
    current_job_index: Arc<std::sync::Mutex<Option<usize>>>,
    completed_jobs: Arc<std::sync::Mutex<Vec<String>>>,
    failed_jobs: Arc<std::sync::Mutex<Vec<String>>>,
    active_job: Arc<std::sync::Mutex<Option<ActiveJobView>>>,
    first_runtime: Option<JobRuntime>,
    checkpoint_store: Option<CheckpointStore>,
}

fn checkpoint_from_state(
    workflow_id: &str,
    jobs: &[JobConfig],
    status: &Arc<RwLock<WorkflowStatus>>,
    current_job_index: &Arc<std::sync::Mutex<Option<usize>>>,
    completed_jobs: &Arc<std::sync::Mutex<Vec<String>>>,
    failed_jobs: &Arc<std::sync::Mutex<Vec<String>>>,
) -> WorkflowCheckpoint {
    let current_job_index = *current_job_index
        .lock()
        .expect("current job mutex poisoned");
    let current_job = current_job_index.and_then(|index| jobs.get(index).map(|job| job.id.clone()));

    WorkflowCheckpoint::new(
        workflow_id,
        status
            .read()
            .expect("workflow status lock poisoned")
            .clone(),
        current_job,
        completed_jobs
            .lock()
            .expect("completed jobs mutex poisoned")
            .clone(),
        failed_jobs
            .lock()
            .expect("failed jobs mutex poisoned")
            .clone(),
    )
}

fn save_checkpoint_from_driver(args: &WorkflowDriverArgs) {
    let Some(store) = &args.checkpoint_store else {
        return;
    };

    let checkpoint = checkpoint_from_state(
        &args.workflow_id,
        &args.jobs,
        &args.status,
        &args.current_job_index,
        &args.completed_jobs,
        &args.failed_jobs,
    );

    if let Err(err) = store.save_workflow(&checkpoint) {
        tracing::error!(
            "Failed to save checkpoint for workflow '{}': {}",
            args.workflow_id,
            err
        );
    }
}

async fn run_workflow_jobs(mut args: WorkflowDriverArgs) {
    loop {
        let Some(job_index) = *args
            .current_job_index
            .lock()
            .expect("current job mutex poisoned")
        else {
            *args.status.write().expect("workflow status lock poisoned") =
                WorkflowStatus::Completed;
            *args.active_job.lock().expect("active job mutex poisoned") = None;
            save_checkpoint_from_driver(&args);
            return;
        };

        let Some(job) = args.jobs.get(job_index).cloned() else {
            *args.status.write().expect("workflow status lock poisoned") =
                WorkflowStatus::Failed(format!("current job index {} is out of bounds", job_index));
            *args.active_job.lock().expect("active job mutex poisoned") = None;
            save_checkpoint_from_driver(&args);
            return;
        };

        let runtime_result = if let Some(runtime) = args.first_runtime.take() {
            Ok(runtime)
        } else {
            load_job_resource_map(&args.jobs, job_index, &args.workflow_resources, &job).and_then(
                |resources| {
                    JobRuntime::spawn(
                        args.workflow_id.clone(),
                        &job,
                        &args.registry,
                        resources,
                        args.channel_capacity,
                        args.clock_interval,
                    )
                },
            )
        };

        let mut runtime = match runtime_result {
            Ok(runtime) => runtime,
            Err(err) => {
                args.failed_jobs
                    .lock()
                    .expect("failed jobs mutex poisoned")
                    .push(job.id.clone());
                *args.status.write().expect("workflow status lock poisoned") =
                    WorkflowStatus::Failed(err.to_string());
                *args.active_job.lock().expect("active job mutex poisoned") = None;
                save_checkpoint_from_driver(&args);
                return;
            }
        };

        *args.active_job.lock().expect("active job mutex poisoned") = Some(runtime.view());
        *args.status.write().expect("workflow status lock poisoned") = WorkflowStatus::Running;
        save_checkpoint_from_driver(&args);

        if let Err(err) = runtime.start() {
            args.failed_jobs
                .lock()
                .expect("failed jobs mutex poisoned")
                .push(job.id.clone());
            *args.status.write().expect("workflow status lock poisoned") =
                WorkflowStatus::Failed(err.to_string());
            *args.active_job.lock().expect("active job mutex poisoned") = None;
            save_checkpoint_from_driver(&args);
            return;
        }

        let outcome = match runtime.wait().await {
            Ok(outcome) => outcome,
            Err(err) => JobOutcome::Failed {
                task_id: "<join>".to_string(),
                error: err.to_string(),
            },
        };

        *args.active_job.lock().expect("active job mutex poisoned") = None;

        match outcome {
            JobOutcome::Completed => {
                args.completed_jobs
                    .lock()
                    .expect("completed jobs mutex poisoned")
                    .push(job.id.clone());
                let next = job_index + 1;
                *args
                    .current_job_index
                    .lock()
                    .expect("current job mutex poisoned") = if next < args.jobs.len() {
                    Some(next)
                } else {
                    None
                };

                if next >= args.jobs.len() {
                    *args.status.write().expect("workflow status lock poisoned") =
                        WorkflowStatus::Completed;
                    save_checkpoint_from_driver(&args);
                    return;
                }
                save_checkpoint_from_driver(&args);
            }
            JobOutcome::Failed { task_id, error } => {
                args.failed_jobs
                    .lock()
                    .expect("failed jobs mutex poisoned")
                    .push(job.id.clone());

                match job.on_error {
                    JobErrorPolicy::Fail => {
                        *args.status.write().expect("workflow status lock poisoned") =
                            WorkflowStatus::Failed(format!(
                                "job '{}' task '{}' failed: {}",
                                job.id, task_id, error
                            ));
                        save_checkpoint_from_driver(&args);
                        return;
                    }
                    JobErrorPolicy::Continue | JobErrorPolicy::ContinueWithWarnings => {
                        let next = job_index + 1;
                        *args
                            .current_job_index
                            .lock()
                            .expect("current job mutex poisoned") = if next < args.jobs.len() {
                            Some(next)
                        } else {
                            None
                        };

                        if next >= args.jobs.len() {
                            *args.status.write().expect("workflow status lock poisoned") =
                                WorkflowStatus::Completed;
                            save_checkpoint_from_driver(&args);
                            return;
                        }
                        save_checkpoint_from_driver(&args);
                    }
                }
            }
            JobOutcome::Stopped => {
                *args.status.write().expect("workflow status lock poisoned") =
                    WorkflowStatus::Stopped;
                save_checkpoint_from_driver(&args);
                return;
            }
        }
    }
}

fn load_job_resource_map(
    jobs: &[JobConfig],
    job_index: usize,
    workflow_resources: &Arc<ResourceMap>,
    job: &JobConfig,
) -> Result<Arc<ResourceMap>> {
    let mut resources = (**workflow_resources).clone();
    let artifacts = available_artifacts_before(jobs, job_index);
    for (id, value) in load_resources_with_artifacts(&job.resources, &artifacts)? {
        if resources.insert(id.clone(), value).is_some() {
            return Err(EngineError::config(format!(
                "resource '{}' in job '{}' shadows a workflow resource",
                id, job.id
            )));
        }
    }

    Ok(Arc::new(resources))
}

fn available_artifacts_before(jobs: &[JobConfig], job_index: usize) -> ArtifactMap {
    let mut artifacts = ArtifactMap::new();
    for job in jobs.iter().take(job_index) {
        for artifact in &job.artifacts {
            artifacts.insert(artifact.id.clone(), artifact.clone());
        }
    }
    artifacts
}

struct CheckpointProgress {
    status: WorkflowStatus,
    current_job_index: Option<usize>,
    completed_jobs: Vec<String>,
    failed_jobs: Vec<String>,
}

fn checkpoint_progress(
    jobs: &[JobConfig],
    checkpoint: Option<&WorkflowCheckpoint>,
) -> CheckpointProgress {
    let Some(checkpoint) = checkpoint else {
        return CheckpointProgress {
            status: WorkflowStatus::Idle,
            current_job_index: if jobs.is_empty() { None } else { Some(0) },
            completed_jobs: Vec::new(),
            failed_jobs: Vec::new(),
        };
    };

    let completed_set: HashSet<&str> = checkpoint
        .completed_jobs
        .iter()
        .map(String::as_str)
        .collect();
    let failed_set: HashSet<&str> = checkpoint.failed_jobs.iter().map(String::as_str).collect();

    let completed_jobs = jobs
        .iter()
        .filter(|job| completed_set.contains(job.id.as_str()))
        .map(|job| job.id.clone())
        .collect::<Vec<_>>();
    let failed_jobs = jobs
        .iter()
        .filter(|job| failed_set.contains(job.id.as_str()))
        .map(|job| job.id.clone())
        .collect::<Vec<_>>();
    let current_job_index = jobs
        .iter()
        .position(|job| !completed_set.contains(job.id.as_str()));

    let status = if current_job_index.is_none() {
        WorkflowStatus::Completed
    } else if let WorkflowStatus::Failed(error) = &checkpoint.status {
        WorkflowStatus::Failed(error.clone())
    } else {
        WorkflowStatus::Stopped
    };

    CheckpointProgress {
        status,
        current_job_index,
        completed_jobs,
        failed_jobs,
    }
}

/// Builder for constructing workflows
///
/// Provides a fluent API for building complex workflows.
pub struct WorkflowBuilder<'r> {
    id: String,
    name: Option<String>,
    description: Option<String>,
    resources: Vec<ResourceConfig>,
    tasks: Vec<TaskConfig>,
    jobs: Vec<JobConfig>,
    checkpoint: Option<WorkflowCheckpoint>,
    checkpoint_store: Option<CheckpointStore>,
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
            resources: Vec::new(),
            tasks: Vec::new(),
            jobs: Vec::new(),
            checkpoint: None,
            checkpoint_store: None,
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

    /// Add a job to the workflow.
    pub fn add_job(mut self, config: JobConfig) -> Self {
        self.jobs.push(config);
        self
    }

    /// Set workflow-scope resources.
    pub fn resources(mut self, resources: Vec<ResourceConfig>) -> Self {
        self.resources = resources;
        self
    }

    /// Add one workflow-scope resource.
    pub fn add_resource(mut self, config: ResourceConfig) -> Self {
        self.resources.push(config);
        self
    }

    /// Set the checkpoint metadata loaded for this workflow.
    pub fn checkpoint(mut self, checkpoint: Option<WorkflowCheckpoint>) -> Self {
        self.checkpoint = checkpoint;
        self
    }

    /// Set the store used to persist workflow/job checkpoint metadata.
    pub fn checkpoint_store(mut self, store: Option<CheckpointStore>) -> Self {
        self.checkpoint_store = store;
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
        let jobs = self.job_configs()?;

        // Validate the task graph before creating runtime resources. Task-specific
        // parameters and declared output contracts are checked before spawn.
        self.validate_channel_capacity()?;
        self.validate_structure_for_jobs(&jobs)?;
        self.validate_resource_visibility_for_jobs(&jobs)?;
        self.validate_task_params_for_jobs(&jobs)?;
        let workflow_resources = Arc::new(load_resources(&self.resources)?);

        let checkpoint_progress = checkpoint_progress(&jobs, self.checkpoint.as_ref());
        let status = Arc::new(RwLock::new(checkpoint_progress.status));

        let mut workflow = Workflow {
            id: self.id,
            name: self.name,
            description: self.description,
            status,
            jobs,
            current_job_index: Arc::new(std::sync::Mutex::new(
                checkpoint_progress.current_job_index,
            )),
            completed_jobs: Arc::new(std::sync::Mutex::new(checkpoint_progress.completed_jobs)),
            failed_jobs: Arc::new(std::sync::Mutex::new(checkpoint_progress.failed_jobs)),
            active_job_runtime: None,
            active_job: Arc::new(std::sync::Mutex::new(None)),
            workflow_resources,
            registry: self.registry.clone(),
            channel_capacity: self.channel_capacity,
            clock_interval: self.clock_interval,
            driver: None,
            checkpoint_store: self.checkpoint_store,
        };

        workflow.spawn_active_job()?;

        tracing::info!(
            "Workflow '{}' built with {} tasks",
            workflow.id,
            workflow
                .active_job_runtime
                .as_ref()
                .map(JobRuntime::task_count)
                .unwrap_or_default()
        );

        Ok(workflow)
    }

    /// Validate the workflow configuration without spawning tasks.
    pub fn validate(&self) -> Result<()> {
        let jobs = self.job_configs()?;
        self.validate_channel_capacity()?;
        self.validate_structure_for_jobs(&jobs)?;
        self.validate_resource_visibility_for_jobs(&jobs)?;
        self.validate_task_params_for_jobs(&jobs)?;
        Ok(())
    }

    fn validate_channel_capacity(&self) -> Result<()> {
        if self.channel_capacity == 0 {
            return Err(EngineError::Workflow(WorkflowError::InvalidConfig(
                self.id.clone(),
                "channel capacity must be greater than zero".to_string(),
            )));
        }

        Ok(())
    }

    fn job_configs(&self) -> Result<Vec<JobConfig>> {
        if !self.tasks.is_empty() && !self.jobs.is_empty() {
            return Err(EngineError::Workflow(WorkflowError::InvalidConfig(
                self.id.clone(),
                "workflow builder may not mix add_task and add_job".to_string(),
            )));
        }

        if self.jobs.is_empty() {
            Ok(vec![JobConfig {
                id: "default".to_string(),
                name: None,
                description: None,
                on_error: JobErrorPolicy::default(),
                resources: Vec::new(),
                artifacts: Vec::new(),
                tasks: self.tasks.clone(),
            }])
        } else {
            Ok(self.jobs.clone())
        }
    }

    /// Validate task graph shape and channel references for all jobs.
    fn validate_structure_for_jobs(&self, jobs: &[JobConfig]) -> Result<()> {
        let mut task_ids = HashSet::new();

        for job in jobs {
            self.validate_job_structure(job, &mut task_ids)?;
        }

        Ok(())
    }

    fn validate_job_structure(
        &self,
        job: &JobConfig,
        task_ids: &mut HashSet<String>,
    ) -> Result<()> {
        // Check that all task types exist in the registry
        for task in &job.tasks {
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
        for task in &job.tasks {
            if !task_ids.insert(task.id.clone()) {
                return Err(EngineError::Workflow(WorkflowError::InvalidConfig(
                    self.id.clone(),
                    format!("Duplicate task ID: '{}'", task.id),
                )));
            }
        }

        // Collect all output channel IDs
        let mut available_channels = HashSet::new();
        for task in &job.tasks {
            for (_label, channel_ids) in &task.outputs {
                for channel_id in channel_ids {
                    available_channels.insert(channel_id.as_str());
                }
            }
        }

        // Check that all dependencies reference existing output channels
        for task in &job.tasks {
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
        self.check_cycles(&job.id, &job.tasks)?;

        Ok(())
    }

    fn validate_resource_visibility_for_jobs(&self, jobs: &[JobConfig]) -> Result<()> {
        let mut workflow_resources = HashSet::new();

        for resource in &self.resources {
            if resource.effective_scope(ResourceScope::Workflow) != ResourceScope::Workflow {
                return Err(EngineError::Workflow(WorkflowError::InvalidConfig(
                    self.id.clone(),
                    format!(
                        "top-level resource '{}' must have workflow scope",
                        resource.id
                    ),
                )));
            }

            if !workflow_resources.insert(resource.id.clone()) {
                return Err(EngineError::Workflow(WorkflowError::InvalidConfig(
                    self.id.clone(),
                    format!("duplicate workflow resource ID '{}'", resource.id),
                )));
            }
        }

        for job in jobs {
            let mut visible_resources = workflow_resources.clone();
            let mut job_resources = HashSet::new();

            for resource in &job.resources {
                if resource.effective_scope(ResourceScope::Job) != ResourceScope::Job {
                    return Err(EngineError::Workflow(WorkflowError::InvalidConfig(
                        self.id.clone(),
                        format!(
                            "resource '{}' in job '{}' must have job scope",
                            resource.id, job.id
                        ),
                    )));
                }

                if workflow_resources.contains(&resource.id) {
                    return Err(EngineError::Workflow(WorkflowError::InvalidConfig(
                        self.id.clone(),
                        format!(
                            "resource '{}' in job '{}' shadows a workflow resource",
                            resource.id, job.id
                        ),
                    )));
                }

                if !job_resources.insert(resource.id.clone()) {
                    return Err(EngineError::Workflow(WorkflowError::InvalidConfig(
                        self.id.clone(),
                        format!(
                            "duplicate resource ID '{}' in job '{}'",
                            resource.id, job.id
                        ),
                    )));
                }

                visible_resources.insert(resource.id.clone());
            }

            for task in &job.tasks {
                for resource_id in &task.uses {
                    if !visible_resources.contains(resource_id) {
                        return Err(EngineError::Workflow(WorkflowError::InvalidConfig(
                            self.id.clone(),
                            format!(
                                "task '{}' in job '{}' uses resource '{}' which is not visible to that task",
                                task.id, job.id, resource_id
                            ),
                        )));
                    }
                }
            }
        }

        Ok(())
    }

    /// Instantiate task factories to validate task-specific parameters.
    fn validate_task_params_for_jobs(&self, jobs: &[JobConfig]) -> Result<()> {
        for job in jobs {
            for task in &job.tasks {
                let instance = self
                    .registry
                    .create(&task.kind, task.id.clone(), task.params.clone())
                    .map_err(|e| {
                        EngineError::Workflow(WorkflowError::InvalidConfig(
                            self.id.clone(),
                            format!("Failed to create task '{}': {}", task.id, e),
                        ))
                    })?;

                for required_output in instance.required_outputs() {
                    if !task.outputs.contains_key(*required_output) {
                        return Err(EngineError::Workflow(WorkflowError::InvalidConfig(
                            self.id.clone(),
                            format!(
                                "Task '{}' of type '{}' requires output label '{}'",
                                task.id, task.kind, required_output
                            ),
                        )));
                    }
                }
            }
        }

        Ok(())
    }

    /// Check for circular dependencies using DFS
    fn check_cycles(&self, job_id: &str, tasks: &[TaskConfig]) -> Result<()> {
        // A shared channel can have multiple producers; every producer forms
        // an upstream edge for consumers of that channel.
        let mut channel_to_tasks: HashMap<&str, Vec<&str>> = HashMap::new();
        for task in tasks {
            for (_label, channel_ids) in &task.outputs {
                for channel_id in channel_ids {
                    channel_to_tasks
                        .entry(channel_id.as_str())
                        .or_default()
                        .push(task.id.as_str());
                }
            }
        }

        // Build task dependency graph (task -> upstream tasks)
        let mut graph: HashMap<&str, Vec<&str>> = HashMap::new();
        for task in tasks {
            let mut upstream_tasks = Vec::new();
            for (_port, channel_ids) in &task.dependencies {
                for dep_channel_id in channel_ids {
                    if let Some(upstream_task_ids) = channel_to_tasks.get(dep_channel_id.as_str()) {
                        upstream_tasks.extend(upstream_task_ids.iter().copied());
                    }
                }
            }
            graph.insert(&task.id, upstream_tasks);
        }

        // Check each task for cycles
        let mut visited = HashSet::new();
        let mut rec_stack = HashSet::new();

        for task in tasks {
            if !visited.contains(task.id.as_str()) {
                if self.has_cycle(&graph, &task.id, &mut visited, &mut rec_stack) {
                    return Err(EngineError::Workflow(WorkflowError::CircularDependency(
                        format!("{}:{}", self.id, job_id),
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
        visited: &mut HashSet<String>,
        rec_stack: &mut HashSet<String>,
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
    use crate::cfg::{ResourceConfig, ResourceFormat, ResourceSource, TaskConfig};
    use async_trait::async_trait;
    use serde::Deserialize;
    use serde_json::{Value, json};
    use std::fs;
    use std::sync::{Mutex, OnceLock};

    static JOB_TEST_EVENTS: OnceLock<Mutex<Vec<String>>> = OnceLock::new();

    #[derive(Debug, Deserialize)]
    struct RecordingParams {
        label: String,
        #[serde(default)]
        fail: bool,
        #[serde(default = "default_recording_ticks")]
        ticks: u64,
        #[serde(default)]
        delay_ms: u64,
    }

    fn default_recording_ticks() -> u64 {
        1
    }

    struct RecordingTask {
        id: String,
        params: RecordingParams,
    }

    impl RecordingTask {
        fn create(id: String, params: Value) -> Result<Box<dyn Task>> {
            let params = serde_json::from_value(params)
                .map_err(|e| EngineError::invalid_params(&id, e.to_string()))?;
            Ok(Box::new(Self { id, params }))
        }
    }

    #[async_trait]
    impl Task for RecordingTask {
        fn name(&self) -> &str {
            "RecordingTask"
        }

        async fn execute(&self, ctx: Arc<TaskContext>) -> Result<()> {
            record_event(format!("{}:start", self.params.label));

            for _ in 0..self.params.ticks {
                if !ctx.running().await {
                    record_event(format!("{}:stopped", self.params.label));
                    return Ok(());
                }

                if self.params.delay_ms > 0 {
                    tokio::time::sleep(Duration::from_millis(self.params.delay_ms)).await;
                }
            }

            if self.params.fail {
                record_event(format!("{}:fail", self.params.label));
                return Err(EngineError::task_execution(
                    self.id.clone(),
                    "forced failure",
                ));
            }

            record_event(format!("{}:done", self.params.label));
            Ok(())
        }
    }

    fn registry() -> TaskRegistry {
        TaskRegistry::with_builtins()
    }

    fn registry_with_recording() -> TaskRegistry {
        let mut r = registry();
        r.register("recording", RecordingTask::create);
        r
    }

    fn record_event(event: String) {
        JOB_TEST_EVENTS
            .get_or_init(|| Mutex::new(Vec::new()))
            .lock()
            .expect("job test event mutex poisoned")
            .push(event);
    }

    fn events_for(prefix: &str) -> Vec<String> {
        JOB_TEST_EVENTS
            .get_or_init(|| Mutex::new(Vec::new()))
            .lock()
            .expect("job test event mutex poisoned")
            .iter()
            .filter(|event| event.starts_with(prefix))
            .cloned()
            .collect()
    }

    fn recording_job(
        id: &str,
        on_error: JobErrorPolicy,
        task_id: &str,
        label: &str,
        fail: bool,
        delay_ms: u64,
    ) -> JobConfig {
        JobConfig {
            id: id.to_string(),
            name: None,
            description: None,
            on_error,
            resources: Vec::new(),
            artifacts: Vec::new(),
            tasks: vec![TaskConfig::new(
                task_id,
                "recording",
                json!({
                    "label": label,
                    "fail": fail,
                    "delay_ms": delay_ms
                }),
            )],
        }
    }

    async fn wait_for_status<F>(wf: &Workflow, predicate: F) -> WorkflowStatus
    where
        F: Fn(&WorkflowStatus) -> bool,
    {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            let status = wf
                .status
                .read()
                .expect("workflow status lock poisoned")
                .clone();
            if predicate(&status) {
                return status;
            }

            assert!(
                tokio::time::Instant::now() < deadline,
                "workflow did not reach expected status; last status: {:?}",
                status
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn wait_for_task_status(wf: &Workflow, task_id: &str, expected: &str) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            let status = wf
                .state()
                .into_iter()
                .find(|info| info.id == task_id)
                .and_then(|info| info.status);
            if status.as_deref() == Some(expected) {
                return;
            }

            assert!(
                tokio::time::Instant::now() < deadline,
                "task '{}' did not reach status '{}'; last status: {:?}",
                task_id,
                expected,
                status
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
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

    #[tokio::test]
    async fn test_one_job_workflow_runtime_lifecycle() {
        let r = registry();
        let job = JobConfig {
            id: "default".to_string(),
            name: None,
            description: None,
            on_error: JobErrorPolicy::Fail,
            resources: Vec::new(),
            artifacts: Vec::new(),
            tasks: vec![
                TaskConfig::new(
                    "gen",
                    "number_generator",
                    json!({
                        "min": 1,
                        "max": 10,
                        "interval_ms": 10
                    }),
                )
                .with_output("out", vec!["numbers".to_string()]),
                TaskConfig::new("log", "logger", json!({})).with_dependency("numbers"),
            ],
        };

        let mut wf = WorkflowBuilder::new("job_runtime_test", &r)
            .name("Job Runtime Test")
            .add_job(job)
            .build()
            .unwrap();

        assert_eq!(wf.jobs.len(), 1);
        assert_eq!(
            *wf.current_job_index
                .lock()
                .expect("current job mutex poisoned"),
            Some(0)
        );
        assert!(wf.active_job_runtime.is_some());
        assert_eq!(wf.info().task_count, 2);
        assert_eq!(wf.state().len(), 2);

        wf.start().await.unwrap();
        assert_eq!(
            *wf.status.read().expect("workflow status lock poisoned"),
            WorkflowStatus::Running
        );
        wait_for_task_status(&wf, "gen", "Running").await;

        tokio::time::sleep(tokio::time::Duration::from_millis(30)).await;

        wf.pause().await.unwrap();
        assert_eq!(
            *wf.status.read().expect("workflow status lock poisoned"),
            WorkflowStatus::Paused
        );
        wait_for_task_status(&wf, "gen", "Paused").await;

        wf.start().await.unwrap();
        assert_eq!(
            *wf.status.read().expect("workflow status lock poisoned"),
            WorkflowStatus::Running
        );
        wait_for_task_status(&wf, "gen", "Running").await;

        wf.stop().await.unwrap();
        assert_eq!(
            *wf.status.read().expect("workflow status lock poisoned"),
            WorkflowStatus::Stopped
        );
    }

    #[tokio::test]
    async fn test_sequential_jobs_start_after_previous_job_completes() {
        let r = registry_with_recording();
        let prefix = "seq_jobs";
        let mut wf = WorkflowBuilder::new("sequential_jobs", &r)
            .add_job(recording_job(
                "first",
                JobErrorPolicy::Fail,
                "first_task",
                &format!("{prefix}:first"),
                false,
                5,
            ))
            .add_job(recording_job(
                "second",
                JobErrorPolicy::Fail,
                "second_task",
                &format!("{prefix}:second"),
                false,
                5,
            ))
            .build()
            .unwrap();

        wf.start().await.unwrap();
        wait_for_status(&wf, |status| *status == WorkflowStatus::Completed).await;

        assert_eq!(
            events_for(prefix),
            vec![
                "seq_jobs:first:start",
                "seq_jobs:first:done",
                "seq_jobs:second:start",
                "seq_jobs:second:done"
            ]
        );

        let info = wf.info();
        assert_eq!(info.status, WorkflowStatus::Completed);
        assert_eq!(info.current_job, None);
        assert_eq!(info.completed_jobs, vec!["first", "second"]);
        assert!(info.failed_jobs.is_empty());
    }

    #[tokio::test]
    async fn test_failed_job_with_fail_policy_stops_workflow() {
        let r = registry_with_recording();
        let prefix = "fail_policy";
        let mut wf = WorkflowBuilder::new("fail_policy_jobs", &r)
            .add_job(recording_job(
                "first",
                JobErrorPolicy::Fail,
                "first_task",
                &format!("{prefix}:first"),
                true,
                5,
            ))
            .add_job(recording_job(
                "second",
                JobErrorPolicy::Fail,
                "second_task",
                &format!("{prefix}:second"),
                false,
                5,
            ))
            .build()
            .unwrap();

        wf.start().await.unwrap();
        let status =
            wait_for_status(&wf, |status| matches!(status, WorkflowStatus::Failed(_))).await;

        match status {
            WorkflowStatus::Failed(error) => {
                assert!(error.contains("job 'first' task 'first_task' failed"));
            }
            other => panic!("expected failed status, got {other:?}"),
        }

        assert_eq!(
            events_for(prefix),
            vec!["fail_policy:first:start", "fail_policy:first:fail"]
        );

        let info = wf.info();
        assert_eq!(info.current_job, Some("first".to_string()));
        assert!(info.completed_jobs.is_empty());
        assert_eq!(info.failed_jobs, vec!["first"]);
    }

    #[tokio::test]
    async fn test_failed_job_with_continue_policy_runs_next_job() {
        let r = registry_with_recording();
        let prefix = "continue_policy";
        let mut wf = WorkflowBuilder::new("continue_policy_jobs", &r)
            .add_job(recording_job(
                "first",
                JobErrorPolicy::Continue,
                "first_task",
                &format!("{prefix}:first"),
                true,
                5,
            ))
            .add_job(recording_job(
                "second",
                JobErrorPolicy::Fail,
                "second_task",
                &format!("{prefix}:second"),
                false,
                5,
            ))
            .build()
            .unwrap();

        wf.start().await.unwrap();
        wait_for_status(&wf, |status| *status == WorkflowStatus::Completed).await;

        assert_eq!(
            events_for(prefix),
            vec![
                "continue_policy:first:start",
                "continue_policy:first:fail",
                "continue_policy:second:start",
                "continue_policy:second:done"
            ]
        );

        let info = wf.info();
        assert_eq!(info.status, WorkflowStatus::Completed);
        assert_eq!(info.current_job, None);
        assert_eq!(info.completed_jobs, vec!["second"]);
        assert_eq!(info.failed_jobs, vec!["first"]);
    }

    #[tokio::test]
    async fn test_stop_stops_active_job_without_starting_next_job() {
        let r = registry_with_recording();
        let prefix = "stop_policy";
        let long_first = JobConfig {
            id: "first".to_string(),
            name: None,
            description: None,
            on_error: JobErrorPolicy::Fail,
            resources: Vec::new(),
            artifacts: Vec::new(),
            tasks: vec![TaskConfig::new(
                "first_task",
                "recording",
                json!({
                    "label": format!("{prefix}:first"),
                    "ticks": 100,
                    "delay_ms": 10
                }),
            )],
        };

        let mut wf = WorkflowBuilder::new("stop_active_job", &r)
            .add_job(long_first)
            .add_job(recording_job(
                "second",
                JobErrorPolicy::Fail,
                "second_task",
                &format!("{prefix}:second"),
                false,
                5,
            ))
            .build()
            .unwrap();

        wf.start().await.unwrap();
        wait_for_status(&wf, |status| *status == WorkflowStatus::Running).await;
        tokio::time::sleep(Duration::from_millis(30)).await;

        wf.stop().await.unwrap();
        wait_for_status(&wf, |status| *status == WorkflowStatus::Stopped).await;
        tokio::time::sleep(Duration::from_millis(30)).await;

        let events = events_for(prefix);
        assert!(events.contains(&"stop_policy:first:start".to_string()));
        assert!(!events.iter().any(|event| event.contains(":second:")));

        let info = wf.info();
        assert_eq!(info.current_job, Some("first".to_string()));
        assert!(info.completed_jobs.is_empty());
        assert!(info.failed_jobs.is_empty());
    }

    #[tokio::test]
    async fn test_one_job_workflow_loads_workflow_and_job_resources() {
        let dir = tempfile::tempdir().unwrap();
        let workflow_path = dir.path().join("workflow.json");
        let job_path = dir.path().join("job.txt");
        fs::write(&workflow_path, r#"{"scope":"workflow"}"#).unwrap();
        fs::write(&job_path, "job resource").unwrap();

        let r = registry();
        let job = JobConfig {
            id: "default".to_string(),
            name: None,
            description: None,
            on_error: JobErrorPolicy::Fail,
            resources: vec![file_resource("job_notes", job_path, ResourceFormat::Text)],
            artifacts: Vec::new(),
            tasks: vec![TaskConfig::new("task", "dummy", json!({}))],
        };

        let wf = WorkflowBuilder::new("resource_test", &r)
            .resources(vec![file_resource(
                "workflow_config",
                workflow_path,
                ResourceFormat::Json,
            )])
            .add_job(job)
            .build()
            .unwrap();

        let ctx = wf
            .active_job_runtime
            .as_ref()
            .unwrap()
            .contexts
            .get("task")
            .unwrap();

        match ctx.resource("workflow_config").unwrap() {
            crate::resource::ResourceValue::Json(value) => assert_eq!(value["scope"], "workflow"),
            _ => panic!("expected workflow json resource"),
        }
        match ctx.resource("job_notes").unwrap() {
            crate::resource::ResourceValue::Text(value) => {
                assert_eq!(value.as_str(), "job resource")
            }
            _ => panic!("expected job text resource"),
        }
    }

    #[tokio::test]
    async fn test_later_job_loads_resource_from_earlier_job_artifact() {
        let dir = tempfile::tempdir().unwrap();
        let artifact_path = dir.path().join("raw.csv");
        fs::write(&artifact_path, "id,value\n1,10\n").unwrap();

        let r = registry();
        let extract = JobConfig {
            id: "extract".to_string(),
            name: None,
            description: None,
            on_error: JobErrorPolicy::Fail,
            resources: Vec::new(),
            artifacts: vec![crate::cfg::ArtifactConfig {
                id: "raw_csv".to_string(),
                path: artifact_path.clone(),
                format: ResourceFormat::Csv,
            }],
            tasks: vec![TaskConfig::new("extract_task", "dummy", json!({}))],
        };
        let load = JobConfig {
            id: "load".to_string(),
            name: None,
            description: None,
            on_error: JobErrorPolicy::Fail,
            resources: vec![ResourceConfig {
                id: "raw".to_string(),
                scope: None,
                source: ResourceSource::Artifact {
                    artifact_ref: "raw_csv".to_string(),
                    format: ResourceFormat::Csv,
                },
            }],
            artifacts: Vec::new(),
            tasks: vec![TaskConfig {
                uses: vec!["raw".to_string()],
                ..TaskConfig::new("load_task", "dummy", json!({}))
            }],
        };

        let wf = WorkflowBuilder::new("artifact_resource_test", &r)
            .checkpoint(Some(WorkflowCheckpoint::new(
                "artifact_resource_test",
                WorkflowStatus::Stopped,
                Some("load".to_string()),
                vec!["extract".to_string()],
                Vec::new(),
            )))
            .add_job(extract)
            .add_job(load)
            .build()
            .unwrap();

        let ctx = wf
            .active_job_runtime
            .as_ref()
            .unwrap()
            .contexts
            .get("load_task")
            .unwrap();

        match ctx.resource("raw").unwrap() {
            crate::resource::ResourceValue::Csv(value) => {
                assert_eq!(value.path(), artifact_path.as_path())
            }
            _ => panic!("expected csv resource"),
        }
    }

    #[test]
    fn test_workflow_builder_rejects_missing_resource_reference() {
        let r = registry();
        let job = JobConfig {
            id: "default".to_string(),
            name: None,
            description: None,
            on_error: JobErrorPolicy::Fail,
            resources: Vec::new(),
            artifacts: Vec::new(),
            tasks: vec![TaskConfig {
                uses: vec!["missing".to_string()],
                ..TaskConfig::new("task", "dummy", json!({}))
            }],
        };

        let result = WorkflowBuilder::new("missing_resource", &r)
            .add_job(job)
            .build();
        let err = match result {
            Ok(_) => panic!("expected missing resource reference to fail"),
            Err(err) => err.to_string(),
        };

        assert!(err.contains("uses resource 'missing'"));
    }

    #[test]
    fn test_workflow_builder_fails_when_resource_file_is_missing() {
        let dir = tempfile::tempdir().unwrap();
        let r = registry();

        let result = WorkflowBuilder::new("missing_file", &r)
            .resources(vec![file_resource(
                "config",
                dir.path().join("missing.json"),
                ResourceFormat::Json,
            )])
            .add_task(TaskConfig::new("task", "dummy", json!({})))
            .build();
        let err = match result {
            Ok(_) => panic!("expected missing resource file to fail"),
            Err(err) => err.to_string(),
        };

        assert!(err.contains("failed to load resource 'config'"));
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

    #[tokio::test]
    async fn test_workflow_stop_and_restart() {
        let r = registry_with_recording();
        let mut wf = WorkflowBuilder::new("restart_test", &r)
            .name("Restart Test")
            .add_task(TaskConfig::new(
                "task1",
                "recording",
                serde_json::json!({
                    "label": "restart_test:task",
                    "ticks": 100,
                    "delay_ms": 10
                }),
            ))
            .build()
            .unwrap();

        // First start
        wf.start().await.unwrap();
        assert_eq!(
            *wf.status.read().expect("workflow status lock poisoned"),
            WorkflowStatus::Running
        );

        // Stop
        wf.stop().await.unwrap();
        assert_eq!(
            *wf.status.read().expect("workflow status lock poisoned"),
            WorkflowStatus::Stopped
        );

        // Wait a bit for runners to shut down
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // Restart — should rebuild and start successfully
        wf.start().await.unwrap();
        assert_eq!(
            *wf.status.read().expect("workflow status lock poisoned"),
            WorkflowStatus::Running
        );

        // Stop again to clean up
        wf.stop().await.unwrap();
        assert_eq!(
            *wf.status.read().expect("workflow status lock poisoned"),
            WorkflowStatus::Stopped
        );
    }

    fn file_resource(id: &str, path: std::path::PathBuf, format: ResourceFormat) -> ResourceConfig {
        ResourceConfig {
            id: id.to_string(),
            scope: None,
            source: ResourceSource::File { path, format },
        }
    }
}
