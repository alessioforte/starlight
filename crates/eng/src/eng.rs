use crate::cfg::{Config, JobConfig};
use crate::checkpoint::{CheckpointStore, WorkflowCheckpoint};
use crate::err::{EngineError, Result, WorkflowError};
use crate::tasks::{CreateFn, TaskRegistry};
use crate::wf::{Workflow, WorkflowBuilder, WorkflowInfo};
use std::collections::HashMap;

pub struct Engine {
    workflows: HashMap<String, Workflow>,
    registry: TaskRegistry,
    checkpoint_store: Option<CheckpointStore>,
}

impl Engine {
    /// Create a new engine with all built-in tasks registered.
    pub fn new() -> Self {
        Engine {
            workflows: HashMap::new(),
            registry: TaskRegistry::with_builtins(),
            checkpoint_store: Some(CheckpointStore::default_from_env()),
        }
    }

    /// Create a new engine with a custom task registry.
    pub fn with_registry(registry: TaskRegistry) -> Self {
        Engine {
            workflows: HashMap::new(),
            registry,
            checkpoint_store: Some(CheckpointStore::default_from_env()),
        }
    }

    pub fn with_checkpoint_dir(mut self, checkpoint_dir: impl Into<std::path::PathBuf>) -> Self {
        self.checkpoint_store = Some(CheckpointStore::new(checkpoint_dir));
        self
    }

    pub fn with_engine_dir(mut self, engine_dir: impl Into<std::path::PathBuf>) -> Self {
        self.checkpoint_store = Some(CheckpointStore::from_engine_dir(engine_dir));
        self
    }

    pub fn without_checkpoints(mut self) -> Self {
        self.checkpoint_store = None;
        self
    }

    /// Get a reference to the task registry.
    pub fn registry(&self) -> &TaskRegistry {
        &self.registry
    }

    /// Get a mutable reference to the task registry.
    ///
    /// Use this to register custom tasks before adding workflows.
    pub fn registry_mut(&mut self) -> &mut TaskRegistry {
        &mut self.registry
    }

    /// Register a custom task type on the engine.
    ///
    /// Shorthand for `engine.registry_mut().register(name, factory)`.
    pub fn register_task(&mut self, name: impl Into<String>, factory: CreateFn) -> &mut Self {
        self.registry.register(name, factory);
        self
    }

    pub fn list(&self) -> Vec<WorkflowInfo> {
        let mut workflows = Vec::new();
        for workflow in self.workflows.values() {
            let info = workflow.info();
            workflows.push(info);
        }
        workflows
    }

    pub fn get(&self, id: &str) -> Option<&Workflow> {
        self.workflows.get(id)
    }

    pub fn validate_config(&self, config: &Config) -> Result<()> {
        config.validate()?;

        for job in config.normalized_jobs()? {
            builder_from_job(config, &job, &self.registry).validate()?;
        }

        Ok(())
    }

    pub fn add(&mut self, config: Config) -> Result<&Workflow> {
        let id = config.id.clone();
        let checkpoint = self
            .checkpoint_store
            .as_ref()
            .map(|store| store.load_workflow(&id))
            .transpose()?
            .flatten();
        let workflow = builder_from_config(
            config,
            &self.registry,
            self.checkpoint_store.clone(),
            checkpoint,
        )?
        .build()?;
        self.workflows.insert(id.clone(), workflow);
        Ok(self.workflows.get(&id).unwrap())
    }

    pub fn remove(&mut self, id: &str) -> Result<()> {
        if let Some(workflow) = self.workflows.get_mut(id) {
            workflow.abort();
            self.workflows.remove(id);
            Ok(())
        } else {
            Err(EngineError::Workflow(WorkflowError::NotFound(
                id.to_string(),
            )))
        }
    }

    pub async fn start(&mut self, id: &str) -> Result<()> {
        if let Some(workflow) = self.workflows.get_mut(id) {
            workflow.start().await?;
            Ok(())
        } else {
            Err(EngineError::Workflow(WorkflowError::NotFound(
                id.to_string(),
            )))
        }
    }

    pub async fn pause(&mut self, id: &str) -> Result<()> {
        if let Some(workflow) = self.workflows.get_mut(id) {
            workflow.pause().await?;
            Ok(())
        } else {
            Err(EngineError::Workflow(WorkflowError::NotFound(
                id.to_string(),
            )))
        }
    }

    pub async fn stop(&mut self, id: &str) -> Result<()> {
        if let Some(workflow) = self.workflows.get_mut(id) {
            workflow.stop().await?;
            Ok(())
        } else {
            Err(EngineError::Workflow(WorkflowError::NotFound(
                id.to_string(),
            )))
        }
    }
}

fn builder_from_config<'r>(
    config: Config,
    registry: &'r crate::tasks::TaskRegistry,
    checkpoint_store: Option<CheckpointStore>,
    checkpoint: Option<WorkflowCheckpoint>,
) -> Result<WorkflowBuilder<'r>> {
    let jobs = config.runtime_jobs()?;
    let mut builder = WorkflowBuilder::new(config.id, registry)
        .name(config.name)
        .resources(config.resources)
        .checkpoint_store(checkpoint_store)
        .checkpoint(checkpoint);

    if let Some(desc) = config.description {
        builder = builder.description(desc);
    }

    if let Some(buffer_size) = config.channel_buffer_size {
        builder = builder.channel_capacity(buffer_size);
    }

    for job in jobs {
        builder = builder.add_job(job);
    }

    Ok(builder)
}

fn builder_from_job<'r>(
    config: &Config,
    job: &JobConfig,
    registry: &'r crate::tasks::TaskRegistry,
) -> WorkflowBuilder<'r> {
    let builder_id = if config.jobs.is_empty() {
        config.id.clone()
    } else {
        format!("{}:{}", config.id, job.id)
    };

    let mut builder =
        WorkflowBuilder::new(builder_id, registry).resources(config.resources.clone());

    if let Some(buffer_size) = config.channel_buffer_size {
        builder = builder.channel_capacity(buffer_size);
    }

    builder.add_job(job.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cfg::TaskConfig;
    use crate::checkpoint::{CheckpointStore, WorkflowCheckpoint};
    use crate::wf::WorkflowStatus;
    use serde_json::json;
    use std::time::Duration;

    fn two_job_config(id: &str) -> Config {
        serde_json::from_value(json!({
            "id": id,
            "name": "ETL",
            "jobs": [
                {
                    "id": "extract",
                    "tasks": [
                        {
                            "id": "extract_task",
                            "type": "dummy",
                            "params": {},
                            "dependencies": [],
                            "outputs": {}
                        }
                    ]
                },
                {
                    "id": "load",
                    "tasks": [
                        {
                            "id": "load_task",
                            "type": "dummy",
                            "params": {},
                            "dependencies": [],
                            "outputs": {}
                        }
                    ]
                }
            ]
        }))
        .unwrap()
    }

    async fn wait_for_engine_status<F>(engine: &Engine, id: &str, predicate: F) -> WorkflowStatus
    where
        F: Fn(&WorkflowStatus) -> bool,
    {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            let status = engine.get(id).unwrap().info().status;
            if predicate(&status) {
                return status;
            }

            assert!(
                tokio::time::Instant::now() < deadline,
                "workflow '{}' did not reach expected status; last status: {:?}",
                id,
                status
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[test]
    fn validate_config_does_not_add_workflow() {
        let engine = Engine::new();
        let config = Config {
            id: "valid".into(),
            name: "Valid".into(),
            description: None,
            channel_buffer_size: None,
            resources: vec![],
            tasks: vec![TaskConfig::new("task1", "dummy", json!({}))],
            jobs: vec![],
        };

        engine.validate_config(&config).unwrap();

        assert!(engine.get("valid").is_none());
    }

    #[test]
    fn validate_config_rejects_invalid_task_params() {
        let engine = Engine::new();
        let config = Config {
            id: "invalid_params".into(),
            name: "Invalid Params".into(),
            description: None,
            channel_buffer_size: None,
            resources: vec![],
            tasks: vec![TaskConfig::new(
                "gen",
                "number_generator",
                json!({ "min": 1 }),
            )],
            jobs: vec![],
        };

        let err = engine.validate_config(&config).unwrap_err().to_string();

        assert!(err.contains("Failed to create task 'gen'"));
        assert!(err.contains("missing field `max`"));
    }

    #[test]
    fn validate_config_accepts_multi_job_shape() {
        let engine = Engine::new();
        let config: Config = serde_json::from_value(json!({
            "id": "etl",
            "name": "ETL",
            "jobs": [
                { "id": "extract", "tasks": [] },
                { "id": "load", "tasks": [] }
            ]
        }))
        .unwrap();

        engine.validate_config(&config).unwrap();
    }

    #[test]
    fn validate_config_accepts_visible_task_resource_uses() {
        let engine = Engine::new();
        let config: Config = serde_json::from_value(json!({
            "id": "resource_uses",
            "name": "Resource Uses",
            "resources": [
                {
                    "id": "aliases",
                    "source": {
                        "type": "file",
                        "path": "does/not/need/to/exist/for/validation.json",
                        "format": "json"
                    }
                }
            ],
            "tasks": [
                {
                    "id": "task",
                    "type": "dummy",
                    "params": {},
                    "uses": ["aliases"],
                    "dependencies": [],
                    "outputs": {}
                }
            ]
        }))
        .unwrap();

        engine.validate_config(&config).unwrap();
    }

    #[tokio::test]
    async fn add_accepts_multi_job_runtime() {
        let mut engine = Engine::new();
        let config: Config = serde_json::from_value(json!({
            "id": "etl",
            "name": "ETL",
            "jobs": [
                { "id": "extract", "tasks": [] },
                { "id": "load", "tasks": [] }
            ]
        }))
        .unwrap();

        engine.add(config).unwrap();
        assert!(engine.get("etl").is_some());
    }

    #[tokio::test]
    async fn add_loads_checkpoint_and_resumes_first_incomplete_job() {
        let dir = tempfile::tempdir().unwrap();
        let checkpoint_dir = dir.path().join("checkpoints");
        let store = CheckpointStore::new(&checkpoint_dir);
        store
            .save_workflow(&WorkflowCheckpoint::new(
                "etl",
                WorkflowStatus::Stopped,
                Some("load".to_string()),
                vec!["extract".to_string()],
                Vec::new(),
            ))
            .unwrap();

        let mut engine = Engine::new().with_checkpoint_dir(&checkpoint_dir);
        engine.add(two_job_config("etl")).unwrap();

        let info = engine.get("etl").unwrap().info();
        assert_eq!(info.status, WorkflowStatus::Stopped);
        assert_eq!(info.current_job, Some("load".to_string()));
        assert_eq!(info.completed_jobs, vec!["extract"]);

        engine.start("etl").await.unwrap();
        wait_for_engine_status(&engine, "etl", |status| {
            *status == WorkflowStatus::Completed
        })
        .await;

        let checkpoint = store.load_workflow("etl").unwrap().unwrap();
        assert_eq!(checkpoint.status, WorkflowStatus::Completed);
        assert_eq!(checkpoint.current_job, None);
        assert_eq!(checkpoint.completed_jobs, vec!["extract", "load"]);
    }

    #[tokio::test]
    async fn workflow_progress_is_persisted_after_completion() {
        let dir = tempfile::tempdir().unwrap();
        let checkpoint_dir = dir.path().join("checkpoints");
        let store = CheckpointStore::new(&checkpoint_dir);
        let mut engine = Engine::new().with_checkpoint_dir(&checkpoint_dir);

        engine.add(two_job_config("persisted")).unwrap();
        engine.start("persisted").await.unwrap();
        wait_for_engine_status(&engine, "persisted", |status| {
            *status == WorkflowStatus::Completed
        })
        .await;

        let checkpoint = store.load_workflow("persisted").unwrap().unwrap();
        assert_eq!(checkpoint.workflow_id, "persisted");
        assert_eq!(checkpoint.status, WorkflowStatus::Completed);
        assert_eq!(checkpoint.current_job, None);
        assert_eq!(checkpoint.completed_jobs, vec!["extract", "load"]);
        assert!(checkpoint.failed_jobs.is_empty());
        assert!(!checkpoint.updated_at.is_empty());
    }
}
