use crate::cfg::Config;
use crate::err::{EngineError, Result, WorkflowError};
use crate::tasks::{CreateFn, TaskRegistry};
use crate::wf::{Workflow, WorkflowBuilder, WorkflowInfo};
use std::collections::HashMap;

pub struct Engine {
    workflows: HashMap<String, Workflow>,
    registry: TaskRegistry,
}

impl Engine {
    /// Create a new engine with all built-in tasks registered.
    pub fn new() -> Self {
        Engine {
            workflows: HashMap::new(),
            registry: TaskRegistry::with_builtins(),
        }
    }

    /// Create a new engine with a custom task registry.
    pub fn with_registry(registry: TaskRegistry) -> Self {
        Engine {
            workflows: HashMap::new(),
            registry,
        }
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

    pub async fn list(&self) -> Vec<WorkflowInfo> {
        let mut workflows = Vec::new();
        for workflow in self.workflows.values() {
            let info = workflow.info().await;
            workflows.push(info);
        }
        workflows
    }

    pub fn get(&self, id: &str) -> Option<&Workflow> {
        self.workflows.get(id)
    }

    pub fn add(&mut self, config: Config) -> Result<&Workflow> {
        let mut builder = WorkflowBuilder::new(config.id.clone(), &self.registry)
            .name(config.name);

        if let Some(desc) = config.description {
            builder = builder.description(desc);
        }

        if let Some(buffer_size) = config.channel_buffer_size {
            builder = builder.channel_capacity(buffer_size);
        }

        for task in config.tasks {
            builder = builder.add_task(task);
        }
        let workflow = builder.build()?;
        self.workflows.insert(config.id.clone(), workflow);
        Ok(self.workflows.get(&config.id).unwrap())
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
