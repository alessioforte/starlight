use crate::cfg::Config;
use crate::err::{EngineError, Result, WorkflowError};
// use crate::task::TaskInfo;
use crate::wf::{Workflow, WorkflowBuilder, WorkflowInfo};
use std::collections::HashMap;

pub struct Engine {
    workflows: HashMap<String, Workflow>,
}

impl Engine {
    pub fn new() -> Self {
        Engine {
            workflows: HashMap::new(),
        }
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
        let mut builder = WorkflowBuilder::new(config.id.clone()).name(config.name);

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

    // pub async fn info(&self, id: &str) -> Result<WorkflowInfo> {
    //     if let Some(workflow) = self.workflows.get(id) {
    //         let info = workflow.info().await;
    //         Ok(info)
    //     } else {
    //         Err(EngineError::Workflow(WorkflowError::NotFound(
    //             id.to_string(),
    //         )))
    //     }
    // }

    // pub fn state(&self, id: &str) -> Result<Vec<TaskInfo>> {
    //     if let Some(workflow) = self.workflows.get(id) {
    //         let state = workflow.state();
    //         Ok(state)
    //     } else {
    //         Err(EngineError::Workflow(WorkflowError::NotFound(
    //             id.to_string(),
    //         )))
    //     }
    // }
}
