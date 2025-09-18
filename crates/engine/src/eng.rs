use super::cfg::Config;
use super::err::WorkflowError;
use super::tasks::Status;
use super::wf::Workflow;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct WorkflowInfo {
    pub id: String,
    pub name: Option<String>,
    pub description: Option<String>,
    pub status: Status,
}

pub struct Engine {
    workflows: HashMap<String, Workflow>,
}

impl Engine {
    pub fn new() -> Self {
        Engine {
            workflows: HashMap::new(),
        }
    }

    /// Returns a list of all workflows with their IDs, names, and descriptions
    pub fn list(&self) -> Vec<WorkflowInfo> {
        self.workflows
            .iter()
            .map(|(id, workflow)| WorkflowInfo {
                id: id.clone(),
                name: workflow.name.clone(),
                description: workflow.description.clone(),
                status: workflow.status.clone(),
            })
            .collect()
    }

    /// Returns a workflow by its ID, or None if it doesn't exist
    pub fn get(&self, id: &str) -> Option<&Workflow> {
        self.workflows.get(id)
    }

    /// Adds a new workflow to the engine
    /// The workflow is created from the provided configuration
    /// and is not loaded or executed immediately.
    pub fn add(&mut self, config: Config) -> Result<&Workflow, WorkflowError> {
        let id = config.id.clone();
        let workflow = Workflow::new(
            config.id.clone(),
            config.name,
            config.description,
            config.tasks,
        );
        self.workflows.insert(config.id, workflow);
        Ok(self.workflows.get(&id).unwrap())
    }

    /// Loads a workflow from the engine by its ID.
    /// This method initializes the workflow, preparing it for execution.
    pub fn load(&mut self, id: &str) -> Result<(), WorkflowError> {
        if let Some(workflow) = self.workflows.get_mut(id) {
            if matches!(workflow.status, Status::Idle | Status::Stopped) {
                workflow.load();
            }
            Ok(())
        } else {
            Err(WorkflowError::WorkflowNotFound)
        }
    }

    /// Executes a workflow by its ID.
    pub fn execute(&mut self, id: &str) -> Result<(), WorkflowError> {
        if let Some(workflow) = self.workflows.get_mut(id) {
            workflow.execute();
            Ok(())
        } else {
            Err(WorkflowError::WorkflowNotFound)
        }
    }

    /// Pauses a workflow by its ID.
    pub fn pause(&mut self, id: &str) -> Result<(), WorkflowError> {
        if let Some(workflow) = self.workflows.get_mut(id) {
            workflow.pause();
            Ok(())
        } else {
            Err(WorkflowError::WorkflowNotFound)
        }
    }

    /// Stops a workflow by its ID.
    pub fn stop(&mut self, id: &str) -> Result<(), WorkflowError> {
        if let Some(workflow) = self.workflows.get_mut(id) {
            workflow.stop();
            Ok(())
        } else {
            Err(WorkflowError::WorkflowNotFound)
        }
    }

    /// Returns information about a workflow by its ID.
    pub fn info(&self, id: &str) -> Result<WorkflowInfo, WorkflowError> {
        if let Some(workflow) = self.workflows.get(id) {
            Ok(WorkflowInfo {
                id: workflow.id.clone(),
                name: workflow.name.clone(),
                description: workflow.description.clone(),
                status: workflow.status.clone(),
            })
        } else {
            Err(WorkflowError::WorkflowNotFound)
        }
    }

    /// Displays the workflow by its ID.
    pub async fn display(&self, id: &str) -> Result<(), WorkflowError> {
        if let Some(workflow) = self.workflows.get(id) {
            workflow.display().await;
            Ok(())
        } else {
            Err(WorkflowError::WorkflowNotFound)
        }
    }
}
