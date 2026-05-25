use crate::err::Result;
use crate::wf::WorkflowStatus;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowCheckpoint {
    pub workflow_id: String,
    pub status: WorkflowStatus,
    pub current_job: Option<String>,
    pub completed_jobs: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub failed_jobs: Vec<String>,
    pub updated_at: String,
}

impl WorkflowCheckpoint {
    pub fn new(
        workflow_id: impl Into<String>,
        status: WorkflowStatus,
        current_job: Option<String>,
        completed_jobs: Vec<String>,
        failed_jobs: Vec<String>,
    ) -> Self {
        Self {
            workflow_id: workflow_id.into(),
            status,
            current_job,
            completed_jobs,
            failed_jobs,
            updated_at: Utc::now().to_rfc3339(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CheckpointStore {
    root: PathBuf,
}

impl CheckpointStore {
    pub fn from_engine_dir(engine_dir: impl Into<PathBuf>) -> Self {
        Self {
            root: engine_dir.into().join("checkpoints"),
        }
    }

    pub fn default_from_env() -> Self {
        let engine_dir =
            std::env::var("ENGINE_DIR").unwrap_or_else(|_| ".starlight/engine".to_string());
        Self::from_engine_dir(engine_dir)
    }

    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn workflow_path(&self, workflow_id: &str) -> PathBuf {
        self.root.join(workflow_id).join("workflow.json")
    }

    pub fn load_workflow(&self, workflow_id: &str) -> Result<Option<WorkflowCheckpoint>> {
        let path = self.workflow_path(workflow_id);
        if !path.exists() {
            return Ok(None);
        }

        let file = std::fs::File::open(path)?;
        let checkpoint = serde_json::from_reader(file)?;
        Ok(Some(checkpoint))
    }

    pub fn save_workflow(&self, checkpoint: &WorkflowCheckpoint) -> Result<()> {
        let path = self.workflow_path(&checkpoint.workflow_id);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let tmp_path = path.with_extension("json.tmp");
        let file = std::fs::File::create(&tmp_path)?;
        serde_json::to_writer_pretty(file, checkpoint)?;
        std::fs::rename(tmp_path, path)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checkpoint_path_uses_engine_dir_convention() {
        let dir = tempfile::tempdir().unwrap();
        let store = CheckpointStore::from_engine_dir(dir.path());

        assert_eq!(
            store.workflow_path("econometrics"),
            dir.path()
                .join("checkpoints")
                .join("econometrics")
                .join("workflow.json")
        );
    }

    #[test]
    fn save_and_load_workflow_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let store = CheckpointStore::new(dir.path());
        let checkpoint = WorkflowCheckpoint::new(
            "econometrics",
            WorkflowStatus::Stopped,
            Some("observations".to_string()),
            vec!["countries".to_string(), "indicators".to_string()],
            Vec::new(),
        );

        store.save_workflow(&checkpoint).unwrap();
        let loaded = store.load_workflow("econometrics").unwrap().unwrap();

        assert_eq!(loaded.workflow_id, "econometrics");
        assert_eq!(loaded.status, WorkflowStatus::Stopped);
        assert_eq!(loaded.current_job, Some("observations".to_string()));
        assert_eq!(loaded.completed_jobs, vec!["countries", "indicators"]);
        assert!(!loaded.updated_at.is_empty());
    }
}
