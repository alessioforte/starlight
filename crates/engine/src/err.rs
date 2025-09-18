use thiserror::Error;

#[derive(Error, Debug)]
pub enum WorkflowError {
    #[error("JSON parsing error: {0}")]
    JsonError(#[from] serde_json::Error),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("JsonPath error: {0}")]
    JsonPathError(String),

    #[error("Task not found: {0}")]
    TaskNotFound(String),

    #[error("Workflow not found")]
    WorkflowNotFound,
}
