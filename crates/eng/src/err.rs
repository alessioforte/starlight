//! Error types for the simple workflow engine

use thiserror::Error;

/// Result type alias for the engine
pub type Result<T> = std::result::Result<T, EngineError>;

/// Main error type for the workflow engine
#[derive(Error, Debug)]
pub enum EngineError {
    #[error("Task error: {0}")]
    Task(#[from] TaskError),

    #[error("Workflow error: {0}")]
    Workflow(#[from] WorkflowError),

    #[error("Channel error: {0}")]
    Channel(#[from] ChannelError),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Other error: {0}")]
    Other(#[from] anyhow::Error),
}

/// Task-specific errors
#[derive(Error, Debug)]
pub enum TaskError {
    #[error("Task '{0}' not found")]
    NotFound(String),

    #[error("Task '{0}' failed to execute: {1}")]
    ExecutionFailed(String, String),

    #[error("Task '{0}' invalid parameters: {1}")]
    InvalidParams(String, String),

    #[error("Task '{0}' initialization failed: {1}")]
    InitializationFailed(String, String),

    #[error("Task '{0}' is in invalid state: {1}")]
    InvalidState(String, String),
}

/// Workflow-specific errors
#[derive(Error, Debug)]
pub enum WorkflowError {
    #[error("Workflow '{0}' not found")]
    NotFound(String),

    #[error("Workflow '{0}' already exists")]
    AlreadyExists(String),

    #[error("Workflow '{0}' invalid configuration: {1}")]
    InvalidConfig(String, String),

    #[error("Workflow '{0}' invalid state transition: {1}")]
    InvalidTransition(String, String),

    #[error("Workflow '{0}' has circular dependencies")]
    CircularDependency(String),

    #[error("Workflow '{0}' failed to start: {1}")]
    StartFailed(String, String),

    #[error("Workflow '{0}' failed to stop: {1}")]
    StopFailed(String, String),
}

/// Channel communication errors
#[derive(Error, Debug)]
pub enum ChannelError {
    #[error("Input channel '{0}' not found")]
    InputNotFound(String),

    #[error("Output channel '{0}' not found")]
    OutputNotFound(String),

    #[error("Channel '{0}' send failed: {1}")]
    SendFailed(String, String),

    #[error("Channel '{0}' receive failed: {1}")]
    RecvFailed(String, String),

    #[error("Channel '{0}' is closed")]
    Closed(String),

    #[error("All receivers dropped for channel '{0}'")]
    NoReceivers(String),
}

impl EngineError {
    /// Create a config error
    pub fn config(msg: impl Into<String>) -> Self {
        EngineError::Config(msg.into())
    }

    /// Create a task execution error
    pub fn task_execution(task_id: impl Into<String>, msg: impl Into<String>) -> Self {
        EngineError::Task(TaskError::ExecutionFailed(task_id.into(), msg.into()))
    }

    /// Create an invalid params error
    pub fn invalid_params(task_id: impl Into<String>, msg: impl Into<String>) -> Self {
        EngineError::Task(TaskError::InvalidParams(task_id.into(), msg.into()))
    }
}

/// Convert from task join errors
impl From<tokio::task::JoinError> for EngineError {
    fn from(err: tokio::task::JoinError) -> Self {
        EngineError::Other(anyhow::anyhow!("Task join error: {}", err))
    }
}
