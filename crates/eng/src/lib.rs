mod cfg;
mod checkpoint;
mod ctx;
mod eng;
mod err;
mod metrics;
mod msg;
mod resource;
mod task;
mod tasks;
mod wf;

pub use cfg::{
    ArtifactConfig, Config, JobConfig, JobErrorPolicy, ResourceCacheConfig, ResourceCachePolicy,
    ResourceConfig, ResourceFormat, ResourceScope, ResourceSource, RetryConfig, TaskConfig,
};
pub use checkpoint::{CheckpointStore, WorkflowCheckpoint};
pub use ctx::{Input, Output, TaskContext};
pub use eng::Engine;
pub use err::{EngineError, Result, TaskError, WorkflowError};
pub use metrics::{CoarseClock, MetricsSnapshot, TaskMetrics};
pub use msg::Msg;
pub use resource::{
    ArtifactMap, CsvResource, ResourceMap, ResourceValue, load_resources,
    load_resources_with_artifacts,
};
pub use task::{BaseTask, Command, Task, TaskInfo, TaskStatus};
pub use tasks::{CreateFn, TaskRegistry};
pub use wf::{JobOutcome, Workflow, WorkflowBuilder, WorkflowInfo};

pub mod prelude {
    pub use crate::cfg::{
        ArtifactConfig, Config, JobConfig, JobErrorPolicy, ResourceCacheConfig,
        ResourceCachePolicy, ResourceConfig, ResourceFormat, ResourceScope, ResourceSource,
        RetryConfig, TaskConfig,
    };
    pub use crate::checkpoint::{CheckpointStore, WorkflowCheckpoint};
    pub use crate::ctx::{Input, Output, TaskContext};
    pub use crate::eng::Engine;
    pub use crate::err::{EngineError, Result, TaskError, WorkflowError};
    pub use crate::metrics::{CoarseClock, MetricsSnapshot, TaskMetrics};
    pub use crate::msg::Msg;
    pub use crate::resource::{
        ArtifactMap, CsvResource, ResourceMap, ResourceValue, load_resources,
        load_resources_with_artifacts,
    };
    pub use crate::task::{BaseTask, Command, Task, TaskInfo, TaskStatus};
    pub use crate::tasks::{CreateFn, TaskRegistry};
    pub use crate::wf::{JobOutcome, Workflow, WorkflowBuilder, WorkflowInfo};
    pub use async_trait::async_trait;
    pub use std::sync::Arc;
}

// =^.^=
// A workflow engine in Rust 🦀
