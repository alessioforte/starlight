mod cfg;
mod ctx;
mod eng;
mod err;
mod metrics;
mod task;
mod tasks;
mod wf;

pub use cfg::Config;
pub use ctx::{Input, Output, TaskContext};
pub use eng::Engine;
pub use err::{EngineError, Result, TaskError, WorkflowError};
pub use metrics::{CoarseClock, MetricsSnapshot, TaskMetrics};
pub use task::{BaseTask, Command, Task, TaskInfo};
pub use wf::{Workflow, WorkflowInfo};

pub mod prelude {
    pub use crate::cfg::Config;
    pub use crate::ctx::{Input, Output, TaskContext};
    pub use crate::eng::Engine;
    pub use crate::err::{EngineError, Result, TaskError, WorkflowError};
    pub use crate::metrics::{CoarseClock, MetricsSnapshot, TaskMetrics};
    pub use crate::task::{BaseTask, Command, Task, TaskInfo};
    pub use crate::wf::{Workflow, WorkflowInfo};
    pub use async_trait::async_trait;
    pub use std::sync::Arc;
}

// =^.^=
// A workflow engine in Rust 🦀
