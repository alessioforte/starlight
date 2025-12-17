//! Simple Workflow Engine
//!
//! A clean, performant workflow engine for building data processing pipelines.
//!
//! # Features
//!
//! - Simple task API: developers only focus on business logic
//! - Type-safe channel abstractions
//! - Automatic lifecycle management
//! - Built-in error handling
//! - Pause/resume support
//!
//! # Example
//!
//! ```rust,no_run
//! use simple_engine::prelude::*;
//!
//! // Implement your task
//! #[async_trait]
//! impl Task for MyTask {
//!     async fn execute(&self, ctx: Arc<TaskContext>) -> Result<()> {
//!         let mut input = ctx.merged_input()?;
//!         let output = ctx.output("out")?;
//!
//!         while ctx.is_running() {
//!             let data = input.recv().await?;
//!             // Process data...
//!             output.send(result)?;
//!         }
//!         Ok(())
//!     }
//!
//!     fn name(&self) -> &str { "MyTask" }
//! }
//! ```

pub mod context;
pub mod error;
pub mod task;
pub mod tasks;
pub mod workflow;

/// Re-exports for convenience
pub mod prelude {
    pub use crate::context::{Input, Output, TaskContext};
    pub use crate::error::{EngineError, Result};
    pub use crate::task::{BaseTask, Command, Task};
    pub use crate::workflow::{TaskConfig, Workflow, WorkflowBuilder};
    pub use async_trait::async_trait;
    pub use std::sync::Arc;
}

#[cfg(test)]
mod tests {
    use super::*;
    use prelude::*;

    #[test]
    fn test_basic_workflow() {
        let result = WorkflowBuilder::new("test".to_string())
            .name("Test Workflow")
            .build();

        assert!(result.is_ok());
    }
}
