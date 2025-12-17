//! Simple Workflow Example
//!
//! This example demonstrates how to build a workflow using the simple_engine.
//! It creates a pipeline that:
//! 1. Generates random numbers
//! 2. Maps them to a different format
//! 3. Logs the results to stdout
//!
//! Run with: cargo run --example simple_workflow

use serde_json::json;
use simple_engine::prelude::*;
use simple_engine::tasks::{JsonMapper, Logger, NumberGenerator};
use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    log::info!("Building workflow...");

    // Build the workflow
    let workflow = WorkflowBuilder::new("simple-example")
        .name("Simple Number Processing Pipeline")
        // Task 1: Generate random numbers (source task - no inputs)
        .add_task(
            TaskConfig::new(
                "generator",
                Box::new(NumberGenerator::create),
                json!({
                    "min": 1,
                    "max": 100,
                    "interval_ms": 500,
                    "count": 20
                }),
            )
            .with_output("out", vec!["generator_out".to_string()]),
        )
        // Task 2: Map the generated numbers (processing task)
        .add_task(
            TaskConfig::new(
                "mapper",
                Box::new(JsonMapper::create),
                json!({
                    "mappings": {
                        "number": "value",
                        "sequence": "index"
                    },
                    "pass_through": false
                }),
            )
            .with_dependency("generator_out")
            .with_output("out", vec!["mapper_out".to_string()]),
        )
        // Task 3: Log the results (sink task - no outputs)
        .add_task(
            TaskConfig::new(
                "logger",
                Box::new(Logger::create),
                json!({
                    "level": "info",
                    "prefix": "[RESULT]",
                    "pretty": false
                }),
            )
            .with_dependency("mapper_out"),
        )
        .build()?;

    log::info!("Workflow built successfully!");
    log::info!("Starting workflow execution...");

    // Start the workflow
    workflow.start().await?;

    // Wait for completion or timeout after 15 seconds
    tokio::select! {
        result = workflow.wait() => {
            match result {
                Ok(_) => log::info!("Workflow completed successfully!"),
                Err(e) => log::error!("Workflow failed: {}", e),
            }
        }
        _ = tokio::time::sleep(tokio::time::Duration::from_secs(15)) => {
            log::info!("Workflow timeout - stopping...");
        }
    }

    log::info!("Example finished!");
    Ok(())
}
