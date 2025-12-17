//! Workflow Control Example
//!
//! This example demonstrates how to control a workflow during execution:
//! - Start a workflow with long-running tasks
//! - Pause the workflow while tasks are executing
//! - Resume the workflow after pause
//! - Stop the workflow before completion
//!
//! Run with: cargo run --example workflow_control

use serde_json::json;
use simple_engine::prelude::*;
use simple_engine::tasks::{Logger, NumberGenerator};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    log::info!("Workflow Control Example");
    log::info!("========================");
    log::info!("This example demonstrates pause and stop commands during execution.\n");

    // Build a workflow with a long-running generator
    let workflow = WorkflowBuilder::new("control-example")
        .name("Workflow Control Demo")
        .add_task(
            TaskConfig::new(
                "generator",
                Box::new(NumberGenerator::create),
                json!({
                    "min": 1,
                    "max": 100,
                    "interval_ms": 200,
                    "count": 100  // Will take ~20 seconds to complete
                }),
            )
            .with_output("out", vec!["numbers".to_string()]),
        )
        .add_task(
            TaskConfig::new(
                "logger",
                Box::new(Logger::create),
                json!({
                    "level": "info",
                    "prefix": "[NUMBER]",
                    "pretty": false
                }),
            )
            .with_dependency("numbers"),
        )
        .build()?;

    log::info!("✓ Workflow built successfully\n");

    // Start the workflow
    log::info!("▶ Starting workflow...");
    workflow.start().await?;
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Pause after 2 seconds
    log::info!("\n⏸ Pausing workflow after 2 seconds...");
    workflow.pause().await?;
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    log::info!("✓ Workflow paused - tasks should stop processing\n");

    // Resume after 2 seconds
    log::info!("▶ Resuming workflow...");
    workflow.start().await?;
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    log::info!("✓ Workflow resumed - tasks should continue processing\n");

    // Stop after 2 more seconds
    log::info!("⏹ Stopping workflow before completion...");
    workflow.stop().await?;
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    log::info!("✓ Workflow stopped\n");

    // Wait for cleanup
    workflow.wait().await?;

    log::info!("\n✓ Example complete!");
    log::info!("Summary:");
    log::info!("  - Workflow started and ran for ~2 seconds");
    log::info!("  - Workflow paused (tasks stopped processing)");
    log::info!("  - Workflow resumed (tasks continued processing)");
    log::info!("  - Workflow stopped before completing all 100 numbers");
    log::info!("  - Stop command was received during task execution");

    Ok(())
}
