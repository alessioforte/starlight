//! Workflow Status Example
//!
//! This example demonstrates how workflow status changes in response to commands.
//! It shows that the workflow status is properly updated when:
//! - Workflow is started (Idle -> Running)
//! - Workflow is paused (Running -> Paused)
//! - Workflow is resumed (Paused -> Running)
//! - Workflow is stopped (Running -> Stopped)
//!
//! Run with: cargo run --example workflow_status

use serde_json::json;
use simple_engine::prelude::*;
use simple_engine::tasks::{Logger, NumberGenerator};
use simple_engine::workflow::WorkflowStatus;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    log::info!("Workflow Status Tracking Example");
    log::info!("=================================\n");

    // Build a workflow
    let workflow = WorkflowBuilder::new("status-example")
        .name("Status Tracking Demo")
        .add_task(
            TaskConfig::new(
                "generator",
                Box::new(NumberGenerator::create),
                json!({
                    "min": 1,
                    "max": 100,
                    "interval_ms": 100,
                    "count": 200
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
                    "prefix": "[NUM]",
                    "pretty": false
                }),
            )
            .with_dependency("numbers"),
        )
        .build()?;

    log::info!("✓ Workflow built\n");

    // Check initial status
    let status = workflow.status().await;
    log::info!("Initial status: {:?}", status);
    assert_eq!(
        status,
        WorkflowStatus::Idle,
        "Workflow should start in Idle state"
    );

    // Start workflow
    log::info!("\n▶ Starting workflow...");
    workflow.start().await?;
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let status = workflow.status().await;
    log::info!("Status after start: {:?}", status);
    assert_eq!(
        status,
        WorkflowStatus::Running,
        "Workflow should be Running after start"
    );

    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Pause workflow
    log::info!("\n⏸ Pausing workflow...");
    workflow.pause().await?;
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let status = workflow.status().await;
    log::info!("Status after pause: {:?}", status);
    assert_eq!(
        status,
        WorkflowStatus::Paused,
        "Workflow should be Paused after pause command"
    );

    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Resume workflow
    log::info!("\n▶ Resuming workflow...");
    workflow.start().await?;
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let status = workflow.status().await;
    log::info!("Status after resume: {:?}", status);
    assert_eq!(
        status,
        WorkflowStatus::Running,
        "Workflow should be Running after resume"
    );

    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Stop workflow
    log::info!("\n⏹ Stopping workflow...");
    workflow.stop().await?;
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let status = workflow.status().await;
    log::info!("Status after stop: {:?}", status);
    assert_eq!(
        status,
        WorkflowStatus::Stopped,
        "Workflow should be Stopped after stop command"
    );

    // Wait for cleanup
    workflow.wait().await?;

    log::info!("\n✅ All status transitions verified successfully!");
    log::info!("\nStatus lifecycle:");
    log::info!("  1. Idle    → start()  → Running");
    log::info!("  2. Running → pause()  → Paused");
    log::info!("  3. Paused  → start()  → Running");
    log::info!("  4. Running → stop()   → Stopped");
    log::info!("\nNote: Once stopped, a workflow cannot be restarted.");
    log::info!("      Create a new workflow instance to run again.");

    Ok(())
}
