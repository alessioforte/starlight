//! Multi-Input Example
//!
//! This example demonstrates how tasks can receive from multiple input channels.
//! It creates a workflow where multiple number generators feed into a single logger,
//! showing that the logger receives messages from all upstream sources.
//!
//! Run with: cargo run --example multi_input

use serde_json::json;
use simple_engine::prelude::*;
use simple_engine::tasks::{Logger, NumberGenerator};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    log::info!("Multi-Input Workflow Example");
    log::info!("============================");

    // Build the workflow with multiple generators feeding into one logger
    let workflow = WorkflowBuilder::new("multi-input-example")
        .name("Multiple Generators to Single Logger")
        // Generator 1: Small numbers
        .add_task(
            TaskConfig::new(
                "generator_small",
                Box::new(NumberGenerator::create),
                json!({
                    "min": 1,
                    "max": 10,
                    "interval_ms": 300,
                    "count": 10,
                    "seed": 42
                }),
            )
            .with_output("out", vec!["small_numbers".to_string()]),
        )
        // Generator 2: Large numbers
        .add_task(
            TaskConfig::new(
                "generator_large",
                Box::new(NumberGenerator::create),
                json!({
                    "min": 100,
                    "max": 200,
                    "interval_ms": 500,
                    "count": 10,
                    "seed": 123
                }),
            )
            .with_output("out", vec!["large_numbers".to_string()]),
        )
        // Generator 3: Medium numbers
        .add_task(
            TaskConfig::new(
                "generator_medium",
                Box::new(NumberGenerator::create),
                json!({
                    "min": 50,
                    "max": 75,
                    "interval_ms": 400,
                    "count": 10,
                    "seed": 999
                }),
            )
            .with_output("out", vec!["medium_numbers".to_string()]),
        )
        // Logger: Receives from ALL three generators via merged input
        .add_task(
            TaskConfig::new(
                "logger",
                Box::new(Logger::create),
                json!({
                    "level": "info",
                    "prefix": "[MULTI-INPUT]",
                    "pretty": false
                }),
            )
            // This task depends on all three channel IDs
            .with_dependencies(vec![
                "small_numbers".to_string(),
                "large_numbers".to_string(),
                "medium_numbers".to_string(),
            ]),
        )
        .build()?;

    log::info!("Workflow built successfully!");
    log::info!("The logger task will receive numbers from all three generators.");
    log::info!("Starting workflow execution...\n");

    // Start the workflow
    workflow.start().await?;

    // Wait for completion or timeout after 10 seconds
    tokio::select! {
        result = workflow.wait() => {
            match result {
                Ok(_) => log::info!("\n✓ Workflow completed successfully!"),
                Err(e) => log::error!("\n✗ Workflow failed: {}", e),
            }
        }
        _ = tokio::time::sleep(tokio::time::Duration::from_secs(10)) => {
            log::info!("\n⏱ Workflow timeout - stopping...");
        }
    }

    log::info!("\nExample finished!");
    log::info!("Notice how the logger received messages from all three generators,");
    log::info!("merged into a single stream (values range from 1-10, 50-75, and 100-200).");

    Ok(())
}
