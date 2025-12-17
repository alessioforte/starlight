//! Workflow Restart Behavior Example
//!
//! This example demonstrates the restart behavior of workflows:
//! - Paused workflows CAN be restarted with start()
//! - Stopped workflows CANNOT be restarted (must create new instance)
//!
//! Run with: cargo run --example workflow_restart

use serde_json::json;
use simple_engine::prelude::*;
use simple_engine::tasks::{Logger, NumberGenerator};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    log::info!("Workflow Restart Behavior Example");
    log::info!("==================================\n");

    // Build a workflow
    let workflow = WorkflowBuilder::new("restart-example")
        .name("Restart Behavior Demo")
        .add_task(
            TaskConfig::new(
                "generator",
                Box::new(NumberGenerator::create),
                json!({
                    "min": 1,
                    "max": 100,
                    "interval_ms": 200,
                    "count": 50
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

    // Scenario 1: Pause and Resume
    log::info!("=== Scenario 1: Pause and Resume ===");
    log::info!("▶ Starting workflow...");
    workflow.start().await?;
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    log::info!("⏸ Pausing workflow...");
    workflow.pause().await?;
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    log::info!("▶ Resuming workflow (this works!)...");
    let can_restart = workflow.can_restart().await;
    log::info!("Can restart? {}", can_restart);
    assert!(can_restart, "Paused workflow should be restartable");

    match workflow.start().await {
        Ok(_) => log::info!("✅ Successfully resumed after pause\n"),
        Err(e) => {
            log::error!("❌ Failed to resume: {}", e);
            return Err(e);
        }
    }

    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Scenario 2: Stop and Attempt Restart
    log::info!("=== Scenario 2: Stop and Attempt Restart ===");
    log::info!("⏹ Stopping workflow permanently...");
    workflow.stop().await?;
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    let can_restart = workflow.can_restart().await;
    log::info!("Can restart after stop? {}", can_restart);
    assert!(!can_restart, "Stopped workflow should NOT be restartable");

    log::info!("▶ Attempting to restart stopped workflow...");
    match workflow.start().await {
        Ok(_) => {
            log::error!("❌ Should not have been able to restart!");
            panic!("Stopped workflow should not be restartable");
        }
        Err(e) => {
            log::info!("✅ Correctly prevented restart: {}", e);
            log::info!("   (This is expected behavior)\n");
        }
    }

    // Wait for cleanup
    workflow.wait().await?;

    log::info!("=== Summary ===");
    log::info!("✓ Paused workflows CAN be resumed with start()");
    log::info!("✓ Stopped workflows CANNOT be restarted");
    log::info!("\n💡 Best Practices:");
    log::info!("   • Use pause() if you need to resume later");
    log::info!("   • Use stop() only when permanently done");
    log::info!("   • Create a new workflow instance to run again after stop");
    log::info!("\n📝 Why this limitation?");
    log::info!("   When a workflow is stopped, all task execution threads exit");
    log::info!("   completely. Restarting would require spawning new tasks, which");
    log::info!("   is equivalent to building a new workflow. For clarity and");
    log::info!("   resource management, we require explicit workflow creation.");

    Ok(())
}
