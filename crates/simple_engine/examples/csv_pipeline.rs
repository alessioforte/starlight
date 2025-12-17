//! CSV Processing Pipeline Example
//!
//! This example demonstrates a complete CSV processing workflow:
//! 1. Reads data from a CSV file
//! 2. Transforms the data using JSON mapping
//! 3. Writes the results to a new CSV file
//! 4. Also logs the data to stdout
//!
//! Run with: cargo run --example csv_pipeline

use serde_json::json;
use simple_engine::prelude::*;
use simple_engine::tasks::{CsvReader, CsvWriter, JsonMapper, Logger};
use std::io::Write;
use std::path::Path;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    log::info!("CSV Pipeline Example");
    log::info!("===================");

    // Create sample input CSV file
    let input_file = "input_data.csv";
    let output_file = "output_data.csv";

    create_sample_csv(input_file)?;
    log::info!("Created sample input file: {}", input_file);

    // Build the workflow
    let workflow = WorkflowBuilder::new("csv-pipeline")
        .name("CSV Processing Pipeline")
        .channel_capacity(100) // Smaller buffer for demo
        // Task 1: Read from CSV file
        .add_task(
            TaskConfig::new(
                "csv_reader",
                Box::new(CsvReader::create),
                json!({
                    "filename": input_file,
                    "delimiter": ",",
                    "interval_ms": 200  // Slow down for demo
                }),
            )
            .with_output("out", vec!["csv_reader_out".to_string()]),
        )
        // Task 2: Transform the data
        .add_task(
            TaskConfig::new(
                "transformer",
                Box::new(JsonMapper::create),
                json!({
                    "mappings": {
                        "employee_id": "id",
                        "full_name": "name",
                        "salary": "salary",
                        "dept": "department"
                    },
                    "pass_through": false
                }),
            )
            .with_dependency("csv_reader_out")
            .with_output("out", vec!["transformer_out".to_string()]),
        )
        // Task 3: Write to output CSV
        .add_task(
            TaskConfig::new(
                "csv_writer",
                Box::new(CsvWriter::create),
                json!({
                    "filename": output_file,
                    "delimiter": ",",
                    "append": false
                }),
            )
            .with_dependency("transformer_out"),
        )
        // Task 4: Also log the transformed data
        .add_task(
            TaskConfig::new(
                "logger",
                Box::new(Logger::create),
                json!({
                    "level": "info",
                    "prefix": "[TRANSFORMED]",
                    "pretty": false
                }),
            )
            .with_dependency("transformer_out"),
        )
        .build()?;

    log::info!(
        "Workflow built with {} tasks",
        workflow.info().await.task_count
    );
    log::info!("Starting workflow...");

    // Start the workflow
    workflow.start().await?;

    // Wait for completion
    match workflow.wait().await {
        Ok(_) => {
            log::info!("✓ Workflow completed successfully!");
            log::info!("Output written to: {}", output_file);

            // Show output file contents
            if Path::new(output_file).exists() {
                log::info!("\nOutput file contents:");
                if let Ok(contents) = std::fs::read_to_string(output_file) {
                    for line in contents.lines().take(5) {
                        log::info!("  {}", line);
                    }
                }
            }
        }
        Err(e) => {
            log::error!("✗ Workflow failed: {}", e);
        }
    }

    // Cleanup
    cleanup_files(&[input_file, output_file])?;
    log::info!("\nExample finished!");

    Ok(())
}

/// Create a sample CSV file for demonstration
fn create_sample_csv(filename: &str) -> Result<()> {
    let mut file = std::fs::File::create(filename)
        .map_err(|e| EngineError::config(format!("Failed to create sample file: {}", e)))?;

    writeln!(file, "id,name,department,salary").map_err(|e| EngineError::config(e.to_string()))?;
    writeln!(file, "1,Alice Smith,Engineering,75000")
        .map_err(|e| EngineError::config(e.to_string()))?;
    writeln!(file, "2,Bob Johnson,Marketing,65000")
        .map_err(|e| EngineError::config(e.to_string()))?;
    writeln!(file, "3,Carol Williams,Engineering,80000")
        .map_err(|e| EngineError::config(e.to_string()))?;
    writeln!(file, "4,David Brown,Sales,70000").map_err(|e| EngineError::config(e.to_string()))?;
    writeln!(file, "5,Eve Davis,Engineering,85000")
        .map_err(|e| EngineError::config(e.to_string()))?;
    writeln!(file, "6,Frank Miller,Marketing,68000")
        .map_err(|e| EngineError::config(e.to_string()))?;
    writeln!(file, "7,Grace Wilson,Sales,72000").map_err(|e| EngineError::config(e.to_string()))?;
    writeln!(file, "8,Henry Moore,Engineering,78000")
        .map_err(|e| EngineError::config(e.to_string()))?;
    writeln!(file, "9,Iris Taylor,Marketing,66000")
        .map_err(|e| EngineError::config(e.to_string()))?;
    writeln!(file, "10,Jack Anderson,Sales,73000")
        .map_err(|e| EngineError::config(e.to_string()))?;

    Ok(())
}

/// Cleanup temporary files
fn cleanup_files(files: &[&str]) -> Result<()> {
    for file in files {
        if Path::new(file).exists() {
            std::fs::remove_file(file)
                .map_err(|e| EngineError::config(format!("Failed to cleanup {}: {}", file, e)))?;
            log::info!("Cleaned up: {}", file);
        }
    }
    Ok(())
}
