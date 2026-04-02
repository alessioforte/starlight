use crate::api;
use crate::ctx;
use crate::tui;
use tabled::{Table, Tabled, settings::Style};

#[derive(Tabled, Clone, Debug)]
#[tabled(rename_all = "UPPERCASE")]
pub struct TableContext {
    #[tabled(display("display_bool"))]
    current: bool,
    name: String,
    description: String,
    endpoint: String,
}

fn display_bool(value: &bool) -> String {
    if *value {
        "*".to_string()
    } else {
        "".to_string()
    }
}

// ---------------------------------------------------------------------------
// Workflow commands
// ---------------------------------------------------------------------------

pub async fn list_workflows() -> Result<(), anyhow::Error> {
    match api::get_workflows().await {
        Ok(workflows) => {
            if workflows.is_empty() {
                println!("No workflows loaded.");
                return Ok(());
            }
            let mut table = Table::new(workflows);
            table.with(Style::blank());
            println!("{}", table);
            Ok(())
        }
        Err(e) => {
            eprintln!("Error listing workflows: {}", e);
            Err(anyhow::anyhow!("Failed to list workflows"))
        }
    }
}

pub async fn list_all_workflows() -> Result<(), anyhow::Error> {
    match api::list_workflow_files().await {
        Ok(files) => {
            if files.is_empty() {
                println!("No workflow files found.");
                return Ok(());
            }
            let mut table = Table::new(files);
            table.with(Style::blank());
            println!("{}", table);
            Ok(())
        }
        Err(e) => {
            eprintln!("Error listing workflow files: {}", e);
            Err(anyhow::anyhow!("Failed to list workflow files"))
        }
    }
}

pub async fn push_workflow(file: &str) -> Result<(), anyhow::Error> {
    let path = std::path::Path::new(file);
    if !path.exists() {
        return Err(anyhow::anyhow!("File not found: {}", file));
    }

    let f = std::fs::File::open(path)?;
    let config: eng::Config = match path.extension().and_then(|e| e.to_str()) {
        Some("json") => serde_json::from_reader(f)?,
        Some("yaml" | "yml") => serde_yaml_bw::from_reader(f)?,
        _ => {
            return Err(anyhow::anyhow!(
                "Unsupported file format. Use .json or .yaml"
            ));
        }
    };

    match api::push_workflow_file(config).await {
        Ok(res) => {
            println!("{}", serde_json::to_string_pretty(&res)?);
            Ok(())
        }
        Err(e) => {
            eprintln!("Error pushing workflow: {}", e);
            Err(anyhow::anyhow!("Failed to push workflow"))
        }
    }
}

pub async fn pull_workflow(id: &str, output: Option<String>) -> Result<(), anyhow::Error> {
    match api::get_workflow_file(id).await {
        Ok(config) => {
            let format = output.as_deref().unwrap_or("yaml");
            match format {
                "json" => {
                    let content = serde_json::to_string_pretty(&config)?;
                    let filename = format!("{}.json", id);
                    std::fs::write(&filename, &content)?;
                    println!("Saved to {}", filename);
                }
                "yaml" | "yml" => {
                    let content = serde_yaml_bw::to_string(&config)?;
                    let filename = format!("{}.yaml", id);
                    std::fs::write(&filename, &content)?;
                    println!("Saved to {}", filename);
                }
                _ => {
                    return Err(anyhow::anyhow!("Unsupported format. Use 'json' or 'yaml'."));
                }
            }
            Ok(())
        }
        Err(e) => {
            eprintln!("Error pulling workflow {}: {}", id, e);
            Err(anyhow::anyhow!("Failed to pull workflow"))
        }
    }
}

pub async fn mount_workflow(id: &str) -> Result<(), anyhow::Error> {
    match api::mount_workflow(id).await {
        Ok(res) => {
            println!("{}", serde_json::to_string_pretty(&res)?);
            Ok(())
        }
        Err(e) => {
            eprintln!("Error mounting workflow {}: {}", id, e);
            Err(anyhow::anyhow!("Failed to mount workflow"))
        }
    }
}

pub async fn unmount_workflow(id: &str) -> Result<(), anyhow::Error> {
    match api::unmount_workflow(id).await {
        Ok(res) => {
            println!("{}", serde_json::to_string_pretty(&res)?);
            Ok(())
        }
        Err(e) => {
            eprintln!("Error unmounting workflow {}: {}", id, e);
            Err(anyhow::anyhow!("Failed to unmount workflow"))
        }
    }
}

pub async fn remove_workflow(id: &str) -> Result<(), anyhow::Error> {
    match api::delete_workflow_file(id).await {
        Ok(res) => {
            println!("{}", serde_json::to_string_pretty(&res)?);
            Ok(())
        }
        Err(e) => {
            eprintln!("Error removing workflow {}: {}", id, e);
            Err(anyhow::anyhow!("Failed to remove workflow"))
        }
    }
}

// ---------------------------------------------------------------------------
// Engine control
// ---------------------------------------------------------------------------

pub async fn run_workflow(id: &str) -> Result<(), anyhow::Error> {
    api::mount_workflow(id).await.map_err(|e| {
        eprintln!("Error mounting workflow {}: {}", id, e);
        anyhow::anyhow!("Failed to mount workflow")
    })?;
    api::send_command(id, "start").await.map_err(|e| {
        eprintln!("Error starting workflow {}: {}", id, e);
        anyhow::anyhow!("Failed to start workflow")
    })?;
    println!("Workflow {} is running.", id);
    Ok(())
}

pub async fn start_workflow(id: &str) -> Result<(), anyhow::Error> {
    match api::send_command(id, "start").await {
        Ok(_) => Ok(()),
        Err(e) => {
            eprintln!("Error starting workflow {}: {}", id, e);
            Err(anyhow::anyhow!("Failed to start workflow"))
        }
    }
}

pub async fn pause_workflow(id: &str) -> Result<(), anyhow::Error> {
    match api::send_command(id, "pause").await {
        Ok(_) => Ok(()),
        Err(e) => {
            eprintln!("Error pausing workflow {}: {}", id, e);
            Err(anyhow::anyhow!("Failed to pause workflow"))
        }
    }
}

pub async fn stop_workflow(id: &str) -> Result<(), anyhow::Error> {
    match api::send_command(id, "stop").await {
        Ok(_) => Ok(()),
        Err(e) => {
            eprintln!("Error stopping workflow {}: {}", id, e);
            Err(anyhow::anyhow!("Failed to stop workflow"))
        }
    }
}

pub async fn get_workflow_state(id: &str) -> Result<(), anyhow::Error> {
    match api::get_workflow_state(id).await {
        Ok(state) => {
            println!("\n{}\n", serde_json::to_string_pretty(&state)?);
            Ok(())
        }
        Err(e) => {
            eprintln!("Error getting state for workflow {}: {}", id, e);
            Err(anyhow::anyhow!("Failed to get workflow state"))
        }
    }
}

// ---------------------------------------------------------------------------
// Context management
// ---------------------------------------------------------------------------

pub async fn list_contexts() -> Result<(), anyhow::Error> {
    let current = match ctx::get_current_context() {
        Ok(ctx) => Some(ctx),
        Err(_) => None,
    };
    match ctx::get_contexts() {
        Ok(contexts) => {
            if contexts.is_empty() {
                println!("No contexts found.");
                return Ok(());
            }
            let list = contexts
                .into_iter()
                .map(|ctx| TableContext {
                    name: ctx.name.clone(),
                    description: ctx.description,
                    endpoint: ctx.endpoint,
                    current: current.as_ref().map_or(false, |c| c.name == ctx.name),
                })
                .collect::<Vec<_>>();
            let mut table = Table::new(list);
            table.with(Style::blank());
            println!("{}", table);
            Ok(())
        }
        Err(e) => {
            eprintln!("Error listing contexts: {}", e);
            Err(anyhow::anyhow!("Failed to list contexts"))
        }
    }
}

pub async fn set_context(name: &str) -> Result<(), anyhow::Error> {
    match ctx::set_current_context(name) {
        Ok(_) => {
            println!("Current context set to '{}'", name);
            Ok(())
        }
        Err(e) => {
            eprintln!("Error setting context '{}': {}", name, e);
            Err(anyhow::anyhow!("Failed to set context"))
        }
    }
}

// ---------------------------------------------------------------------------
// AI TUI
// ---------------------------------------------------------------------------

pub async fn generate() -> Result<(), anyhow::Error> {
    tui::run().await
}
