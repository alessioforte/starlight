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

pub async fn list_workflows() -> Result<(), anyhow::Error> {
    match api::get_workflows().await {
        Ok(workflows) => {
            if workflows.is_empty() {
                println!("No workflows found.");
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

// pub async fn run_workflow(id: &str) -> Result<(), anyhow::Error> {
//     match api::load_workflow(id).await {
//         Ok(_) => match api::send_command(id, "execute").await {
//             Ok(_) => Ok(()),
//             Err(e) => {
//                 eprintln!("Error executing workflow {}: {}", id, e);
//                 Err(anyhow::anyhow!("Failed to execute workflow"))
//             }
//         },
//         Err(e) => {
//             eprintln!("Error running workflow {}: {}", id, e);
//             Err(anyhow::anyhow!("Failed to run workflow"))
//         }
//     }
// }

// pub async fn load_workflow(id: &str) -> Result<(), anyhow::Error> {
//     match api::load_workflow(id).await {
//         Ok(_) => Ok(()),
//         Err(e) => {
//             eprintln!("Error loading workflow {}: {}", id, e);
//             Err(anyhow::anyhow!("Failed to load workflow"))
//         }
//     }
// }

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

pub async fn remove_workflow(id: &str) -> Result<(), anyhow::Error> {
    match api::remove_workflow(id).await {
        Ok(_) => Ok(()),
        Err(e) => {
            eprintln!("Error removing workflow {}: {}", id, e);
            Err(anyhow::anyhow!("Failed to remove workflow"))
        }
    }
}

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

pub async fn generate() -> Result<(), anyhow::Error> {
    tui::run().await
}

pub async fn get_workflow(id: &str, output: Option<String>) -> Result<(), anyhow::Error> {
    match api::get_workflow(id).await {
        Ok(config) => {
            match output.as_deref() {
                Some("json") => {
                    println!("\n{}\n", serde_json::to_string_pretty(&config)?);
                }
                Some("yaml") => {
                    let yaml = serde_yaml_bw::to_string(&config)?;
                    println!("\n{}\n", yaml);
                }
                _ => {
                    eprintln!("Unsupported output format. Use 'json' or 'yaml'.");
                    return Err(anyhow::anyhow!("Unsupported output format"));
                }
            }
            Ok(())
        }
        Err(e) => {
            eprintln!("Error getting workflow {}: {}", id, e);
            Err(anyhow::anyhow!("Failed to get workflow"))
        }
    }
}
