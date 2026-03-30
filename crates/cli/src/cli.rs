use crate::cmd;
use clap::{Parser, Subcommand};

#[derive(Parser)]
struct EngineCli {
    #[command(subcommand)]
    pub command: EngineCommands,
}

#[derive(Subcommand)]
enum EngineCommands {
    /// Start the engine
    Start,

    /// Stop the engine
    Stop,

    /// Show the status of the engine
    Status,
}

#[derive(Parser)]
struct ConfigCli {
    #[command(subcommand)]
    pub command: ConfigCommands,
}

#[derive(Subcommand)]
enum ConfigCommands {
    GetContext {
        /// Get the current context
        #[arg(short, long)]
        current: bool,
    },
    SetContext {
        /// Set the current context by name
        #[arg(short, long)]
        name: String,
    },
    GetContexts,
}

#[derive(Subcommand)]
enum Commands {
    /// Configuration management commands
    #[command(name = "config", subcommand_help_heading = "Config Commands")]
    Config(ConfigCli),

    /// Engine management commands
    #[command(name = "engine", subcommand_help_heading = "Engine Commands")]
    Engine(EngineCli),

    /// Load a single workflow file or a directory
    Push {
        #[arg(short, long)]
        file: Option<String>,

        #[arg(short, long)]
        dir: Option<String>,
    },

    /// List all loaded workflows
    #[clap(alias = "ls")]
    List,

    /// Run a workflow
    Run { id: String },

    /// Start a workflow
    Start { id: String },

    /// Pause a workflow
    Pause { id: String },

    /// Stop a workflow
    Stop { id: String },

    /// Delete a workflow
    #[clap(alias = "rm")]
    Remove { id: String },
    /// Show status of a workflow or all
    Status { id: Option<String> },

    /// Get the state of a workflow
    State { id: String },

    /// Describe a workflow
    Describe { id: String },

    /// Get details of a workflow
    Get {
        id: String,
        /// Output format, e.g., yaml, json
        #[arg(short, long, default_value = "yaml")]
        output: Option<String>,
    },

    /// Generate a workflow interactively with AI
    Generate,
}

#[derive(Parser)]
#[command(name = "sl", about = "CLI for managing workflows", long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    command: Commands,
}

impl Cli {
    pub async fn run(self) -> anyhow::Result<()> {
        match self.command {
            // Engine
            Commands::Engine(engine_cli) => match engine_cli.command {
                EngineCommands::Start => {
                    // Handle engine start command
                    println!("Starting the engine...");
                    Ok(())
                }
                EngineCommands::Stop => {
                    // Handle engine stop command
                    println!("Stopping the engine...");
                    Ok(())
                }
                EngineCommands::Status => {
                    // Handle engine status command
                    println!("Checking engine status...");
                    Ok(())
                }
            },

            // Config
            Commands::Config(config_cli) => match config_cli.command {
                ConfigCommands::GetContext { current } => {
                    if current {
                        // Get the current context
                        println!("Getting current context...");
                        // Here you would implement the logic to get the current context
                        Ok(())
                    } else {
                        // Get all contexts
                        println!("Getting all contexts...");
                        // Here you would implement the logic to get all contexts
                        Ok(())
                    }
                }
                ConfigCommands::SetContext { name } => {
                    // Set the current context by name
                    println!("Setting context to {}", name);
                    // Here you would implement the logic to set the context
                    Ok(())
                }
                ConfigCommands::GetContexts => cmd::list_contexts().await,
            },

            Commands::List => cmd::list_workflows().await,
            Commands::Get { id, output } => cmd::get_workflow(&id, output).await,
            Commands::Run { id } => cmd::start_workflow(&id).await,
            Commands::Start { id } => cmd::start_workflow(&id).await,
            Commands::Pause { id } => cmd::pause_workflow(&id).await,
            Commands::Stop { id } => cmd::stop_workflow(&id).await,
            Commands::Remove { id } => cmd::remove_workflow(&id).await,
            Commands::State { id } => cmd::get_workflow_state(&id).await,
            Commands::Generate => cmd::generate().await,
            _ => {
                eprintln!("Unknown command");
                Err(anyhow::anyhow!("Unknown command"))
            }
        }
    }
}
