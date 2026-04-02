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

    /// Push a workflow file to the engine directory
    Push {
        /// Path to a workflow file (.yaml or .json)
        file: String,
    },

    /// Pull a workflow config from the engine directory and save to file
    Pull {
        /// Workflow ID
        id: String,
        /// Output format: yaml or json
        #[arg(short, long, default_value = "yaml")]
        output: Option<String>,
    },

    /// List workflows (loaded in engine by default, or all files with --all)
    #[clap(alias = "ls")]
    List {
        /// List all workflow files (including unloaded)
        #[arg(short, long)]
        all: bool,
    },

    /// Mount (load) a workflow from the filesystem into the engine
    Mount {
        /// Workflow ID
        id: String,
    },

    /// Unmount (unload) a workflow from the engine without deleting the file
    Unmount {
        /// Workflow ID
        id: String,
    },

    /// Mount and start a workflow
    Run { id: String },

    /// Start a workflow
    Start { id: String },

    /// Pause a workflow
    Pause { id: String },

    /// Stop a workflow
    Stop { id: String },

    /// Delete a workflow from engine and filesystem
    #[clap(alias = "rm")]
    Remove { id: String },

    /// Get the state of a workflow
    State { id: String },

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
                    println!("Starting the engine...");
                    Ok(())
                }
                EngineCommands::Stop => {
                    println!("Stopping the engine...");
                    Ok(())
                }
                EngineCommands::Status => {
                    println!("Checking engine status...");
                    Ok(())
                }
            },

            // Config
            Commands::Config(config_cli) => match config_cli.command {
                ConfigCommands::GetContext { current } => {
                    if current {
                        println!("Getting current context...");
                        Ok(())
                    } else {
                        println!("Getting all contexts...");
                        Ok(())
                    }
                }
                ConfigCommands::SetContext { name } => cmd::set_context(&name).await,
                ConfigCommands::GetContexts => cmd::list_contexts().await,
            },

            // Workflow commands
            Commands::Push { file } => cmd::push_workflow(&file).await,
            Commands::Pull { id, output } => cmd::pull_workflow(&id, output).await,
            Commands::List { all } => {
                if all {
                    cmd::list_all_workflows().await
                } else {
                    cmd::list_workflows().await
                }
            }
            Commands::Mount { id } => cmd::mount_workflow(&id).await,
            Commands::Unmount { id } => cmd::unmount_workflow(&id).await,
            Commands::Run { id } => cmd::run_workflow(&id).await,
            Commands::Start { id } => cmd::start_workflow(&id).await,
            Commands::Pause { id } => cmd::pause_workflow(&id).await,
            Commands::Stop { id } => cmd::stop_workflow(&id).await,
            Commands::Remove { id } => cmd::remove_workflow(&id).await,
            Commands::State { id } => cmd::get_workflow_state(&id).await,
            Commands::Generate => cmd::generate().await,
        }
    }
}
