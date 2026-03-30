mod api;
mod cli;
mod cmd;
mod ctx;
mod tui;

use clap::Parser;
use cli::Cli;

// ✔ × ▲ ▶ ▼ ◀ ♥ ▬

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    cli.run().await?;
    Ok(())
}
