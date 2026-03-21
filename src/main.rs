mod act;
mod api;
mod etc;
mod tasks;

use crate::etc::cfg::AppState;
use axum::Router;
use dotenvy::dotenv;
// use engine::Engine;
use eng::Engine;
use std::sync::Arc;
use tokio::sync::Mutex;

// =^.^=
// 🦀
// Starlight ✨
// Workflow Engine API Server
// // This server provides an API to manage workflows in the engine.
#[tokio::main]
async fn main() {
    println!("{}", etc::logo::LOGO);

    dotenv().ok();
    console_subscriber::init();
    let mut engine = Engine::new();
    engine.register_task("simulator", tasks::simulator::SimulatorTask::create);
    let engine = Arc::new(Mutex::new(engine));

    act::load_workflow_from_dir(&engine).await;

    let app = Router::new().merge(api::routes()).with_state(AppState {
        engine: engine.clone(),
    });

    let port = std::env::var("PORT").unwrap_or_else(|_| "8246".to_string());
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", port))
        .await
        .expect("Failed to bind TCP listener");
    tracing::debug!("listening on {}", listener.local_addr().unwrap());
    axum::serve(listener, app).await.unwrap();
}
