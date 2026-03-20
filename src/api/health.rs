use axum::Json;
use serde_json::{Value, json};

pub async fn health() -> Json<Value> {
    let version = env!("CARGO_PKG_VERSION");
    Json(json!({
        "version": version,
        "message": "The engine is up and running 🦀",
    }))
}
