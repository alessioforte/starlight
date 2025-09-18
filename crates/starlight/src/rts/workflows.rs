use crate::etc::cfg::AppState;
use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    routing::{get, patch, post, put},
};
use engine::Config;
use serde_json::{Value, json};

pub async fn list_workflows(State(state): State<AppState>) -> impl axum::response::IntoResponse {
    let engine = state.engine.lock().await;
    Json(engine.list())
}

pub async fn get_workflow(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<axum::response::Json<Config>, StatusCode> {
    let engine = state.engine.lock().await;
    if let Some(workflow) = engine.get(&id) {
        Ok(Json(workflow.get_config()))
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

pub async fn add_workflow(
    State(state): State<AppState>,
    Json(config): Json<Config>,
) -> Result<axum::response::Json<Config>, StatusCode> {
    let mut engine = state.engine.lock().await;
    match engine.add(config) {
        Ok(workflow) => Ok(Json(workflow.get_config())),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

pub async fn load_workflow(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<axum::response::Json<Value>, StatusCode> {
    let mut engine = state.engine.lock().await;
    if engine.load(&id).is_ok() {
        Ok(Json(json!({"status": "loaded"})))
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

#[derive(serde::Deserialize)]
pub struct CommandPayload {
    pub command: String,
}
pub async fn send_command(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(payload): Json<CommandPayload>,
) -> Result<axum::response::Json<Value>, StatusCode> {
    let mut engine = state.engine.lock().await;
    match payload.command.as_str() {
        "execute" => {
            if engine.execute(&id).is_ok() {
                Ok(Json(json!({"status": "executing"})))
            } else {
                Err(StatusCode::NOT_FOUND)
            }
        }
        "pause" => {
            if engine.pause(&id).is_ok() {
                Ok(Json(json!({"status": "paused"})))
            } else {
                Err(StatusCode::NOT_FOUND)
            }
        }
        "stop" => {
            if engine.stop(&id).is_ok() {
                Ok(Json(json!({"status": "stopped"})))
            } else {
                Err(StatusCode::NOT_FOUND)
            }
        }
        _ => Err(StatusCode::BAD_REQUEST),
    }
}

pub fn routes() -> axum::Router<AppState> {
    axum::Router::new()
        .route("/workflows", get(list_workflows))
        .route("/workflows/{id}", get(get_workflow))
        .route("/workflows", post(add_workflow))
        .route("/workflows/{id}", put(load_workflow))
        .route("/workflows/{id}", patch(send_command))
}
