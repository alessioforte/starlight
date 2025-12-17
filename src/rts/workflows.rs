use crate::etc::cfg::AppState;
use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    routing::{delete, get, patch, post},
};
// use engine::Config;
use eng::{Config, TaskInfo, WorkflowInfo};
use serde_json::{Value, json};

pub async fn list_workflows(
    State(state): State<AppState>,
) -> Result<axum::response::Json<Vec<WorkflowInfo>>, StatusCode> {
    let engine = state.engine.lock().await;
    let workflows = engine.list().await;
    Ok(Json(workflows))
}

pub async fn get_workflow(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<axum::response::Json<WorkflowInfo>, StatusCode> {
    let engine = state.engine.lock().await;
    if let Some(workflow) = engine.get(&id) {
        Ok(Json(workflow.info().await))
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

pub async fn add_workflow(
    State(state): State<AppState>,
    Json(config): Json<Config>,
) -> Result<axum::response::Json<WorkflowInfo>, StatusCode> {
    let mut engine = state.engine.lock().await;
    match engine.add(config) {
        Ok(workflow) => Ok(Json(workflow.info().await)),
        Err(e) => {
            tracing::error!("Failed to add workflow: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

pub async fn remove_workflow(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<axum::response::Json<Value>, StatusCode> {
    let mut engine = state.engine.lock().await;
    if engine.remove(&id).is_ok() {
        Ok(Json(json!({"status": "removed"})))
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

pub async fn get_workflow_state(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<axum::response::Json<Vec<TaskInfo>>, StatusCode> {
    let engine = state.engine.lock().await;
    if let Some(workflow) = engine.get(&id) {
        let state = workflow.state();
        Ok(Json(state))
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

// pub async fn load_workflow(
//     State(state): State<AppState>,
//     Path(id): Path<String>,
// ) -> Result<axum::response::Json<Value>, StatusCode> {
//     let mut engine = state.engine.lock().await;
//     if engine.load(&id).is_ok() {
//         Ok(Json(json!({"status": "loaded"})))
//     } else {
//         Err(StatusCode::NOT_FOUND)
//     }
// }

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
        // "execute" => {
        //     if engine.execute(&id).is_ok() {
        //         Ok(Json(json!({"status": "executing"})))
        //     } else {
        //         Err(StatusCode::NOT_FOUND)
        //     }
        // }
        "start" => {
            if engine.start(&id).await.is_ok() {
                Ok(Json(json!({"status": "running"})))
            } else {
                Err(StatusCode::NOT_FOUND)
            }
        }
        "pause" => {
            if engine.pause(&id).await.is_ok() {
                Ok(Json(json!({"status": "paused"})))
            } else {
                Err(StatusCode::NOT_FOUND)
            }
        }
        "stop" => {
            if engine.stop(&id).await.is_ok() {
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
        .route("/workflows/{id}", delete(remove_workflow))
        .route("/workflows/{id}/state", get(get_workflow_state))
        .route("/workflows/{id}", patch(send_command))
}
