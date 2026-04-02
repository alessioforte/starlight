use crate::etc::cfg::AppState;
use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    routing::{delete, get, patch, post},
};
use eng::{Config, TaskInfo, WorkflowInfo};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::path::PathBuf;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn workflows_dir() -> PathBuf {
    let dir = std::env::var("ENGINE_DIR").unwrap_or_else(|_| ".starlight/engine".to_string());
    PathBuf::from(dir).join("workflows")
}

/// Read a Config from a .yaml or .json file on disk.
fn read_config_file(path: &std::path::Path) -> Result<Config, String> {
    let file = std::fs::File::open(path).map_err(|e| e.to_string())?;
    match path.extension().and_then(|e| e.to_str()) {
        Some("json") => serde_json::from_reader(file).map_err(|e| e.to_string()),
        Some("yaml" | "yml") => serde_yaml_bw::from_reader(file).map_err(|e| e.to_string()),
        _ => Err("unsupported file extension".into()),
    }
}

/// Find a workflow file by id (tries .yaml then .json).
fn find_workflow_file(id: &str) -> Option<PathBuf> {
    let dir = workflows_dir();
    for ext in &["yaml", "json"] {
        let path = dir.join(format!("{id}.{ext}"));
        if path.exists() {
            return Some(path);
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Engine workflows (loaded / mounted)
// ---------------------------------------------------------------------------

pub async fn list_workflows(
    State(state): State<AppState>,
) -> Result<Json<Vec<WorkflowInfo>>, StatusCode> {
    let engine = state.engine.lock().await;
    let workflows = engine.list().await;
    Ok(Json(workflows))
}

pub async fn get_workflow(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<WorkflowInfo>, StatusCode> {
    let engine = state.engine.lock().await;
    if let Some(workflow) = engine.get(&id) {
        Ok(Json(workflow.info().await))
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

pub async fn get_workflow_state(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Vec<TaskInfo>>, StatusCode> {
    let engine = state.engine.lock().await;
    if let Some(workflow) = engine.get(&id) {
        let state = workflow.state();
        Ok(Json(state))
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

#[derive(Deserialize)]
pub struct CommandPayload {
    pub command: String,
}

pub async fn send_command(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(payload): Json<CommandPayload>,
) -> Result<Json<Value>, StatusCode> {
    let mut engine = state.engine.lock().await;
    match payload.command.as_str() {
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

// ---------------------------------------------------------------------------
// Mount / Unmount (load into engine / unload from engine)
// ---------------------------------------------------------------------------

/// Load a workflow from disk into the engine.
pub async fn mount_workflow(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<WorkflowInfo>, (StatusCode, Json<Value>)> {
    let path = find_workflow_file(&id).ok_or((
        StatusCode::NOT_FOUND,
        Json(json!({"error": "file_not_found", "message": format!("no workflow file for '{id}'")})),
    ))?;

    let config = read_config_file(&path).map_err(|e| {
        (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(json!({"error": "invalid_config", "message": e})),
        )
    })?;

    let mut engine = state.engine.lock().await;

    // Already loaded — return its info
    if let Some(wf) = engine.get(&id) {
        return Ok(Json(wf.info().await));
    }

    let wf = engine.add(config).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "engine_error", "message": e.to_string()})),
        )
    })?;

    Ok(Json(wf.info().await))
}

/// Unload a workflow from the engine but keep the file on disk.
pub async fn unmount_workflow(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, StatusCode> {
    let mut engine = state.engine.lock().await;
    if engine.remove(&id).is_ok() {
        Ok(Json(json!({"status": "unmounted", "id": id})))
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

// ---------------------------------------------------------------------------
// Filesystem workflows (files on disk)
// ---------------------------------------------------------------------------

#[derive(Serialize)]
pub struct WorkflowFileInfo {
    pub id: String,
    pub name: String,
    pub description: Option<String>,
    pub file: String,
    pub loaded: bool,
}

/// List all workflow files on disk with their loaded/unloaded status.
pub async fn list_workflow_files(
    State(state): State<AppState>,
) -> Result<Json<Vec<WorkflowFileInfo>>, (StatusCode, Json<Value>)> {
    let dir = workflows_dir();
    if !dir.exists() {
        return Ok(Json(vec![]));
    }

    let engine = state.engine.lock().await;
    let mut files = Vec::new();

    let entries = std::fs::read_dir(&dir).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "io_error", "message": e.to_string()})),
        )
    })?;

    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
        if ext != "yaml" && ext != "yml" && ext != "json" {
            continue;
        }
        if let Ok(config) = read_config_file(&path) {
            files.push(WorkflowFileInfo {
                loaded: engine.get(&config.id).is_some(),
                id: config.id,
                name: config.name,
                description: config.description,
                file: path
                    .file_name()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .to_string(),
            });
        }
    }

    files.sort_by(|a, b| a.id.cmp(&b.id));
    Ok(Json(files))
}

/// Save (push) a workflow config to the filesystem as YAML.
pub async fn push_workflow_file(
    Json(config): Json<Config>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let dir = workflows_dir();
    std::fs::create_dir_all(&dir).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "io_error", "message": e.to_string()})),
        )
    })?;

    let filename = format!("{}.yaml", config.id);
    let path = dir.join(&filename);

    let yaml = serde_yaml_bw::to_string(&config).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "serialization_error", "message": e.to_string()})),
        )
    })?;

    std::fs::write(&path, &yaml).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "io_error", "message": e.to_string()})),
        )
    })?;

    Ok(Json(
        json!({"status": "saved", "file": filename, "id": config.id}),
    ))
}

/// Download (get) a workflow config from the filesystem.
pub async fn get_workflow_file(
    Path(id): Path<String>,
) -> Result<Json<Config>, (StatusCode, Json<Value>)> {
    let path = find_workflow_file(&id).ok_or((
        StatusCode::NOT_FOUND,
        Json(json!({"error": "file_not_found", "message": format!("no workflow file for '{id}'")})),
    ))?;

    let config = read_config_file(&path).map_err(|e| {
        (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(json!({"error": "invalid_config", "message": e})),
        )
    })?;

    Ok(Json(config))
}

/// Delete a workflow file from disk (and unmount from engine if loaded).
pub async fn delete_workflow_file(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    // Unmount from engine if loaded
    {
        let mut engine = state.engine.lock().await;
        let _ = engine.remove(&id);
    }

    let path = find_workflow_file(&id).ok_or((
        StatusCode::NOT_FOUND,
        Json(json!({"error": "file_not_found", "message": format!("no workflow file for '{id}'")})),
    ))?;

    std::fs::remove_file(&path).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "io_error", "message": e.to_string()})),
        )
    })?;

    Ok(Json(json!({"status": "deleted", "id": id})))
}

// ---------------------------------------------------------------------------
// Routes
// ---------------------------------------------------------------------------

pub fn routes() -> axum::Router<AppState> {
    axum::Router::new()
        // Engine (loaded workflows)
        .route("/workflows", get(list_workflows))
        .route("/workflows/{id}", get(get_workflow))
        .route("/workflows/{id}/state", get(get_workflow_state))
        .route("/workflows/{id}", patch(send_command))
        // Mount / Unmount
        .route("/workflows/{id}/mount", post(mount_workflow))
        .route("/workflows/{id}/unmount", post(unmount_workflow))
        // Filesystem
        .route("/workflows/files", get(list_workflow_files))
        .route("/workflows/files", post(push_workflow_file))
        .route("/workflows/files/{id}", get(get_workflow_file))
        .route("/workflows/files/{id}", delete(delete_workflow_file))
}
