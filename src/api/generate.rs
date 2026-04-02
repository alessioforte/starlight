use crate::etc::cfg::AppState;
use axum::{
    Json,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::post,
};
use eng::{Config, WorkflowInfo};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::path::PathBuf;

// ---------------------------------------------------------------------------
// Request / Response
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatMessage {
    pub role: String,
    pub content: String,
}

#[derive(Deserialize)]
pub struct GenerateRequest {
    /// First user message (used when starting a new conversation).
    #[serde(default)]
    pub prompt: Option<String>,
    /// Full conversation history (used for follow-ups).
    #[serde(default)]
    pub messages: Option<Vec<ChatMessage>>,
    #[serde(default = "default_model")]
    pub model: String,
    #[serde(default = "default_base_url")]
    pub base_url: String,
    #[serde(default)]
    pub auto_load: bool,
}

fn default_model() -> String {
    std::env::var("LLM_MODEL").unwrap_or_else(|_| "qwen3.5-9b".to_string())
}

fn default_base_url() -> String {
    std::env::var("LLM_BASE_URL").unwrap_or_else(|_| "http://localhost:1234/v1".to_string())
}

#[derive(Serialize)]
pub struct GenerateResponse {
    /// `"questions"` when the model needs more info, `"completed"` when config is ready.
    pub status: String,
    /// The model's question text (present when status is "questions").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// The generated workflow config (present when status is "completed").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config: Option<Config>,
    /// Workflow info if auto_load was requested (present when status is "completed").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workflow: Option<WorkflowInfo>,
    /// Full conversation history — send this back in the next request to continue.
    pub messages: Vec<ChatMessage>,
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

enum GenerateError {
    BadRequest(String),
    LlmConnection(String),
    LlmResponse(String),
    UnknownTaskType {
        task_type: String,
        available: Vec<String>,
    },
    Engine(String),
    PromptFile(String),
}

impl IntoResponse for GenerateError {
    fn into_response(self) -> Response {
        let (status, body) = match self {
            Self::BadRequest(e) => (
                StatusCode::BAD_REQUEST,
                json!({"error": "bad_request", "message": e}),
            ),
            Self::LlmConnection(e) => (
                StatusCode::BAD_GATEWAY,
                json!({"error": "llm_connection_failed", "message": e}),
            ),
            Self::LlmResponse(e) => (
                StatusCode::BAD_GATEWAY,
                json!({"error": "llm_response_invalid", "message": e}),
            ),
            Self::UnknownTaskType {
                task_type,
                available,
            } => (
                StatusCode::UNPROCESSABLE_ENTITY,
                json!({"error": "unknown_task_type", "task_type": task_type, "available": available}),
            ),
            Self::Engine(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                json!({"error": "engine_error", "message": e}),
            ),
            Self::PromptFile(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                json!({"error": "prompt_file_error", "message": e}),
            ),
        };
        (status, Json(body)).into_response()
    }
}

// ---------------------------------------------------------------------------
// System prompt
// ---------------------------------------------------------------------------

fn prompt_file_path() -> PathBuf {
    let dir = std::env::var("ENGINE_DIR").unwrap_or_else(|_| ".starlight/engine".to_string());
    PathBuf::from(dir).join("prompt.md")
}

fn build_system_prompt(task_types: &[&str]) -> Result<String, GenerateError> {
    let reference = std::fs::read_to_string(prompt_file_path())
        .map_err(|e| GenerateError::PromptFile(format!("{}: {e}", prompt_file_path().display())))?;

    Ok(format!(
        r#"You are a workflow generator for the Starlight engine.
Your job is to help users build valid workflow configurations.

{reference}

Available task types registered in this engine: {types}

## Interaction rules

1. Analyze the user's request carefully. If critical information is missing to produce a correct workflow, ask clarifying questions BEFORE generating the config. Examples of missing info:
   - File paths for csv_reader or csv_writer
   - Column names or field names for json_mapper, filter, aggregator
   - URL for http_sender
   - Specific numeric ranges, intervals, or thresholds
   - Ambiguous workflow topology (unclear what connects to what)

2. Ask all your questions in a single concise message. Do not ask one question at a time.

3. When you have enough information, output ONLY the JSON workflow config. No explanations, no markdown fences, no text before or after — just the raw JSON object.

4. Every task must have: id, type, dependencies, params, outputs.
   - Source tasks (no input): `"dependencies": []`
   - Sink tasks (no output): `"outputs": {{}}`"#,
        types = task_types.join(", ")
    ))
}

// ---------------------------------------------------------------------------
// LLM client
// ---------------------------------------------------------------------------

async fn call_llm(
    base_url: &str,
    model: &str,
    messages: &[ChatMessage],
    system_prompt: &str,
) -> Result<String, GenerateError> {
    let client = reqwest::Client::new();
    let url = format!("{}/chat/completions", base_url.trim_end_matches('/'));

    let mut api_messages = vec![json!({"role": "system", "content": system_prompt})];
    for msg in messages {
        api_messages.push(json!({"role": msg.role, "content": msg.content}));
    }

    let body = json!({
        "model": model,
        "messages": api_messages,
        "temperature": 0.3,
        "max_tokens": 2048
    });

    let response = client
        .post(&url)
        .header("Content-Type", "application/json")
        .json(&body)
        .timeout(std::time::Duration::from_secs(120))
        .send()
        .await
        .map_err(|e| GenerateError::LlmConnection(e.to_string()))?;

    let json: Value = response
        .json()
        .await
        .map_err(|e| GenerateError::LlmResponse(e.to_string()))?;

    json["choices"][0]["message"]["content"]
        .as_str()
        .map(|s| s.to_string())
        .ok_or_else(|| GenerateError::LlmResponse(format!("unexpected response format: {json}")))
}

// ---------------------------------------------------------------------------
// JSON extraction
// ---------------------------------------------------------------------------

fn extract_json(raw: &str) -> Option<String> {
    // Try ```json ... ``` fences
    if let Some(start) = raw.find("```json") {
        let after = &raw[start + 7..];
        if let Some(end) = after.find("```") {
            return Some(after[..end].trim().to_string());
        }
    }
    // Try bare ``` fences
    if let Some(start) = raw.find("```") {
        let after = &raw[start + 3..];
        if let Some(nl) = after.find('\n') {
            let content = &after[nl + 1..];
            if let Some(end) = content.find("```") {
                return Some(content[..end].trim().to_string());
            }
        }
    }

    // Find the first { and last } to extract the JSON object
    if let (Some(start), Some(end)) = (raw.find('{'), raw.rfind('}')) {
        if start < end {
            return Some(raw[start..=end].to_string());
        }
    }

    None
}

fn try_parse_config(raw: &str) -> Option<Config> {
    let json_str = extract_json(raw)?;
    serde_json::from_str(&json_str).ok()
}

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

async fn generate_workflow(
    State(state): State<AppState>,
    Json(request): Json<GenerateRequest>,
) -> Result<Json<GenerateResponse>, GenerateError> {
    // 1. Build conversation messages
    let mut messages = match (&request.prompt, &request.messages) {
        (Some(prompt), None) => vec![ChatMessage {
            role: "user".into(),
            content: prompt.clone(),
        }],
        (None, Some(msgs)) => msgs.clone(),
        (Some(_), Some(_)) => {
            return Err(GenerateError::BadRequest(
                "provide either 'prompt' or 'messages', not both".into(),
            ));
        }
        (None, None) => {
            return Err(GenerateError::BadRequest(
                "provide either 'prompt' or 'messages'".into(),
            ));
        }
    };

    // 2. Build system prompt with registered task types
    let task_types: Vec<String> = {
        let engine = state.engine.lock().await;
        engine
            .registry()
            .list()
            .iter()
            .map(|s| s.to_string())
            .collect()
    };
    let task_refs: Vec<&str> = task_types.iter().map(|s| s.as_str()).collect();
    let system_prompt = build_system_prompt(&task_refs)?;

    // 3. Call LLM (engine lock released)
    let raw_output = call_llm(&request.base_url, &request.model, &messages, &system_prompt).await?;

    // 4. Append assistant response to conversation history
    messages.push(ChatMessage {
        role: "assistant".into(),
        content: raw_output.clone(),
    });

    // 5. Try to parse as Config — if it fails, the model is asking questions
    let config = match try_parse_config(&raw_output) {
        Some(cfg) => cfg,
        None => {
            return Ok(Json(GenerateResponse {
                status: "questions".into(),
                message: Some(raw_output),
                config: None,
                workflow: None,
                messages,
            }));
        }
    };

    // 6. Validate task types
    for task in &config.tasks {
        if !task_refs.contains(&task.kind.as_str()) {
            return Err(GenerateError::UnknownTaskType {
                task_type: task.kind.clone(),
                available: task_types.clone(),
            });
        }
    }

    // 7. Optionally auto-load
    let workflow_info = if request.auto_load {
        let mut engine = state.engine.lock().await;
        let wf = engine
            .add(config.clone())
            .map_err(|e| GenerateError::Engine(e.to_string()))?;
        Some(wf.info().await)
    } else {
        None
    };

    Ok(Json(GenerateResponse {
        status: "completed".into(),
        message: None,
        config: Some(config),
        workflow: workflow_info,
        messages,
    }))
}

// ---------------------------------------------------------------------------
// Routes
// ---------------------------------------------------------------------------

pub fn routes() -> axum::Router<AppState> {
    axum::Router::new().route("/workflows/generate", post(generate_workflow))
}
