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
    /// `"questions"` when the model needs more info, `"completed"` when config is ready,
    /// `"validation_failed"` when the model could not repair an invalid config.
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
    /// Validation summary for generated configs.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub validation: Option<ValidationReport>,
    /// Full conversation history — send this back in the next request to continue.
    pub messages: Vec<ChatMessage>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ValidationReport {
    pub valid: bool,
    pub attempts: usize,
    pub errors: Vec<String>,
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

enum GenerateError {
    BadRequest(String),
    LlmConnection(String),
    LlmResponse(String),
    Engine(String),
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
            Self::Engine(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                json!({"error": "engine_error", "message": e}),
            ),
        };
        (status, Json(body)).into_response()
    }
}

// ---------------------------------------------------------------------------
// System prompt
// ---------------------------------------------------------------------------

fn build_system_prompt(task_types: &[&str]) -> Result<String, GenerateError> {
    let prompt = include_str!("prompt.md");
    Ok(format!(
        r#"
{prompt}

Available task types registered in this engine: {types}"#,
        types = task_types.join(", ")
    ))
}

// ---------------------------------------------------------------------------
// LLM client
// ---------------------------------------------------------------------------

const MAX_VALIDATION_ATTEMPTS: usize = 3;

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

enum ParsedOutput {
    Config(Config),
    Questions(String),
    InvalidConfigJson(String),
}

fn parse_llm_output(raw: &str) -> ParsedOutput {
    let Some(json_str) = extract_json(raw) else {
        return ParsedOutput::Questions(raw.to_string());
    };

    match serde_json::from_str(&json_str) {
        Ok(config) => ParsedOutput::Config(config),
        Err(e) => ParsedOutput::InvalidConfigJson(e.to_string()),
    }
}

fn build_json_repair_prompt(error: &str) -> String {
    format!(
        r#"The previous response contained JSON but it was not a valid Starlight workflow config.

Parser error:
{error}

Return a corrected raw JSON workflow config only. If the user's request is missing critical details, ask all clarifying questions in one concise message instead of returning JSON."#
    )
}

fn build_validation_repair_prompt(error: &str) -> String {
    format!(
        r#"The previous workflow config failed Starlight engine validation.

Validation error:
{error}

Return a corrected raw JSON workflow config only. Preserve the user's intended workflow, but fix schema, task parameters, task IDs, channels, dependencies, and task types as needed. If the user's request is missing critical details, ask all clarifying questions in one concise message instead of returning JSON."#
    )
}

fn validation_failed_message(errors: &[String]) -> String {
    let latest_error = errors.last().map(String::as_str).unwrap_or("unknown error");
    format!(
        "I generated a workflow config, but it still failed validation after {MAX_VALIDATION_ATTEMPTS} attempts.\n\nLatest validation error:\n\n```text\n{latest_error}\n```\n\nPlease clarify the workflow details or adjust the request."
    )
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

    // 3. Agent loop: generate, validate, and feed validation errors back for repair.
    let mut validation_errors = Vec::new();

    for attempt in 1..=MAX_VALIDATION_ATTEMPTS {
        let raw_output =
            call_llm(&request.base_url, &request.model, &messages, &system_prompt).await?;

        messages.push(ChatMessage {
            role: "assistant".into(),
            content: raw_output.clone(),
        });

        let config = match parse_llm_output(&raw_output) {
            ParsedOutput::Config(config) => config,
            ParsedOutput::Questions(question) => {
                return Ok(Json(GenerateResponse {
                    status: "questions".into(),
                    message: Some(question),
                    config: None,
                    workflow: None,
                    validation: None,
                    messages,
                }));
            }
            ParsedOutput::InvalidConfigJson(error) => {
                validation_errors.push(format!("invalid JSON workflow config: {error}"));
                if attempt == MAX_VALIDATION_ATTEMPTS {
                    let message = validation_failed_message(&validation_errors);
                    return Ok(Json(GenerateResponse {
                        status: "validation_failed".into(),
                        message: Some(message),
                        config: None,
                        workflow: None,
                        validation: Some(ValidationReport {
                            valid: false,
                            attempts: attempt,
                            errors: validation_errors,
                        }),
                        messages,
                    }));
                }

                messages.push(ChatMessage {
                    role: "user".into(),
                    content: build_json_repair_prompt(&error),
                });
                continue;
            }
        };

        let validation_error = {
            let engine = state.engine.lock().await;
            engine.validate_config(&config).err().map(|e| e.to_string())
        };

        if let Some(error) = validation_error {
            validation_errors.push(error.clone());
            if attempt == MAX_VALIDATION_ATTEMPTS {
                let message = validation_failed_message(&validation_errors);
                return Ok(Json(GenerateResponse {
                    status: "validation_failed".into(),
                    message: Some(message),
                    config: None,
                    workflow: None,
                    validation: Some(ValidationReport {
                        valid: false,
                        attempts: attempt,
                        errors: validation_errors,
                    }),
                    messages,
                }));
            }

            messages.push(ChatMessage {
                role: "user".into(),
                content: build_validation_repair_prompt(&error),
            });
            continue;
        }

        // 4. Optionally auto-load after validation has passed.
        let workflow_info = if request.auto_load {
            let mut engine = state.engine.lock().await;
            let wf = engine
                .add(config.clone())
                .map_err(|e| GenerateError::Engine(e.to_string()))?;
            Some(wf.info().await)
        } else {
            None
        };

        return Ok(Json(GenerateResponse {
            status: "completed".into(),
            message: None,
            config: Some(config),
            workflow: workflow_info,
            validation: Some(ValidationReport {
                valid: true,
                attempts: attempt,
                errors: validation_errors,
            }),
            messages,
        }));
    }

    unreachable!("validation attempts loop always returns")
}

// ---------------------------------------------------------------------------
// Routes
// ---------------------------------------------------------------------------

pub fn routes() -> axum::Router<AppState> {
    axum::Router::new().route("/workflows/generate", post(generate_workflow))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_llm_output_treats_plain_text_as_questions() {
        match parse_llm_output("Which CSV file should I read?") {
            ParsedOutput::Questions(question) => {
                assert_eq!(question, "Which CSV file should I read?");
            }
            _ => panic!("expected questions"),
        }
    }

    #[test]
    fn parse_llm_output_reports_invalid_workflow_json() {
        let raw = r#"{"id":"bad","tasks":[]}"#;

        match parse_llm_output(raw) {
            ParsedOutput::InvalidConfigJson(error) => {
                assert!(error.contains("missing field `name`"));
            }
            _ => panic!("expected invalid config JSON"),
        }
    }
}
