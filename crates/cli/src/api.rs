use reqwest::{Client, Method};
use serde::{Deserialize, Serialize};
use tabled::Tabled;

struct Request {
    method: Method,
    url: String,
    api_key: Option<String>,
    client: Client,
}

impl Request {
    pub fn new(method: Method, path: &str) -> Self {
        let base_url = crate::ctx::get_current_context()
            .map(|ctx| ctx.endpoint().to_string())
            .unwrap_or_else(|_| "http://localhost:8246".to_string());
        let api_key = crate::ctx::get_current_context()
            .ok()
            .and_then(|ctx| Some(ctx.api_key().to_string()));
        let url = format!("{}/{}", base_url, path);
        let client = Client::default();
        Self {
            method,
            url,
            api_key,
            client,
        }
    }

    pub async fn send(self) -> Result<reqwest::Response, reqwest::Error> {
        self.client
            .request(self.method, self.url)
            .header("x-api-key", self.api_key.unwrap_or_default())
            .send()
            .await
    }

    pub async fn send_json<T: serde::Serialize>(
        self,
        data: T,
    ) -> Result<reqwest::Response, reqwest::Error> {
        self.client
            .request(self.method, self.url)
            .header("x-api-key", self.api_key.unwrap_or_default())
            .json(&data)
            .send()
            .await
    }
}

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

#[derive(Tabled, Serialize, Deserialize, Clone, Debug)]
#[tabled(rename_all = "UPPERCASE")]
pub struct Workflow {
    pub id: String,
    pub name: String,
    pub status: String,
}

#[derive(Tabled, Serialize, Deserialize, Clone, Debug)]
#[tabled(rename_all = "UPPERCASE")]
pub struct WorkflowFile {
    pub id: String,
    pub name: String,
    pub file: String,
    #[tabled(display("display_loaded"))]
    pub loaded: bool,
}

fn display_loaded(value: &bool) -> String {
    if *value {
        "loaded".to_string()
    } else {
        "-".to_string()
    }
}

// ---------------------------------------------------------------------------
// Engine (loaded workflows)
// ---------------------------------------------------------------------------

pub async fn get_workflows() -> Result<Vec<Workflow>, reqwest::Error> {
    let res = Request::new(Method::GET, "workflows").send().await?;
    res.json().await
}

pub async fn get_workflow_state(id: &str) -> Result<Vec<eng::TaskInfo>, reqwest::Error> {
    let res = Request::new(Method::GET, &format!("workflows/{}/state", id))
        .send()
        .await?;
    res.json().await
}

pub async fn send_command(id: &str, command: &str) -> Result<(), reqwest::Error> {
    let _ = Request::new(Method::PATCH, &format!("workflows/{}", id))
        .send_json(serde_json::json!({ "command": command }))
        .await;
    Ok(())
}

// ---------------------------------------------------------------------------
// Mount / Unmount
// ---------------------------------------------------------------------------

pub async fn mount_workflow(id: &str) -> Result<serde_json::Value, reqwest::Error> {
    let res = Request::new(Method::POST, &format!("workflows/{}/mount", id))
        .send()
        .await?;
    res.json().await
}

pub async fn unmount_workflow(id: &str) -> Result<serde_json::Value, reqwest::Error> {
    let res = Request::new(Method::POST, &format!("workflows/{}/unmount", id))
        .send()
        .await?;
    res.json().await
}

// ---------------------------------------------------------------------------
// Filesystem
// ---------------------------------------------------------------------------

pub async fn list_workflow_files() -> Result<Vec<WorkflowFile>, reqwest::Error> {
    let res = Request::new(Method::GET, "workflows/files").send().await?;
    res.json().await
}

pub async fn push_workflow_file(config: eng::Config) -> Result<serde_json::Value, reqwest::Error> {
    let res = Request::new(Method::POST, "workflows/files")
        .send_json(config)
        .await?;
    res.json().await
}

pub async fn get_workflow_file(id: &str) -> Result<eng::Config, reqwest::Error> {
    let res = Request::new(Method::GET, &format!("workflows/files/{}", id))
        .send()
        .await?;
    res.json().await
}

pub async fn delete_workflow_file(id: &str) -> Result<serde_json::Value, reqwest::Error> {
    let res = Request::new(Method::DELETE, &format!("workflows/files/{}", id))
        .send()
        .await?;
    res.json().await
}

// ---------------------------------------------------------------------------
// Generate (AI workflow generation)
// ---------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ChatMessage {
    pub role: String,
    pub content: String,
}

#[derive(Deserialize, Debug)]
pub struct GenerateResponse {
    pub status: String,
    pub message: Option<String>,
    pub config: Option<eng::Config>,
    pub validation: Option<GenerateValidation>,
    pub messages: Vec<ChatMessage>,
}

#[derive(Deserialize, Debug)]
pub struct GenerateValidation {
    pub valid: bool,
    pub attempts: usize,
    pub errors: Vec<String>,
}

pub async fn generate_workflow(
    prompt: Option<&str>,
    messages: Option<&[ChatMessage]>,
) -> Result<GenerateResponse, reqwest::Error> {
    let body = match (prompt, messages) {
        (Some(p), None) => serde_json::json!({ "prompt": p }),
        (None, Some(msgs)) => serde_json::json!({ "messages": msgs }),
        _ => serde_json::json!({}),
    };
    let res = Request::new(Method::POST, "workflows/generate")
        .send_json(body)
        .await?;
    res.json().await
}
