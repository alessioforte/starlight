use reqwest::{Client, Method};
use serde::{Deserialize, Serialize};
use tabled::Tabled;

struct Request {
    method: Method,
    url: String,
    client: Client,
}

impl Request {
    pub fn new(method: Method, path: &str) -> Self {
        let base_url =
            std::env::var("ENGINE_URL").unwrap_or_else(|_| "http://localhost:8246".to_string());
        let url = format!("{}/{}", base_url, path);
        let client = Client::default();
        Self {
            method,
            url,
            client,
        }
    }

    pub async fn send(self) -> Result<reqwest::Response, reqwest::Error> {
        self.client.request(self.method, self.url).send().await
    }

    pub async fn send_json<T: serde::Serialize>(
        self,
        data: T,
    ) -> Result<reqwest::Response, reqwest::Error> {
        self.client
            .request(self.method, self.url)
            .json(&data)
            .send()
            .await
    }
}

#[derive(Tabled, Serialize, Deserialize, Clone, Debug)]
#[tabled(rename_all = "UPPERCASE")]
pub struct Workflow {
    id: String,
    name: String,
    // description: String,
    status: String,
}

pub async fn get_workflows() -> Result<Vec<Workflow>, reqwest::Error> {
    let res = Request::new(Method::GET, "workflows").send().await?;
    res.json().await.map_err(|e| e)
}

pub async fn get_workflow(id: &str) -> Result<eng::Config, reqwest::Error> {
    let res = Request::new(Method::GET, &format!("workflows/{}", id))
        .send()
        .await?;
    res.json().await.map_err(|e| e)
}

pub async fn remove_workflow(id: &str) -> Result<(), reqwest::Error> {
    let _ = Request::new(Method::DELETE, &format!("workflows/{}", id))
        .send()
        .await;
    Ok(())
}

pub async fn get_workflow_state(id: &str) -> Result<Vec<eng::TaskInfo>, reqwest::Error> {
    let res = Request::new(Method::GET, &format!("workflows/{}/state", id))
        .send()
        .await?;
    res.json().await.map_err(|e| e)
}

// pub async fn add_workflow(config: serde_json::Value) -> Result<reqwest::Response, reqwest::Error> {
//     Request::new(Method::POST, "workflows")
//         .send_json(config)
//         .await
// }

// pub async fn load_workflow(id: &str) -> Result<reqwest::Response, reqwest::Error> {
//     Request::new(Method::PUT, &format!("workflows/{}", id))
//         .send()
//         .await
// }

pub async fn send_command(id: &str, command: &str) -> Result<(), reqwest::Error> {
    let _ = Request::new(Method::PATCH, &format!("workflows/{}", id))
        .send_json(serde_json::json!({ "command": command }))
        .await;
    Ok(())
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
    pub messages: Vec<ChatMessage>,
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
