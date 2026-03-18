//! HTTP Sender Task
//!
//! Sends each incoming message as an HTTP request to a configurable endpoint.
//! Successful responses go to `"out"`, failures to `"error"` (optional).

use crate::ctx::TaskContext;
use crate::err::Result;
use crate::task::{BaseTask, Task, TaskInfo};
use async_trait::async_trait;
use reqwest::{Client, Method, header::{HeaderMap, HeaderName, HeaderValue}};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// HTTP method to use for requests.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum HttpMethod {
    Get,
    Post,
    Put,
    Patch,
    Delete,
}

impl HttpMethod {
    fn to_reqwest(&self) -> Method {
        match self {
            HttpMethod::Get => Method::GET,
            HttpMethod::Post => Method::POST,
            HttpMethod::Put => Method::PUT,
            HttpMethod::Patch => Method::PATCH,
            HttpMethod::Delete => Method::DELETE,
        }
    }
}

impl Default for HttpMethod {
    fn default() -> Self {
        Self::Post
    }
}

/// What to include in the request body.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BodyMode {
    /// Send the entire incoming message as JSON body (default)
    Full,
    /// Send only a specific field from the message
    Field(String),
    /// No body (useful for GET/DELETE)
    None,
}

impl Default for BodyMode {
    fn default() -> Self {
        Self::Full
    }
}

/// Retry configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryConfig {
    /// Maximum number of retry attempts (default: 3)
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
    /// Initial backoff in milliseconds (default: 1000). Doubles on each retry.
    #[serde(default = "default_backoff_ms")]
    pub backoff_ms: u64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: default_max_retries(),
            backoff_ms: default_backoff_ms(),
        }
    }
}

fn default_max_retries() -> u32 {
    3
}

fn default_backoff_ms() -> u64 {
    1000
}

fn default_timeout_ms() -> u64 {
    30_000
}

/// HTTP Sender task parameters.
#[derive(Debug, Serialize, Deserialize)]
pub struct Params {
    /// Target URL
    pub url: String,
    /// HTTP method (default: POST)
    #[serde(default)]
    pub method: HttpMethod,
    /// Static headers added to every request
    #[serde(default)]
    pub headers: HashMap<String, String>,
    /// What to send as body (default: full message)
    #[serde(default)]
    pub body: BodyMode,
    /// Request timeout in milliseconds (default: 30 000)
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
    /// Retry configuration
    #[serde(default)]
    pub retry: RetryConfig,
    /// Maximum number of concurrent in-flight requests (default: 1 — sequential)
    #[serde(default = "default_concurrency")]
    pub concurrency: usize,
}

fn default_concurrency() -> usize {
    1
}

/// HTTP Sender state (stateless).
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct State {}

// ---------------------------------------------------------------------------
// Task implementation
// ---------------------------------------------------------------------------

/// HTTP Sender task
///
/// Sends each incoming message as an HTTP request.
///
/// # Outputs
///
/// | Label     | Description                                        |
/// |-----------|----------------------------------------------------|
/// | `"out"`   | Response body (JSON) for successful requests (2xx) |
/// | `"error"` | Original message + error info for failures (opt.)  |
///
/// # Example Configuration
///
/// ```json
/// {
///   "url": "https://api.example.com/ingest",
///   "method": "POST",
///   "headers": { "Authorization": "Bearer token123" },
///   "timeout_ms": 5000,
///   "retry": { "max_retries": 2, "backoff_ms": 500 },
///   "concurrency": 4
/// }
/// ```
pub struct HttpSender {
    base: BaseTask<Params, State>,
}

impl HttpSender {
    pub fn create(id: String, params: Value) -> Result<Box<dyn Task>> {
        Ok(Box::new(Self {
            base: BaseTask::new(id, params)?,
        }))
    }

    /// Build a reusable reqwest client from the task params.
    fn build_client(params: &Params) -> std::result::Result<Client, reqwest::Error> {
        let mut default_headers = HeaderMap::new();
        for (k, v) in &params.headers {
            if let (Ok(name), Ok(val)) = (
                k.parse::<HeaderName>(),
                HeaderValue::from_str(v),
            ) {
                default_headers.insert(name, val);
            }
        }
        Client::builder()
            .timeout(Duration::from_millis(params.timeout_ms))
            .default_headers(default_headers)
            .build()
    }

    /// Extract the body to send from the incoming message.
    fn extract_body(msg: &Value, mode: &BodyMode) -> Option<Value> {
        match mode {
            BodyMode::Full => Some(msg.clone()),
            BodyMode::Field(field) => msg.get(field).cloned(),
            BodyMode::None => None,
        }
    }

    /// Send a single request with retries. Returns `Ok(response_body)` or `Err(error_string)`.
    async fn send_request(
        client: &Client,
        method: &Method,
        url: &str,
        body: Option<&Value>,
        retry: &RetryConfig,
    ) -> std::result::Result<Value, String> {
        let mut attempt = 0u32;
        let mut backoff = Duration::from_millis(retry.backoff_ms);

        loop {
            let mut req = client.request(method.clone(), url);
            if let Some(b) = body {
                req = req.json(b);
            }

            match req.send().await {
                Ok(resp) => {
                    let status = resp.status();
                    if status.is_success() {
                        let resp_body = resp.json::<Value>().await.unwrap_or(json!(null));
                        return Ok(resp_body);
                    }

                    // Non-retryable client errors (4xx)
                    if status.is_client_error() {
                        let text = resp.text().await.unwrap_or_default();
                        return Err(format!("HTTP {}: {}", status, text));
                    }

                    // Server errors (5xx) — retryable
                    attempt += 1;
                    if attempt > retry.max_retries {
                        let text = resp.text().await.unwrap_or_default();
                        return Err(format!(
                            "HTTP {} after {} retries: {}",
                            status, retry.max_retries, text
                        ));
                    }

                    tracing::warn!(
                        "HTTP {}: retry {}/{} in {:?}",
                        status,
                        attempt,
                        retry.max_retries,
                        backoff,
                    );
                    tokio::time::sleep(backoff).await;
                    backoff *= 2;
                }
                Err(e) => {
                    attempt += 1;
                    if attempt > retry.max_retries {
                        return Err(format!(
                            "Request failed after {} retries: {}",
                            retry.max_retries, e
                        ));
                    }

                    tracing::warn!(
                        "Request error: {}. Retry {}/{} in {:?}",
                        e,
                        attempt,
                        retry.max_retries,
                        backoff,
                    );
                    tokio::time::sleep(backoff).await;
                    backoff *= 2;
                }
            }
        }
    }
}

#[async_trait]
impl Task for HttpSender {
    fn name(&self) -> &str {
        "HttpSender"
    }

    fn set_status_handle(&mut self, status: Arc<tokio::sync::RwLock<crate::task::TaskStatus>>) {
        self.base.status = Some(status);
    }

    fn get_info(&self) -> TaskInfo {
        let current_status = if let Some(status_lock) = &self.base.status {
            status_lock.try_read().ok().map(|s| format!("{:?}", *s))
        } else {
            None
        };

        TaskInfo {
            id: self.base.id.clone(),
            params: serde_json::to_value(&self.base.params).unwrap_or(json!({})),
            state: serde_json::to_value(&self.base.state).unwrap_or(json!({})),
            status: current_status,
            metrics: None,
        }
    }

    async fn execute(&self, ctx: Arc<TaskContext>) -> Result<()> {
        let params = &self.base.params;
        let mut input = ctx.merged_input().await?;
        let output = ctx.output("out")?;
        let error_output = ctx.output("error").ok();

        let client = Self::build_client(params).map_err(|e| {
            crate::err::EngineError::invalid_params(
                &self.base.id,
                format!("Failed to build HTTP client: {}", e),
            )
        })?;

        let method = params.method.to_reqwest();

        tracing::info!(
            "HttpSender [{}]: {} {} (concurrency={}, timeout={}ms, retries={})",
            self.base.id,
            method,
            params.url,
            params.concurrency,
            params.timeout_ms,
            params.retry.max_retries,
        );

        if params.concurrency <= 1 {
            // Sequential mode — simple loop
            while ctx.running().await {
                match input.recv().await {
                    Ok(msg) => {
                        let body = Self::extract_body(&msg, &params.body);
                        match Self::send_request(
                            &client,
                            &method,
                            &params.url,
                            body.as_ref(),
                            &params.retry,
                        )
                        .await
                        {
                            Ok(resp) => {
                                output.send(resp).await?;
                            }
                            Err(err_msg) => {
                                tracing::warn!(
                                    "HttpSender [{}]: {}",
                                    self.base.id,
                                    err_msg,
                                );
                                if let Some(ref err_out) = error_output {
                                    err_out
                                        .send(json!({
                                            "error": err_msg,
                                            "original": msg,
                                        }))
                                        .await?;
                                }
                            }
                        }
                    }
                    Err(_) => {
                        tracing::debug!(
                            "HttpSender [{}]: Input channel closed",
                            self.base.id
                        );
                        break;
                    }
                }
            }
        } else {
            // Concurrent mode — use a semaphore to limit in-flight requests
            let semaphore = Arc::new(tokio::sync::Semaphore::new(params.concurrency));
            let client = Arc::new(client);
            let method = Arc::new(method);
            let url = Arc::new(params.url.clone());
            let retry = Arc::new(params.retry.clone());
            let body_mode = Arc::new(params.body.clone());
            let output = Arc::new(output);
            let error_output = Arc::new(error_output);
            let task_id = self.base.id.clone();

            while ctx.running().await {
                match input.recv().await {
                    Ok(msg) => {
                        let permit = semaphore.clone().acquire_owned().await.unwrap();
                        let client = Arc::clone(&client);
                        let method = Arc::clone(&method);
                        let url = Arc::clone(&url);
                        let retry = Arc::clone(&retry);
                        let body_mode = Arc::clone(&body_mode);
                        let output = Arc::clone(&output);
                        let error_output = Arc::clone(&error_output);
                        let task_id = task_id.clone();

                        tokio::spawn(async move {
                            let body = Self::extract_body(&msg, &body_mode);
                            match Self::send_request(
                                &client, &method, &url, body.as_ref(), &retry,
                            )
                            .await
                            {
                                Ok(resp) => {
                                    let _ = output.send(resp).await;
                                }
                                Err(err_msg) => {
                                    tracing::warn!(
                                        "HttpSender [{}]: {}",
                                        task_id,
                                        err_msg,
                                    );
                                    if let Some(ref err_out) = *error_output {
                                        let _ = err_out
                                            .send(json!({
                                                "error": err_msg,
                                                "original": msg,
                                            }))
                                            .await;
                                    }
                                }
                            }
                            drop(permit);
                        });
                    }
                    Err(_) => {
                        tracing::debug!(
                            "HttpSender [{}]: Input channel closed",
                            task_id
                        );
                        break;
                    }
                }
            }

            // Wait for all in-flight requests to finish
            let _ = semaphore.acquire_many(params.concurrency as u32).await;
        }

        tracing::info!("HttpSender [{}]: Finished", self.base.id);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_http_sender_create_minimal() {
        let params = json!({
            "url": "https://example.com/api"
        });
        let result = HttpSender::create("test".into(), params);
        assert!(result.is_ok());
    }

    #[test]
    fn test_http_sender_create_full() {
        let params = json!({
            "url": "https://example.com/api",
            "method": "PUT",
            "headers": { "Authorization": "Bearer abc" },
            "body": "full",
            "timeout_ms": 5000,
            "retry": { "max_retries": 2, "backoff_ms": 500 },
            "concurrency": 8
        });
        let result = HttpSender::create("test".into(), params);
        assert!(result.is_ok());
    }

    #[test]
    fn test_http_sender_create_invalid() {
        let result = HttpSender::create("test".into(), json!({"wrong": true}));
        assert!(result.is_err());
    }

    #[test]
    fn test_http_method_default() {
        let method: HttpMethod = serde_json::from_value(json!("POST")).unwrap();
        assert!(matches!(method, HttpMethod::Post));
    }

    #[test]
    fn test_body_mode_field() {
        let mode: BodyMode = serde_json::from_value(json!({"field": "payload"})).unwrap();
        assert!(matches!(mode, BodyMode::Field(f) if f == "payload"));
    }

    #[test]
    fn test_body_mode_none() {
        let mode: BodyMode = serde_json::from_value(json!("none")).unwrap();
        assert!(matches!(mode, BodyMode::None));
    }

    #[test]
    fn test_extract_body_full() {
        let msg = json!({"a": 1, "b": 2});
        let body = HttpSender::extract_body(&msg, &BodyMode::Full);
        assert_eq!(body, Some(json!({"a": 1, "b": 2})));
    }

    #[test]
    fn test_extract_body_field() {
        let msg = json!({"payload": {"x": 1}, "meta": "info"});
        let body = HttpSender::extract_body(&msg, &BodyMode::Field("payload".into()));
        assert_eq!(body, Some(json!({"x": 1})));
    }

    #[test]
    fn test_extract_body_field_missing() {
        let msg = json!({"a": 1});
        let body = HttpSender::extract_body(&msg, &BodyMode::Field("missing".into()));
        assert_eq!(body, None);
    }

    #[test]
    fn test_extract_body_none() {
        let msg = json!({"a": 1});
        let body = HttpSender::extract_body(&msg, &BodyMode::None);
        assert_eq!(body, None);
    }

    #[test]
    fn test_build_client() {
        let params = Params {
            url: "https://example.com".into(),
            method: HttpMethod::Post,
            headers: HashMap::from([
                ("Content-Type".into(), "application/json".into()),
                ("X-Custom".into(), "value".into()),
            ]),
            body: BodyMode::Full,
            timeout_ms: 5000,
            retry: RetryConfig::default(),
            concurrency: 1,
        };
        let client = HttpSender::build_client(&params);
        assert!(client.is_ok());
    }

    #[test]
    fn test_retry_config_defaults() {
        let cfg = RetryConfig::default();
        assert_eq!(cfg.max_retries, 3);
        assert_eq!(cfg.backoff_ms, 1000);
    }
}
