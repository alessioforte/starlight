use crate::cfg::{
    ArtifactConfig, ResourceCacheConfig, ResourceCachePolicy, ResourceConfig, ResourceFormat,
    ResourceSource, RetryConfig,
};
use crate::err::{EngineError, Result};
use reqwest::Method;
use reqwest::blocking::Client;
use serde_json::Value;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

pub type ResourceMap = HashMap<String, ResourceValue>;
pub type ArtifactMap = HashMap<String, ArtifactConfig>;

const DEFAULT_HTTP_TIMEOUT_MS: u64 = 30_000;

#[derive(Debug, Clone)]
pub enum ResourceValue {
    Json(Arc<Value>),
    Csv(Arc<CsvResource>),
    Text(Arc<String>),
    Bytes(Arc<Vec<u8>>),
}

#[derive(Debug, Clone)]
pub struct CsvResource {
    path: PathBuf,
}

impl CsvResource {
    pub fn path(&self) -> &Path {
        &self.path
    }
}

pub fn load_resources(resources: &[ResourceConfig]) -> Result<ResourceMap> {
    load_resources_with_artifacts(resources, &ArtifactMap::new())
}

pub fn load_resources_with_artifacts(
    resources: &[ResourceConfig],
    artifacts: &ArtifactMap,
) -> Result<ResourceMap> {
    let mut loaded = ResourceMap::new();

    for resource in resources {
        let value = load_resource(resource, artifacts)?;
        if loaded.insert(resource.id.clone(), value).is_some() {
            return Err(EngineError::config(format!(
                "duplicate loaded resource '{}'",
                resource.id
            )));
        }
    }

    Ok(loaded)
}

fn load_resource(resource: &ResourceConfig, artifacts: &ArtifactMap) -> Result<ResourceValue> {
    match &resource.source {
        ResourceSource::File { path, format } => load_file_resource(&resource.id, path, *format),
        ResourceSource::Http {
            url,
            method,
            format,
            timeout_ms,
            retry,
            cache,
        } => load_http_resource(
            &resource.id,
            url,
            method.as_deref(),
            *format,
            *timeout_ms,
            retry.as_ref(),
            cache.as_ref(),
        ),
        ResourceSource::Artifact {
            artifact_ref,
            format,
        } => {
            let artifact = artifacts.get(artifact_ref).ok_or_else(|| {
                EngineError::config(format!(
                    "resource '{}' references unknown artifact '{}'",
                    resource.id, artifact_ref
                ))
            })?;

            if artifact.format != *format {
                return Err(EngineError::config(format!(
                    "resource '{}' expects artifact '{}' as {:?}, but the artifact is declared as {:?}",
                    resource.id, artifact_ref, format, artifact.format
                )));
            }

            load_file_resource(&resource.id, &artifact.path, *format)
        }
    }
}

fn load_http_resource(
    resource_id: &str,
    url: &str,
    method: Option<&str>,
    format: ResourceFormat,
    timeout_ms: Option<u64>,
    retry: Option<&RetryConfig>,
    cache: Option<&ResourceCacheConfig>,
) -> Result<ResourceValue> {
    let cache_policy = cache
        .and_then(|cache| cache.policy)
        .unwrap_or(ResourceCachePolicy::PreferCache);

    if let Some(cache) = cache
        && cache_policy == ResourceCachePolicy::PreferCache
        && cache.path.exists()
    {
        return load_file_resource(resource_id, &cache.path, format);
    }

    let retry = retry.cloned().unwrap_or_default();
    let timeout_ms = timeout_ms.unwrap_or(DEFAULT_HTTP_TIMEOUT_MS);
    let fetched = fetch_http_with_retries(
        resource_id,
        url,
        method.unwrap_or("GET"),
        &retry,
        timeout_ms,
    );

    match fetched {
        Ok(bytes) => {
            let csv_path = if let Some(cache) = cache {
                write_http_cache(resource_id, &cache.path, &bytes)?;
                Some(cache.path.as_path())
            } else {
                None
            };

            parse_bytes_resource(resource_id, url, bytes, format, csv_path)
        }
        Err(err) if cache_policy == ResourceCachePolicy::Refresh => {
            if let Some(cache) = cache
                && cache.path.exists()
            {
                load_file_resource(resource_id, &cache.path, format)
            } else {
                Err(err)
            }
        }
        Err(err) => Err(err),
    }
}

fn load_file_resource(
    resource_id: &str,
    path: &Path,
    format: ResourceFormat,
) -> Result<ResourceValue> {
    match format {
        ResourceFormat::Json => {
            let file =
                std::fs::File::open(path).map_err(|e| resource_io_error(resource_id, path, e))?;
            let value = serde_json::from_reader(file).map_err(|e| {
                EngineError::config(format!(
                    "failed to parse JSON resource '{}' from '{}': {}",
                    resource_id,
                    path.display(),
                    e
                ))
            })?;
            Ok(ResourceValue::Json(Arc::new(value)))
        }
        ResourceFormat::Yaml => {
            let file =
                std::fs::File::open(path).map_err(|e| resource_io_error(resource_id, path, e))?;
            let value = serde_yaml_bw::from_reader(file).map_err(|e| {
                EngineError::config(format!(
                    "failed to parse YAML resource '{}' from '{}': {}",
                    resource_id,
                    path.display(),
                    e
                ))
            })?;
            Ok(ResourceValue::Json(Arc::new(value)))
        }
        ResourceFormat::Csv => {
            std::fs::File::open(path).map_err(|e| resource_io_error(resource_id, path, e))?;
            Ok(ResourceValue::Csv(Arc::new(CsvResource {
                path: path.to_path_buf(),
            })))
        }
        ResourceFormat::Text => {
            let text = std::fs::read_to_string(path)
                .map_err(|e| resource_io_error(resource_id, path, e))?;
            Ok(ResourceValue::Text(Arc::new(text)))
        }
        ResourceFormat::Bytes => {
            let bytes = std::fs::read(path).map_err(|e| resource_io_error(resource_id, path, e))?;
            Ok(ResourceValue::Bytes(Arc::new(bytes)))
        }
    }
}

fn parse_bytes_resource(
    resource_id: &str,
    source: &str,
    bytes: Vec<u8>,
    format: ResourceFormat,
    csv_path: Option<&Path>,
) -> Result<ResourceValue> {
    match format {
        ResourceFormat::Json => {
            let value = serde_json::from_slice(&bytes).map_err(|e| {
                EngineError::config(format!(
                    "failed to parse JSON resource '{}' from '{}': {}",
                    resource_id, source, e
                ))
            })?;
            Ok(ResourceValue::Json(Arc::new(value)))
        }
        ResourceFormat::Yaml => {
            let value = serde_yaml_bw::from_slice(&bytes).map_err(|e| {
                EngineError::config(format!(
                    "failed to parse YAML resource '{}' from '{}': {}",
                    resource_id, source, e
                ))
            })?;
            Ok(ResourceValue::Json(Arc::new(value)))
        }
        ResourceFormat::Csv => {
            let path = csv_path.ok_or_else(|| {
                EngineError::config(format!(
                    "HTTP CSV resource '{}' from '{}' requires cache.path so tasks can access it as a file",
                    resource_id, source
                ))
            })?;
            Ok(ResourceValue::Csv(Arc::new(CsvResource {
                path: path.to_path_buf(),
            })))
        }
        ResourceFormat::Text => {
            let text = String::from_utf8(bytes).map_err(|e| {
                EngineError::config(format!(
                    "failed to parse text resource '{}' from '{}': {}",
                    resource_id, source, e
                ))
            })?;
            Ok(ResourceValue::Text(Arc::new(text)))
        }
        ResourceFormat::Bytes => Ok(ResourceValue::Bytes(Arc::new(bytes))),
    }
}

fn write_http_cache(resource_id: &str, path: &Path, bytes: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| {
            EngineError::config(format!(
                "failed to create cache directory for resource '{}' at '{}': {}",
                resource_id,
                parent.display(),
                e
            ))
        })?;
    }

    std::fs::write(path, bytes).map_err(|e| {
        EngineError::config(format!(
            "failed to write HTTP cache for resource '{}' to '{}': {}",
            resource_id,
            path.display(),
            e
        ))
    })
}

fn fetch_http_with_retries(
    resource_id: &str,
    url: &str,
    method: &str,
    retry: &RetryConfig,
    timeout_ms: u64,
) -> Result<Vec<u8>> {
    let method = validate_http_method(resource_id, method)?;
    let client = Client::builder()
        .timeout(Duration::from_millis(timeout_ms))
        .user_agent("starlight-resource-loader/0.1")
        .build()
        .map_err(|e| {
            EngineError::config(format!(
                "failed to build HTTP client for resource '{}': {}",
                resource_id, e
            ))
        })?;
    let mut attempt = 0;
    let mut backoff = Duration::from_millis(retry.backoff_ms);

    loop {
        match fetch_http_once(&client, resource_id, url, method.clone()) {
            Ok(response) if response.status.is_success() => return Ok(response.body),
            Ok(response) if response.status.is_client_error() => {
                return Err(EngineError::config(format!(
                    "HTTP resource '{}' returned non-retryable status {} from '{}': {}",
                    resource_id,
                    response.status,
                    url,
                    response.body_preview()
                )));
            }
            Ok(response) => {
                if attempt >= retry.max_retries {
                    return Err(EngineError::config(format!(
                        "HTTP resource '{}' returned status {} from '{}' after {} retries: {}",
                        resource_id,
                        response.status,
                        url,
                        retry.max_retries,
                        response.body_preview()
                    )));
                }
            }
            Err(err) => {
                if attempt >= retry.max_retries {
                    return Err(EngineError::config(format!(
                        "failed to fetch HTTP resource '{}' from '{}' after {} retries: {}",
                        resource_id, url, retry.max_retries, err
                    )));
                }
            }
        }

        attempt += 1;
        if !backoff.is_zero() {
            std::thread::sleep(backoff);
            backoff = backoff.saturating_mul(2);
        }
    }
}

fn validate_http_method(resource_id: &str, method: &str) -> Result<Method> {
    let method = method.trim().to_ascii_uppercase();
    Method::from_bytes(method.as_bytes()).map_err(|e| {
        EngineError::config(format!(
            "HTTP resource '{}' has invalid method '{}': {}",
            resource_id, method, e
        ))
    })
}

struct HttpResponse {
    status: reqwest::StatusCode,
    body: Vec<u8>,
}

impl HttpResponse {
    fn body_preview(&self) -> String {
        let text = String::from_utf8_lossy(&self.body);
        text.chars().take(200).collect()
    }
}

fn fetch_http_once(
    client: &Client,
    resource_id: &str,
    url: &str,
    method: Method,
) -> Result<HttpResponse> {
    let response = client.request(method, url).send().map_err(|e| {
        EngineError::config(format!(
            "failed to fetch HTTP resource '{}' from '{}': {}",
            resource_id, url, e
        ))
    })?;
    let status = response.status();
    let body = response.bytes().map_err(|e| {
        EngineError::config(format!(
            "failed to read HTTP resource '{}' from '{}': {}",
            resource_id, url, e
        ))
    })?;

    Ok(HttpResponse {
        status,
        body: body.to_vec(),
    })
}

fn resource_io_error(resource_id: &str, path: &Path, error: std::io::Error) -> EngineError {
    EngineError::config(format!(
        "failed to load resource '{}' from '{}': {}",
        resource_id,
        path.display(),
        error
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cfg::{
        ResourceCacheConfig, ResourceCachePolicy, ResourceConfig, ResourceFormat, ResourceSource,
        RetryConfig,
    };
    use std::fs;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread::{self, JoinHandle};
    use std::time::Duration;

    #[test]
    fn load_json_text_bytes_and_csv_file_resources() {
        let dir = tempfile::tempdir().unwrap();
        let json_path = dir.path().join("config.json");
        let text_path = dir.path().join("notes.txt");
        let bytes_path = dir.path().join("blob.bin");
        let csv_path = dir.path().join("data.csv");

        fs::write(&json_path, r#"{"enabled":true}"#).unwrap();
        fs::write(&text_path, "hello").unwrap();
        fs::write(&bytes_path, [1_u8, 2, 3]).unwrap();
        fs::write(&csv_path, "code,name\nIT,Italy\n").unwrap();

        let resources = load_resources(&[
            file_resource("config", json_path, ResourceFormat::Json),
            file_resource("notes", text_path, ResourceFormat::Text),
            file_resource("blob", bytes_path, ResourceFormat::Bytes),
            file_resource("countries", csv_path.clone(), ResourceFormat::Csv),
        ])
        .unwrap();

        match resources.get("config").unwrap() {
            ResourceValue::Json(value) => assert_eq!(value["enabled"], true),
            _ => panic!("expected json resource"),
        }
        match resources.get("notes").unwrap() {
            ResourceValue::Text(value) => assert_eq!(value.as_str(), "hello"),
            _ => panic!("expected text resource"),
        }
        match resources.get("blob").unwrap() {
            ResourceValue::Bytes(value) => assert_eq!(value.as_slice(), &[1, 2, 3]),
            _ => panic!("expected bytes resource"),
        }
        match resources.get("countries").unwrap() {
            ResourceValue::Csv(value) => assert_eq!(value.path(), csv_path.as_path()),
            _ => panic!("expected csv resource"),
        }
    }

    #[test]
    fn load_yaml_file_resource_as_json_value() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, "enabled: true\n").unwrap();

        let resources =
            load_resources(&[file_resource("config", path, ResourceFormat::Yaml)]).unwrap();

        match resources.get("config").unwrap() {
            ResourceValue::Json(value) => assert_eq!(value["enabled"], true),
            _ => panic!("expected json resource"),
        }
    }

    #[test]
    fn load_artifact_resource_from_declared_artifact_path() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("artifact.csv");
        fs::write(&path, "id,value\n1,10\n").unwrap();

        let mut artifacts = ArtifactMap::new();
        artifacts.insert(
            "raw_csv".to_string(),
            ArtifactConfig {
                id: "raw_csv".to_string(),
                path: path.clone(),
                format: ResourceFormat::Csv,
            },
        );

        let resources = load_resources_with_artifacts(
            &[ResourceConfig {
                id: "raw".to_string(),
                scope: None,
                source: ResourceSource::Artifact {
                    artifact_ref: "raw_csv".to_string(),
                    format: ResourceFormat::Csv,
                },
            }],
            &artifacts,
        )
        .unwrap();

        match resources.get("raw").unwrap() {
            ResourceValue::Csv(value) => assert_eq!(value.path(), path.as_path()),
            _ => panic!("expected csv resource"),
        }
    }

    #[test]
    fn rejects_artifact_resource_with_mismatched_format() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("artifact.csv");
        fs::write(&path, "id,value\n1,10\n").unwrap();

        let mut artifacts = ArtifactMap::new();
        artifacts.insert(
            "raw_csv".to_string(),
            ArtifactConfig {
                id: "raw_csv".to_string(),
                path,
                format: ResourceFormat::Csv,
            },
        );

        let err = load_resources_with_artifacts(
            &[ResourceConfig {
                id: "raw".to_string(),
                scope: None,
                source: ResourceSource::Artifact {
                    artifact_ref: "raw_csv".to_string(),
                    format: ResourceFormat::Json,
                },
            }],
            &artifacts,
        )
        .unwrap_err()
        .to_string();

        assert!(err.contains("expects artifact 'raw_csv' as Json"));
    }

    #[test]
    fn load_http_json_resource_from_local_server() {
        let server = spawn_http_server(vec![http_response(200, r#"{"ok":true}"#)]);
        let hits = Arc::clone(&server.hits);

        let resources = load_resources(&[http_resource(
            "catalog",
            &server.url,
            ResourceFormat::Json,
            None,
            None,
        )])
        .unwrap();

        server.join();
        assert_eq!(hits.load(Ordering::SeqCst), 1);
        match resources.get("catalog").unwrap() {
            ResourceValue::Json(value) => assert_eq!(value["ok"], true),
            _ => panic!("expected json resource"),
        }
    }

    #[test]
    fn http_resource_retries_and_writes_cache() {
        let dir = tempfile::tempdir().unwrap();
        let cache_path = dir.path().join("cache").join("catalog.txt");
        let server = spawn_http_server(vec![
            http_response(500, "temporary error"),
            http_response(200, "fresh catalog"),
        ]);
        let hits = Arc::clone(&server.hits);

        let resources = load_resources(&[http_resource(
            "catalog",
            &server.url,
            ResourceFormat::Text,
            Some(RetryConfig {
                max_retries: 1,
                backoff_ms: 1,
            }),
            Some(ResourceCacheConfig {
                path: cache_path.clone(),
                policy: Some(ResourceCachePolicy::RequireFresh),
            }),
        )])
        .unwrap();

        server.join();
        assert_eq!(hits.load(Ordering::SeqCst), 2);
        assert_eq!(fs::read_to_string(&cache_path).unwrap(), "fresh catalog");
        match resources.get("catalog").unwrap() {
            ResourceValue::Text(value) => assert_eq!(value.as_str(), "fresh catalog"),
            _ => panic!("expected text resource"),
        }
    }

    #[test]
    fn prefer_cache_uses_existing_cache_without_network() {
        let dir = tempfile::tempdir().unwrap();
        let cache_path = dir.path().join("catalog.json");
        fs::write(&cache_path, r#"{"cached":true}"#).unwrap();

        let resources = load_resources(&[http_resource(
            "catalog",
            "http://127.0.0.1:9/not-used",
            ResourceFormat::Json,
            Some(RetryConfig {
                max_retries: 0,
                backoff_ms: 0,
            }),
            Some(ResourceCacheConfig {
                path: cache_path,
                policy: Some(ResourceCachePolicy::PreferCache),
            }),
        )])
        .unwrap();

        match resources.get("catalog").unwrap() {
            ResourceValue::Json(value) => assert_eq!(value["cached"], true),
            _ => panic!("expected json resource"),
        }
    }

    #[test]
    fn refresh_cache_falls_back_when_fetch_fails() {
        let dir = tempfile::tempdir().unwrap();
        let cache_path = dir.path().join("catalog.txt");
        fs::write(&cache_path, "cached catalog").unwrap();
        let url = unused_local_url();

        let resources = load_resources(&[http_resource(
            "catalog",
            &url,
            ResourceFormat::Text,
            Some(RetryConfig {
                max_retries: 0,
                backoff_ms: 0,
            }),
            Some(ResourceCacheConfig {
                path: cache_path,
                policy: Some(ResourceCachePolicy::Refresh),
            }),
        )])
        .unwrap();

        match resources.get("catalog").unwrap() {
            ResourceValue::Text(value) => assert_eq!(value.as_str(), "cached catalog"),
            _ => panic!("expected text resource"),
        }
    }

    #[test]
    fn http_csv_resource_requires_cache_path() {
        let server = spawn_http_server(vec![http_response(200, "id,value\n1,10\n")]);

        let err = load_resources(&[http_resource(
            "rows",
            &server.url,
            ResourceFormat::Csv,
            None,
            None,
        )])
        .unwrap_err()
        .to_string();

        server.join();
        assert!(err.contains("requires cache.path"));
    }

    #[test]
    fn http_csv_resource_uses_cache_path() {
        let dir = tempfile::tempdir().unwrap();
        let cache_path = dir.path().join("rows.csv");
        let server = spawn_http_server(vec![http_response(200, "id,value\n1,10\n")]);

        let resources = load_resources(&[http_resource(
            "rows",
            &server.url,
            ResourceFormat::Csv,
            None,
            Some(ResourceCacheConfig {
                path: cache_path.clone(),
                policy: Some(ResourceCachePolicy::RequireFresh),
            }),
        )])
        .unwrap();

        server.join();
        assert_eq!(fs::read_to_string(&cache_path).unwrap(), "id,value\n1,10\n");
        match resources.get("rows").unwrap() {
            ResourceValue::Csv(value) => assert_eq!(value.path(), cache_path.as_path()),
            _ => panic!("expected csv resource"),
        }
    }

    fn file_resource(id: &str, path: PathBuf, format: ResourceFormat) -> ResourceConfig {
        ResourceConfig {
            id: id.to_string(),
            scope: None,
            source: ResourceSource::File { path, format },
        }
    }

    fn http_resource(
        id: &str,
        url: &str,
        format: ResourceFormat,
        retry: Option<RetryConfig>,
        cache: Option<ResourceCacheConfig>,
    ) -> ResourceConfig {
        ResourceConfig {
            id: id.to_string(),
            scope: None,
            source: ResourceSource::Http {
                url: url.to_string(),
                method: Some("GET".to_string()),
                format,
                timeout_ms: Some(1_000),
                retry,
                cache,
            },
        }
    }

    struct TestServer {
        url: String,
        hits: Arc<AtomicUsize>,
        handle: JoinHandle<()>,
    }

    impl TestServer {
        fn join(self) {
            self.handle.join().unwrap();
        }
    }

    fn spawn_http_server(responses: Vec<String>) -> TestServer {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let hits = Arc::new(AtomicUsize::new(0));
        let thread_hits = Arc::clone(&hits);
        let handle = thread::spawn(move || {
            for response in responses {
                let (mut stream, _) = listener.accept().unwrap();
                thread_hits.fetch_add(1, Ordering::SeqCst);

                let _ = stream.set_read_timeout(Some(Duration::from_secs(1)));
                let mut request = [0; 1024];
                let _ = stream.read(&mut request);
                stream.write_all(response.as_bytes()).unwrap();
            }
        });

        TestServer {
            url: format!("http://{}/resource", addr),
            hits,
            handle,
        }
    }

    fn http_response(status: u16, body: &str) -> String {
        let reason = match status {
            200 => "OK",
            500 => "Internal Server Error",
            _ => "Unknown",
        };

        format!(
            "HTTP/1.1 {} {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            status,
            reason,
            body.len(),
            body
        )
    }

    fn unused_local_url() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        format!("http://127.0.0.1:{}/missing", port)
    }
}
