use crate::err::{EngineError, Result as EngineResult, WorkflowError};
use serde::de::{self, Deserializer};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

/// Configuration for a workflow
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    pub id: String,
    pub name: String,
    pub description: Option<String>,
    pub channel_buffer_size: Option<usize>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resources: Vec<ResourceConfig>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tasks: Vec<TaskConfig>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub jobs: Vec<JobConfig>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct JobConfig {
    pub id: String,
    pub name: Option<String>,
    pub description: Option<String>,
    #[serde(default)]
    pub on_error: JobErrorPolicy,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resources: Vec<ResourceConfig>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub artifacts: Vec<ArtifactConfig>,
    pub tasks: Vec<TaskConfig>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum JobErrorPolicy {
    Fail,
    Continue,
    ContinueWithWarnings,
}

impl Default for JobErrorPolicy {
    fn default() -> Self {
        Self::Fail
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ResourceConfig {
    pub id: String,
    pub scope: Option<ResourceScope>,
    pub source: ResourceSource,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceScope {
    Workflow,
    Job,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ResourceSource {
    File {
        path: PathBuf,
        format: ResourceFormat,
    },
    Http {
        url: String,
        method: Option<String>,
        format: ResourceFormat,
        timeout_ms: Option<u64>,
        retry: Option<RetryConfig>,
        cache: Option<ResourceCacheConfig>,
    },
    Artifact {
        #[serde(rename = "ref")]
        artifact_ref: String,
        format: ResourceFormat,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceFormat {
    Json,
    Yaml,
    Csv,
    Text,
    Bytes,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RetryConfig {
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
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

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ResourceCacheConfig {
    pub path: PathBuf,
    pub policy: Option<ResourceCachePolicy>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceCachePolicy {
    PreferCache,
    Refresh,
    RequireFresh,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ArtifactConfig {
    pub id: String,
    pub path: PathBuf,
    pub format: ResourceFormat,
}

/// Configuration for a single task in the workflow
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskConfig {
    /// Unique task identifier
    pub id: String,

    /// Task type name (e.g. `"filter"`, `"aggregator"`, `"my_custom_task"`).
    ///
    /// Resolved at build time via the [`TaskRegistry`].
    #[serde(rename = "type")]
    pub kind: String,

    /// Task parameters as JSON
    pub params: Value,

    /// Resource IDs the task expects to access.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub uses: Vec<String>,

    /// Named input ports mapping to upstream channel IDs.
    ///
    /// Key: port name (e.g., `"in"`, `"left"`, `"right"`)
    /// Value: list of channel IDs this port receives from
    ///
    /// Backward compatible: a flat `Vec<String>` is auto-mapped to port `"in"`.
    #[serde(default, deserialize_with = "deserialize_dependencies")]
    pub dependencies: HashMap<String, Vec<String>>,

    /// Map of output labels to downstream channel IDs.
    /// Key: output label (e.g., "out", "error")
    /// Value: list of channel IDs that this output writes to
    pub outputs: HashMap<String, Vec<String>>,
}

impl Config {
    /// Return the job list used internally by the batch-aware config shape.
    ///
    /// Legacy top-level `tasks` configs are represented as a synthetic
    /// `default` job. The runtime still decides separately which normalized
    /// shapes it can execute.
    pub fn normalized_jobs(&self) -> EngineResult<Vec<JobConfig>> {
        self.validate_task_job_layout()?;

        if self.jobs.is_empty() {
            Ok(vec![JobConfig {
                id: "default".to_string(),
                name: None,
                description: None,
                on_error: JobErrorPolicy::default(),
                resources: Vec::new(),
                artifacts: Vec::new(),
                tasks: self.tasks.clone(),
            }])
        } else {
            Ok(self.jobs.clone())
        }
    }

    /// Validate the config-only invariants introduced by the job/resource
    /// schema. Task type and task parameter validation still belongs to the
    /// task registry and workflow builder.
    pub fn validate(&self) -> EngineResult<()> {
        self.validate_workflow_options()?;
        let jobs = self.normalized_jobs()?;
        let workflow_resources = self.validate_workflow_resources()?;
        self.validate_jobs(&jobs, &workflow_resources)
    }

    /// Return the tasks the current flat workflow runtime can execute.
    ///
    /// Phase 1 accepts and validates multi-job configs, but the sequential job
    /// runtime is added in later phases. This prevents mounting a multi-job ETL
    /// config and accidentally running every job as one concurrent DAG.
    pub fn runtime_tasks(&self) -> EngineResult<Vec<TaskConfig>> {
        let jobs = self.runtime_jobs()?;
        if jobs.len() > 1 {
            return Err(invalid_config(
                &self.id,
                "runtime_tasks is only available for single-job workflows",
            ));
        }

        Ok(jobs
            .into_iter()
            .next()
            .map(|job| job.tasks)
            .unwrap_or_default())
    }

    pub fn runtime_jobs(&self) -> EngineResult<Vec<JobConfig>> {
        self.validate()?;
        self.normalized_jobs()
    }

    fn validate_workflow_options(&self) -> EngineResult<()> {
        if self.id.is_empty()
            || matches!(self.id.as_str(), "." | "..")
            || self
                .id
                .chars()
                .any(|ch| ch == '/' || ch == '\\' || ch.is_control())
        {
            return Err(invalid_config(
                &self.id,
                "workflow ID must be a non-empty filesystem-safe path component",
            ));
        }

        if self.channel_buffer_size == Some(0) {
            return Err(invalid_config(
                &self.id,
                "channel_buffer_size must be greater than zero",
            ));
        }

        Ok(())
    }

    fn validate_task_job_layout(&self) -> EngineResult<()> {
        if !self.tasks.is_empty() && !self.jobs.is_empty() {
            return Err(invalid_config(
                &self.id,
                "configs may not define both non-empty top-level tasks and jobs",
            ));
        }

        Ok(())
    }

    fn validate_workflow_resources(&self) -> EngineResult<HashSet<String>> {
        let mut resource_ids = HashSet::new();

        for resource in &self.resources {
            let scope = resource.effective_scope(ResourceScope::Workflow);
            if scope != ResourceScope::Workflow {
                return Err(invalid_config(
                    &self.id,
                    format!(
                        "top-level resource '{}' must have workflow scope",
                        resource.id
                    ),
                ));
            }

            if let ResourceSource::Artifact { artifact_ref, .. } = &resource.source {
                return Err(invalid_config(
                    &self.id,
                    format!(
                        "top-level resource '{}' cannot reference artifact '{}'; artifact resources must be job-scoped",
                        resource.id, artifact_ref
                    ),
                ));
            }

            if !resource_ids.insert(resource.id.clone()) {
                return Err(invalid_config(
                    &self.id,
                    format!("duplicate workflow resource ID '{}'", resource.id),
                ));
            }
        }

        Ok(resource_ids)
    }

    fn validate_jobs(
        &self,
        jobs: &[JobConfig],
        workflow_resources: &HashSet<String>,
    ) -> EngineResult<()> {
        let mut job_ids = HashSet::new();
        let mut task_ids = HashSet::new();
        let mut artifact_ids = HashSet::new();
        let mut available_artifacts = HashMap::new();

        for job in jobs {
            if !job_ids.insert(job.id.clone()) {
                return Err(invalid_config(
                    &self.id,
                    format!("duplicate job ID '{}'", job.id),
                ));
            }

            let job_resources =
                self.validate_job_resources(job, workflow_resources, &available_artifacts)?;
            self.validate_task_ids_and_uses(
                job,
                workflow_resources,
                &job_resources,
                &mut task_ids,
            )?;
            self.validate_job_channels(job)?;
            self.validate_job_artifacts(job, &mut artifact_ids)?;

            for artifact in &job.artifacts {
                available_artifacts.insert(artifact.id.clone(), artifact.format);
            }
        }

        Ok(())
    }

    fn validate_job_resources(
        &self,
        job: &JobConfig,
        workflow_resources: &HashSet<String>,
        available_artifacts: &HashMap<String, ResourceFormat>,
    ) -> EngineResult<HashSet<String>> {
        let mut job_resources = HashSet::new();

        for resource in &job.resources {
            let scope = resource.effective_scope(ResourceScope::Job);
            if scope != ResourceScope::Job {
                return Err(invalid_config(
                    &self.id,
                    format!(
                        "resource '{}' in job '{}' must have job scope",
                        resource.id, job.id
                    ),
                ));
            }

            if workflow_resources.contains(&resource.id) {
                return Err(invalid_config(
                    &self.id,
                    format!(
                        "resource '{}' in job '{}' shadows a workflow resource",
                        resource.id, job.id
                    ),
                ));
            }

            if !job_resources.insert(resource.id.clone()) {
                return Err(invalid_config(
                    &self.id,
                    format!(
                        "duplicate resource ID '{}' in job '{}'",
                        resource.id, job.id
                    ),
                ));
            }

            if let ResourceSource::Artifact {
                artifact_ref,
                format,
            } = &resource.source
            {
                let Some(artifact_format) = available_artifacts.get(artifact_ref) else {
                    return Err(invalid_config(
                        &self.id,
                        format!(
                            "resource '{}' in job '{}' references artifact '{}' which was not produced by an earlier job",
                            resource.id, job.id, artifact_ref
                        ),
                    ));
                };

                if format != artifact_format {
                    return Err(invalid_config(
                        &self.id,
                        format!(
                            "resource '{}' in job '{}' references artifact '{}' as {:?}, but the artifact is declared as {:?}",
                            resource.id, job.id, artifact_ref, format, artifact_format
                        ),
                    ));
                }
            }
        }

        Ok(job_resources)
    }

    fn validate_task_ids_and_uses(
        &self,
        job: &JobConfig,
        workflow_resources: &HashSet<String>,
        job_resources: &HashSet<String>,
        task_ids: &mut HashSet<String>,
    ) -> EngineResult<()> {
        let mut visible_resources = workflow_resources.clone();
        visible_resources.extend(job_resources.iter().cloned());

        for task in &job.tasks {
            if !task_ids.insert(task.id.clone()) {
                return Err(invalid_config(
                    &self.id,
                    format!("duplicate task ID '{}'", task.id),
                ));
            }

            for resource_id in &task.uses {
                if !visible_resources.contains(resource_id) {
                    return Err(invalid_config(
                        &self.id,
                        format!(
                            "task '{}' in job '{}' uses resource '{}' which is not visible to that task",
                            task.id, job.id, resource_id
                        ),
                    ));
                }
            }
        }

        Ok(())
    }

    fn validate_job_channels(&self, job: &JobConfig) -> EngineResult<()> {
        let mut available_channels = HashSet::new();

        for task in &job.tasks {
            for channel_ids in task.outputs.values() {
                for channel_id in channel_ids {
                    available_channels.insert(channel_id.as_str());
                }
            }
        }

        for task in &job.tasks {
            for (port_name, channel_ids) in &task.dependencies {
                for channel_id in channel_ids {
                    if !available_channels.contains(channel_id.as_str()) {
                        return Err(invalid_config(
                            &self.id,
                            format!(
                                "task '{}' in job '{}' port '{}' depends on channel '{}' which is not produced inside the same job",
                                task.id, job.id, port_name, channel_id
                            ),
                        ));
                    }
                }
            }
        }

        Ok(())
    }

    fn validate_job_artifacts(
        &self,
        job: &JobConfig,
        artifact_ids: &mut HashSet<String>,
    ) -> EngineResult<()> {
        for artifact in &job.artifacts {
            if !artifact_ids.insert(artifact.id.clone()) {
                return Err(invalid_config(
                    &self.id,
                    format!("duplicate artifact ID '{}'", artifact.id),
                ));
            }
        }

        Ok(())
    }
}

impl ResourceConfig {
    pub fn effective_scope(&self, default: ResourceScope) -> ResourceScope {
        self.scope.unwrap_or(default)
    }
}

fn invalid_config(workflow_id: &str, message: impl Into<String>) -> EngineError {
    EngineError::Workflow(WorkflowError::InvalidConfig(
        workflow_id.to_string(),
        message.into(),
    ))
}

/// Custom deserializer: accepts both `Vec<String>` (legacy) and `HashMap<String, Vec<String>>`.
///
/// A flat list like `["ch1", "ch2"]` is auto-mapped to the default port `"in"`.
fn deserialize_dependencies<'de, D>(
    deserializer: D,
) -> Result<HashMap<String, Vec<String>>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum RawDeps {
        Flat(Vec<String>),
        Named(HashMap<String, Vec<String>>),
    }

    match RawDeps::deserialize(deserializer) {
        Ok(RawDeps::Flat(channels)) => {
            if channels.is_empty() {
                Ok(HashMap::new())
            } else {
                let mut map = HashMap::new();
                map.insert("in".to_string(), channels);
                Ok(map)
            }
        }
        Ok(RawDeps::Named(map)) => Ok(map),
        Err(e) => Err(de::Error::custom(format!(
            "dependencies must be a list of channel IDs or a map of port names to channel IDs: {}",
            e
        ))),
    }
}

impl TaskConfig {
    /// Create a new task configuration
    pub fn new(id: impl Into<String>, kind: impl Into<String>, params: Value) -> Self {
        Self {
            id: id.into(),
            kind: kind.into(),
            params,
            uses: Vec::new(),
            dependencies: HashMap::new(),
            outputs: HashMap::new(),
        }
    }

    /// Add a dependency on the default `"in"` port.
    pub fn with_dependency(mut self, channel_id: impl Into<String>) -> Self {
        self.dependencies
            .entry("in".to_string())
            .or_default()
            .push(channel_id.into());
        self
    }

    /// Add multiple dependencies on the default `"in"` port.
    pub fn with_dependencies(mut self, channel_ids: Vec<String>) -> Self {
        self.dependencies
            .entry("in".to_string())
            .or_default()
            .extend(channel_ids);
        self
    }

    /// Add a dependency on a specific named input port.
    pub fn with_input(mut self, port: impl Into<String>, channel_id: impl Into<String>) -> Self {
        self.dependencies
            .entry(port.into())
            .or_default()
            .push(channel_id.into());
        self
    }

    /// Add multiple dependencies on a specific named input port.
    pub fn with_inputs(mut self, port: impl Into<String>, channel_ids: Vec<String>) -> Self {
        self.dependencies
            .entry(port.into())
            .or_default()
            .extend(channel_ids);
        self
    }

    /// Add an output mapping
    pub fn with_output(mut self, label: impl Into<String>, targets: Vec<String>) -> Self {
        self.outputs.insert(label.into(), targets);
        self
    }

    /// Add multiple outputs
    pub fn with_outputs(mut self, outputs: HashMap<String, Vec<String>>) -> Self {
        self.outputs.extend(outputs);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_deserialize_flat_dependencies() {
        let json = json!({
            "id": "t1",
            "type": "filter",
            "params": {},
            "dependencies": ["ch1", "ch2"],
            "outputs": {}
        });
        let cfg: TaskConfig = serde_json::from_value(json).unwrap();
        assert_eq!(cfg.dependencies.len(), 1);
        assert_eq!(cfg.dependencies["in"], vec!["ch1", "ch2"]);
    }

    #[test]
    fn test_deserialize_named_dependencies() {
        let json = json!({
            "id": "t1",
            "type": "join",
            "params": {},
            "dependencies": {
                "left": ["ch_csv"],
                "right": ["ch_api"]
            },
            "outputs": {}
        });
        let cfg: TaskConfig = serde_json::from_value(json).unwrap();
        assert_eq!(cfg.dependencies.len(), 2);
        assert_eq!(cfg.dependencies["left"], vec!["ch_csv"]);
        assert_eq!(cfg.dependencies["right"], vec!["ch_api"]);
    }

    #[test]
    fn test_deserialize_empty_dependencies() {
        let json = json!({
            "id": "t1",
            "type": "timer",
            "params": {},
            "dependencies": [],
            "outputs": {}
        });
        let cfg: TaskConfig = serde_json::from_value(json).unwrap();
        assert!(cfg.dependencies.is_empty());
    }

    #[test]
    fn test_with_dependency_defaults_to_in() {
        let cfg = TaskConfig::new("t1", "filter", json!({}))
            .with_dependency("ch1")
            .with_dependency("ch2");
        assert_eq!(cfg.dependencies["in"], vec!["ch1", "ch2"]);
    }

    #[test]
    fn test_with_input_named_port() {
        let cfg = TaskConfig::new("t1", "join", json!({}))
            .with_input("left", "ch_csv")
            .with_input("right", "ch_api");
        assert_eq!(cfg.dependencies["left"], vec!["ch_csv"]);
        assert_eq!(cfg.dependencies["right"], vec!["ch_api"]);
    }

    #[test]
    fn test_mixed_default_and_named() {
        let cfg = TaskConfig::new("t1", "enricher", json!({}))
            .with_dependency("ch_main")
            .with_input("lookup", "ch_ref");
        assert_eq!(cfg.dependencies["in"], vec!["ch_main"]);
        assert_eq!(cfg.dependencies["lookup"], vec!["ch_ref"]);
    }

    #[test]
    fn test_legacy_tasks_normalize_to_default_job() {
        let json = json!({
            "id": "stream",
            "name": "Stream",
            "tasks": [
                {
                    "id": "log",
                    "type": "logger",
                    "params": {},
                    "dependencies": [],
                    "outputs": {}
                }
            ]
        });

        let cfg: Config = serde_json::from_value(json).unwrap();
        cfg.validate().unwrap();

        let jobs = cfg.normalized_jobs().unwrap();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].id, "default");
        assert_eq!(jobs[0].tasks[0].id, "log");
    }

    #[test]
    fn test_deserialize_job_resources_artifacts_and_task_uses() {
        let json = json!({
            "id": "etl",
            "name": "ETL",
            "resources": [
                {
                    "id": "catalog",
                    "source": {
                        "type": "http",
                        "url": "http://localhost:9000/catalog",
                        "format": "json",
                        "timeout_ms": 5000,
                        "retry": { "max_retries": 2 },
                        "cache": {
                            "path": ".starlight/engine/cache/catalog.json",
                            "policy": "prefer_cache"
                        }
                    }
                }
            ],
            "jobs": [
                {
                    "id": "countries",
                    "resources": [
                        {
                            "id": "aliases",
                            "source": {
                                "type": "file",
                                "path": "data/config/country_aliases.json",
                                "format": "json"
                            }
                        }
                    ],
                    "artifacts": [
                        {
                            "id": "countries_csv",
                            "path": "data/countries.csv",
                            "format": "csv"
                        }
                    ],
                    "tasks": [
                        {
                            "id": "merge",
                            "type": "dummy",
                            "params": {},
                            "uses": ["catalog", "aliases"],
                            "dependencies": [],
                            "outputs": {}
                        }
                    ]
                },
                {
                    "id": "load",
                    "resources": [
                        {
                            "id": "countries",
                            "source": {
                                "type": "artifact",
                                "ref": "countries_csv",
                                "format": "csv"
                            }
                        }
                    ],
                    "tasks": [
                        {
                            "id": "consume",
                            "type": "dummy",
                            "params": {},
                            "uses": ["catalog", "countries"],
                            "dependencies": [],
                            "outputs": {}
                        }
                    ]
                }
            ]
        });

        let cfg: Config = serde_json::from_value(json).unwrap();
        cfg.validate().unwrap();

        assert_eq!(cfg.resources.len(), 1);
        match &cfg.resources[0].source {
            ResourceSource::Http {
                timeout_ms,
                retry,
                cache,
                ..
            } => {
                assert_eq!(*timeout_ms, Some(5000));
                assert_eq!(retry.as_ref().unwrap().max_retries, 2);
                assert_eq!(retry.as_ref().unwrap().backoff_ms, 1000);
                assert_eq!(
                    cache.as_ref().unwrap().policy,
                    Some(ResourceCachePolicy::PreferCache)
                );
            }
            _ => panic!("expected http resource"),
        }

        assert_eq!(cfg.jobs[0].on_error, JobErrorPolicy::Fail);
        assert_eq!(cfg.jobs[0].tasks[0].uses, vec!["catalog", "aliases"]);
    }

    #[test]
    fn test_rejects_non_empty_tasks_and_jobs() {
        let json = json!({
            "id": "bad",
            "name": "Bad",
            "tasks": [
                {
                    "id": "top",
                    "type": "dummy",
                    "params": {},
                    "dependencies": [],
                    "outputs": {}
                }
            ],
            "jobs": [
                {
                    "id": "job",
                    "tasks": [
                        {
                            "id": "nested",
                            "type": "dummy",
                            "params": {},
                            "dependencies": [],
                            "outputs": {}
                        }
                    ]
                }
            ]
        });

        let cfg: Config = serde_json::from_value(json).unwrap();
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("top-level tasks and jobs"));
    }

    #[test]
    fn test_rejects_duplicate_job_ids() {
        let json = json!({
            "id": "bad",
            "name": "Bad",
            "jobs": [
                { "id": "extract", "tasks": [] },
                { "id": "extract", "tasks": [] }
            ]
        });

        let cfg: Config = serde_json::from_value(json).unwrap();
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("duplicate job ID 'extract'"));
    }

    #[test]
    fn test_rejects_job_resource_shadowing_workflow_resource() {
        let json = json!({
            "id": "bad",
            "name": "Bad",
            "resources": [
                {
                    "id": "catalog",
                    "source": {
                        "type": "file",
                        "path": "data/catalog.json",
                        "format": "json"
                    }
                }
            ],
            "jobs": [
                {
                    "id": "extract",
                    "resources": [
                        {
                            "id": "catalog",
                            "source": {
                                "type": "file",
                                "path": "data/local_catalog.json",
                                "format": "json"
                            }
                        }
                    ],
                    "tasks": []
                }
            ]
        });

        let cfg: Config = serde_json::from_value(json).unwrap();
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("shadows a workflow resource"));
    }

    #[test]
    fn test_rejects_artifact_resource_from_later_job() {
        let json = json!({
            "id": "bad",
            "name": "Bad",
            "jobs": [
                {
                    "id": "load",
                    "resources": [
                        {
                            "id": "countries",
                            "source": {
                                "type": "artifact",
                                "ref": "countries_csv",
                                "format": "csv"
                            }
                        }
                    ],
                    "tasks": []
                },
                {
                    "id": "countries",
                    "artifacts": [
                        {
                            "id": "countries_csv",
                            "path": "data/countries.csv",
                            "format": "csv"
                        }
                    ],
                    "tasks": []
                }
            ]
        });

        let cfg: Config = serde_json::from_value(json).unwrap();
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("was not produced by an earlier job"));
    }

    #[test]
    fn test_rejects_artifact_resource_format_mismatch() {
        let json = json!({
            "id": "bad",
            "name": "Bad",
            "jobs": [
                {
                    "id": "extract",
                    "artifacts": [
                        {
                            "id": "raw_csv",
                            "path": "data/raw.csv",
                            "format": "csv"
                        }
                    ],
                    "tasks": []
                },
                {
                    "id": "load",
                    "resources": [
                        {
                            "id": "raw",
                            "source": {
                                "type": "artifact",
                                "ref": "raw_csv",
                                "format": "json"
                            }
                        }
                    ],
                    "tasks": []
                }
            ]
        });

        let cfg: Config = serde_json::from_value(json).unwrap();
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("artifact 'raw_csv' as Json"));
    }

    #[test]
    fn test_rejects_task_uses_resource_not_visible_to_job() {
        let json = json!({
            "id": "bad",
            "name": "Bad",
            "jobs": [
                {
                    "id": "extract",
                    "tasks": [
                        {
                            "id": "task",
                            "type": "dummy",
                            "params": {},
                            "uses": ["missing"],
                            "dependencies": [],
                            "outputs": {}
                        }
                    ]
                }
            ]
        });

        let cfg: Config = serde_json::from_value(json).unwrap();
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("uses resource 'missing'"));
    }

    #[test]
    fn test_rejects_cross_job_channel_dependency() {
        let json = json!({
            "id": "bad",
            "name": "Bad",
            "jobs": [
                {
                    "id": "extract",
                    "tasks": [
                        {
                            "id": "source",
                            "type": "dummy",
                            "params": {},
                            "dependencies": [],
                            "outputs": { "out": ["rows"] }
                        }
                    ]
                },
                {
                    "id": "load",
                    "tasks": [
                        {
                            "id": "sink",
                            "type": "dummy",
                            "params": {},
                            "dependencies": ["rows"],
                            "outputs": {}
                        }
                    ]
                }
            ]
        });

        let cfg: Config = serde_json::from_value(json).unwrap();
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("not produced inside the same job"));
    }

    #[test]
    fn test_runtime_tasks_rejects_multi_job_configs() {
        let json = json!({
            "id": "etl",
            "name": "ETL",
            "jobs": [
                { "id": "first", "tasks": [] },
                { "id": "second", "tasks": [] }
            ]
        });

        let cfg: Config = serde_json::from_value(json).unwrap();
        cfg.validate().unwrap();

        let err = cfg.runtime_tasks().unwrap_err().to_string();
        assert!(err.contains("runtime_tasks is only available for single-job workflows"));
    }

    #[test]
    fn test_runtime_jobs_accepts_multi_job_configs() {
        let json = json!({
            "id": "etl",
            "name": "ETL",
            "jobs": [
                { "id": "first", "tasks": [] },
                { "id": "second", "tasks": [] }
            ]
        });

        let cfg: Config = serde_json::from_value(json).unwrap();
        let jobs = cfg.runtime_jobs().unwrap();
        assert_eq!(jobs.len(), 2);
        assert_eq!(jobs[0].id, "first");
        assert_eq!(jobs[1].id, "second");
    }
}
