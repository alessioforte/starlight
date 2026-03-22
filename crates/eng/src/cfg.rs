use serde::de::{self, Deserializer};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

/// Configuration for a workflow
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    pub id: String,
    pub name: String,
    pub description: Option<String>,
    pub channel_buffer_size: Option<usize>,
    pub tasks: Vec<TaskConfig>,
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
}
