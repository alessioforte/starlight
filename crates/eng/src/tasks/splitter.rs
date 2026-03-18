//! Splitter Task
//!
//! Routes each incoming message to a named output based on a field value.
//! Supports a configurable default output for unmatched messages.

use crate::ctx::TaskContext;
use crate::err::Result;
use crate::task::{BaseTask, Task, TaskInfo};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// How to determine the output route for a message.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SplitMode {
    /// Use the field value directly as the output label.
    /// E.g. field `"region"` with value `"eu"` → output `"eu"`.
    Value,
    /// Map field values to output labels via an explicit table.
    /// E.g. `{"us": "north_america", "ca": "north_america", "de": "europe"}`.
    Map(HashMap<String, String>),
    /// Route based on ranges (numeric field). Each range maps to an output label.
    /// Ranges are evaluated in order; the first match wins.
    Ranges(Vec<RangeRoute>),
}

/// A numeric range that maps to an output label.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RangeRoute {
    /// Inclusive lower bound (default: -∞)
    #[serde(default)]
    pub min: Option<f64>,
    /// Exclusive upper bound (default: +∞)
    #[serde(default)]
    pub max: Option<f64>,
    /// Output label for this range
    pub output: String,
}

impl RangeRoute {
    fn matches(&self, v: f64) -> bool {
        let above_min = self.min.map_or(true, |lo| v >= lo);
        let below_max = self.max.map_or(true, |hi| v < hi);
        above_min && below_max
    }
}

/// Splitter task parameters.
#[derive(Debug, Serialize, Deserialize)]
pub struct Params {
    /// Field path (dot notation) whose value determines the route.
    pub field: String,
    /// Splitting strategy.
    pub mode: SplitMode,
    /// Output label for messages that don't match any route.
    /// If `None`, unmatched messages are silently dropped.
    #[serde(default)]
    pub default_output: Option<String>,
}

/// Splitter state (stateless).
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct State {}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Resolve a dot-notation path to a nested JSON value.
fn get_nested<'a>(value: &'a Value, path: &str) -> Option<&'a Value> {
    path.split('.').fold(Some(value), |acc, key| {
        acc.and_then(|v| match v {
            Value::Object(map) => map.get(key),
            Value::Array(arr) => key.parse::<usize>().ok().and_then(|i| arr.get(i)),
            _ => None,
        })
    })
}

fn as_f64(v: &Value) -> Option<f64> {
    v.as_f64().or_else(|| v.as_i64().map(|n| n as f64))
}

/// Stringify a JSON value for use as a lookup key / output label.
fn value_to_key(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        _ => v.to_string(),
    }
}

// ---------------------------------------------------------------------------
// Task implementation
// ---------------------------------------------------------------------------

/// Splitter task
///
/// Routes each message to a named output based on a field value.
///
/// # Outputs
///
/// Dynamic — one output per distinct route label. Wire them in the
/// workflow config by name.  An optional `default_output` catches
/// everything that doesn't match.
///
/// # Example Configurations
///
/// ## Value mode
/// ```json
/// {
///   "field": "region",
///   "mode": "value",
///   "default_output": "other"
/// }
/// ```
/// A message `{"region": "eu", ...}` is sent to output `"eu"`.
///
/// ## Map mode
/// ```json
/// {
///   "field": "country",
///   "mode": {
///     "map": {
///       "us": "americas",
///       "ca": "americas",
///       "de": "europe",
///       "fr": "europe"
///     }
///   },
///   "default_output": "rest"
/// }
/// ```
///
/// ## Ranges mode
/// ```json
/// {
///   "field": "score",
///   "mode": {
///     "ranges": [
///       { "max": 50, "output": "low" },
///       { "min": 50, "max": 80, "output": "mid" },
///       { "min": 80, "output": "high" }
///     ]
///   }
/// }
/// ```
pub struct Splitter {
    base: BaseTask<Params, State>,
}

impl Splitter {
    pub fn create(id: String, params: Value) -> Result<Box<dyn Task>> {
        Ok(Box::new(Self {
            base: BaseTask::new(id, params)?,
        }))
    }

    /// Determine the output label for a message.
    fn route(&self, msg: &Value) -> Option<String> {
        let field_val = get_nested(msg, &self.base.params.field)?;

        match &self.base.params.mode {
            SplitMode::Value => Some(value_to_key(field_val)),
            SplitMode::Map(table) => {
                let key = value_to_key(field_val);
                table.get(&key).cloned()
            }
            SplitMode::Ranges(ranges) => {
                let n = as_f64(field_val)?;
                ranges.iter().find(|r| r.matches(n)).map(|r| r.output.clone())
            }
        }
    }
}

#[async_trait]
impl Task for Splitter {
    fn name(&self) -> &str {
        "Splitter"
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
        let mut input = ctx.merged_input().await?;

        // Resolve all available outputs upfront (they must be wired in the workflow config)
        let output_labels = ctx.output_labels();
        let mut outputs = HashMap::new();
        for label in &output_labels {
            if let Ok(out) = ctx.output(label) {
                outputs.insert(label.to_string(), out);
            }
        }

        tracing::info!(
            "Splitter [{}]: field=\"{}\", outputs=[{}], default={:?}",
            self.base.id,
            self.base.params.field,
            output_labels.join(", "),
            self.base.params.default_output,
        );

        let mut routed = 0u64;
        let mut defaulted = 0u64;
        let mut dropped = 0u64;

        while ctx.running().await {
            match input.recv().await {
                Ok(msg) => {
                    let label = self.route(&msg);
                    let target_label = label
                        .as_deref()
                        .or(self.base.params.default_output.as_deref());

                    match target_label {
                        Some(lbl) => {
                            if let Some(out) = outputs.get(lbl) {
                                out.send(msg).await?;
                                if label.is_some() {
                                    routed += 1;
                                } else {
                                    defaulted += 1;
                                }
                            } else {
                                // Output not wired — drop silently
                                dropped += 1;
                            }
                        }
                        None => {
                            dropped += 1;
                        }
                    }

                    let total = routed + defaulted + dropped;
                    if total > 0 && total % 10_000 == 0 {
                        tracing::debug!(
                            "Splitter [{}]: {} routed, {} defaulted, {} dropped",
                            self.base.id,
                            routed,
                            defaulted,
                            dropped,
                        );
                    }
                }
                Err(_) => {
                    tracing::debug!("Splitter [{}]: Input channel closed", self.base.id);
                    break;
                }
            }
        }

        tracing::info!(
            "Splitter [{}]: Finished — {} routed, {} defaulted, {} dropped",
            self.base.id,
            routed,
            defaulted,
            dropped,
        );

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

    fn make_splitter(field: &str, mode: SplitMode, default: Option<&str>) -> Splitter {
        Splitter {
            base: BaseTask {
                id: "test".into(),
                params: Params {
                    field: field.into(),
                    mode,
                    default_output: default.map(Into::into),
                },
                state: State {},
                status: None,
            },
        }
    }

    // --- Value mode ---

    #[test]
    fn test_value_mode_string() {
        let s = make_splitter("region", SplitMode::Value, None);
        assert_eq!(s.route(&json!({"region": "eu"})), Some("eu".into()));
        assert_eq!(s.route(&json!({"region": "us"})), Some("us".into()));
    }

    #[test]
    fn test_value_mode_number() {
        let s = make_splitter("code", SplitMode::Value, None);
        assert_eq!(s.route(&json!({"code": 42})), Some("42".into()));
    }

    #[test]
    fn test_value_mode_bool() {
        let s = make_splitter("active", SplitMode::Value, None);
        assert_eq!(s.route(&json!({"active": true})), Some("true".into()));
    }

    #[test]
    fn test_value_mode_missing_field() {
        let s = make_splitter("missing", SplitMode::Value, None);
        assert_eq!(s.route(&json!({"other": 1})), None);
    }

    #[test]
    fn test_value_mode_nested() {
        let s = make_splitter("meta.type", SplitMode::Value, None);
        assert_eq!(
            s.route(&json!({"meta": {"type": "alert"}})),
            Some("alert".into())
        );
    }

    // --- Map mode ---

    #[test]
    fn test_map_mode_match() {
        let table = HashMap::from([
            ("us".into(), "americas".into()),
            ("ca".into(), "americas".into()),
            ("de".into(), "europe".into()),
        ]);
        let s = make_splitter("country", SplitMode::Map(table), None);
        assert_eq!(s.route(&json!({"country": "us"})), Some("americas".into()));
        assert_eq!(s.route(&json!({"country": "de"})), Some("europe".into()));
    }

    #[test]
    fn test_map_mode_no_match() {
        let table = HashMap::from([("us".into(), "americas".into())]);
        let s = make_splitter("country", SplitMode::Map(table), None);
        assert_eq!(s.route(&json!({"country": "jp"})), None);
    }

    // --- Ranges mode ---

    #[test]
    fn test_ranges_mode() {
        let ranges = vec![
            RangeRoute { min: None, max: Some(50.0), output: "low".into() },
            RangeRoute { min: Some(50.0), max: Some(80.0), output: "mid".into() },
            RangeRoute { min: Some(80.0), max: None, output: "high".into() },
        ];
        let s = make_splitter("score", SplitMode::Ranges(ranges), None);

        assert_eq!(s.route(&json!({"score": 10})), Some("low".into()));
        assert_eq!(s.route(&json!({"score": 49.9})), Some("low".into()));
        assert_eq!(s.route(&json!({"score": 50})), Some("mid".into()));
        assert_eq!(s.route(&json!({"score": 79.9})), Some("mid".into()));
        assert_eq!(s.route(&json!({"score": 80})), Some("high".into()));
        assert_eq!(s.route(&json!({"score": 100})), Some("high".into()));
    }

    #[test]
    fn test_ranges_mode_non_numeric() {
        let ranges = vec![
            RangeRoute { min: None, max: Some(10.0), output: "low".into() },
        ];
        let s = make_splitter("val", SplitMode::Ranges(ranges), None);
        assert_eq!(s.route(&json!({"val": "not a number"})), None);
    }

    #[test]
    fn test_ranges_unbounded() {
        let ranges = vec![
            RangeRoute { min: None, max: None, output: "catch_all".into() },
        ];
        let s = make_splitter("x", SplitMode::Ranges(ranges), None);
        assert_eq!(s.route(&json!({"x": -999})), Some("catch_all".into()));
        assert_eq!(s.route(&json!({"x": 999})), Some("catch_all".into()));
    }

    // --- RangeRoute ---

    #[test]
    fn test_range_route_matches() {
        let r = RangeRoute { min: Some(10.0), max: Some(20.0), output: "a".into() };
        assert!(!r.matches(9.9));
        assert!(r.matches(10.0));
        assert!(r.matches(15.0));
        assert!(!r.matches(20.0)); // max is exclusive
    }

    // --- Factory ---

    #[test]
    fn test_splitter_create_value_mode() {
        let params = json!({
            "field": "type",
            "mode": "value"
        });
        assert!(Splitter::create("test".into(), params).is_ok());
    }

    #[test]
    fn test_splitter_create_map_mode() {
        let params = json!({
            "field": "country",
            "mode": { "map": { "us": "americas", "de": "europe" } },
            "default_output": "other"
        });
        assert!(Splitter::create("test".into(), params).is_ok());
    }

    #[test]
    fn test_splitter_create_ranges_mode() {
        let params = json!({
            "field": "score",
            "mode": { "ranges": [
                { "max": 50, "output": "low" },
                { "min": 50, "output": "high" }
            ]}
        });
        assert!(Splitter::create("test".into(), params).is_ok());
    }

    #[test]
    fn test_splitter_create_invalid() {
        assert!(Splitter::create("test".into(), json!({"wrong": true})).is_err());
    }
}
