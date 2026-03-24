//! Aggregator Task
//!
//! Collects incoming messages and emits aggregated results based on
//! count windows, time windows, or both. Optionally groups by a field.

use crate::ctx::TaskContext;
use crate::err::Result;
use crate::task::{BaseTask, Task, TaskInfo};
use async_trait::async_trait;
use jb::{as_f64, get};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Aggregation function applied to a numeric field.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AggFn {
    /// Number of messages in the window
    Count,
    /// Sum of a numeric field
    Sum,
    /// Arithmetic mean
    Avg,
    /// Minimum value
    Min,
    /// Maximum value
    Max,
    /// Collect all values into an array
    Collect,
}

/// A single aggregation to compute.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggColumn {
    /// Field path (dot notation) to aggregate
    pub field: String,
    /// Aggregation function
    #[serde(rename = "fn")]
    pub func: AggFn,
    /// Output name in the emitted object (defaults to `"field_fn"`)
    #[serde(default)]
    pub alias: Option<String>,
}

impl AggColumn {
    fn output_name(&self) -> String {
        self.alias
            .clone()
            .unwrap_or_else(|| format!("{}_{}", self.field, self.func_label()))
    }

    fn func_label(&self) -> &str {
        match self.func {
            AggFn::Count => "count",
            AggFn::Sum => "sum",
            AggFn::Avg => "avg",
            AggFn::Min => "min",
            AggFn::Max => "max",
            AggFn::Collect => "collect",
        }
    }
}

/// Aggregator task parameters.
#[derive(Debug, Serialize, Deserialize)]
pub struct Params {
    /// Aggregations to compute
    pub columns: Vec<AggColumn>,
    /// Emit after receiving this many messages (per group). `null` = no count trigger.
    #[serde(default)]
    pub window_count: Option<usize>,
    /// Emit after this many milliseconds since the first message in the window.
    /// `null` = no time trigger.
    #[serde(default)]
    pub window_ms: Option<u64>,
    /// Optional field path to group by. Each distinct value gets its own window.
    #[serde(default)]
    pub group_by: Option<String>,
}

/// Aggregator state (stateless across restarts — windows are in-memory only).
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct State {}

// ---------------------------------------------------------------------------
// Window accumulator
// ---------------------------------------------------------------------------

/// Tracks running aggregation state for one window (or one group).
#[derive(Debug)]
struct Window {
    count: u64,
    sums: HashMap<String, f64>,
    mins: HashMap<String, f64>,
    maxs: HashMap<String, f64>,
    collections: HashMap<String, Vec<Value>>,
    opened_at: Instant,
}

impl Window {
    fn new() -> Self {
        Self {
            count: 0,
            sums: HashMap::new(),
            mins: HashMap::new(),
            maxs: HashMap::new(),
            collections: HashMap::new(),
            opened_at: Instant::now(),
        }
    }

    /// Ingest one message into this window.
    fn push(&mut self, msg: &Value, columns: &[AggColumn]) {
        self.count += 1;

        // Collect the distinct fields we need numeric values for
        // to avoid double-accumulation when multiple columns reference the same field.
        let mut seen_sum = std::collections::HashSet::new();
        let mut seen_min = std::collections::HashSet::new();
        let mut seen_max = std::collections::HashSet::new();
        let mut seen_collect = std::collections::HashSet::new();

        for col in columns {
            match col.func {
                AggFn::Count => {}
                AggFn::Sum | AggFn::Avg => {
                    if seen_sum.insert(&col.field) {
                        if let Some(n) = get(msg, &col.field).and_then(as_f64) {
                            *self.sums.entry(col.field.clone()).or_default() += n;
                        }
                    }
                }
                AggFn::Min => {
                    if seen_min.insert(&col.field) {
                        if let Some(n) = get(msg, &col.field).and_then(as_f64) {
                            let entry = self.mins.entry(col.field.clone()).or_insert(f64::INFINITY);
                            if n < *entry {
                                *entry = n;
                            }
                        }
                    }
                }
                AggFn::Max => {
                    if seen_max.insert(&col.field) {
                        if let Some(n) = get(msg, &col.field).and_then(as_f64) {
                            let entry = self
                                .maxs
                                .entry(col.field.clone())
                                .or_insert(f64::NEG_INFINITY);
                            if n > *entry {
                                *entry = n;
                            }
                        }
                    }
                }
                AggFn::Collect => {
                    if seen_collect.insert(&col.field) {
                        if let Some(v) = get(msg, &col.field) {
                            self.collections
                                .entry(col.field.clone())
                                .or_default()
                                .push(v.clone());
                        }
                    }
                }
            }
        }
    }

    /// Build the output JSON object from the accumulated state.
    fn emit(&self, columns: &[AggColumn]) -> Value {
        let mut obj = serde_json::Map::new();
        for col in columns {
            let key = col.output_name();
            let val = match col.func {
                AggFn::Count => json!(self.count),
                AggFn::Sum => {
                    let s = self.sums.get(&col.field).copied().unwrap_or(0.0);
                    json!(s)
                }
                AggFn::Avg => {
                    let s = self.sums.get(&col.field).copied().unwrap_or(0.0);
                    if self.count > 0 {
                        json!(s / self.count as f64)
                    } else {
                        json!(null)
                    }
                }
                AggFn::Min => match self.mins.get(&col.field) {
                    Some(v) if v.is_finite() => json!(v),
                    _ => json!(null),
                },
                AggFn::Max => match self.maxs.get(&col.field) {
                    Some(v) if v.is_finite() => json!(v),
                    _ => json!(null),
                },
                AggFn::Collect => {
                    let arr = self
                        .collections
                        .get(&col.field)
                        .cloned()
                        .unwrap_or_default();
                    json!(arr)
                }
            };
            obj.insert(key, val);
        }
        Value::Object(obj)
    }

    /// Whether the count window has been reached.
    fn count_ready(&self, limit: Option<usize>) -> bool {
        limit.is_some_and(|n| self.count as usize >= n)
    }

    /// Whether the time window has elapsed.
    fn time_ready(&self, limit: Option<Duration>) -> bool {
        limit.is_some_and(|d| self.opened_at.elapsed() >= d)
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Extract the group key from a message (stringified for HashMap key).
fn group_key(msg: &Value, group_by: &Option<String>) -> String {
    match group_by {
        Some(path) => match get(msg, path) {
            Some(Value::String(s)) => s.clone(),
            Some(v) => v.to_string(),
            None => "__null__".to_string(),
        },
        None => "__all__".to_string(),
    }
}

// ---------------------------------------------------------------------------
// Task implementation
// ---------------------------------------------------------------------------

/// Aggregator task
///
/// Collects messages into windows and emits aggregated results.
///
/// Windows close (and emit) when **either** trigger fires:
/// - `window_count` messages received (per group)
/// - `window_ms` milliseconds since the window opened (per group)
///
/// At least one of `window_count` or `window_ms` must be set.
///
/// # Outputs
///
/// | Label   | Description                |
/// |---------|----------------------------|
/// | `"out"` | Aggregated result objects  |
///
/// # Example Configuration
///
/// ```json
/// {
///   "columns": [
///     { "field": "value", "fn": "avg" },
///     { "field": "value", "fn": "min" },
///     { "field": "value", "fn": "max" },
///     { "field": "value", "fn": "count" }
///   ],
///   "window_count": 100,
///   "window_ms": 5000,
///   "group_by": "category"
/// }
/// ```
pub struct Aggregator {
    base: BaseTask<Params, State>,
}

impl Aggregator {
    pub fn create(id: String, params: Value) -> Result<Box<dyn Task>> {
        Ok(Box::new(Self {
            base: BaseTask::new(id, params)?,
        }))
    }

    /// Emit a window's aggregation result and reset it.
    async fn flush_window(
        window: &Window,
        group_key: &str,
        group_by: &Option<String>,
        columns: &[AggColumn],
        output: &crate::ctx::Output,
    ) -> Result<()> {
        let mut result = window.emit(columns);
        // Attach the group key to the output when grouping
        if let (Some(field), Value::Object(map)) = (group_by.as_ref(), &mut result) {
            if group_key != "__all__" {
                map.insert(field.clone(), json!(group_key));
            }
        }
        output.send(result.into()).await
    }
}

#[async_trait]
impl Task for Aggregator {
    fn name(&self) -> &str {
        "Aggregator"
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

        if params.window_count.is_none() && params.window_ms.is_none() {
            return Err(crate::err::EngineError::invalid_params(
                &self.base.id,
                "At least one of window_count or window_ms must be set".to_string(),
            ));
        }

        let mut input = ctx.merged_input().await?;
        let output = ctx.output("out")?;

        let time_limit = params.window_ms.map(Duration::from_millis);
        let tick_interval = time_limit.map(|d| d.min(Duration::from_millis(100)));

        tracing::info!(
            "Aggregator [{}]: window_count={:?}, window_ms={:?}, group_by={:?}, columns={}",
            self.base.id,
            params.window_count,
            params.window_ms,
            params.group_by,
            params.columns.len(),
        );

        let mut windows: HashMap<String, Window> = HashMap::new();
        let mut emitted = 0u64;

        loop {
            // If time-based windows are enabled, use a timeout on recv
            let recv_result = if let Some(tick) = tick_interval {
                tokio::select! {
                    biased;
                    r = input.recv() => Some(r),
                    _ = tokio::time::sleep(tick) => None,
                }
            } else {
                Some(input.recv().await)
            };

            // Check if we should still be running
            if !ctx.is_running() {
                // Stopped — flush remaining windows below
                break;
            }

            match recv_result {
                // Got a message
                Some(Ok(msg)) => {
                    let key = group_key(&msg, &params.group_by);
                    let window = windows.entry(key.clone()).or_insert_with(Window::new);
                    window.push(&msg, &params.columns);

                    // Check count trigger
                    if window.count_ready(params.window_count) || window.time_ready(time_limit) {
                        Self::flush_window(
                            window,
                            &key,
                            &params.group_by,
                            &params.columns,
                            &output,
                        )
                        .await?;
                        emitted += 1;
                        windows.remove(&key);
                    }
                }
                // Input channel closed
                Some(Err(_)) => {
                    tracing::debug!("Aggregator [{}]: Input channel closed", self.base.id);
                    break;
                }
                // Tick timeout — check time-based windows
                None => {
                    let mut to_flush = Vec::new();
                    for (key, window) in &windows {
                        if window.time_ready(time_limit) {
                            to_flush.push(key.clone());
                        }
                    }
                    for key in to_flush {
                        if let Some(window) = windows.remove(&key) {
                            Self::flush_window(
                                &window,
                                &key,
                                &params.group_by,
                                &params.columns,
                                &output,
                            )
                            .await?;
                            emitted += 1;
                        }
                    }

                    // Re-check running state
                    if !ctx.is_running() {
                        break;
                    }
                }
            }
        }

        // Flush any remaining open windows
        for (key, window) in &windows {
            if window.count > 0 {
                Self::flush_window(window, key, &params.group_by, &params.columns, &output).await?;
                emitted += 1;
            }
        }

        tracing::info!(
            "Aggregator [{}]: Finished — emitted {} aggregations",
            self.base.id,
            emitted,
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

    // --- Window unit tests ---

    fn test_columns() -> Vec<AggColumn> {
        vec![
            AggColumn {
                field: "value".into(),
                func: AggFn::Sum,
                alias: None,
            },
            AggColumn {
                field: "value".into(),
                func: AggFn::Avg,
                alias: None,
            },
            AggColumn {
                field: "value".into(),
                func: AggFn::Min,
                alias: None,
            },
            AggColumn {
                field: "value".into(),
                func: AggFn::Max,
                alias: None,
            },
            AggColumn {
                field: "value".into(),
                func: AggFn::Count,
                alias: Some("cnt".into()),
            },
        ]
    }

    #[test]
    fn test_window_single_message() {
        let cols = test_columns();
        let mut w = Window::new();
        w.push(&json!({"value": 10}), &cols);

        let out = w.emit(&cols);
        assert_eq!(out["value_sum"], 10.0);
        assert_eq!(out["value_avg"], 10.0);
        assert_eq!(out["value_min"], 10.0);
        assert_eq!(out["value_max"], 10.0);
        assert_eq!(out["cnt"], 1);
    }

    #[test]
    fn test_window_multiple_messages() {
        let cols = test_columns();
        let mut w = Window::new();
        for v in [10, 20, 30] {
            w.push(&json!({"value": v}), &cols);
        }

        let out = w.emit(&cols);
        assert_eq!(out["value_sum"], 60.0);
        assert_eq!(out["value_avg"], 20.0);
        assert_eq!(out["value_min"], 10.0);
        assert_eq!(out["value_max"], 30.0);
        assert_eq!(out["cnt"], 3);
    }

    #[test]
    fn test_window_collect() {
        let cols = vec![AggColumn {
            field: "tag".into(),
            func: AggFn::Collect,
            alias: None,
        }];
        let mut w = Window::new();
        w.push(&json!({"tag": "a"}), &cols);
        w.push(&json!({"tag": "b"}), &cols);
        w.push(&json!({"tag": "a"}), &cols);

        let out = w.emit(&cols);
        assert_eq!(out["tag_collect"], json!(["a", "b", "a"]));
    }

    #[test]
    fn test_window_missing_field() {
        let cols = vec![AggColumn {
            field: "value".into(),
            func: AggFn::Sum,
            alias: None,
        }];
        let mut w = Window::new();
        w.push(&json!({"other": 10}), &cols);
        w.push(&json!({"value": 5}), &cols);

        let out = w.emit(&cols);
        assert_eq!(out["value_sum"], 5.0);
    }

    #[test]
    fn test_window_count_ready() {
        let cols = test_columns();
        let mut w = Window::new();
        assert!(!w.count_ready(Some(3)));

        w.push(&json!({"value": 1}), &cols);
        w.push(&json!({"value": 2}), &cols);
        assert!(!w.count_ready(Some(3)));

        w.push(&json!({"value": 3}), &cols);
        assert!(w.count_ready(Some(3)));
    }

    #[test]
    fn test_window_count_ready_none() {
        let w = Window::new();
        assert!(!w.count_ready(None));
    }

    #[test]
    fn test_window_empty_emit() {
        let cols = test_columns();
        let w = Window::new();
        let out = w.emit(&cols);
        assert_eq!(out["value_sum"], 0.0);
        assert_eq!(out["value_min"], json!(null));
        assert_eq!(out["value_max"], json!(null));
        assert_eq!(out["cnt"], 0);
    }

    #[test]
    fn test_window_nested_field() {
        let cols = vec![AggColumn {
            field: "data.value".into(),
            func: AggFn::Sum,
            alias: Some("total".into()),
        }];
        let mut w = Window::new();
        w.push(&json!({"data": {"value": 3}}), &cols);
        w.push(&json!({"data": {"value": 7}}), &cols);

        let out = w.emit(&cols);
        assert_eq!(out["total"], 10.0);
    }

    #[test]
    fn test_window_float_values() {
        let cols = vec![
            AggColumn {
                field: "v".into(),
                func: AggFn::Sum,
                alias: None,
            },
            AggColumn {
                field: "v".into(),
                func: AggFn::Avg,
                alias: None,
            },
        ];
        let mut w = Window::new();
        w.push(&json!({"v": 1.5}), &cols);
        w.push(&json!({"v": 2.5}), &cols);

        let out = w.emit(&cols);
        assert_eq!(out["v_sum"], 4.0);
        assert_eq!(out["v_avg"], 2.0);
    }

    // --- Group key ---

    #[test]
    fn test_group_key_no_group() {
        let key = group_key(&json!({"a": 1}), &None);
        assert_eq!(key, "__all__");
    }

    #[test]
    fn test_group_key_string_field() {
        let key = group_key(&json!({"cat": "A"}), &Some("cat".into()));
        assert_eq!(key, "A");
    }

    #[test]
    fn test_group_key_numeric_field() {
        let key = group_key(&json!({"id": 42}), &Some("id".into()));
        assert_eq!(key, "42");
    }

    #[test]
    fn test_group_key_missing_field() {
        let key = group_key(&json!({"a": 1}), &Some("missing".into()));
        assert_eq!(key, "__null__");
    }

    // --- Factory ---

    #[test]
    fn test_aggregator_create() {
        let params = json!({
            "columns": [
                { "field": "value", "fn": "sum" },
                { "field": "value", "fn": "count" }
            ],
            "window_count": 10
        });
        let result = Aggregator::create("test".into(), params);
        assert!(result.is_ok());
    }

    #[test]
    fn test_aggregator_create_with_time_window() {
        let params = json!({
            "columns": [{ "field": "v", "fn": "avg" }],
            "window_ms": 5000,
            "group_by": "category"
        });
        let result = Aggregator::create("test".into(), params);
        assert!(result.is_ok());
    }

    #[test]
    fn test_aggregator_create_invalid() {
        let result = Aggregator::create("test".into(), json!({"wrong": true}));
        assert!(result.is_err());
    }
}
