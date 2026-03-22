//! Filter Task
//!
//! Routes messages based on configurable conditions.
//! Matching messages go to `"out"`, non-matching to `"reject"` (optional).

use crate::ctx::TaskContext;
use crate::err::Result;
use crate::task::{BaseTask, Task, TaskInfo};
use async_trait::async_trait;
use jb::{as_f64, get};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Comparison operator for a single condition.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Operator {
    /// Equal (`==`)
    Eq,
    /// Not equal (`!=`)
    Ne,
    /// Greater than (`>`)
    Gt,
    /// Greater than or equal (`>=`)
    Gte,
    /// Less than (`<`)
    Lt,
    /// Less than or equal (`<=`)
    Lte,
    /// String / array contains value
    Contains,
    /// Field exists (value is ignored)
    Exists,
}

/// How to combine multiple conditions.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FilterMode {
    /// All conditions must match (default)
    And,
    /// At least one condition must match
    Or,
}

impl Default for FilterMode {
    fn default() -> Self {
        Self::And
    }
}

/// A single filter condition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Condition {
    /// Field path using dot notation (e.g. `"user.age"`)
    pub field: String,
    /// Comparison operator
    pub operator: Operator,
    /// Value to compare against (ignored for `Exists`)
    #[serde(default)]
    pub value: Value,
}

/// Filter task parameters.
#[derive(Debug, Serialize, Deserialize)]
pub struct Params {
    /// One or more conditions to evaluate per message
    pub conditions: Vec<Condition>,
    /// Combine conditions with AND (default) or OR
    #[serde(default)]
    pub mode: FilterMode,
}

/// Filter task state (stateless — no persistent state needed).
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct State {}

// ---------------------------------------------------------------------------
// Task implementation
// ---------------------------------------------------------------------------

/// Filter task
///
/// Evaluates each incoming message against a set of conditions.
///
/// # Outputs
///
/// | Label      | Description                           |
/// |------------|---------------------------------------|
/// | `"out"`    | Messages that match the conditions    |
/// | `"reject"` | Messages that do **not** match (opt.) |
///
/// If `"reject"` is not wired in the workflow config the non-matching
/// messages are silently dropped.
///
/// # Example Configuration
///
/// ```json
/// {
///   "conditions": [
///     { "field": "value", "operator": "gt", "value": 50 },
///     { "field": "status", "operator": "eq", "value": "active" }
///   ],
///   "mode": "and"
/// }
/// ```
pub struct Filter {
    base: BaseTask<Params, State>,
}

impl Filter {
    pub fn create(id: String, params: Value) -> Result<Box<dyn Task>> {
        Ok(Box::new(Self {
            base: BaseTask::new(id, params)?,
        }))
    }

    /// Evaluate a single condition against a message.
    fn eval_condition(msg: &Value, cond: &Condition) -> bool {
        let field_val = get(msg, &cond.field);

        match cond.operator {
            Operator::Exists => field_val.is_some(),

            Operator::Eq => field_val.is_some_and(|v| v == &cond.value),
            Operator::Ne => field_val.is_some_and(|v| v != &cond.value),

            Operator::Gt => Self::cmp_numbers(field_val, &cond.value, |a, b| a > b),
            Operator::Gte => Self::cmp_numbers(field_val, &cond.value, |a, b| a >= b),
            Operator::Lt => Self::cmp_numbers(field_val, &cond.value, |a, b| a < b),
            Operator::Lte => Self::cmp_numbers(field_val, &cond.value, |a, b| a <= b),

            Operator::Contains => Self::eval_contains(field_val, &cond.value),
        }
    }

    /// Numeric comparison helper.  Converts both sides to f64.
    fn cmp_numbers(field: Option<&Value>, target: &Value, cmp: fn(f64, f64) -> bool) -> bool {
        let a = field.and_then(as_f64);
        let b = as_f64(target);
        matches!((a, b), (Some(a), Some(b)) if cmp(a, b))
    }

    /// `Contains` semantics:
    /// - String field: checks if substring is present
    /// - Array field: checks if array includes the value
    fn eval_contains(field: Option<&Value>, target: &Value) -> bool {
        match field {
            Some(Value::String(s)) => target.as_str().is_some_and(|t| s.contains(t)),
            Some(Value::Array(arr)) => arr.contains(target),
            _ => false,
        }
    }

    /// Evaluate all conditions according to the filter mode.
    fn matches(&self, msg: &Value) -> bool {
        let conditions = &self.base.params.conditions;
        match self.base.params.mode {
            FilterMode::And => conditions.iter().all(|c| Self::eval_condition(msg, c)),
            FilterMode::Or => conditions.iter().any(|c| Self::eval_condition(msg, c)),
        }
    }
}

#[async_trait]
impl Task for Filter {
    fn name(&self) -> &str {
        "Filter"
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
        let output = ctx.output("out")?;
        let reject = ctx.output("reject").ok(); // optional — may not be wired

        tracing::info!(
            "Filter [{}]: Started with {} condition(s), mode={:?}",
            self.base.id,
            self.base.params.conditions.len(),
            self.base.params.mode,
        );

        let mut matched = 0u64;
        let mut rejected = 0u64;

        while ctx.running().await {
            match input.recv().await {
                Ok(msg) => {
                    if self.matches(&msg) {
                        output.send(msg).await?;
                        matched += 1;
                    } else if let Some(ref rej) = reject {
                        rej.send(msg).await?;
                        rejected += 1;
                    } else {
                        rejected += 1;
                    }

                    if (matched + rejected) % 10_000 == 0 {
                        tracing::debug!(
                            "Filter [{}]: {} matched, {} rejected",
                            self.base.id,
                            matched,
                            rejected,
                        );
                    }
                }
                Err(_) => {
                    tracing::debug!("Filter [{}]: Input channel closed", self.base.id);
                    break;
                }
            }
        }

        tracing::info!(
            "Filter [{}]: Finished — {} matched, {} rejected",
            self.base.id,
            matched,
            rejected,
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

    fn make_filter(conditions: Vec<Condition>, mode: FilterMode) -> Filter {
        let params = Params { conditions, mode };
        Filter {
            base: BaseTask {
                id: "test".to_string(),
                params,
                state: State {},
                status: None,
            },
        }
    }

    // --- Operator tests ---

    #[test]
    fn test_eq() {
        let f = make_filter(
            vec![Condition {
                field: "status".into(),
                operator: Operator::Eq,
                value: json!("active"),
            }],
            FilterMode::And,
        );
        assert!(f.matches(&json!({"status": "active"})));
        assert!(!f.matches(&json!({"status": "inactive"})));
    }

    #[test]
    fn test_ne() {
        let f = make_filter(
            vec![Condition {
                field: "status".into(),
                operator: Operator::Ne,
                value: json!("deleted"),
            }],
            FilterMode::And,
        );
        assert!(f.matches(&json!({"status": "active"})));
        assert!(!f.matches(&json!({"status": "deleted"})));
    }

    #[test]
    fn test_gt_lt() {
        let f = make_filter(
            vec![Condition {
                field: "value".into(),
                operator: Operator::Gt,
                value: json!(50),
            }],
            FilterMode::And,
        );
        assert!(f.matches(&json!({"value": 51})));
        assert!(!f.matches(&json!({"value": 50})));
        assert!(!f.matches(&json!({"value": 49})));

        let f2 = make_filter(
            vec![Condition {
                field: "value".into(),
                operator: Operator::Lt,
                value: json!(10.5),
            }],
            FilterMode::And,
        );
        assert!(f2.matches(&json!({"value": 10.4})));
        assert!(!f2.matches(&json!({"value": 10.5})));
    }

    #[test]
    fn test_gte_lte() {
        let f = make_filter(
            vec![Condition {
                field: "x".into(),
                operator: Operator::Gte,
                value: json!(5),
            }],
            FilterMode::And,
        );
        assert!(f.matches(&json!({"x": 5})));
        assert!(f.matches(&json!({"x": 6})));
        assert!(!f.matches(&json!({"x": 4})));
    }

    #[test]
    fn test_exists() {
        let f = make_filter(
            vec![Condition {
                field: "email".into(),
                operator: Operator::Exists,
                value: json!(null),
            }],
            FilterMode::And,
        );
        assert!(f.matches(&json!({"email": "a@b.com"})));
        assert!(!f.matches(&json!({"name": "John"})));
    }

    #[test]
    fn test_contains_string() {
        let f = make_filter(
            vec![Condition {
                field: "name".into(),
                operator: Operator::Contains,
                value: json!("oh"),
            }],
            FilterMode::And,
        );
        assert!(f.matches(&json!({"name": "John"})));
        assert!(!f.matches(&json!({"name": "Jane"})));
    }

    #[test]
    fn test_contains_array() {
        let f = make_filter(
            vec![Condition {
                field: "tags".into(),
                operator: Operator::Contains,
                value: json!("rust"),
            }],
            FilterMode::And,
        );
        assert!(f.matches(&json!({"tags": ["rust", "wasm"]})));
        assert!(!f.matches(&json!({"tags": ["go", "wasm"]})));
    }

    // --- Nested field ---

    #[test]
    fn test_nested_field() {
        let f = make_filter(
            vec![Condition {
                field: "user.age".into(),
                operator: Operator::Gte,
                value: json!(18),
            }],
            FilterMode::And,
        );
        assert!(f.matches(&json!({"user": {"age": 25}})));
        assert!(!f.matches(&json!({"user": {"age": 16}})));
    }

    // --- AND / OR mode ---

    #[test]
    fn test_and_mode() {
        let f = make_filter(
            vec![
                Condition {
                    field: "a".into(),
                    operator: Operator::Eq,
                    value: json!(1),
                },
                Condition {
                    field: "b".into(),
                    operator: Operator::Eq,
                    value: json!(2),
                },
            ],
            FilterMode::And,
        );
        assert!(f.matches(&json!({"a": 1, "b": 2})));
        assert!(!f.matches(&json!({"a": 1, "b": 9})));
    }

    #[test]
    fn test_or_mode() {
        let f = make_filter(
            vec![
                Condition {
                    field: "a".into(),
                    operator: Operator::Eq,
                    value: json!(1),
                },
                Condition {
                    field: "b".into(),
                    operator: Operator::Eq,
                    value: json!(2),
                },
            ],
            FilterMode::Or,
        );
        assert!(f.matches(&json!({"a": 1, "b": 9})));
        assert!(f.matches(&json!({"a": 9, "b": 2})));
        assert!(!f.matches(&json!({"a": 9, "b": 9})));
    }

    // --- Missing field ---

    #[test]
    fn test_missing_field_does_not_match() {
        let f = make_filter(
            vec![Condition {
                field: "missing".into(),
                operator: Operator::Eq,
                value: json!(1),
            }],
            FilterMode::And,
        );
        assert!(!f.matches(&json!({"other": 1})));
    }

    // --- Creation via factory ---

    #[test]
    fn test_filter_create() {
        let params = json!({
            "conditions": [
                { "field": "x", "operator": "gt", "value": 10 }
            ]
        });
        let result = Filter::create("test".to_string(), params);
        assert!(result.is_ok());
    }

    #[test]
    fn test_filter_create_invalid() {
        let result = Filter::create("test".to_string(), json!({"wrong": true}));
        assert!(result.is_err());
    }
}
