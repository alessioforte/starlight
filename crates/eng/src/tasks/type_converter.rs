//! Type Converter Task
//!
//! Converts field types within JSON messages. Useful for normalising data
//! coming from loosely-typed sources (CSV, HTTP, etc.).

use crate::ctx::TaskContext;
use crate::err::Result;
use crate::task::{BaseTask, Task, TaskInfo};
use async_trait::async_trait;
use jb::{get, get_mut, remove};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Target type for conversion.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TargetType {
    /// Convert to string
    String,
    /// Convert to integer (i64)
    Int,
    /// Convert to float (f64)
    Float,
    /// Convert to boolean
    Bool,
    /// Parse a JSON-encoded string into a Value
    Json,
    /// Convert to string with a strftime-like format (for timestamps)
    /// The inner string is unused on input — it just tags the variant.
    /// Converts epoch millis/secs to RFC 3339 string.
    Timestamp,
    /// Convert to array (wraps scalar in a single-element array; no-op if already array)
    Array,
    /// Convert to null (always replaces the value)
    Null,
}

/// A single conversion rule.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Conversion {
    /// Field path (dot notation).
    pub field: String,
    /// Target type.
    #[serde(rename = "to")]
    pub target: TargetType,
    /// If `true`, remove the field when conversion fails instead of leaving
    /// the original value. Default: `false`.
    #[serde(default)]
    pub remove_on_error: bool,
}

/// What to do when a field referenced by a conversion doesn't exist.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MissingField {
    /// Silently skip (default).
    Ignore,
    /// Insert a null value for the field.
    InsertNull,
}

impl Default for MissingField {
    fn default() -> Self {
        Self::Ignore
    }
}

/// Type Converter task parameters.
#[derive(Debug, Serialize, Deserialize)]
pub struct Params {
    /// List of conversions to apply (in order).
    pub conversions: Vec<Conversion>,
    /// Behaviour when a target field is missing from the message.
    #[serde(default)]
    pub on_missing: MissingField,
}

/// Type Converter state (stateless).
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct State {}

// ---------------------------------------------------------------------------
// Conversion logic
// ---------------------------------------------------------------------------

/// Try to convert a JSON value to the target type.
/// Returns `Some(new_value)` on success, `None` on failure.
fn convert(value: &Value, target: &TargetType) -> Option<Value> {
    match target {
        TargetType::String => Some(match value {
            Value::String(_) => value.clone(),
            Value::Null => json!(""),
            Value::Bool(b) => json!(b.to_string()),
            Value::Number(n) => json!(n.to_string()),
            Value::Array(_) | Value::Object(_) => json!(value.to_string()),
        }),

        TargetType::Int => match value {
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Some(json!(i))
                } else if let Some(f) = n.as_f64() {
                    Some(json!(f as i64))
                } else {
                    None
                }
            }
            Value::String(s) => s.trim().parse::<i64>().ok().map(|i| json!(i)),
            Value::Bool(b) => Some(json!(if *b { 1 } else { 0 })),
            _ => None,
        },

        TargetType::Float => match value {
            Value::Number(n) => n.as_f64().map(|f| json!(f)),
            Value::String(s) => s.trim().parse::<f64>().ok().map(|f| json!(f)),
            Value::Bool(b) => Some(json!(if *b { 1.0 } else { 0.0 })),
            _ => None,
        },

        TargetType::Bool => match value {
            Value::Bool(_) => Some(value.clone()),
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Some(json!(i != 0))
                } else {
                    n.as_f64().map(|f| json!(f != 0.0))
                }
            }
            Value::String(s) => match s.trim().to_lowercase().as_str() {
                "true" | "1" | "yes" | "on" => Some(json!(true)),
                "false" | "0" | "no" | "off" | "" => Some(json!(false)),
                _ => None,
            },
            Value::Null => Some(json!(false)),
            _ => None,
        },

        TargetType::Json => match value {
            Value::String(s) => serde_json::from_str(s).ok(),
            other => Some(other.clone()), // already a Value
        },

        TargetType::Timestamp => match value {
            // Epoch seconds or millis → RFC 3339
            Value::Number(n) => {
                let epoch = n.as_i64()?;
                let dt = if epoch > 1_000_000_000_000 {
                    // Likely millis
                    chrono::DateTime::from_timestamp_millis(epoch)?
                } else {
                    chrono::DateTime::from_timestamp(epoch, 0)?
                };
                Some(json!(dt.to_rfc3339()))
            }
            Value::String(s) => {
                // Try parsing as epoch number string
                if let Ok(epoch) = s.trim().parse::<i64>() {
                    let dt = if epoch > 1_000_000_000_000 {
                        chrono::DateTime::from_timestamp_millis(epoch)?
                    } else {
                        chrono::DateTime::from_timestamp(epoch, 0)?
                    };
                    Some(json!(dt.to_rfc3339()))
                } else {
                    // Already a string timestamp — leave as-is
                    Some(value.clone())
                }
            }
            _ => None,
        },

        TargetType::Array => match value {
            Value::Array(_) => Some(value.clone()),
            other => Some(json!([other])),
        },

        TargetType::Null => Some(json!(null)),
    }
}

// ---------------------------------------------------------------------------
// Task implementation
// ---------------------------------------------------------------------------

/// Type Converter task
///
/// Applies type conversions to fields in each message.
///
/// # Outputs
///
/// | Label   | Description                          |
/// |---------|--------------------------------------|
/// | `"out"` | Messages with converted field types  |
///
/// # Example Configuration
///
/// ```json
/// {
///   "conversions": [
///     { "field": "age", "to": "int" },
///     { "field": "score", "to": "float" },
///     { "field": "active", "to": "bool" },
///     { "field": "created_at", "to": "timestamp" },
///     { "field": "tags", "to": "array" },
///     { "field": "metadata", "to": "json" }
///   ],
///   "on_missing": "ignore"
/// }
/// ```
pub struct TypeConverter {
    base: BaseTask<Params, State>,
}

impl TypeConverter {
    pub fn create(id: String, params: Value) -> Result<Box<dyn Task>> {
        Ok(Box::new(Self {
            base: BaseTask::new(id, params)?,
        }))
    }

    /// Apply all conversions to a message (in place).
    fn apply(&self, msg: &mut Value) {
        for conv in &self.base.params.conversions {
            let existing = get(msg, &conv.field).cloned();

            match existing {
                Some(val) => {
                    match convert(&val, &conv.target) {
                        Some(new_val) => {
                            if let Some(slot) = get_mut(msg, &conv.field) {
                                *slot = new_val;
                            }
                        }
                        None => {
                            // Conversion failed
                            if conv.remove_on_error {
                                remove(msg, &conv.field);
                            }
                            // else: leave original value
                        }
                    }
                }
                None => {
                    // Field missing
                    if let MissingField::InsertNull = self.base.params.on_missing {
                        if let Some(slot) = get_mut(msg, &conv.field) {
                            *slot = Value::Null;
                        }
                    }
                }
            }
        }
    }
}

#[async_trait]
impl Task for TypeConverter {
    fn name(&self) -> &str {
        "TypeConverter"
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

        tracing::info!(
            "TypeConverter [{}]: {} conversion(s)",
            self.base.id,
            self.base.params.conversions.len(),
        );

        let mut processed = 0u64;

        while ctx.running().await {
            match input.recv().await {
                Ok(msg) => {
                    let mut msg = msg.into_owned();
                    self.apply(&mut msg);
                    output.send(msg.into()).await?;
                    processed += 1;

                    if processed % 10_000 == 0 {
                        tracing::debug!(
                            "TypeConverter [{}]: {} messages processed",
                            self.base.id,
                            processed,
                        );
                    }
                }
                Err(_) => {
                    tracing::debug!("TypeConverter [{}]: Input channel closed", self.base.id,);
                    break;
                }
            }
        }

        tracing::info!(
            "TypeConverter [{}]: Finished — {} messages processed",
            self.base.id,
            processed,
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

    // --- convert() unit tests ---

    #[test]
    fn test_to_string_from_number() {
        assert_eq!(convert(&json!(42), &TargetType::String), Some(json!("42")));
    }

    #[test]
    fn test_to_string_from_bool() {
        assert_eq!(
            convert(&json!(true), &TargetType::String),
            Some(json!("true"))
        );
    }

    #[test]
    fn test_to_string_from_null() {
        assert_eq!(convert(&json!(null), &TargetType::String), Some(json!("")));
    }

    #[test]
    fn test_to_string_from_string() {
        assert_eq!(
            convert(&json!("hi"), &TargetType::String),
            Some(json!("hi"))
        );
    }

    #[test]
    fn test_to_int_from_string() {
        assert_eq!(convert(&json!("42"), &TargetType::Int), Some(json!(42)));
    }

    #[test]
    fn test_to_int_from_float() {
        assert_eq!(convert(&json!(3.9), &TargetType::Int), Some(json!(3)));
    }

    #[test]
    fn test_to_int_from_bool() {
        assert_eq!(convert(&json!(true), &TargetType::Int), Some(json!(1)));
        assert_eq!(convert(&json!(false), &TargetType::Int), Some(json!(0)));
    }

    #[test]
    fn test_to_int_from_bad_string() {
        assert_eq!(convert(&json!("abc"), &TargetType::Int), None);
    }

    #[test]
    fn test_to_float_from_string() {
        assert_eq!(
            convert(&json!("3.14"), &TargetType::Float),
            Some(json!(3.14))
        );
    }

    #[test]
    fn test_to_float_from_int() {
        assert_eq!(convert(&json!(5), &TargetType::Float), Some(json!(5.0)));
    }

    #[test]
    fn test_to_bool_from_string() {
        assert_eq!(
            convert(&json!("true"), &TargetType::Bool),
            Some(json!(true))
        );
        assert_eq!(convert(&json!("yes"), &TargetType::Bool), Some(json!(true)));
        assert_eq!(convert(&json!("1"), &TargetType::Bool), Some(json!(true)));
        assert_eq!(
            convert(&json!("false"), &TargetType::Bool),
            Some(json!(false))
        );
        assert_eq!(convert(&json!("no"), &TargetType::Bool), Some(json!(false)));
        assert_eq!(convert(&json!("0"), &TargetType::Bool), Some(json!(false)));
        assert_eq!(convert(&json!(""), &TargetType::Bool), Some(json!(false)));
        assert_eq!(convert(&json!("on"), &TargetType::Bool), Some(json!(true)));
        assert_eq!(
            convert(&json!("off"), &TargetType::Bool),
            Some(json!(false))
        );
    }

    #[test]
    fn test_to_bool_from_number() {
        assert_eq!(convert(&json!(1), &TargetType::Bool), Some(json!(true)));
        assert_eq!(convert(&json!(0), &TargetType::Bool), Some(json!(false)));
    }

    #[test]
    fn test_to_bool_from_null() {
        assert_eq!(convert(&json!(null), &TargetType::Bool), Some(json!(false)));
    }

    #[test]
    fn test_to_bool_invalid() {
        assert_eq!(convert(&json!("maybe"), &TargetType::Bool), None);
    }

    #[test]
    fn test_to_json_from_string() {
        let result = convert(&json!(r#"{"a":1}"#), &TargetType::Json);
        assert_eq!(result, Some(json!({"a": 1})));
    }

    #[test]
    fn test_to_json_invalid() {
        assert_eq!(convert(&json!("not json{"), &TargetType::Json), None);
    }

    #[test]
    fn test_to_json_already_value() {
        let v = json!({"a": 1});
        assert_eq!(convert(&v, &TargetType::Json), Some(v));
    }

    #[test]
    fn test_to_timestamp_epoch_secs() {
        let result = convert(&json!(1700000000), &TargetType::Timestamp);
        assert!(result.is_some());
        let s = result.unwrap();
        assert!(s.as_str().unwrap().contains("2023"));
    }

    #[test]
    fn test_to_timestamp_epoch_millis() {
        let result = convert(&json!(1700000000000_i64), &TargetType::Timestamp);
        assert!(result.is_some());
        let s = result.unwrap();
        assert!(s.as_str().unwrap().contains("2023"));
    }

    #[test]
    fn test_to_timestamp_string_epoch() {
        let result = convert(&json!("1700000000"), &TargetType::Timestamp);
        assert!(result.is_some());
    }

    #[test]
    fn test_to_timestamp_string_passthrough() {
        let result = convert(&json!("2023-11-14T22:13:20+00:00"), &TargetType::Timestamp);
        assert_eq!(result, Some(json!("2023-11-14T22:13:20+00:00")));
    }

    #[test]
    fn test_to_array_scalar() {
        assert_eq!(convert(&json!(42), &TargetType::Array), Some(json!([42])));
        assert_eq!(
            convert(&json!("hi"), &TargetType::Array),
            Some(json!(["hi"]))
        );
    }

    #[test]
    fn test_to_array_already_array() {
        assert_eq!(
            convert(&json!([1, 2]), &TargetType::Array),
            Some(json!([1, 2]))
        );
    }

    #[test]
    fn test_to_null() {
        assert_eq!(convert(&json!(42), &TargetType::Null), Some(json!(null)));
        assert_eq!(convert(&json!("hi"), &TargetType::Null), Some(json!(null)));
    }

    // --- nested field helpers ---

    #[test]
    fn test_get_nested_mut_creates_intermediate() {
        let mut msg = json!({"a": {}});
        let slot = get_mut(&mut msg, "a.b");
        assert!(slot.is_some());
        *slot.unwrap() = json!(42);
        assert_eq!(msg, json!({"a": {"b": 42}}));
    }

    #[test]
    fn test_remove() {
        let mut msg = json!({"a": {"b": 1, "c": 2}});
        assert!(remove(&mut msg, "a.b"));
        assert_eq!(msg, json!({"a": {"c": 2}}));
    }

    #[test]
    fn test_remove_nested_top_level() {
        let mut msg = json!({"x": 1, "y": 2});
        assert!(remove(&mut msg, "x"));
        assert_eq!(msg, json!({"y": 2}));
    }

    #[test]
    fn test_remove_nested_missing() {
        let mut msg = json!({"a": 1});
        assert!(!remove(&mut msg, "b"));
    }

    // --- apply() integration ---

    fn make_converter(conversions: Vec<Conversion>, on_missing: MissingField) -> TypeConverter {
        TypeConverter {
            base: BaseTask {
                id: "test".into(),
                params: Params {
                    conversions,
                    on_missing,
                },
                state: State {},
                status: None,
            },
        }
    }

    #[test]
    fn test_apply_single_conversion() {
        let tc = make_converter(
            vec![Conversion {
                field: "age".into(),
                target: TargetType::Int,
                remove_on_error: false,
            }],
            MissingField::Ignore,
        );
        let mut msg = json!({"name": "Alice", "age": "30"});
        tc.apply(&mut msg);
        assert_eq!(msg["age"], json!(30));
        assert_eq!(msg["name"], json!("Alice")); // untouched
    }

    #[test]
    fn test_apply_multiple_conversions() {
        let tc = make_converter(
            vec![
                Conversion {
                    field: "age".into(),
                    target: TargetType::Int,
                    remove_on_error: false,
                },
                Conversion {
                    field: "active".into(),
                    target: TargetType::Bool,
                    remove_on_error: false,
                },
                Conversion {
                    field: "score".into(),
                    target: TargetType::Float,
                    remove_on_error: false,
                },
            ],
            MissingField::Ignore,
        );
        let mut msg = json!({"age": "25", "active": "yes", "score": "9.5"});
        tc.apply(&mut msg);
        assert_eq!(msg["age"], json!(25));
        assert_eq!(msg["active"], json!(true));
        assert_eq!(msg["score"], json!(9.5));
    }

    #[test]
    fn test_apply_nested_field() {
        let tc = make_converter(
            vec![Conversion {
                field: "data.value".into(),
                target: TargetType::Float,
                remove_on_error: false,
            }],
            MissingField::Ignore,
        );
        let mut msg = json!({"data": {"value": "3.14"}});
        tc.apply(&mut msg);
        assert_eq!(msg["data"]["value"], json!(3.14));
    }

    #[test]
    fn test_apply_remove_on_error() {
        let tc = make_converter(
            vec![Conversion {
                field: "bad".into(),
                target: TargetType::Int,
                remove_on_error: true,
            }],
            MissingField::Ignore,
        );
        let mut msg = json!({"bad": "not_a_number", "good": 1});
        tc.apply(&mut msg);
        assert!(msg.get("bad").is_none());
        assert_eq!(msg["good"], json!(1));
    }

    #[test]
    fn test_apply_keep_on_error() {
        let tc = make_converter(
            vec![Conversion {
                field: "val".into(),
                target: TargetType::Int,
                remove_on_error: false,
            }],
            MissingField::Ignore,
        );
        let mut msg = json!({"val": "abc"});
        tc.apply(&mut msg);
        assert_eq!(msg["val"], json!("abc")); // unchanged
    }

    #[test]
    fn test_apply_missing_ignore() {
        let tc = make_converter(
            vec![Conversion {
                field: "missing".into(),
                target: TargetType::Int,
                remove_on_error: false,
            }],
            MissingField::Ignore,
        );
        let mut msg = json!({"other": 1});
        tc.apply(&mut msg);
        assert!(msg.get("missing").is_none());
    }

    #[test]
    fn test_apply_missing_insert_null() {
        let tc = make_converter(
            vec![Conversion {
                field: "missing".into(),
                target: TargetType::Int,
                remove_on_error: false,
            }],
            MissingField::InsertNull,
        );
        let mut msg = json!({"other": 1});
        tc.apply(&mut msg);
        assert_eq!(msg["missing"], json!(null));
    }

    // --- Factory ---

    #[test]
    fn test_type_converter_create() {
        let params = json!({
            "conversions": [
                { "field": "age", "to": "int" },
                { "field": "active", "to": "bool" }
            ]
        });
        assert!(TypeConverter::create("test".into(), params).is_ok());
    }

    #[test]
    fn test_type_converter_create_invalid() {
        assert!(TypeConverter::create("test".into(), json!({"wrong": true})).is_err());
    }
}
