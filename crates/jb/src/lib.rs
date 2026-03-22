//! `jb` — JSON Builder & utilities
//!
//! Provides two APIs:
//!
//! 1. **Free functions** — work directly on `&Value` / `&mut Value` without
//!    wrapping. Ideal for pipeline tasks that process messages in-place.
//!
//!    ```rust
//!    use jb::{get, set, remove, as_f64};
//!    ```
//!
//! 2. **`JsonBuilder`** — wraps a `Value` and offers a fluent, stateful API
//!    with error reporting, type inspection, and deep merge.
//!
//!    ```rust
//!    use jb::JsonBuilder;
//!    ```
//!
//! Both APIs support dot-notation paths with array index resolution
//! (e.g. `"items.0.name"`).

use serde_json::{Value, json};

// ===========================================================================
// Error type
// ===========================================================================

/// Error type for JSON path operations.
#[derive(Debug, PartialEq, Eq)]
pub enum JsonPathError {
    /// A segment in the path is empty (e.g. `"a..b"`).
    InvalidPath,
    /// The target field was not found.
    NotFound,
}

impl std::fmt::Display for JsonPathError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidPath => write!(f, "invalid JSON path"),
            Self::NotFound => write!(f, "path not found"),
        }
    }
}

impl std::error::Error for JsonPathError {}

// ===========================================================================
// Free functions — work directly on &Value / &mut Value
// ===========================================================================

/// Resolve a dot-notation path to a nested JSON value.
///
/// Supports both object keys and array indices:
/// - `"a.b.c"` → `obj["a"]["b"]["c"]`
/// - `"items.0.id"` → `obj["items"][0]["id"]`
///
/// Returns `None` if any segment is missing or the intermediate value
/// is not an object/array.
pub fn get<'a>(value: &'a Value, path: &str) -> Option<&'a Value> {
    path.split('.').fold(Some(value), |acc, key| {
        acc.and_then(|v| match v {
            Value::Object(map) => map.get(key),
            Value::Array(arr) => key.parse::<usize>().ok().and_then(|i| arr.get(i)),
            _ => None,
        })
    })
}

/// Get a mutable reference to a nested field, creating intermediate
/// objects as needed.
///
/// Returns `None` only if a non-object intermediate already exists
/// and cannot be descended into.
pub fn get_mut<'a>(root: &'a mut Value, path: &str) -> Option<&'a mut Value> {
    let parts: Vec<&str> = path.split('.').collect();
    let mut current = root;
    for part in &parts {
        match current {
            Value::Object(map) => {
                current = map.entry(part.to_string()).or_insert(Value::Null);
            }
            _ => return None,
        }
    }
    Some(current)
}

/// Set a value via dot-notation path, creating intermediate objects
/// as needed.
///
/// No-op if a non-object intermediate value blocks descent.
pub fn set(root: &mut Value, path: &str, val: Value) {
    let parts: Vec<&str> = path.split('.').collect();
    let mut current = root;
    for (i, part) in parts.iter().enumerate() {
        if i == parts.len() - 1 {
            if let Value::Object(map) = current {
                map.insert(part.to_string(), val);
            }
            return;
        }
        if let Value::Object(map) = current {
            current = map.entry(part.to_string()).or_insert_with(|| json!({}));
        } else {
            return;
        }
    }
}

/// Remove a nested field by dot-notation path.
///
/// Returns `true` if the field was found and removed.
pub fn remove(root: &mut Value, path: &str) -> bool {
    let parts: Vec<&str> = path.split('.').collect();
    if parts.is_empty() {
        return false;
    }
    if parts.len() == 1 {
        return match root {
            Value::Object(map) => map.remove(parts[0]).is_some(),
            _ => false,
        };
    }
    // Navigate to the parent of the leaf
    let parent_path = &parts[..parts.len() - 1];
    let leaf = parts[parts.len() - 1];
    let mut current = root;
    for part in parent_path {
        match current {
            Value::Object(map) => match map.get_mut(*part) {
                Some(v) => current = v,
                None => return false,
            },
            _ => return false,
        }
    }
    match current {
        Value::Object(map) => map.remove(leaf).is_some(),
        _ => false,
    }
}

/// Coerce a JSON value to `f64`.
///
/// Handles:
/// - `Number` — int and float
/// - `String` — trimmed, then parsed
/// - `Bool` — `true` → 1.0, `false` → 0.0
///
/// Returns `None` for null, arrays, and objects.
pub fn as_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.trim().parse::<f64>().ok(),
        Value::Bool(b) => Some(if *b { 1.0 } else { 0.0 }),
        _ => None,
    }
}

/// Convert a JSON value to a string key suitable for `HashMap` lookups.
///
/// - Strings are returned as-is
/// - Numbers, bools, null are stringified
/// - Arrays/objects use compact JSON
pub fn value_to_key(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// Return the JSON type name of a value as a static string.
pub fn type_of(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// Deep-merge `source` into `target`.
///
/// - Matching object keys are merged recursively.
/// - All other cases: `source` overwrites `target`.
pub fn merge(target: &mut Value, source: &Value) {
    match (target, source) {
        (Value::Object(t), Value::Object(s)) => {
            for (key, value) in s {
                merge(t.entry(key.clone()).or_insert(json!({})), value);
            }
        }
        (t, s) => {
            *t = s.clone();
        }
    }
}

// ===========================================================================
// JsonBuilder — stateful wrapper
// ===========================================================================

/// A stateful wrapper around a JSON `Value` that provides a fluent API
/// for reading, writing, and transforming nested fields.
pub struct JsonBuilder {
    data: Value,
}

impl JsonBuilder {
    /// Create a new builder wrapping the given value.
    pub fn new(data: Value) -> Self {
        Self { data }
    }

    /// Immutable access to the inner value.
    pub fn data(&self) -> &Value {
        &self.data
    }

    /// Consume the builder and return the inner value.
    pub fn into_inner(self) -> Value {
        self.data
    }

    /// Get an immutable reference to a nested value by dot-notation path.
    pub fn get_value(&self, path: &str) -> Result<&Value, JsonPathError> {
        get(&self.data, path).ok_or(JsonPathError::NotFound)
    }

    /// Get a mutable reference to a nested value, creating intermediate
    /// objects as needed.
    pub fn get_value_mut(&mut self, path: &str) -> Result<&mut Value, JsonPathError> {
        get_mut(&mut self.data, path).ok_or(JsonPathError::InvalidPath)
    }

    /// Set a value at the given dot-notation path, creating intermediate
    /// objects as needed.
    pub fn set_value(&mut self, path: &str, value: Value) -> Result<(), JsonPathError> {
        let parts: Vec<&str> = path.split('.').collect();
        if parts.iter().any(|p| p.is_empty()) {
            return Err(JsonPathError::InvalidPath);
        }
        set(&mut self.data, path, value);
        Ok(())
    }

    /// Remove a field at the given dot-notation path.
    ///
    /// Returns the removed value, or `NotFound` if it didn't exist.
    pub fn remove_value(&mut self, path: &str) -> Result<(), JsonPathError> {
        if remove(&mut self.data, path) {
            Ok(())
        } else {
            Err(JsonPathError::NotFound)
        }
    }

    /// Return the JSON type name of the value at the given path.
    pub fn get_type(&self, path: &str) -> Result<&'static str, JsonPathError> {
        let value = self.get_value(path)?;
        Ok(type_of(value))
    }

    /// Deep-merge another value into this builder's data.
    pub fn merge(&mut self, source: &Value) {
        merge(&mut self.data, source);
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // -----------------------------------------------------------------------
    // Free functions — get
    // -----------------------------------------------------------------------

    #[test]
    fn test_get_object() {
        let v = json!({"a": {"b": {"c": 42}}});
        assert_eq!(get(&v, "a.b.c"), Some(&json!(42)));
    }

    #[test]
    fn test_get_array_index() {
        let v = json!({"items": [10, 20, 30]});
        assert_eq!(get(&v, "items.1"), Some(&json!(20)));
    }

    #[test]
    fn test_get_nested_array_object() {
        let v = json!({"items": [{"id": "a"}, {"id": "b"}]});
        assert_eq!(get(&v, "items.1.id"), Some(&json!("b")));
    }

    #[test]
    fn test_get_missing() {
        let v = json!({"a": 1});
        assert_eq!(get(&v, "b.c"), None);
    }

    #[test]
    fn test_get_top_level() {
        let v = json!({"x": 99});
        assert_eq!(get(&v, "x"), Some(&json!(99)));
    }

    // -----------------------------------------------------------------------
    // Free functions — get_mut
    // -----------------------------------------------------------------------

    #[test]
    fn test_get_mut_creates_intermediate() {
        let mut v = json!({"a": {}});
        let slot = get_mut(&mut v, "a.b");
        assert!(slot.is_some());
        *slot.unwrap() = json!(42);
        assert_eq!(v, json!({"a": {"b": 42}}));
    }

    #[test]
    fn test_get_mut_blocked_by_non_object() {
        let mut v = json!({"a": "string"});
        assert!(get_mut(&mut v, "a.b").is_none());
    }

    // -----------------------------------------------------------------------
    // Free functions — set
    // -----------------------------------------------------------------------

    #[test]
    fn test_set_creates_path() {
        let mut v = json!({});
        set(&mut v, "a.b.c", json!(99));
        assert_eq!(v, json!({"a": {"b": {"c": 99}}}));
    }

    #[test]
    fn test_set_overwrites() {
        let mut v = json!({"x": 1});
        set(&mut v, "x", json!(2));
        assert_eq!(v, json!({"x": 2}));
    }

    #[test]
    fn test_set_noop_on_non_object() {
        let mut v = json!("string");
        set(&mut v, "a", json!(1));
        assert_eq!(v, json!("string")); // unchanged
    }

    // -----------------------------------------------------------------------
    // Free functions — remove
    // -----------------------------------------------------------------------

    #[test]
    fn test_remove_deep() {
        let mut v = json!({"a": {"b": 1, "c": 2}});
        assert!(remove(&mut v, "a.b"));
        assert_eq!(v, json!({"a": {"c": 2}}));
    }

    #[test]
    fn test_remove_top() {
        let mut v = json!({"x": 1, "y": 2});
        assert!(remove(&mut v, "x"));
        assert_eq!(v, json!({"y": 2}));
    }

    #[test]
    fn test_remove_missing() {
        let mut v = json!({"a": 1});
        assert!(!remove(&mut v, "b"));
    }

    #[test]
    fn test_remove_nested_missing_parent() {
        let mut v = json!({"a": 1});
        assert!(!remove(&mut v, "x.y.z"));
    }

    // -----------------------------------------------------------------------
    // Free functions — as_f64
    // -----------------------------------------------------------------------

    #[test]
    fn test_as_f64_number() {
        assert_eq!(as_f64(&json!(3.14)), Some(3.14));
        assert_eq!(as_f64(&json!(42)), Some(42.0));
    }

    #[test]
    fn test_as_f64_string() {
        assert_eq!(as_f64(&json!("3.14")), Some(3.14));
        assert_eq!(as_f64(&json!(" 42 ")), Some(42.0)); // trimmed
        assert_eq!(as_f64(&json!("abc")), None);
    }

    #[test]
    fn test_as_f64_bool() {
        assert_eq!(as_f64(&json!(true)), Some(1.0));
        assert_eq!(as_f64(&json!(false)), Some(0.0));
    }

    #[test]
    fn test_as_f64_null() {
        assert_eq!(as_f64(&json!(null)), None);
    }

    #[test]
    fn test_as_f64_array() {
        assert_eq!(as_f64(&json!([1, 2])), None);
    }

    // -----------------------------------------------------------------------
    // Free functions — value_to_key
    // -----------------------------------------------------------------------

    #[test]
    fn test_value_to_key_string() {
        assert_eq!(value_to_key(&json!("hello")), "hello");
    }

    #[test]
    fn test_value_to_key_number() {
        assert_eq!(value_to_key(&json!(42)), "42");
    }

    #[test]
    fn test_value_to_key_bool() {
        assert_eq!(value_to_key(&json!(true)), "true");
    }

    #[test]
    fn test_value_to_key_null() {
        assert_eq!(value_to_key(&json!(null)), "null");
    }

    // -----------------------------------------------------------------------
    // Free functions — type_of
    // -----------------------------------------------------------------------

    #[test]
    fn test_type_of() {
        assert_eq!(type_of(&json!(null)), "null");
        assert_eq!(type_of(&json!(true)), "bool");
        assert_eq!(type_of(&json!(42)), "number");
        assert_eq!(type_of(&json!("hi")), "string");
        assert_eq!(type_of(&json!([1])), "array");
        assert_eq!(type_of(&json!({"a": 1})), "object");
    }

    // -----------------------------------------------------------------------
    // Free functions — merge
    // -----------------------------------------------------------------------

    #[test]
    fn test_merge_objects() {
        let mut target = json!({"a": 1, "b": {"x": 10}});
        let source = json!({"b": {"y": 20}, "c": 3});
        merge(&mut target, &source);
        assert_eq!(target, json!({"a": 1, "b": {"x": 10, "y": 20}, "c": 3}));
    }

    #[test]
    fn test_merge_overwrite_scalar() {
        let mut target = json!({"a": 1});
        let source = json!({"a": 2});
        merge(&mut target, &source);
        assert_eq!(target, json!({"a": 2}));
    }

    #[test]
    fn test_merge_scalar_into_object() {
        let mut target = json!({"a": {"nested": true}});
        let source = json!({"a": "replaced"});
        merge(&mut target, &source);
        assert_eq!(target, json!({"a": "replaced"}));
    }

    // -----------------------------------------------------------------------
    // JsonBuilder
    // -----------------------------------------------------------------------

    #[test]
    fn test_builder_get_value() {
        let b = JsonBuilder::new(json!({"a": {"b": [1, 2, 3]}}));
        assert_eq!(b.get_value("a.b").unwrap(), &json!([1, 2, 3]));
        assert_eq!(b.get_value("a.b.1").unwrap(), &json!(2));
    }

    #[test]
    fn test_builder_get_value_not_found() {
        let b = JsonBuilder::new(json!({"a": 1}));
        assert_eq!(b.get_value("x"), Err(JsonPathError::NotFound));
    }

    #[test]
    fn test_builder_set_value() {
        let mut b = JsonBuilder::new(json!({"a": 1}));
        b.set_value("b.c", json!(42)).unwrap();
        assert_eq!(b.data(), &json!({"a": 1, "b": {"c": 42}}));
    }

    #[test]
    fn test_builder_set_value_invalid_path() {
        let mut b = JsonBuilder::new(json!({}));
        assert_eq!(
            b.set_value("a..b", json!(1)),
            Err(JsonPathError::InvalidPath)
        );
    }

    #[test]
    fn test_builder_remove_value() {
        let mut b = JsonBuilder::new(json!({"a": 1, "b": 2}));
        b.remove_value("a").unwrap();
        assert_eq!(b.data(), &json!({"b": 2}));
    }

    #[test]
    fn test_builder_remove_not_found() {
        let mut b = JsonBuilder::new(json!({"a": 1}));
        assert_eq!(b.remove_value("x"), Err(JsonPathError::NotFound));
    }

    #[test]
    fn test_builder_get_type() {
        let b = JsonBuilder::new(json!({"n": 42, "s": "hi", "a": [1]}));
        assert_eq!(b.get_type("n").unwrap(), "number");
        assert_eq!(b.get_type("s").unwrap(), "string");
        assert_eq!(b.get_type("a").unwrap(), "array");
    }

    #[test]
    fn test_builder_get_value_mut() {
        let mut b = JsonBuilder::new(json!({"a": {"b": 1}}));
        let slot = b.get_value_mut("a.b").unwrap();
        *slot = json!(99);
        assert_eq!(b.data(), &json!({"a": {"b": 99}}));
    }

    #[test]
    fn test_builder_merge() {
        let mut b = JsonBuilder::new(json!({"a": 1}));
        b.merge(&json!({"b": 2}));
        assert_eq!(b.data(), &json!({"a": 1, "b": 2}));
    }

    #[test]
    fn test_builder_into_inner() {
        let b = JsonBuilder::new(json!({"x": 1}));
        let v = b.into_inner();
        assert_eq!(v, json!({"x": 1}));
    }
}
