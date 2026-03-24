//! Zero-copy message wrapper with COW semantics
//!
//! `Msg` wraps `serde_json::Value` and provides copy-on-write behavior:
//! - Fan-out = 1: the message passes through as `Owned` (zero clone)
//! - Fan-out > 1: the message is promoted to `Shared` (`Arc<Value>`) and
//!   downstream consumers get cheap `Arc::clone` references
//! - Tasks that need to mutate call `into_owned()` which is zero-copy if
//!   the message is already `Owned` or the last `Arc` reference

use serde_json::Value;
use std::sync::Arc;

/// A message flowing between tasks.
///
/// Wraps a `serde_json::Value` with copy-on-write semantics.
/// Read-only access is always zero-copy via [`as_ref()`](Msg::as_ref) or `Deref`.
/// Mutable access uses [`into_owned()`](Msg::into_owned) which avoids cloning
/// when possible.
#[derive(Debug)]
pub enum Msg {
    /// Exclusively owned value (zero-copy path for fan-out = 1)
    Owned(Value),
    /// Shared immutable value (cheap clone via Arc for fan-out > 1)
    Shared(Arc<Value>),
}

impl Msg {
    /// Get an immutable reference to the inner `Value`. Always zero-copy.
    #[inline]
    pub fn as_ref(&self) -> &Value {
        match self {
            Msg::Owned(v) => v,
            Msg::Shared(arc) => arc,
        }
    }

    /// Convert to an owned `Value`.
    ///
    /// - `Owned` → zero-copy (unwrap)
    /// - `Shared` with refcount 1 → zero-copy (`Arc::try_unwrap`)
    /// - `Shared` with refcount > 1 → clone (unavoidable)
    #[inline]
    pub fn into_owned(self) -> Value {
        match self {
            Msg::Owned(v) => v,
            Msg::Shared(arc) => Arc::try_unwrap(arc).unwrap_or_else(|a| (*a).clone()),
        }
    }

    /// Promote to `Shared` for fan-out > 1.
    /// If already `Shared`, this is a no-op.
    #[inline]
    pub(crate) fn to_shared(self) -> Msg {
        match self {
            Msg::Owned(v) => Msg::Shared(Arc::new(v)),
            Msg::Shared(arc) => Msg::Shared(arc),
        }
    }
}

impl Clone for Msg {
    #[inline]
    fn clone(&self) -> Self {
        match self {
            Msg::Owned(v) => Msg::Owned(v.clone()),
            Msg::Shared(arc) => Msg::Shared(Arc::clone(arc)),
        }
    }
}

impl std::ops::Deref for Msg {
    type Target = Value;

    #[inline]
    fn deref(&self) -> &Value {
        self.as_ref()
    }
}

impl From<Value> for Msg {
    #[inline]
    fn from(v: Value) -> Self {
        Msg::Owned(v)
    }
}

impl PartialEq for Msg {
    fn eq(&self, other: &Self) -> bool {
        self.as_ref() == other.as_ref()
    }
}

impl PartialEq<Value> for Msg {
    fn eq(&self, other: &Value) -> bool {
        self.as_ref() == other
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
    fn test_owned_as_ref() {
        let msg = Msg::Owned(json!({"a": 1}));
        assert_eq!(msg.as_ref(), &json!({"a": 1}));
    }

    #[test]
    fn test_shared_as_ref() {
        let msg = Msg::Shared(Arc::new(json!({"a": 1})));
        assert_eq!(msg.as_ref(), &json!({"a": 1}));
    }

    #[test]
    fn test_owned_into_owned() {
        let msg = Msg::Owned(json!(42));
        assert_eq!(msg.into_owned(), json!(42));
    }

    #[test]
    fn test_shared_into_owned_last_ref() {
        let msg = Msg::Shared(Arc::new(json!(42)));
        // Single Arc reference → try_unwrap succeeds (zero clone)
        assert_eq!(msg.into_owned(), json!(42));
    }

    #[test]
    fn test_shared_into_owned_multiple_refs() {
        let arc = Arc::new(json!(42));
        let _keep = Arc::clone(&arc);
        let msg = Msg::Shared(arc);
        // Multiple Arc references → clone required
        assert_eq!(msg.into_owned(), json!(42));
    }

    #[test]
    fn test_to_shared_from_owned() {
        let msg = Msg::Owned(json!("hello"));
        let shared = msg.to_shared();
        assert!(matches!(shared, Msg::Shared(_)));
        assert_eq!(shared, json!("hello"));
    }

    #[test]
    fn test_to_shared_from_shared() {
        let msg = Msg::Shared(Arc::new(json!("hello")));
        let shared = msg.to_shared();
        assert!(matches!(shared, Msg::Shared(_)));
        assert_eq!(shared, json!("hello"));
    }

    #[test]
    fn test_clone_owned() {
        let msg = Msg::Owned(json!({"x": 1}));
        let cloned = msg.clone();
        assert_eq!(msg, cloned);
    }

    #[test]
    fn test_clone_shared_is_cheap() {
        let arc = Arc::new(json!({"x": 1}));
        let msg = Msg::Shared(Arc::clone(&arc));
        let cloned = msg.clone();
        // Both point to the same Arc
        assert_eq!(Arc::strong_count(&arc), 3); // arc + msg + cloned
        assert_eq!(msg, cloned);
    }

    #[test]
    fn test_from_value() {
        let msg: Msg = json!({"key": "val"}).into();
        assert!(matches!(msg, Msg::Owned(_)));
        assert_eq!(msg, json!({"key": "val"}));
    }

    #[test]
    fn test_deref() {
        let msg = Msg::Owned(json!({"name": "test"}));
        // Deref allows calling Value methods directly
        assert!(msg.is_object());
        assert_eq!(msg["name"], json!("test"));
    }

    #[test]
    fn test_partial_eq_value() {
        let msg = Msg::Owned(json!(42));
        assert_eq!(msg, json!(42));
    }
}
