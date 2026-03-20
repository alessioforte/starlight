mod aggregator;
mod csv_reader;
mod csv_writer;
mod dummy;
mod filter;
mod http_sender;
mod json_mapper;
mod logger;
mod math_exp_eval;
mod number_generator;
mod splitter;
mod timer;
mod type_converter;

use crate::err::{EngineError, Result};
use crate::task::Task;
use std::collections::HashMap;

/// Function pointer type for task factory functions.
///
/// Every task exposes a `create(id, params) -> Result<Box<dyn Task>>` function
/// that can be stored in the registry.
pub type CreateFn = fn(String, serde_json::Value) -> Result<Box<dyn Task>>;

/// Dynamic task registry.
///
/// Maps task type names (e.g. `"filter"`, `"aggregator"`) to their factory
/// functions. The registry is owned by the [`Engine`] and passed to each
/// [`WorkflowBuilder`] so that workflows can instantiate tasks by name.
///
/// # Built-in tasks
///
/// Call [`TaskRegistry::with_builtins()`] to get a registry pre-loaded with
/// all built-in tasks. You can then register additional custom tasks on top.
///
/// # Custom tasks
///
/// ```ignore
/// use eng::prelude::*;
/// use eng::TaskRegistry;
///
/// fn my_task_create(id: String, params: serde_json::Value) -> eng::Result<Box<dyn Task>> {
///     // ...
/// }
///
/// let mut registry = TaskRegistry::with_builtins();
/// registry.register("my_task", my_task_create);
/// ```
pub struct TaskRegistry {
    factories: HashMap<String, CreateFn>,
}

impl TaskRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self {
            factories: HashMap::new(),
        }
    }

    /// Create a registry pre-loaded with all built-in tasks.
    pub fn with_builtins() -> Self {
        let mut r = Self::new();
        r.register("aggregator", aggregator::Aggregator::create);
        r.register("csv_reader", csv_reader::CsvReader::create);
        r.register("csv_writer", csv_writer::CsvWriter::create);
        r.register("dummy", dummy::Dummy::create);
        r.register("filter", filter::Filter::create);
        r.register("http_sender", http_sender::HttpSender::create);
        r.register("json_mapper", json_mapper::JsonMapper::create);
        r.register("logger", logger::Logger::create);
        r.register("math_exp_eval", math_exp_eval::MathExpEval::create);
        r.register("number_generator", number_generator::NumberGenerator::create);
        r.register("splitter", splitter::Splitter::create);
        r.register("timer", timer::Timer::create);
        r.register("type_converter", type_converter::TypeConverter::create);
        r
    }

    /// Register a task factory under the given name.
    ///
    /// If a factory with the same name already exists it is replaced.
    pub fn register(&mut self, name: impl Into<String>, factory: CreateFn) -> &mut Self {
        self.factories.insert(name.into(), factory);
        self
    }

    /// Instantiate a task by type name.
    pub fn create(&self, type_name: &str, id: String, params: serde_json::Value) -> Result<Box<dyn Task>> {
        let factory = self.factories.get(type_name).ok_or_else(|| {
            EngineError::invalid_params(
                &id,
                format!("unknown task type '{}'. Available: {:?}", type_name, self.list()),
            )
        })?;
        (factory)(id, params)
    }

    /// Check whether a task type is registered.
    pub fn contains(&self, name: &str) -> bool {
        self.factories.contains_key(name)
    }

    /// List all registered task type names (sorted).
    pub fn list(&self) -> Vec<&str> {
        let mut names: Vec<&str> = self.factories.keys().map(|s| s.as_str()).collect();
        names.sort();
        names
    }
}

impl Default for TaskRegistry {
    fn default() -> Self {
        Self::with_builtins()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_registry_builtins() {
        let r = TaskRegistry::with_builtins();
        assert!(r.contains("filter"));
        assert!(r.contains("aggregator"));
        assert!(r.contains("csv_reader"));
        assert!(r.contains("csv_writer"));
        assert!(r.contains("dummy"));
        assert!(r.contains("http_sender"));
        assert!(r.contains("json_mapper"));
        assert!(r.contains("logger"));
        assert!(r.contains("math_exp_eval"));
        assert!(r.contains("number_generator"));
        assert!(r.contains("splitter"));
        assert!(r.contains("timer"));
        assert!(r.contains("type_converter"));
        assert_eq!(r.list().len(), 13);
    }

    #[test]
    fn test_registry_create_builtin() {
        let r = TaskRegistry::with_builtins();
        let task = r.create("dummy", "d1".into(), serde_json::json!({}));
        assert!(task.is_ok());
    }

    #[test]
    fn test_registry_unknown_type() {
        let r = TaskRegistry::with_builtins();
        let result = r.create("nonexistent", "x".into(), serde_json::json!({}));
        assert!(result.is_err());
    }

    #[test]
    fn test_registry_custom_task() {
        fn my_create(id: String, _params: serde_json::Value) -> Result<Box<dyn Task>> {
            // Reuse Dummy internally for testing
            dummy::Dummy::create(id, serde_json::json!({}))
        }

        let mut r = TaskRegistry::with_builtins();
        r.register("my_custom", my_create as CreateFn);
        assert!(r.contains("my_custom"));
        assert!(r.create("my_custom", "c1".into(), serde_json::json!({})).is_ok());
    }

    #[test]
    fn test_registry_replace() {
        let mut r = TaskRegistry::new();
        fn f1(_id: String, _p: serde_json::Value) -> Result<Box<dyn Task>> {
            dummy::Dummy::create("a".into(), serde_json::json!({}))
        }
        fn f2(_id: String, _p: serde_json::Value) -> Result<Box<dyn Task>> {
            dummy::Dummy::create("b".into(), serde_json::json!({}))
        }
        r.register("t", f1 as CreateFn);
        r.register("t", f2 as CreateFn);
        assert_eq!(r.list().len(), 1);
    }

    #[test]
    fn test_registry_empty() {
        let r = TaskRegistry::new();
        assert!(!r.contains("anything"));
        assert!(r.list().is_empty());
    }

    #[test]
    fn test_registry_list_sorted() {
        let r = TaskRegistry::with_builtins();
        let list = r.list();
        let mut sorted = list.clone();
        sorted.sort();
        assert_eq!(list, sorted);
    }

    #[test]
    fn test_registry_default() {
        let r = TaskRegistry::default();
        assert_eq!(r.list().len(), 13);
    }
}
