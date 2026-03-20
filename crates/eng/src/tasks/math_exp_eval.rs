//! Math Expression Evaluator Task
//!
//! Evaluates mathematical expressions on each message, binding JSON fields
//! as variables and writing results back into the message.
//!
//! Expressions are evaluated in order and can reference results of previous
//! expressions (chaining).

use crate::ctx::TaskContext;
use crate::err::Result;
use jb::{as_f64, get, set};
use crate::task::{BaseTask, Task, TaskInfo};
use async_trait::async_trait;
use evalexpr::*;
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Math Expression Evaluator parameters.
///
/// # YAML example
///
/// ```yaml
/// vars:
///   ax: ax
///   ay: ay
///   vmod: vmod
/// consts:
///   g: 9.81
/// expressions:
///   amod: "math::sqrt(math::pow(ax, 2) + math::pow(ay, 2))"
///   v: vmod / 3.6
///   mp: amod * v
/// mapping:
///   amod: result.amod
///   mp: result.mp
/// ```
#[derive(Debug, Serialize, Deserialize)]
pub struct Params {
    /// Variable bindings: `{ eval_name: "json.field.path" }`.
    /// Each entry extracts a numeric value from the message and makes it
    /// available as a float variable in expressions.
    pub vars: HashMap<String, String>,

    /// Optional constants injected into every evaluation context.
    /// These are set once and never change.
    #[serde(default)]
    pub consts: HashMap<String, f64>,

    /// Named expressions, evaluated **in insertion order**.
    /// An expression can reference variables, constants, built-in math
    /// functions, and results of earlier expressions.
    pub expressions: IndexMap<String, String>,

    /// Result mapping: `{ expression_name: "output.json.path" }`.
    /// Each matched result is written into the message at the given path.
    pub mapping: HashMap<String, String>,
}

/// MathExpEval state (stateless).
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct State {}

// ---------------------------------------------------------------------------
// Pre-compiled expressions
// ---------------------------------------------------------------------------

/// Concrete numeric-types alias used throughout.
type NT = DefaultNumericTypes;

/// A pre-compiled (parsed) expression with its name.
#[derive(Debug)]
struct CompiledExpr {
    name: String,
    node: Node<NT>,
}

/// Parse all expressions once at task creation.
fn compile_expressions(exprs: &IndexMap<String, String>) -> std::result::Result<Vec<CompiledExpr>, String> {
    exprs
        .iter()
        .map(|(name, src)| {
            build_operator_tree::<NT>(src)
                .map(|node| CompiledExpr {
                    name: name.clone(),
                    node,
                })
                .map_err(|e| format!("expression '{}': {}", name, e))
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Task implementation
// ---------------------------------------------------------------------------

/// Math Expression Evaluator task.
///
/// Binds numeric fields from each message as variables, evaluates a chain
/// of mathematical expressions, and writes the results back into the message.
///
/// ## Improvements over the legacy version
///
/// - **Pre-compiled expressions** — parsed once at creation, not per message.
/// - **No unwrap / panic** — non-numeric vars and failed evals are handled
///   gracefully (message routed to optional `"error"` output).
/// - **Built-in constants** — `PI`, `E`, `TAU` auto-injected.
/// - **Direct float extraction** — no `to_string().parse::<f64>()` round-trip.
/// - **Constants loaded once** — not cloned per message.
///
/// # Outputs
///
/// | Label     | Description                              |
/// |-----------|------------------------------------------|
/// | `"out"`   | Messages with expression results applied  |
/// | `"error"` | Messages where evaluation failed (opt.)   |
pub struct MathExpEval {
    base: BaseTask<Params, State>,
    compiled: Vec<CompiledExpr>,
}

impl MathExpEval {
    pub fn create(id: String, params: Value) -> Result<Box<dyn Task>> {
        let base: BaseTask<Params, State> = BaseTask::new(id, params)?;

        let compiled = compile_expressions(&base.params.expressions).map_err(|e| {
            crate::err::EngineError::invalid_params(&base.id, e)
        })?;

        Ok(Box::new(Self { base, compiled }))
    }

    /// Build the evalexpr context for a single message.
    /// Returns `None` if a required var cannot be extracted as f64.
    fn build_context(
        &self,
        msg: &Value,
    ) -> std::result::Result<HashMapContext::<NT>, String> {
        let mut ctx = HashMapContext::<NT>::new();

        // Built-in math constants
        let _ = ctx.set_value("PI".into(), evalexpr::Value::<NT>::Float(std::f64::consts::PI));
        let _ = ctx.set_value("E".into(), evalexpr::Value::<NT>::Float(std::f64::consts::E));
        let _ = ctx.set_value("TAU".into(), evalexpr::Value::<NT>::Float(std::f64::consts::TAU));

        // User-defined constants (already f64)
        for (name, val) in &self.base.params.consts {
            let _ = ctx.set_value(name.into(), evalexpr::Value::<NT>::Float(*val));
        }

        // Variables from message fields
        for (var_name, json_path) in &self.base.params.vars {
            match get(msg, json_path) {
                Some(v) => {
                    let num = as_f64(v).ok_or_else(|| {
                        format!(
                            "var '{}' at path '{}': value {:?} is not numeric",
                            var_name, json_path, v
                        )
                    })?;
                    let _ = ctx.set_value(var_name.into(), evalexpr::Value::<NT>::Float(num));
                }
                None => {
                    return Err(format!(
                        "var '{}': field '{}' not found in message",
                        var_name, json_path
                    ));
                }
            }
        }

        Ok(ctx)
    }

    /// Evaluate all compiled expressions in order, returning named results.
    /// Each result is also injected into the context for chaining.
    fn evaluate(
        &self,
        ctx: &mut HashMapContext::<NT>,
    ) -> std::result::Result<Vec<(String, f64)>, String> {
        let mut results = Vec::with_capacity(self.compiled.len());

        for expr in &self.compiled {
            let val = expr
                .node
                .eval_with_context(ctx)
                .map_err(|e| format!("expression '{}': {}", expr.name, e))?;

            let num = eval_as_f64(&val).ok_or_else(|| {
                format!(
                    "expression '{}': result {:?} is not numeric",
                    expr.name, val
                )
            })?;

            // Make result available for subsequent expressions
            let _ = ctx.set_value(expr.name.clone().into(), evalexpr::Value::<NT>::Float(num));
            results.push((expr.name.clone(), num));
        }

        Ok(results)
    }
}

/// Convert an evalexpr Value to f64.
fn eval_as_f64(v: &evalexpr::Value<NT>) -> Option<f64> {
    match v {
        evalexpr::Value::<NT>::Float(f) => Some(*f),
        evalexpr::Value::<NT>::Int(i) => Some(*i as f64),
        evalexpr::Value::<NT>::Boolean(b) => Some(if *b { 1.0 } else { 0.0 }),
        _ => None,
    }
}

#[async_trait]
impl Task for MathExpEval {
    fn name(&self) -> &str {
        "MathExpEval"
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
        let error_out = ctx.output("error").ok();

        tracing::info!(
            "MathExpEval [{}]: {} var(s), {} const(s), {} expression(s), {} mapping(s)",
            self.base.id,
            self.base.params.vars.len(),
            self.base.params.consts.len(),
            self.compiled.len(),
            self.base.params.mapping.len(),
        );

        let mut processed = 0u64;
        let mut errors = 0u64;

        while ctx.running().await {
            match input.recv().await {
                Ok(mut msg) => {
                    // Build eval context from message
                    match self.build_context(&msg) {
                        Ok(mut eval_ctx) => {
                            match self.evaluate(&mut eval_ctx) {
                                Ok(results) => {
                                    // Write results into message via mapping
                                    for (name, value) in &results {
                                        if let Some(path) = self.base.params.mapping.get(name) {
                                            set(&mut msg, path, json!(value));
                                        }
                                    }
                                    output.send(msg).await?;
                                    processed += 1;
                                }
                                Err(e) => {
                                    errors += 1;
                                    tracing::debug!(
                                        "MathExpEval [{}]: eval error: {}",
                                        self.base.id,
                                        e
                                    );
                                    if let Some(ref err_out) = error_out {
                                        msg["_error"] = json!(e);
                                        err_out.send(msg).await?;
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            errors += 1;
                            tracing::debug!(
                                "MathExpEval [{}]: context error: {}",
                                self.base.id,
                                e
                            );
                            if let Some(ref err_out) = error_out {
                                msg["_error"] = json!(e);
                                err_out.send(msg).await?;
                            }
                        }
                    }

                    if (processed + errors) % 10_000 == 0 {
                        tracing::debug!(
                            "MathExpEval [{}]: {} ok, {} errors",
                            self.base.id,
                            processed,
                            errors,
                        );
                    }
                }
                Err(_) => {
                    tracing::debug!(
                        "MathExpEval [{}]: Input channel closed",
                        self.base.id,
                    );
                    break;
                }
            }
        }

        tracing::info!(
            "MathExpEval [{}]: Finished — {} processed, {} errors",
            self.base.id,
            processed,
            errors,
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

    // --- helpers ---

    fn make_eval(params: Value) -> MathExpEval {
        let base: BaseTask<Params, State> = BaseTask::new("test".into(), params).unwrap();
        let compiled = compile_expressions(&base.params.expressions).unwrap();
        MathExpEval { base, compiled }
    }

    // --- get_nested / set_nested ---

    #[test]
    fn test_get_nested_simple() {
        let v = json!({"a": {"b": 42}});
        assert_eq!(get(&v, "a.b"), Some(&json!(42)));
    }

    #[test]
    fn test_get_nested_missing() {
        let v = json!({"a": 1});
        assert_eq!(get(&v, "b"), None);
    }

    #[test]
    fn test_set_nested_creates_intermediate() {
        let mut v = json!({});
        set(&mut v, "a.b.c", json!(99));
        assert_eq!(v, json!({"a": {"b": {"c": 99}}}));
    }

    #[test]
    fn test_set_nested_overwrites() {
        let mut v = json!({"x": 1});
        set(&mut v, "x", json!(2));
        assert_eq!(v, json!({"x": 2}));
    }

    // --- as_f64 ---

    #[test]
    fn test_value_to_f64_number() {
        assert_eq!(as_f64(&json!(3.14)), Some(3.14));
        assert_eq!(as_f64(&json!(42)), Some(42.0));
    }

    #[test]
    fn test_value_to_f64_string() {
        assert_eq!(as_f64(&json!("3.14")), Some(3.14));
        assert_eq!(as_f64(&json!("abc")), None);
    }

    #[test]
    fn test_value_to_f64_bool() {
        assert_eq!(as_f64(&json!(true)), Some(1.0));
        assert_eq!(as_f64(&json!(false)), Some(0.0));
    }

    #[test]
    fn test_value_to_f64_null() {
        assert_eq!(as_f64(&json!(null)), None);
    }

    // --- compile_expressions ---

    #[test]
    fn test_compile_valid() {
        let mut exprs = IndexMap::new();
        exprs.insert("a".into(), "1 + 2".into());
        exprs.insert("b".into(), "a * 3".into());
        assert!(compile_expressions(&exprs).is_ok());
    }

    #[test]
    fn test_eval_undefined_variable() {
        // evalexpr is very permissive at parse time, so we test runtime errors
        let eval = make_eval(json!({
            "vars": {},
            "expressions": {"r": "undefined_var + 1"},
            "mapping": {}
        }));
        let msg = json!({});
        let mut ctx = eval.build_context(&msg).unwrap();
        let result = eval.evaluate(&mut ctx);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("r"));
    }

    // --- build_context ---

    #[test]
    fn test_build_context_success() {
        let eval = make_eval(json!({
            "vars": {"x": "data.x", "y": "data.y"},
            "consts": {"g": 9.81},
            "expressions": {},
            "mapping": {}
        }));
        let msg = json!({"data": {"x": 3.0, "y": 4.0}});
        let ctx = eval.build_context(&msg);
        assert!(ctx.is_ok());
    }

    #[test]
    fn test_build_context_missing_var() {
        let eval = make_eval(json!({
            "vars": {"x": "missing.field"},
            "expressions": {},
            "mapping": {}
        }));
        let msg = json!({"other": 1});
        let ctx = eval.build_context(&msg);
        assert!(ctx.is_err());
        assert!(ctx.unwrap_err().contains("not found"));
    }

    #[test]
    fn test_build_context_non_numeric_var() {
        let eval = make_eval(json!({
            "vars": {"x": "name"},
            "expressions": {},
            "mapping": {}
        }));
        let msg = json!({"name": "alice"});
        let ctx = eval.build_context(&msg);
        assert!(ctx.is_err());
        assert!(ctx.unwrap_err().contains("not numeric"));
    }

    #[test]
    fn test_build_context_builtin_constants() {
        let eval = make_eval(json!({
            "vars": {},
            "expressions": {},
            "mapping": {}
        }));
        let msg = json!({});
        let ctx = eval.build_context(&msg).unwrap();
        // PI should be available
        let result = eval_with_context::<HashMapContext::<NT>>("PI", &ctx).unwrap();
        let f = eval_as_f64(&result).unwrap();
        assert!((f - std::f64::consts::PI).abs() < 1e-10);
    }

    // --- evaluate ---

    #[test]
    fn test_evaluate_simple() {
        let eval = make_eval(json!({
            "vars": {"x": "x"},
            "expressions": {"result": "x * 2 + 1"},
            "mapping": {"result": "out"}
        }));
        let msg = json!({"x": 5});
        let mut ctx = eval.build_context(&msg).unwrap();
        let results = eval.evaluate(&mut ctx).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, "result");
        assert!((results[0].1 - 11.0).abs() < 1e-10);
    }

    #[test]
    fn test_evaluate_chaining() {
        let eval = make_eval(json!({
            "vars": {"x": "x"},
            "expressions": {
                "doubled": "x * 2",
                "tripled": "doubled + x"
            },
            "mapping": {}
        }));
        let msg = json!({"x": 10});
        let mut ctx = eval.build_context(&msg).unwrap();
        let results = eval.evaluate(&mut ctx).unwrap();
        assert_eq!(results.len(), 2);
        assert!((results[0].1 - 20.0).abs() < 1e-10); // doubled = 20
        assert!((results[1].1 - 30.0).abs() < 1e-10); // tripled = 30
    }

    #[test]
    fn test_evaluate_with_consts() {
        let eval = make_eval(json!({
            "vars": {},
            "consts": {"g": 9.81},
            "expressions": {"force": "g * 10"},
            "mapping": {}
        }));
        let msg = json!({});
        let mut ctx = eval.build_context(&msg).unwrap();
        let results = eval.evaluate(&mut ctx).unwrap();
        assert!((results[0].1 - 98.1).abs() < 1e-10);
    }

    #[test]
    fn test_evaluate_math_functions() {
        let eval = make_eval(json!({
            "vars": {"x": "x", "y": "y"},
            "expressions": {
                "hyp": "math::sqrt(math::pow(x, 2) + math::pow(y, 2))"
            },
            "mapping": {}
        }));
        let msg = json!({"x": 3, "y": 4});
        let mut ctx = eval.build_context(&msg).unwrap();
        let results = eval.evaluate(&mut ctx).unwrap();
        assert!((results[0].1 - 5.0).abs() < 1e-10);
    }

    #[test]
    fn test_evaluate_with_pi() {
        let eval = make_eval(json!({
            "vars": {"r": "r"},
            "expressions": {
                "area": "PI * math::pow(r, 2)"
            },
            "mapping": {}
        }));
        let msg = json!({"r": 1});
        let mut ctx = eval.build_context(&msg).unwrap();
        let results = eval.evaluate(&mut ctx).unwrap();
        assert!((results[0].1 - std::f64::consts::PI).abs() < 1e-10);
    }

    // --- full pipeline (build_context + evaluate + mapping) ---

    #[test]
    fn test_full_pipeline() {
        let eval = make_eval(json!({
            "vars": {"ax": "ax", "ay": "ay"},
            "consts": {"g": 9.81},
            "expressions": {
                "amod": "math::sqrt(math::pow(ax, 2) + math::pow(ay, 2))",
                "ratio": "amod / g"
            },
            "mapping": {
                "amod": "result.amod",
                "ratio": "result.ratio"
            }
        }));

        let mut msg = json!({"ax": 3.0, "ay": 4.0});
        let mut ctx = eval.build_context(&msg).unwrap();
        let results = eval.evaluate(&mut ctx).unwrap();

        for (name, value) in &results {
            if let Some(path) = eval.base.params.mapping.get(name) {
                set(&mut msg, path, json!(value));
            }
        }

        assert!((msg["result"]["amod"].as_f64().unwrap() - 5.0).abs() < 1e-10);
        let expected_ratio = 5.0 / 9.81;
        assert!((msg["result"]["ratio"].as_f64().unwrap() - expected_ratio).abs() < 1e-10);
    }

    // --- factory ---

    #[test]
    fn test_create_success() {
        let params = json!({
            "vars": {"x": "x"},
            "expressions": {"y": "x + 1"},
            "mapping": {"y": "y"}
        });
        assert!(MathExpEval::create("test".into(), params).is_ok());
    }

    #[test]
    fn test_create_invalid_params() {
        assert!(MathExpEval::create("test".into(), json!({"wrong": true})).is_err());
    }

    #[test]
    fn test_eval_division_by_zero() {
        let eval = make_eval(json!({
            "vars": {"x": "x"},
            "expressions": {"r": "x / 0"},
            "mapping": {}
        }));
        let msg = json!({"x": 1});
        let mut ctx = eval.build_context(&msg).unwrap();
        // evalexpr may or may not error on div by zero depending on type
        // but at minimum it should not panic
        let _ = eval.evaluate(&mut ctx);
    }

    // --- edge cases ---

    #[test]
    fn test_string_numeric_var() {
        // Strings that look like numbers should be parsed
        let eval = make_eval(json!({
            "vars": {"x": "x"},
            "expressions": {"y": "x + 1"},
            "mapping": {}
        }));
        let msg = json!({"x": "42"});
        let ctx = eval.build_context(&msg);
        assert!(ctx.is_ok());
        let mut ctx = ctx.unwrap();
        let results = eval.evaluate(&mut ctx).unwrap();
        assert!((results[0].1 - 43.0).abs() < 1e-10);
    }

    #[test]
    fn test_empty_expressions() {
        let eval = make_eval(json!({
            "vars": {"x": "x"},
            "expressions": {},
            "mapping": {}
        }));
        let msg = json!({"x": 1});
        let mut ctx = eval.build_context(&msg).unwrap();
        let results = eval.evaluate(&mut ctx).unwrap();
        assert!(results.is_empty());
    }
}
