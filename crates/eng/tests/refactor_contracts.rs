use eng::{Config, Engine, JobConfig, JobErrorPolicy, TaskConfig, TaskRegistry, WorkflowBuilder};
use serde_json::json;
use std::time::Duration;

fn config(id: &str, tasks: Vec<TaskConfig>) -> Config {
    Config {
        id: id.to_string(),
        name: id.to_string(),
        description: None,
        channel_buffer_size: None,
        resources: Vec::new(),
        tasks,
        jobs: Vec::new(),
    }
}

fn job(id: &str, tasks: Vec<TaskConfig>) -> JobConfig {
    JobConfig {
        id: id.to_string(),
        name: None,
        description: None,
        on_error: JobErrorPolicy::Fail,
        resources: Vec::new(),
        artifacts: Vec::new(),
        tasks,
    }
}

fn status_name(workflow: &eng::Workflow) -> String {
    let status = serde_json::to_value(workflow.info()).unwrap()["status"].clone();
    if let Some(name) = status.as_str() {
        return name.to_string();
    }
    if status.get("failed").is_some() {
        return "failed".to_string();
    }
    status.to_string()
}

async fn wait_for_status(workflow: &eng::Workflow, expected: &str) {
    for _ in 0..100 {
        if status_name(workflow) == expected {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    panic!(
        "workflow status was '{}', expected '{}'",
        status_name(workflow),
        expected
    );
}

#[test]
fn rejects_workflow_id_that_can_escape_checkpoint_root() {
    let engine = Engine::new().without_checkpoints();

    for id in [
        "",
        ".",
        "..",
        "../outside",
        "nested/workflow",
        "nested\\workflow",
        "bad\nid",
    ] {
        let cfg = config(id, vec![TaskConfig::new("task", "dummy", json!({}))]);
        assert!(
            engine.validate_config(&cfg).is_err(),
            "accepted unsafe ID '{id}'"
        );
    }
}

#[test]
fn accepts_generated_opaque_workflow_ids() {
    let engine = Engine::new().without_checkpoints();

    for id in [
        "01JZ9V6Q3W8P1M5XT2R7K0ABCD",
        "550e8400-e29b-41d4-a716-446655440000",
        "507f1f77bcf86cd799439011",
    ] {
        let cfg = config(id, vec![TaskConfig::new("task", "dummy", json!({}))]);
        engine
            .validate_config(&cfg)
            .unwrap_or_else(|err| panic!("rejected generated ID '{id}': {err}"));
    }
}

#[test]
fn rejects_zero_channel_capacity_before_runtime_construction() {
    let engine = Engine::new().without_checkpoints();
    let mut cfg = config(
        "zero_capacity",
        vec![TaskConfig::new("task", "dummy", json!({}))],
    );
    cfg.channel_buffer_size = Some(0);

    assert!(engine.validate_config(&cfg).is_err());

    let registry = TaskRegistry::with_builtins();
    assert!(
        WorkflowBuilder::new("zero_capacity_builder", &registry)
            .channel_capacity(0)
            .add_task(TaskConfig::new("task", "dummy", json!({})))
            .build()
            .is_err()
    );
}

#[test]
fn rejects_invalid_builtin_semantics_during_config_validation() {
    let engine = Engine::new().without_checkpoints();
    let bad_generator = config(
        "bad_generator",
        vec![TaskConfig::new(
            "gen",
            "number_generator",
            json!({"min": 10, "max": 1}),
        )],
    );
    let bad_aggregator = config(
        "bad_aggregator",
        vec![TaskConfig::new(
            "agg",
            "aggregator",
            json!({"columns": [{"field": "value", "fn": "sum"}]}),
        )],
    );

    assert!(engine.validate_config(&bad_generator).is_err());
    assert!(engine.validate_config(&bad_aggregator).is_err());
}

#[tokio::test]
async fn rejects_duplicate_workflow_mount() {
    let mut engine = Engine::new().without_checkpoints();
    engine
        .add(config(
            "duplicate",
            vec![TaskConfig::new("task", "dummy", json!({}))],
        ))
        .unwrap();

    assert!(
        engine
            .add(config(
                "duplicate",
                vec![TaskConfig::new("replacement", "dummy", json!({}))],
            ))
            .is_err()
    );
}

#[test]
fn rejects_cycle_hidden_by_shared_channel_producer() {
    let engine = Engine::new().without_checkpoints();
    let cfg = config(
        "shared_channel_cycle",
        vec![
            TaskConfig::new("a", "dummy", json!({}))
                .with_dependency("from_c")
                .with_output("out", vec!["shared".to_string()]),
            TaskConfig::new("b", "dummy", json!({})).with_output("out", vec!["shared".to_string()]),
            TaskConfig::new("c", "dummy", json!({}))
                .with_dependency("shared")
                .with_output("out", vec!["from_c".to_string()]),
        ],
    );

    assert!(engine.validate_config(&cfg).is_err());
}

#[test]
fn rejects_source_without_required_output_during_validation() {
    let engine = Engine::new().without_checkpoints();
    let cfg = config(
        "missing_output",
        vec![TaskConfig::new(
            "gen",
            "number_generator",
            json!({"min": 1, "max": 1, "count": 1}),
        )],
    );

    assert!(engine.validate_config(&cfg).is_err());
}

#[tokio::test]
async fn configured_but_unconsumed_output_does_not_fail_source() {
    let registry = TaskRegistry::with_builtins();
    let mut workflow = WorkflowBuilder::new("unused_output", &registry)
        .add_task(
            TaskConfig::new(
                "gen",
                "number_generator",
                json!({"min": 1, "max": 1, "interval_ms": 0, "count": 1}),
            )
            .with_output("out", vec!["unused".to_string()]),
        )
        .build()
        .unwrap();

    workflow.start().await.unwrap();
    wait_for_status(&workflow, "completed").await;
}

#[tokio::test]
async fn empty_job_completes_without_start_failure() {
    let registry = TaskRegistry::with_builtins();
    let mut workflow = WorkflowBuilder::new("empty_job", &registry)
        .add_job(job("empty", Vec::new()))
        .build()
        .unwrap();

    workflow.start().await.unwrap();
    wait_for_status(&workflow, "completed").await;
}

#[tokio::test]
async fn completed_workflow_cannot_be_paused() {
    let registry = TaskRegistry::with_builtins();
    let mut workflow = WorkflowBuilder::new("completed_pause", &registry)
        .add_task(TaskConfig::new("done", "dummy", json!({})))
        .build()
        .unwrap();

    workflow.start().await.unwrap();
    wait_for_status(&workflow, "completed").await;

    assert!(workflow.pause().await.is_err());
    assert_eq!(status_name(&workflow), "completed");
}
