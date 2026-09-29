# Starlight — Agent Guide

Starlight is a Rust workflow engine. A workflow is now an ordered sequence of jobs; each job is an isolated DAG of asynchronous tasks connected by named channels. Legacy single-DAG configs remain supported and are normalized to one synthetic job named `default`.

This guide describes the code at branch `jobs`, commit `09fd10b` (reviewed 2026-07-19). See [JOB_PHASES_AUDIT.md](JOB_PHASES_AUDIT.md) for the implementation and security review, including known defects.

---

## Workspace layout

```text
starlight/
├── src/                         # Root Axum server package
│   ├── main.rs                  # Engine setup, simulator registration, listener
│   ├── api/
│   │   ├── workflows.rs         # Workflow commands and workflow-file CRUD
│   │   ├── generate.rs          # OpenAI-compatible workflow generation endpoint
│   │   ├── health.rs
│   │   └── prompt.md             # Generator prompt (currently legacy-schema only)
│   ├── etc/                     # AppState, logo, configuration helpers
│   └── tasks/simulator.rs        # Server-specific `simulator` task adapter
├── crates/
│   ├── eng/                     # Core engine library
│   │   ├── src/cfg.rs            # Workflow/job/resource/artifact/task schema + validation
│   │   ├── src/wf.rs             # Job runtime, sequential driver, workflow builder/lifecycle
│   │   ├── src/eng.rs            # Workflow registry and public engine commands
│   │   ├── src/checkpoint.rs     # Per-workflow JSON checkpoints
│   │   ├── src/resource.rs       # File/HTTP/artifact resource loading
│   │   ├── src/task.rs           # Task trait and TaskRunner
│   │   ├── src/ctx.rs            # Channels, resources, lifecycle gate, metrics
│   │   └── src/tasks/            # Built-in task implementations
│   ├── cli/                     # `sl` CLI and `sl generate` TUI
│   ├── simulator/               # Signal model library
│   └── jb/                      # JSON path/builder helpers
├── .starlight/
│   ├── cli/config.yaml          # CLI contexts
│   └── engine/
│       ├── workflows/           # YAML/JSON workflow files
│       └── checkpoints/         # Runtime-created checkpoint files
└── JOB_PHASES_AUDIT.md          # Current design/correctness/security assessment
```

The root package and the four crates form one Cargo workspace.

---

## Configuration model

### Workflow

`Config` contains:

- `id`, `name`, optional `description`
- optional `channel_buffer_size` (default `1000`, must be non-zero)
- workflow-scoped `resources`
- either legacy top-level `tasks` or ordered `jobs`, but not both when non-empty

`Config::normalized_jobs()` maps legacy `tasks` to a job with ID `default`, `on_error: fail`, and no job resources or artifacts. Runtime code operates on normalized jobs.

Workflow IDs accepted by `Config::validate()` must be non-empty and may not be `.`, `..`, contain `/` or `\`, or contain control characters. This validation is not consistently called by every API or public builder path; consult the audit before treating IDs as filesystem-safe.

### Jobs

Jobs run strictly in declaration order. The driver creates and starts only the active job, waits for all of its task runners, records its outcome, then advances to the next job.

Each `JobConfig` has:

- `id` and optional `name`
- `on_error`: `fail`, `continue`, or `continue_with_warnings`
- job-scoped `resources`
- declared output `artifacts`
- an isolated task DAG

Task IDs are validated as unique across the whole workflow, not merely within a job. Channels are job-local: a dependency must be produced in the same job, and cross-job data flow is expected to use an artifact plus a later artifact resource.

The current runtime applies `on_error` to task-execution outcomes. Resource loading, task creation, and job start failures fail the workflow regardless of the configured policy. `continue` and `continue_with_warnings` currently have identical runtime behavior.

### Resources and artifacts

Resources can be workflow-scoped or job-scoped. Supported sources are:

- file: JSON, YAML, CSV, text, or bytes
- HTTP: method, headers, optional body, timeout, retry, format, and optional cache
- artifact: a reference to an artifact declared by an earlier job

Workflow resources load when the workflow is built/mounted. Job resources load immediately before that job is spawned. Workflow and active-job resources are merged and placed in each task's `TaskContext`.

`TaskConfig.uses` is currently a validation/declaration field only. It verifies that named resources are visible, but it does not inject a restricted subset and built-in tasks do not automatically bind resource values to their filename/URL params. Every task in the job receives the complete visible resource map.

Artifacts are declarations containing `id`, `path`, and `format`. The engine uses their metadata to resolve later artifact resources; it does not verify that a producer created the file, tie an artifact to a particular task, or checksum its contents.

### Example: two sequential jobs

```yaml
id: temperature_etl
name: Temperature ETL
channel_buffer_size: 100
jobs:
  - id: extract
    on_error: fail
    artifacts:
      - id: raw_csv
        path: .starlight/data/raw.csv
        format: csv
    tasks:
      - id: generate
        type: number_generator
        params: { min: 1, max: 100, count: 20, interval_ms: 10 }
        dependencies: []
        outputs: { out: [raw_rows] }
      - id: write_raw
        type: csv_writer
        params: { filename: .starlight/data/raw.csv, write_mode: overwrite }
        dependencies: [raw_rows]
        outputs: {}

  - id: load
    on_error: fail
    resources:
      - id: raw_data
        source: { type: artifact, ref: raw_csv, format: csv }
    tasks:
      - id: read_raw
        type: csv_reader
        uses: [raw_data]
        params: { filename: .starlight/data/raw.csv }
        dependencies: []
        outputs: { out: [loaded_rows] }
      - id: log_rows
        type: logger
        params: { prefix: "[load]" }
        dependencies: [loaded_rows]
        outputs: {}
```

Flat dependencies map to input port `in`. Tasks that need named ports can use:

```yaml
dependencies:
  left: [left_channel]
  right: [right_channel]
```

Do not assign the same channel to two named ports on one task; the current reverse-map wiring cannot represent that correctly.

---

## Validation and construction

`Config::validate()` checks the normalized workflow as a whole:

- workflow options and task/job layout
- duplicate job IDs and globally duplicate task IDs
- resource scope, visibility, and duplicate IDs
- artifact declaration order and duplicate artifact IDs
- task `uses` visibility
- dependencies whose channels are not produced within the same job

`Engine::validate_config()` first calls `Config::validate()`, then builds every job with `WorkflowBuilder` to validate registered task types, task parameters, required outputs, duplicate task IDs, unresolved dependencies, and cycles. Multiple tasks may intentionally produce the same channel.

`Engine::add()` validates, loads a checkpoint if enabled, builds the workflow, loads workflow resources, and prepares the first incomplete job. `WorkflowBuilder` is also public, but its direct validation is not equivalent to `Config::validate()`; callers should validate `Config` before using it.

---

## Runtime and lifecycle

### Task contract

Every task implements:

```rust
#[async_trait]
pub trait Task: Send + Sync + 'static {
    async fn execute(&self, ctx: Arc<TaskContext>) -> Result<()>;
    fn name(&self) -> &str;
    async fn on_start(&self, ctx: Arc<TaskContext>) -> Result<()> { Ok(()) }
    async fn on_pause(&self, ctx: Arc<TaskContext>) -> Result<()> { Ok(()) }
    async fn on_stop(&self, ctx: Arc<TaskContext>) -> Result<()> { Ok(()) }
}
```

Long-running task loops must use `ctx.running().await` as their lifecycle gate. It blocks while paused and returns `false` after stop. Pause-aware channel helpers also stop a task from continuing sends/receives while paused.

Typical accessors:

- `ctx.output("out")?`
- `ctx.merged_input().await?`
- `ctx.input("port").await`
- `ctx.resource("resource_id")`

Task runners are initially spawned waiting for `Command::Start`. A successful `execute()` currently yields task status `Stopped`; there is no task-level `Completed` variant. The workflow's job runtime interprets all non-failed task exits as normal completion unless a stop flag is set.

### Workflow lifecycle

Workflow statuses are `Idle`, `Running`, `Paused`, `Stopped`, `Completed`, and `Failed(String)`.

The intended transitions are:

```text
Idle/Stopped/Failed ──start──> Running ──pause──> Paused
Paused ──start──> Running
Running/Paused ──stop──> Stopped
last job succeeds/continues ──> Completed
fatal job outcome ──> Failed
```

The sequential driver and the active task runners do not share one durable workflow-level command state. There are known races around job boundaries and immediate stop/start, and the interval timer currently exits on pause. Do not assume lifecycle transitions are race-free; see the audit.

`WorkflowInfo` reports `current_job`, `completed_jobs`, and `failed_jobs`. `task_count` and `/state` cover only the currently active job; completed-job task state and metrics are not retained.

Unmount/removal aborts the driver and active runners immediately. It does not perform an orderly stop, invoke task cleanup hooks, or persist a new checkpoint state.

### Checkpoints

Checkpoints are enabled by default and stored as:

```text
${ENGINE_DIR:-.starlight/engine}/checkpoints/<workflow_id>/workflow.json
```

They persist workflow status, current job ID, completed job IDs, failed job IDs, and an update timestamp. They do not persist task state, configuration identity/version, or artifact integrity. A stopped or failed checkpoint is mounted as resumable from the first job not listed as completed. Continued failed jobs are not currently treated as completed during remount, so they can be reopened.

Use `Engine::without_checkpoints()` in isolated tests that do not need persistence, or `with_checkpoint_dir(...)` with a temporary directory.

---

## Built-in tasks

| Type | Role |
|---|---|
| `number_generator` | Emit random integers at an interval |
| `timer` | Emit interval or cron ticks |
| `csv_reader` | Stream rows from a file |
| `csv_writer` | Write incoming objects as CSV |
| `logger` | Log messages with tracing |
| `filter` | Pass/drop messages using field conditions |
| `splitter` | Route messages by values, maps, or ranges |
| `json_mapper` | Project/rename fields |
| `type_converter` | Convert field types |
| `math_exp_eval` | Evaluate expressions and write results |
| `aggregator` | Count/sum/avg/min/max/collect over count/time windows |
| `http_sender` | Send messages over HTTP |
| `dummy` | No-op task used by tests |
| `simulator` | Compose sine/random/random-walk/trend/anomaly models; registered by the root server |

The `eng` registry contains every task except `simulator`; the root server registers that custom type in `main.rs`.

### Adding a custom task

1. Implement `Task` and ensure `execute()` honors `ctx.running().await`.
2. Expose a factory with signature `fn(String, serde_json::Value) -> eng::Result<Box<dyn Task>>`.
3. Prefer `BaseTask<Params, State>` for serde parameter loading and shared state.
4. Register the factory before validating or adding configs that use it.

```rust
use eng::prelude::*;
use serde::Deserialize;

#[derive(Deserialize)]
struct Params { interval_ms: u64 }

pub struct MyTask(BaseTask<Params, ()>);

impl MyTask {
    pub fn create(id: String, params: serde_json::Value) -> eng::Result<Box<dyn Task>> {
        Ok(Box::new(Self(BaseTask::new(id, params)?)))
    }
}

#[async_trait]
impl Task for MyTask {
    fn name(&self) -> &str { "my_task" }

    async fn execute(&self, ctx: Arc<TaskContext>) -> eng::Result<()> {
        let out = ctx.output("out")?;
        while ctx.running().await {
            out.send(serde_json::json!({"tick": true})).await?;
            tokio::time::sleep(
                std::time::Duration::from_millis(self.0.params.interval_ms)
            ).await;
        }
        Ok(())
    }
}
```

---

## Server API

Default address is `0.0.0.0:8246`. `BASE_PATH` prefixes all routes.

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/health` | Health check |
| `GET` | `/workflows` | List mounted workflows and job progress |
| `GET` | `/workflows/{id}` | Get mounted workflow info |
| `GET` | `/workflows/{id}/state` | Get active-job task metrics/state |
| `PATCH` | `/workflows/{id}` | Send `start`, `pause`, or `stop` |
| `POST` | `/workflows/{id}/mount` | Read a workflow file and add it to the engine |
| `POST` | `/workflows/{id}/unmount` | Remove it from memory but keep its files/checkpoint |
| `GET` | `/workflows/files` | List parseable workflow files |
| `POST` | `/workflows/files` | Serialize a workflow config to YAML |
| `GET` | `/workflows/files/{id}` | Download a config |
| `DELETE` | `/workflows/files/{id}` | Unmount and delete its config file |
| `POST` | `/workflows/generate` | Ask an OpenAI-compatible endpoint for a config |

Generation accepts either `prompt` or a `messages` array plus optional `model`, `base_url`, and `auto_load`. It returns `questions`, `completed`, or `validation_failed`, and makes at most three validation/repair attempts.

Security boundary: the server currently has no authentication/authorization middleware, does not enforce the CLI's `x-api-key`, and accepts workflow features that can access process-readable/writable paths and arbitrary HTTP destinations. Treat it as trusted-local development software only until the audit's critical findings are fixed.

---

## CLI

The CLI binary is `sl` (`cargo run -p cli -- ...`). Its implemented workflow commands are:

```text
sl config get-contexts
sl config set-context --name <name>
sl list
sl list --all
sl push <file.yaml>
sl pull <id> [--output yaml|json]
sl mount <id>
sl unmount <id>
sl run <id>
sl start <id>
sl pause <id>
sl stop <id>
sl state <id>
sl remove <id>
sl generate
```

Context configuration lives at `.starlight/cli/config.yaml`. API keys are stored as plain text and sent as `x-api-key`, but the server does not currently validate that header. Several `sl engine`/context mutation commands are declared but remain placeholders. The CLI also does not yet present job history in its list/state views.

---

## Environment variables

| Variable | Default | Purpose |
|---|---|---|
| `PORT` | `8246` | Server port |
| `BASE_PATH` | empty | Prefix for all Axum routes |
| `ENGINE_DIR` | `.starlight/engine` | Workflow-file and checkpoint storage root |
| `LLM_MODEL` | `qwen3.5-9b` | Default generation model |
| `LLM_BASE_URL` | `http://localhost:1234/v1` | Default OpenAI-compatible endpoint |
| `RUST_LOG` | project default/filter | Tracing verbosity |

---

## Build and verification

```bash
cargo build --workspace
cargo test --workspace --all-targets
cargo test -p eng
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo audit
RUST_LOG=debug cargo run
cargo run -p cli -- list
```

Baseline from the 2026-07-19 audit:

- formatting check passes
- full test suite passes: 345 passed, 2 ignored
- ignored tests document unsupported quoted CSV input and incorrect custom-delimiter CSV quoting
- strict Clippy does not pass
- `cargo audit` reports 7 vulnerable dependency entries and 8 unmaintained/unsound warnings

Tests involving local HTTP listeners must run in an environment that permits loopback socket binding.

---

## Key invariants and development rules

- Jobs are sequential; task DAGs inside one job are concurrent.
- Channels are local to a job. Use artifacts/resources between jobs.
- A channel may merge multiple producers and fan out to multiple consumers within its job.
- Task IDs are globally unique across normalized jobs.
- `ctx.running().await` is the required pause/stop gate for long-running tasks.
- Keep blocking filesystem/network resource loading off async executor and engine-lock paths when extending it.
- Validate a complete `Config` before deriving any filesystem path or invoking `WorkflowBuilder` directly.
- Checkpoint compatibility must account for workflow configuration changes, not only matching job IDs.
- Do not expose the current HTTP server to untrusted networks.
- Preserve legacy top-level `tasks` compatibility unless a versioned migration deliberately removes it.
- When changing lifecycle or checkpoint behavior, test pause/stop at job boundaries, immediate stop/start, continued failures, remount after config changes, and resource/spawn/start failures.
