# Starlight — Agent Guide

**Starlight** is a high-frequency workflow engine in Rust. Workflows are DAGs of async tasks connected by named channels. The project is a Cargo workspace with four crates plus the root binary.

---

## Workspace layout

```
starlight/
├── src/                     # Root binary — Axum API server
│   ├── main.rs              # Engine init, custom task registration, server startup
│   ├── api/
│   │   ├── workflows.rs     # Workflow CRUD, mount/unmount, filesystem
│   │   ├── generate.rs      # LLM-powered workflow generation endpoint
│   │   └── prompt.md        # System prompt injected into generate API
│   └── tasks/
│       └── simulator.rs     # Custom "simulator" task registered in main
├── crates/
│   ├── eng/                 # Core engine library (crate: eng)
│   │   └── src/
│   │       ├── eng.rs       # Engine struct: add/remove/start/pause/stop workflows
│   │       ├── wf.rs        # Workflow + WorkflowBuilder: channel wiring, task spawning
│   │       ├── task.rs      # Task trait + TaskRunner lifecycle (Start/Pause/Stop)
│   │       ├── cfg.rs       # Config + TaskConfig deserialization
│   │       ├── ctx.rs       # TaskContext: input/output channel access, running() guard
│   │       ├── metrics.rs   # CoarseClock (AtomicI64, 100ms tick) + TaskMetrics
│   │       ├── msg.rs       # Msg type (serde_json::Value envelope)
│   │       └── tasks/       # Built-in task implementations
│   ├── cli/                 # CLI binary (crate: cli, bin: sl)
│   │   └── src/
│   │       ├── cli.rs       # Clap command definitions
│   │       ├── cmd.rs       # Command implementations (HTTP calls to API)
│   │       ├── api.rs       # Typed API client
│   │       └── tui.rs       # Ratatui TUI for "sl generate" chat
│   ├── simulator/           # Statistical signal models (sine, random walk, anomaly…)
│   └── jb/                  # JSON builder utility used by eng
└── .starlight/engine/
    └── workflows/           # Default workflow file directory (yaml/json)
```

---

## Core concepts

### Task

Every task implements the `Task` trait (`crates/eng/src/task.rs`):

```rust
#[async_trait]
pub trait Task: Send + Sync + 'static {
    async fn execute(&self, ctx: Arc<TaskContext>) -> Result<()>;
    fn name(&self) -> &str;
    // optional hooks:
    async fn on_start(&self, ctx: Arc<TaskContext>) -> Result<()> { Ok(()) }
    async fn on_pause(&self, ctx: Arc<TaskContext>) -> Result<()> { Ok(()) }
    async fn on_stop(&self, ctx: Arc<TaskContext>)  -> Result<()> { Ok(()) }
}
```

Three task archetypes:
- **Source** — no inputs, loops while `ctx.running().await`, sends to `ctx.output("out")?`
- **Processor** — reads from `ctx.merged_input().await?` or named ports, transforms, sends
- **Sink** — reads input, no outputs

Tasks must exit `execute()` when `ctx.running().await` returns `false`.

### TaskContext

`ctx.running().await` blocks while paused, returns `false` when stopped. Use it as the loop guard. Access channels via `ctx.output("label")` and `ctx.merged_input().await` or `ctx.input("port").await`.

### Channels

Tasks are connected by named string channel IDs. One task's `outputs.out` writes to channel `"my_ch"`; another task's `dependencies: ["my_ch"]` receives from it. `mpsc` channels with configurable buffer size (default 1000). One output can fan-out to multiple consumers; one input can merge multiple upstream channels.

### Workflow lifecycle

`Idle → Running → Paused → Running → Stopped`

Stopped/Failed workflows can be restarted — `Workflow::start()` calls `spawn()` which rebuilds all channels and task instances from the stored `task_configs`.

---

## Built-in task types

| Type | Purpose |
|------|---------|
| `number_generator` | Emits random integers at interval |
| `timer` | Ticks on interval or cron schedule, emits payload |
| `csv_reader` | Streams rows from a CSV file |
| `csv_writer` | Writes incoming messages to CSV |
| `logger` | Logs messages to tracing |
| `filter` | Passes/drops messages based on field conditions |
| `splitter` | Routes messages to different output channels by field value |
| `json_mapper` | Remaps fields (projection / rename) |
| `type_converter` | Casts fields to different types |
| `math_exp_eval` | Evaluates math expressions, adds result fields |
| `aggregator` | Windowed count/sum/avg/min/max/collect |
| `http_sender` | POSTs/GETs messages to an HTTP endpoint |
| `dummy` | No-op, for testing |
| `simulator` | Composite signal generator (sine, random, random_walk, trend, anomaly) — registered in `src/main.rs` |

---

## Adding a custom task

1. Implement `Task` for your struct.
2. Expose a factory: `pub fn create(id: String, params: serde_json::Value) -> eng::Result<Box<dyn Task>>`.
3. Use `BaseTask<Params, State>` from `eng::prelude` to deserialize params via serde.
4. Register in `main.rs`: `engine.register_task("my_type", my_mod::MyTask::create);`

```rust
use eng::prelude::*;
use serde::Deserialize;

#[derive(Deserialize)]
struct Params { interval_ms: u64 }

pub struct MyTask(BaseTask<Params, ()>);

impl MyTask {
    pub fn create(id: String, params: serde_json::Value) -> eng::Result<Box<dyn Task>> {
        Ok(Box::new(MyTask(BaseTask::new(id, params)?)))
    }
}

#[async_trait]
impl Task for MyTask {
    fn name(&self) -> &str { "my_task" }
    async fn execute(&self, ctx: Arc<TaskContext>) -> eng::Result<()> {
        let out = ctx.output("out")?;
        while ctx.running().await {
            out.send(serde_json::json!({"tick": true})).await?;
            tokio::time::sleep(std::time::Duration::from_millis(self.0.params.interval_ms)).await;
        }
        Ok(())
    }
}
```

---

## Workflow config format

YAML (preferred) or JSON. Files live in `ENGINE_DIR/workflows/` (default `.starlight/engine/workflows/`).

```yaml
id: my_workflow           # snake_case, unique
name: My Workflow
description: optional
channel_buffer_size: 1000 # optional, default 1000
tasks:
  - id: gen
    type: number_generator
    params:
      min: 1
      max: 100
      interval_ms: 500
    dependencies: []        # source: no upstream channels
    outputs:
      out: [gen_out]        # label: [channel_ids...]

  - id: log
    type: logger
    params:
      prefix: "[demo]"
    dependencies: [gen_out] # flat list maps to port "in"
    outputs: {}             # sink: no downstream channels
```

Named input ports (for tasks with multiple inputs):
```yaml
dependencies:
  left: [channel_a]
  right: [channel_b]
```

---

## API reference

Default port **8246**. Set `BASE_PATH` env to nest routes under a prefix.

### Engine (loaded workflows)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/workflows` | List loaded workflows |
| GET | `/workflows/{id}` | Get workflow info |
| GET | `/workflows/{id}/state` | Get per-task metrics |
| PATCH | `/workflows/{id}` | Send command `{"command": "start"\|"pause"\|"stop"}` |
| POST | `/workflows/{id}/mount` | Load workflow from disk into engine |
| POST | `/workflows/{id}/unmount` | Unload from engine (keep file) |

### Filesystem (workflow files)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/workflows/files` | List all workflow files (with loaded status) |
| POST | `/workflows/files` | Save a Config as YAML to disk |
| GET | `/workflows/files/{id}` | Download a workflow config |
| DELETE | `/workflows/files/{id}` | Delete file and unmount from engine |

### AI generation

| Method | Path | Description |
|--------|------|-------------|
| POST | `/workflows/generate` | Generate a workflow config via LLM |

**Generate request:**
```json
{
  "prompt": "a sine wave logger",
  "model": "qwen3.5-9b",
  "base_url": "http://localhost:1234/v1",
  "auto_load": false
}
```
Use `messages` (array of `{role, content}`) instead of `prompt` for multi-turn. Returns `status: "questions" | "completed" | "validation_failed"`. The engine validates and auto-repairs configs up to 3 attempts.

---

## CLI (`sl`)

```
sl config get-contexts
sl config set-context --name <name>
sl list                       # loaded workflows
sl list --all                 # all files + loaded status
sl push <file.yaml>           # save workflow file to server
sl pull <id> [--output yaml]  # download workflow config
sl mount <id>                 # load file into engine
sl unmount <id>               # unload from engine
sl run <id>                   # mount + start
sl start <id>
sl pause <id>
sl stop <id>
sl state <id>                 # per-task metrics
sl remove <id>                # delete file + unmount
sl generate                   # interactive TUI chat
```

---

## Environment variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `PORT` | `8246` | API server port |
| `BASE_PATH` | `` | URL prefix for all routes |
| `ENGINE_DIR` | `.starlight/engine` | Workflow files root |
| `LLM_MODEL` | `qwen3.5-9b` | Default model for /generate |
| `LLM_BASE_URL` | `http://localhost:1234/v1` | OpenAI-compatible LLM endpoint |

---

## Build & test

```bash
cargo build                   # build all crates
cargo run                     # start API server
cargo run --bin cli           # run CLI
cargo test                    # all tests
cargo test -p eng             # eng crate only
RUST_LOG=debug cargo run      # verbose tracing
```

Tests use `tokio::test` for async. `crates/eng` has unit + integration tests for registry, workflow builder (cycle detection, invalid deps, duplicate IDs), and task lifecycle.

---

## Key invariants

- **Channel IDs are global within a workflow** — unique strings, not scoped to tasks. Duplicate channel IDs across tasks cause fan-out, not conflict.
- **`ctx.running().await` is the pause/stop gate** — tasks that skip it will not respond to pause/stop commands.
- `WorkflowBuilder::build()` validates structure (unknown types, duplicate IDs, unresolved deps, cycles) then calls `spawn()`. Params are validated by task factories during spawn.
- `Engine` is behind `Arc<Mutex<Engine>>` in the server. Lock held only for the duration of each API call.
- Workflow restart (`Stopped → Running`) fully rebuilds channels and task instances from stored `task_configs`.
