# Simple Engine

A clean, performant workflow engine for building data processing pipelines in Rust.

## Overview

Simple Engine provides a straightforward API for building complex data processing workflows. It handles all the complexity of task orchestration, channel management, and lifecycle control, allowing developers to focus solely on business logic.

## Key Features

- **Simple Task API**: Developers only need to implement `execute()` - everything else is handled by the framework
- **Type-Safe Channels**: Clean abstractions (`Input`/`Output`) hide complex channel mechanics
- **Automatic Lifecycle Management**: Start, pause, stop, and resume tasks with built-in state management
- **Zero Unsafe Code**: Built entirely with safe Rust
- **Production Ready**: Comprehensive error handling, logging, and validation
- **Async-First**: Built on Tokio for high-performance async execution
- **Composable**: Easy to build complex pipelines from simple tasks

## Quick Start

Add to your `Cargo.toml`:

```toml
[dependencies]
simple_engine = "0.1"
tokio = { version = "1", features = ["full"] }
serde_json = "1"
async-trait = "0.1"
```

### Basic Example

```rust
use simple_engine::prelude::*;
use serde_json::json;

// 1. Define your task
struct MyProcessor {
    base: BaseTask<MyParams, MyState>,
}

#[derive(Deserialize)]
struct MyParams {
    multiplier: i32,
}

#[derive(Default)]
struct MyState {}

// 2. Implement the Task trait
#[async_trait]
impl Task for MyProcessor {
    fn name(&self) -> &str {
        "MyProcessor"
    }

    async fn execute(&self, ctx: Arc<TaskContext>) -> Result<()> {
        let mut input = ctx.input("in")?;
        let output = ctx.output("out")?;

        while ctx.is_running() {
            let data = input.recv().await?;
            // Process your data...
            let result = process(data, self.base.params.multiplier);
            output.send(result)?;
        }
        Ok(())
    }
}

// 3. Create factory function
impl MyProcessor {
    pub fn create(id: String, params: Value) -> Result<Box<dyn Task>> {
        Ok(Box::new(Self {
            base: BaseTask::new(id, params)?,
        }))
    }
}

// 4. Build and run workflow
#[tokio::main]
async fn main() -> Result<()> {
    let workflow = WorkflowBuilder::new("my-workflow")
        .add_task(
            TaskConfig::new("task1", Box::new(MyProcessor::create), json!({}))
        )
        .build()?;

    workflow.start()?;
    workflow.wait().await?;
    Ok(())
}
```

## Task Types

### Source Tasks (No Inputs)

Generate or read data from external sources:

```rust
#[async_trait]
impl Task for DataGenerator {
    async fn execute(&self, ctx: Arc<TaskContext>) -> Result<()> {
        let output = ctx.output("out")?;

        while ctx.is_running() {
            let data = generate_data();
            output.send(data)?;
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        Ok(())
    }

    fn name(&self) -> &str { "DataGenerator" }
}
```

### Processing Tasks (Inputs and Outputs)

Transform data as it flows through the pipeline:

```rust
#[async_trait]
impl Task for DataTransformer {
    async fn execute(&self, ctx: Arc<TaskContext>) -> Result<()> {
        let mut input = ctx.input("in")?;
        let output = ctx.output("out")?;

        while ctx.is_running() {
            let data = input.recv().await?;
            let transformed = transform(data);
            output.send(transformed)?;
        }
        Ok(())
    }

    fn name(&self) -> &str { "DataTransformer" }
}
```

### Sink Tasks (No Outputs)

Store or export data:

```rust
#[async_trait]
impl Task for DataWriter {
    async fn execute(&self, ctx: Arc<TaskContext>) -> Result<()> {
        let mut input = ctx.input("in")?;

        while ctx.is_running() {
            let data = input.recv().await?;
            write_to_database(data).await?;
        }
        Ok(())
    }

    fn name(&self) -> &str { "DataWriter" }
}
```

## Built-in Tasks

Simple Engine includes several ready-to-use tasks:

### CsvReader

Reads CSV files and streams records as JSON:

```rust
TaskConfig::new(
    "reader",
    Box::new(CsvReader::create),
    json!({
        "filename": "data.csv",
        "delimiter": ",",
        "interval_ms": 100
    })
)
```

### CsvWriter

Writes JSON objects to CSV files:

```rust
TaskConfig::new(
    "writer",
    Box::new(CsvWriter::create),
    json!({
        "filename": "output.csv",
        "delimiter": ",",
        "append": false
    })
)
```

### JsonMapper

Transforms JSON objects using field mappings:

```rust
TaskConfig::new(
    "mapper",
    Box::new(JsonMapper::create),
    json!({
        "mappings": {
            "output_field": "input.nested.field"
        },
        "pass_through": false
    })
)
```

### NumberGenerator

Generates random numbers (useful for testing):

```rust
TaskConfig::new(
    "generator",
    Box::new(NumberGenerator::create),
    json!({
        "min": 1,
        "max": 100,
        "interval_ms": 1000,
        "count": 100
    })
)
```

### Logger

Logs data to stdout/stderr:

```rust
TaskConfig::new(
    "logger",
    Box::new(Logger::create),
    json!({
        "level": "info",
        "prefix": "[DATA]",
        "pretty": true
    })
)
```

## Building Workflows

### Simple Linear Pipeline

```rust
let workflow = WorkflowBuilder::new("pipeline")
    .add_task(
        TaskConfig::new("source", Box::new(Source::create), json!({}))
            .with_output("out", vec!["source".to_string()])
    )
    .add_task(
        TaskConfig::new("processor", Box::new(Processor::create), json!({}))
            .with_dependency("source")
            .with_output("out", vec!["processor".to_string()])
    )
    .add_task(
        TaskConfig::new("sink", Box::new(Sink::create), json!({}))
            .with_dependency("processor")
    )
    .build()?;
```

### Fan-Out Pattern

One task sends to multiple downstream tasks:

```rust
TaskConfig::new("splitter", Box::new(Splitter::create), json!({}))
    .with_output("out", vec!["task1".to_string(), "task2".to_string()])
```

### Fan-In Pattern

Multiple tasks send to one downstream task:

```rust
// Task 1 outputs to "combiner"
TaskConfig::new("task1", ...).with_output("out", vec!["combiner".to_string()])

// Task 2 also outputs to "combiner"  
TaskConfig::new("task2", ...).with_output("out", vec!["combiner".to_string()])

// Combiner receives from both
TaskConfig::new("combiner", ...)
    .with_dependencies(vec!["task1".to_string(), "task2".to_string()])
```

## Lifecycle Management

### Start, Pause, Stop

```rust
let workflow = build_workflow()?;

// Start execution
workflow.start()?;

// Pause all tasks
workflow.pause()?;

// Resume execution
workflow.start()?;

// Stop permanently
workflow.stop()?;
```

### Task Lifecycle Hooks

Implement optional hooks for custom behavior:

```rust
#[async_trait]
impl Task for MyTask {
    async fn execute(&self, ctx: Arc<TaskContext>) -> Result<()> {
        // Main execution logic
    }

    async fn on_start(&self, ctx: Arc<TaskContext>) -> Result<()> {
        // Called when task starts/resumes
        Ok(())
    }

    async fn on_pause(&self, ctx: Arc<TaskContext>) -> Result<()> {
        // Called when task is paused
        Ok(())
    }

    async fn on_stop(&self, ctx: Arc<TaskContext>) -> Result<()> {
        // Called when task stops
        // Cleanup resources here
        Ok(())
    }

    fn name(&self) -> &str { "MyTask" }
}
```

## Error Handling

Simple Engine uses `Result` types throughout:

```rust
#[async_trait]
impl Task for MyTask {
    async fn execute(&self, ctx: Arc<TaskContext>) -> Result<()> {
        let mut input = ctx.input("in")?; // Returns Err if input not found
        
        while ctx.is_running() {
            match input.recv().await {
                Ok(data) => {
                    // Process data
                    let result = process(data)?;
                    ctx.output("out")?.send(result)?;
                }
                Err(e) => {
                    // Handle channel errors
                    log::error!("Receive failed: {}", e);
                    return Err(e);
                }
            }
        }
        Ok(())
    }

    fn name(&self) -> &str { "MyTask" }
}
```

Error types:

- `EngineError`: Top-level error type
- `TaskError`: Task-specific errors
- `WorkflowError`: Workflow configuration/execution errors
- `ChannelError`: Channel communication errors

## Performance Tips

1. **Channel Capacity**: Tune buffer sizes for your workload
   ```rust
   WorkflowBuilder::new("workflow")
       .channel_capacity(10000) // Default is 1000
   ```

2. **Batch Processing**: Process multiple items at once when possible
   ```rust
   while ctx.is_running() {
       let mut batch = Vec::new();
       for _ in 0..100 {
           if let Ok(Some(item)) = input.try_recv()? {
               batch.push(item);
           } else {
               break;
           }
       }
       process_batch(batch)?;
   }
   ```

3. **Avoid Cloning**: Use `send_owned()` when you don't need the value after sending
   ```rust
   output.send_owned(data)?; // Moves data instead of cloning
   ```

4. **Use `try_recv()`**: For non-blocking reads
   ```rust
   if let Some(data) = input.try_recv()? {
       process(data)?;
   }
   ```

## Examples

Run the included examples:

```bash
# Simple number processing pipeline
cargo run --example simple_workflow

# Complete CSV processing pipeline
cargo run --example csv_pipeline
```

## Architecture

### Design Principles

1. **Separation of Concerns**: Task logic is separate from runtime management
2. **Type Safety**: Strong typing prevents many common errors at compile time
3. **Explicit Over Implicit**: Channel relationships are clearly defined
4. **Fail Fast**: Validation happens at workflow build time, not runtime

### Performance Characteristics

- **Zero-copy where possible**: Uses `Arc` and broadcast channels efficiently
- **Bounded channels**: Prevents unbounded memory growth
- **Lock-free operations**: Uses atomic operations for state tracking
- **Async-native**: Non-blocking I/O throughout

### Comparison to Other Engines

| Feature | Simple Engine | Old Engine | Other Solutions |
|---------|--------------|------------|-----------------|
| Task API Complexity | ⭐⭐⭐⭐⭐ Simple | ⭐⭐ Complex | ⭐⭐⭐ Moderate |
| Type Safety | ⭐⭐⭐⭐⭐ Full | ⭐⭐⭐ Partial | ⭐⭐⭐⭐ Good |
| Error Handling | ⭐⭐⭐⭐⭐ Comprehensive | ⭐⭐ Panics | ⭐⭐⭐⭐ Good |
| Performance | ⭐⭐⭐⭐⭐ Excellent | ⭐⭐⭐ Good | ⭐⭐⭐⭐ Good |
| Documentation | ⭐⭐⭐⭐⭐ Extensive | ⭐⭐ Minimal | ⭐⭐⭐ Adequate |

## Testing

Simple Engine is designed for easy testing:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_my_task() {
        let params = json!({"key": "value"});
        let task = MyTask::create("test".to_string(), params).unwrap();
        
        let ctx = create_test_context();
        let result = task.execute(Arc::new(ctx)).await;
        
        assert!(result.is_ok());
    }
}
```

## Contributing

Contributions are welcome! Please ensure:

1. Code follows Rust style guidelines
2. All tests pass: `cargo test`
3. Examples work: `cargo run --example simple_workflow`
4. Documentation is updated

## License

See workspace license.

## Credits

Built with ❤️ using:
- [Tokio](https://tokio.rs) - Async runtime
- [Serde](https://serde.rs) - Serialization
- [Thiserror](https://github.com/dtolnay/thiserror) - Error handling
- [DashMap](https://github.com/xacrimon/dashmap) - Concurrent hashmaps

---

**Simple Engine** - Making workflow orchestration simple, safe, and fast.