# Channel Architecture and Multi-Input Support

## Overview

This document explains how channels work in the Simple Engine, specifically addressing the relationship between task IDs, channel IDs, and how tasks process multiple inputs.

## Key Concepts

### 1. Task IDs vs Channel IDs

**Task IDs** identify individual tasks in the workflow:
```yaml
tasks:
  - id: number_generator  # This is a TASK ID
    type: number_generator
```

**Channel IDs** identify data streams between tasks:
```yaml
tasks:
  - id: number_generator
    outputs:
      out:
        - number_generator_out  # This is a CHANNEL ID
```

### 2. Dependencies are Channel IDs, Not Task IDs

When a task declares dependencies, it specifies **channel IDs** that it wants to receive data from:

```yaml
tasks:
  - id: logger
    dependencies:
      - number_generator_out  # Channel ID from upstream task's output
```

This design allows:
- One task to have multiple outputs with different channel IDs
- Multiple tasks to write to the same channel ID
- Clear data routing without ambiguity

## Multi-Input Processing

### The Problem

In a workflow, a task may need to receive data from multiple upstream sources:

```yaml
tasks:
  - id: generator_1
    outputs:
      out: [channel_a]
  
  - id: generator_2
    outputs:
      out: [channel_b]
  
  - id: processor
    dependencies:
      - channel_a  # Input from generator_1
      - channel_b  # Input from generator_2
```

The `processor` task needs to handle messages from **both** `channel_a` and `channel_b`.

### The Solution: Merged Input

The Simple Engine provides `merged_input()` which automatically combines all input channels into a single stream:

```rust
#[async_trait]
impl Task for Processor {
    async fn execute(&self, ctx: Arc<TaskContext>) -> Result<()> {
        // Get merged input from ALL dependencies
        let mut input = ctx.merged_input()?;
        let output = ctx.output("out")?;

        while ctx.is_running() {
            // Receives from ALL input channels as a unified stream
            let data = input.recv().await?;
            // Process data...
            output.send(result)?;
        }
        Ok(())
    }

    fn name(&self) -> &str { "Processor" }
}
```

### How It Works

1. **Single Input**: If a task has only one dependency, `merged_input()` returns that channel directly (no overhead).

2. **Multiple Inputs**: If a task has multiple dependencies, `merged_input()` spawns background tasks that forward all input channels to a single merged broadcast channel.

3. **Transparent**: The task code doesn't need to know how many upstream sources exist—it just receives a unified stream.

## API Methods

### `ctx.merged_input()` - Recommended for Most Tasks

Returns a single `Input` that receives from all input channels:

```rust
let mut input = ctx.merged_input()?;
while ctx.is_running() {
    let data = input.recv().await?;
    // Process data from any upstream channel
}
```

**Use when**: Your task doesn't care which upstream source sent the data.

### `ctx.input(channel_id)` - For Specific Channels

Returns an `Input` for a specific channel ID:

```rust
let mut input_a = ctx.input("channel_a")?;
let mut input_b = ctx.input("channel_b")?;

// Process each channel differently
let data_a = input_a.recv().await?;
let data_b = input_b.recv().await?;
```

**Use when**: You need to handle different input sources differently.

### `ctx.inputs()` - For Advanced Control

Returns a vector of all input channels:

```rust
let mut inputs = ctx.inputs()?;

while ctx.is_running() {
    for input in &mut inputs {
        if let Ok(Some(data)) = input.try_recv() {
            // Process with knowledge of which channel it came from
            process(data, input.id());
        }
    }
    tokio::time::sleep(Duration::from_millis(10)).await;
}
```

**Use when**: You need fine-grained control over polling each channel separately.

### `ctx.first_input()` - Legacy/Convenience

Returns the first available input channel:

```rust
let mut input = ctx.first_input()?;
```

**Use when**: You know the task has exactly one input and want a shorthand.

## Complete Example

Here's a workflow with multiple generators feeding into a single processor:

```yaml
id: multi_input_example
tasks:
  - id: gen_a
    type: number_generator
    params:
      min: 1
      max: 10
    outputs:
      out: [small_numbers]

  - id: gen_b
    type: number_generator
    params:
      min: 100
      max: 200
    outputs:
      out: [large_numbers]

  - id: logger
    type: logger
    dependencies:
      - small_numbers
      - large_numbers
    params:
      prefix: "[MERGED]"
```

The logger task receives numbers from both generators seamlessly:

```rust
// Logger implementation
async fn execute(&self, ctx: Arc<TaskContext>) -> Result<()> {
    let mut input = ctx.merged_input()?; // Gets both channels
    
    while ctx.is_running() {
        let data = input.recv().await?; // Could be from gen_a OR gen_b
        log::info!("{}", data); // Logger doesn't care which source
    }
    Ok(())
}
```

## Best Practices

1. **Use meaningful channel IDs**: Name channels after what they carry, not just the task that produces them.
   - Good: `validated_orders`, `error_records`, `processed_data`
   - Bad: `task1_out`, `output`, `data`

2. **Default to `merged_input()`**: Unless you have a specific reason to handle channels separately, use the merged input for simplicity.

3. **Document channel semantics**: If different input channels have different meanings, document this in your task and consider using `ctx.input()` to handle them explicitly.

4. **One output label per semantic type**: If a task produces different types of data, use different output labels (`out`, `error`, `stats`) rather than mixing them.

## Migration from Old Architecture

If you have code that used task IDs for dependencies, here's how to migrate:

### Old (Incorrect):
```yaml
tasks:
  - id: generator
    outputs:
      out: [processor]  # Using task ID - WRONG
  - id: processor
    dependencies: [generator]  # Using task ID - WRONG
```

### New (Correct):
```yaml
tasks:
  - id: generator
    outputs:
      out: [generator_out]  # Channel ID
  - id: processor
    dependencies: [generator_out]  # Channel ID
```

### Code Changes:
```rust
// Old - assumed input was called "in"
let mut input = ctx.input("in")?;

// New - works with any channel ID(s)
let mut input = ctx.merged_input()?;
```

## Validation

The workflow builder validates:

1. **No duplicate task IDs**: Each task must have a unique ID.
2. **Dependencies reference existing channels**: All channel IDs in `dependencies` must exist in some task's `outputs`.
3. **No circular dependencies**: The task dependency graph must be acyclic.
4. **Output channels are created**: All channel IDs declared in `outputs` are created before tasks start.

Errors are caught at build time, before any tasks execute.

## Performance Considerations

- **Merged Input**: Adds minimal overhead (one spawn per extra input channel)
- **Broadcast Channels**: Used internally for fan-out patterns (one producer, many consumers)
- **Bounded Channels**: Default capacity of 1000 messages per channel (configurable via `WorkflowBuilder::channel_capacity()`)
- **No Copies**: Messages are cloned only when necessary for multiple receivers

## See Also

- `examples/multi_input.rs` - Working example of multiple inputs
- `ARCHITECTURE.md` - Overall system design
- `examples/simple_workflow.rs` - Basic single-input example