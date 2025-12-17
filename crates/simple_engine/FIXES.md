# Fixes to Channel and Dependency Architecture

## Summary

This document describes the critical fixes made to the Simple Engine's channel and dependency system based on the workflow configuration requirements.

## Issues Identified

### 1. Dependencies Used Task IDs Instead of Channel IDs

**Problem**: The original implementation expected dependencies to be task IDs, but the workflow configuration (YAML) uses channel IDs.

**Example Configuration**:
```yaml
tasks:
  - id: number_generator
    outputs:
      out:
        - number_generator_out  # This is a CHANNEL ID

  - id: logger
    dependencies:
      - number_generator_out    # This is a CHANNEL ID, not "number_generator"
```

**What Was Wrong**:
- Validation checked if dependencies existed as task IDs
- Channel creation logic assumed dependencies were task IDs
- Task context stored inputs by task ID instead of channel ID

**What Was Fixed**:
- Dependencies now correctly represent channel IDs from upstream outputs
- Validation ensures dependency channel IDs match existing output channel IDs
- Channels are created based on output declarations, not dependencies
- Task context maps inputs by channel ID

### 2. Tasks Could Only Access Single Input Channel

**Problem**: Tasks using `ctx.input("in")` assumed a hardcoded channel name and could only access one input at a time, even when multiple dependencies existed.

**What Was Wrong**:
```rust
// Old approach - hardcoded "in" channel name
let mut input = ctx.input("in")?;  // Doesn't work with dynamic channel IDs
```

**What Was Fixed**:
- Added `ctx.merged_input()` - combines ALL input channels into a single stream
- Added `ctx.inputs()` - returns all input channels as a vector
- Added `ctx.first_input()` - convenience method for single-input tasks
- Updated all built-in tasks to use `merged_input()`

### 3. No Support for Multi-Input Processing

**Problem**: When a task had multiple dependencies (multiple input channels), there was no clean way to receive from all of them.

**Example Scenario**:
```yaml
- id: aggregator
  dependencies:
    - source_a_out
    - source_b_out
    - source_c_out
```

**What Was Fixed**:
- `merged_input()` automatically spawns forwarders for each input channel
- All input channels are merged into a single broadcast channel
- Tasks receive a unified stream regardless of number of upstream sources
- Zero overhead for single-input case (no merging needed)

## Changes Made

### 1. Workflow Builder (`workflow.rs`)

**Channel Creation**:
```rust
// OLD: Created channels per dependency (task ID)
for dep in &task.dependencies {
    channels.entry(dep.clone()).or_insert_with(|| broadcast::channel(capacity).0);
}

// NEW: Create channels per output declaration (channel ID)
for task in &self.tasks {
    for (_label, channel_ids) in &task.outputs {
        for channel_id in channel_ids {
            channels.entry(channel_id.clone()).or_insert_with(|| broadcast::channel(capacity).0);
        }
    }
}
```

**Validation**:
```rust
// OLD: Check dependencies exist as task IDs
for dep in &task.dependencies {
    if !task_ids.contains(dep) {
        return Err(...);
    }
}

// NEW: Check dependencies exist as output channel IDs
let mut available_channels = HashSet::new();
for task in &self.tasks {
    for (_label, channel_ids) in &task.outputs {
        available_channels.extend(channel_ids);
    }
}

for task in &self.tasks {
    for dep_channel_id in &task.dependencies {
        if !available_channels.contains(dep_channel_id) {
            return Err(...);
        }
    }
}
```

**Dependency Graph for Cycle Detection**:
```rust
// Build map from channel ID to producing task
let mut channel_to_task: HashMap<&str, &str> = HashMap::new();
for task in &self.tasks {
    for (_label, channel_ids) in &task.outputs {
        for channel_id in channel_ids {
            channel_to_task.insert(channel_id, task.id);
        }
    }
}

// Build task dependency graph
let mut graph: HashMap<&str, Vec<&str>> = HashMap::new();
for task in &self.tasks {
    let mut upstream_tasks = Vec::new();
    for dep_channel_id in &task.dependencies {
        if let Some(&upstream_task_id) = channel_to_task.get(dep_channel_id) {
            upstream_tasks.push(upstream_task_id);
        }
    }
    graph.insert(&task.id, upstream_tasks);
}
```

### 2. Task Context (`context.rs`)

**Added `merged_input()` Method**:
```rust
pub fn merged_input(&self) -> Result<Input> {
    if self.inputs.is_empty() {
        return Err(...);
    }

    // Single input - return directly (no overhead)
    if self.inputs.len() == 1 {
        let (channel_id, tx) = self.inputs.iter().next().unwrap();
        return Ok(Input::new(channel_id.clone(), tx.subscribe()));
    }

    // Multiple inputs - create merged channel
    let (merged_tx, merged_rx) = broadcast::channel(1000);

    // Spawn forwarder for each input channel
    for (channel_id, tx) in self.inputs.iter() {
        let mut rx = tx.subscribe();
        let merged_tx_clone = merged_tx.clone();
        
        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(value) => { let _ = merged_tx_clone.send(value); }
                    Err(_) => break,
                }
            }
        });
    }

    Ok(Input::new("merged".to_string(), merged_rx))
}
```

**Added Helper Methods**:
- `inputs()` - Returns all input channels as a Vec
- `first_input()` - Returns the first input channel
- `input_count()` - Returns the number of input channels

### 3. Built-in Tasks

All built-in tasks updated to use `merged_input()`:

**Logger** (`tasks/logger.rs`):
```rust
// OLD
let mut input = ctx.input("in")?;

// NEW
let mut input = ctx.merged_input()?;
```

**CSV Writer** (`tasks/csv_writer.rs`):
```rust
// OLD
let mut input = ctx.input("in")?;

// NEW
let mut input = ctx.merged_input()?;
```

**JSON Mapper** (`tasks/json_mapper.rs`):
```rust
// OLD
let mut input = ctx.input("in")?;

// NEW
let mut input = ctx.merged_input()?;
```

### 4. Examples

**Updated Existing Examples**:
- `simple_workflow.rs` - Uses proper channel IDs
- `csv_pipeline.rs` - Uses proper channel IDs

**Added New Example**:
- `multi_input.rs` - Demonstrates multiple generators feeding into one logger

**Example Configuration**:
```rust
WorkflowBuilder::new("example")
    .add_task(
        TaskConfig::new("generator", factory, params)
            .with_output("out", vec!["generator_out".to_string()])  // Channel ID
    )
    .add_task(
        TaskConfig::new("processor", factory, params)
            .with_dependency("generator_out")  // Channel ID, not task ID
            .with_output("out", vec!["processor_out".to_string()])
    )
    .add_task(
        TaskConfig::new("logger", factory, params)
            .with_dependency("processor_out")  // Channel ID
    )
    .build()?;
```

## Documentation Added

1. **CHANNELS.md** - Comprehensive guide covering:
   - Task IDs vs Channel IDs
   - Multi-input processing patterns
   - API method reference
   - Best practices
   - Migration guide
   - Performance considerations

2. **Updated API Documentation**:
   - Task trait examples
   - Context method documentation
   - Workflow builder comments

## Testing

**Verified**:
- ✅ Compilation succeeds
- ✅ Multi-input example runs successfully
- ✅ Logger receives from multiple generators
- ✅ Channel IDs properly validated
- ✅ Circular dependency detection works
- ✅ Single-input case has no overhead

**Test Output**:
```
Logger receiving from:
- generator_small: values 1-10
- generator_medium: values 50-75
- generator_large: values 100-200

All messages merged into single stream successfully.
```

## Benefits

1. **Correct Semantics**: Dependencies now properly reference channel IDs, matching the YAML configuration
2. **Multi-Input Support**: Tasks can seamlessly receive from multiple upstream sources
3. **Flexible Routing**: Channel IDs enable one-to-many and many-to-one patterns
4. **Clean API**: `merged_input()` hides complexity from task developers
5. **Zero Overhead**: Single-input case has no merging cost
6. **Type Safety**: All channel routing validated at build time

## Breaking Changes

**For Task Developers**:
```rust
// OLD - Assumed hardcoded channel name
let mut input = ctx.input("in")?;

// NEW - Works with any channel configuration
let mut input = ctx.merged_input()?;
```

**For Workflow Builders**:
```rust
// OLD - Used task IDs (WRONG)
.with_dependency("upstream_task")
.with_output("out", vec!["downstream_task".to_string()])

// NEW - Use channel IDs (CORRECT)
.with_dependency("upstream_task_out")
.with_output("out", vec!["my_task_out".to_string()])
```

## Migration Path

1. Update all workflow configurations to use channel IDs in dependencies and outputs
2. Replace `ctx.input("in")` with `ctx.merged_input()` in all tasks
3. For tasks that need per-channel control, use `ctx.input(channel_id)` or `ctx.inputs()`
4. Test multi-input scenarios to ensure proper message routing

## See Also

- `CHANNELS.md` - Detailed channel architecture documentation
- `examples/multi_input.rs` - Working multi-input example
- `ARCHITECTURE.md` - Overall system design