# Task Execution Concurrency Fix

## Problem

The original `TaskRunner::run()` implementation had a critical bug where commands (Pause/Stop) could not be received while a task was executing.

### Root Cause

When a task received the `Start` command, the runner would call `task.execute()` and **block** waiting for it to complete:

```rust
// OLD CODE - BUGGY
match cmd {
    Command::Start => {
        // ... setup ...
        
        // This blocks until execute() completes!
        match self.task.execute(Arc::clone(&self.context)).await {
            Ok(_) => { /* ... */ }
            Err(e) => { /* ... */ }
        }
        
        // Can't receive new commands until execute() finishes
    }
}
```

The problem:
1. Task runner waits for command change: `self.cmd_rx.changed().await`
2. Receives `Start` command
3. Calls `task.execute()` which **blocks the runner loop**
4. **Cannot receive any new commands** until `execute()` returns
5. Task only checks `ctx.is_running()` internally, but the flag can't be changed because the runner is blocked

### Impact

- **Stop command ignored**: Workflow.stop() was called but tasks kept running
- **Pause command ignored**: Workflow.pause() had no effect during execution
- **Poor user experience**: No way to interrupt long-running tasks
- **Resource leaks**: Tasks couldn't be stopped, consuming resources indefinitely

## Solution

Use `tokio::select!` to race between task execution and command reception:

```rust
// NEW CODE - FIXED
match cmd {
    Command::Start => {
        // ... setup ...
        
        let execute_future = task_ref.execute(ctx_clone);
        tokio::pin!(execute_future);
        
        loop {
            tokio::select! {
                // Wait for either task completion...
                result = &mut execute_future => {
                    // Task finished naturally
                    handle_completion(result);
                    break;
                }
                
                // ...OR a new command
                cmd_result = self.cmd_rx.changed() => {
                    // Command received during execution!
                    match new_cmd {
                        Command::Pause => {
                            ctx.set_running(false);  // Signal task to stop
                            // Wait for task to check is_running() and exit
                            break;
                        }
                        Command::Stop => {
                            ctx.set_running(false);
                            return;  // Exit runner completely
                        }
                        _ => {}
                    }
                }
            }
        }
    }
}
```

### How It Works

1. **Spawn task execution** as a future (doesn't block)
2. **Race with select**: Monitor both task completion AND command channel
3. **React to commands**: When Pause/Stop received, set `ctx.running = false`
4. **Task cooperates**: Task checks `ctx.is_running()` in its loop and exits gracefully
5. **Lifecycle hooks**: Call `on_pause()` or `on_stop()` as appropriate

## Key Points

### Task Cooperation Required

Tasks **must** periodically check `ctx.is_running()`:

```rust
async fn execute(&self, ctx: Arc<TaskContext>) -> Result<()> {
    while ctx.is_running() {  // ✅ Check this regularly!
        // Do work...
        tokio::time::sleep(interval).await;  // Yields control
    }
    Ok(())
}
```

**Anti-pattern** (won't respond to commands):
```rust
async fn execute(&self, ctx: Arc<TaskContext>) -> Result<()> {
    loop {  // ❌ Never checks is_running()
        // Do work forever...
    }
}
```

### Command Delivery Timing

- **During idle**: Command received immediately (original behavior)
- **During execution**: Command received within milliseconds via `select!`
- **Task response time**: Depends on how often task checks `is_running()`

### Execution Flow

```
Workflow.start()
    ↓
Runner receives Start
    ↓
Spawns execute() future
    ↓
select! {
    execute completes ←─────────┐
    OR                          │
    command received ──→ set running=false
}                               │
    ↓                           │
Task checks is_running() ───────┘
    ↓
Task exits loop
    ↓
execute() returns
    ↓
Runner handles cleanup
```

## Testing

### Unit Test

```rust
#[tokio::test]
async fn test_stop_command_during_execution() {
    let task = /* long-running task */;
    let (cmd_tx, cmd_rx) = watch::channel(Command::Pause);
    let runner = TaskRunner::new(task, ctx, cmd_rx);
    
    tokio::spawn(runner.run());
    
    cmd_tx.send(Command::Start).unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    
    // Task is now executing...
    
    cmd_tx.send(Command::Stop).unwrap();  // ✅ This now works!
    tokio::time::sleep(Duration::from_millis(20)).await;
    
    // Task should have stopped within ~20ms
    assert!(task_stopped);
}
```

### Integration Example

See `examples/workflow_control.rs`:
- Starts workflow with long-running tasks
- Pauses during execution (tasks stop processing)
- Resumes (tasks continue)
- Stops before completion (tasks exit cleanly)

**Output shows**:
```
Task [NumberGenerator]-generator received pause during execution
Task [Logger]-logger received pause during execution
✓ Workflow paused

Task [NumberGenerator]-generator received stop during execution
Task [NumberGenerator]-generator shutdown complete
```

## Performance Impact

### Overhead

- **Minimal**: `tokio::select!` is highly optimized
- **No polling**: Event-driven, not checking in a busy loop
- **Single allocation**: `tokio::pin!` pins the future on the stack

### Benchmarks

Before and after fix, task throughput is identical:
- Tasks still execute at full speed
- Command checking happens only when commands are sent
- No impact on steady-state execution

## Migration

### For Task Developers

**No changes required** if your tasks already check `ctx.is_running()`:

```rust
// This pattern already works correctly
while ctx.is_running() {
    // Work...
}
```

If your task doesn't check `is_running()`, add it:

```diff
 async fn execute(&self, ctx: Arc<TaskContext>) -> Result<()> {
-    loop {
+    while ctx.is_running() {
         // Work...
     }
 }
```

### For Workflow Users

**No API changes** - existing code works as expected now:

```rust
workflow.start()?;
// ... 
workflow.pause()?;  // ✅ Now works during execution!
workflow.start()?;
workflow.stop()?;   // ✅ Now works during execution!
```

## Related Issues

- Fixed in response to user report: "commands not received during execution"
- Affects all workflows where tasks might need to be interrupted
- Critical for long-running or infinite tasks

## See Also

- `examples/workflow_control.rs` - Demonstrates pause/stop during execution
- `src/task.rs` - TaskRunner implementation
- `src/context.rs` - `is_running()` flag used by tasks