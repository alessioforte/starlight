# Workflow Status and Restart Behavior Fixes

## Summary

This document describes the fixes made to workflow status tracking and the restart behavior after stop commands.

## Issues Fixed

### 1. Workflow Status Not Updated on Commands

**Problem**: When `workflow.start()`, `workflow.pause()`, or `workflow.stop()` were called, the workflow's internal status field was never updated. The status remained stuck at `Idle` even though tasks were running.

**Impact**:
- `workflow.status()` always returned `Idle`
- No way to programmatically check if workflow was running/paused/stopped
- Confusing for users monitoring workflow state
- Status information was incorrect

**Root Cause**: The command methods only sent commands to tasks but never updated the workflow-level status:

```rust
// OLD CODE - BUGGY
pub fn start(&self) -> Result<()> {
    self.cmd_tx.send(Command::Start)?;
    // Status never updated!
    Ok(())
}
```

**Fix**: Update workflow status when commands are sent:

```rust
// NEW CODE - FIXED
pub async fn start(&self) -> Result<()> {
    self.cmd_tx.send(Command::Start)?;
    
    // Update workflow status
    let mut status = self.status.write().await;
    *status = WorkflowStatus::Running;
    
    Ok(())
}
```

### 2. Stopped Workflows Could Not Be Prevented from Restart Attempts

**Problem**: When a workflow was stopped, users could attempt to call `start()` again, but it would fail silently or cause unexpected behavior because the task threads had already exited.

**Impact**:
- Confusing behavior when trying to restart
- No clear error message
- Users didn't understand why restart failed

**Root Cause**: No validation to prevent restart of stopped workflows.

**Fix**: Added validation and clear error message:

```rust
pub async fn start(&self) -> Result<()> {
    // Check if workflow was stopped
    let current_status = self.status.read().await.clone();
    if current_status == WorkflowStatus::Stopped {
        return Err(EngineError::Workflow(WorkflowError::StartFailed(
            self.id.clone(),
            "Cannot restart a stopped workflow. Create a new workflow instance.".to_string(),
        )));
    }
    
    // ... rest of start logic
}
```

## Changes Made

### 1. Made Control Methods Async

All workflow control methods are now `async` to allow updating the status:

```rust
// Before
pub fn start(&self) -> Result<()>
pub fn pause(&self) -> Result<()>
pub fn stop(&self) -> Result<()>

// After
pub async fn start(&self) -> Result<()>
pub async fn pause(&self) -> Result<()>
pub async fn stop(&self) -> Result<()>
```

### 2. Added Status Updates

Each control method now updates the workflow status:

- `start()` → Sets status to `Running`
- `pause()` → Sets status to `Paused`
- `stop()` → Sets status to `Stopped`

### 3. Added Restart Validation

Added `can_restart()` method and validation in `start()`:

```rust
pub async fn can_restart(&self) -> bool {
    let status = self.status.read().await;
    *status != WorkflowStatus::Stopped
}
```

### 4. Enhanced Documentation

Added clear documentation explaining:
- Stopped workflows cannot be restarted
- Use `pause()` instead of `stop()` if you need to resume
- Must create new workflow instance after stop

## Status Lifecycle

```
┌─────────┐
│  Idle   │ (initial state after build)
└────┬────┘
     │ start()
     ▼
┌─────────┐
│ Running │ ◄──────┐
└────┬────┘        │ start() (resume)
     │             │
     │ pause()     │
     ▼             │
┌─────────┐        │
│ Paused  │────────┘
└─────────┘
     │
     │ Any state can transition to Stopped via stop()
     ▼
┌─────────┐
│ Stopped │ (terminal state - no restart)
└─────────┘
```

## Restart Behavior

### ✅ Can Restart

- **Idle** → `start()` → Running
- **Paused** → `start()` → Running
- **Running** → `start()` → Running (idempotent, no-op)

### ❌ Cannot Restart

- **Stopped** → `start()` → **Error**: "Cannot restart a stopped workflow"

### Why This Limitation?

When a workflow is stopped:
1. Tasks receive `Stop` command
2. Tasks exit their execution loops
3. Task threads complete and JoinHandles resolve
4. All resources are cleaned up

Restarting would require:
- Spawning new task threads
- Recreating all channels
- Reinitializing state

This is functionally equivalent to building a new workflow. For clarity and proper resource management, we require explicit workflow reconstruction via `WorkflowBuilder`.

## Examples

### Check and Update Status

```rust
let workflow = WorkflowBuilder::new("my-workflow")
    .add_task(/* ... */)
    .build()?;

// Check initial status
assert_eq!(workflow.status().await, WorkflowStatus::Idle);

// Start workflow
workflow.start().await?;
assert_eq!(workflow.status().await, WorkflowStatus::Running);

// Pause workflow
workflow.pause().await?;
assert_eq!(workflow.status().await, WorkflowStatus::Paused);

// Resume workflow
workflow.start().await?;
assert_eq!(workflow.status().await, WorkflowStatus::Running);

// Stop workflow
workflow.stop().await?;
assert_eq!(workflow.status().await, WorkflowStatus::Stopped);
```

### Handle Restart Correctly

```rust
// ✅ Good: Check before restart
if workflow.can_restart().await {
    workflow.start().await?;
} else {
    // Build new workflow instead
    let new_workflow = WorkflowBuilder::new("my-workflow")
        .add_task(/* ... */)
        .build()?;
    new_workflow.start().await?;
}

// ✅ Good: Handle restart error
match workflow.start().await {
    Ok(_) => log::info!("Workflow started"),
    Err(e) => log::error!("Cannot start: {}", e),
}
```

### Use Pause for Resumable Stops

```rust
// ❌ Bad: Use stop when you want to resume later
workflow.stop().await?;
// ... later ...
workflow.start().await?; // ERROR!

// ✅ Good: Use pause when you want to resume later
workflow.pause().await?;
// ... later ...
workflow.start().await?; // Works!
```

## Breaking Changes

### For Users

All workflow control methods are now `async`:

```rust
// Before
workflow.start()?;
workflow.pause()?;
workflow.stop()?;

// After
workflow.start().await?;
workflow.pause().await?;
workflow.stop().await?;
```

### For Examples and Tests

All example code has been updated to use `.await` on control methods.

## Testing

### Status Updates Test

See `examples/workflow_status.rs`:
- Verifies status transitions through all states
- Asserts correct status after each command
- Validates the complete lifecycle

### Restart Behavior Test

See `examples/workflow_restart.rs`:
- Demonstrates successful pause/resume
- Shows restart prevention after stop
- Explains the rationale for the limitation

### Output Verification

```
Initial status: Idle
Status after start: Running
Status after pause: Paused
Status after resume: Running
Status after stop: Stopped

Can restart? false
✅ Correctly prevented restart
```

## Migration Guide

### Update Control Calls

Add `.await` to all workflow control methods:

```diff
- workflow.start()?;
+ workflow.start().await?;

- workflow.pause()?;
+ workflow.pause().await?;

- workflow.stop()?;
+ workflow.stop().await?;
```

### Check Status Properly

```rust
// Now returns correct status
let status = workflow.status().await;
match status {
    WorkflowStatus::Running => { /* ... */ },
    WorkflowStatus::Paused => { /* ... */ },
    WorkflowStatus::Stopped => { /* ... */ },
    _ => { /* ... */ }
}
```

### Handle Stopped Workflows

```rust
// Check before restarting
if workflow.can_restart().await {
    workflow.start().await?;
} else {
    // Create new workflow instance
}
```

## See Also

- `examples/workflow_status.rs` - Status tracking demonstration
- `examples/workflow_restart.rs` - Restart behavior demonstration
- `examples/workflow_control.rs` - Pause/resume/stop example
- `TASK_EXECUTION_FIX.md` - Related task execution concurrency fix