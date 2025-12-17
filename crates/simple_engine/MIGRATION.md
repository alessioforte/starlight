# Migration Guide: From Old Engine to Simple Engine

This guide helps you migrate from the original `engine` crate to the new `simple_engine` crate.

## Overview

The new Simple Engine provides a cleaner, safer, and more maintainable API. While the core concepts remain the same, the implementation details are significantly improved.

## Key Changes

### 1. Task Implementation

#### Old Way ❌

```rust
use crate::tasks::{Task, Wiring, Worker};

const NAME: &str = "MyTask";

#[derive(Debug, Clone, Deserialize)]
pub struct MyTask {
    param1: String,
    param2: i32,
}

task! {
    MyTask,
    State,
    async fn execute(&self, id: Option<&str>) {
        let wiring = self.wiring();
        let id = id.unwrap();
        let handle = wiring.in_rxs.get(id).expect("Channel not found");
        let mut rx = handle.tx.subscribe();
        
        while let Ok(payload) = rx.recv().await {
            // Process...
            let out = wiring.out_txs.get("out").unwrap();
            out.iter().for_each(|handle| {
                let _ = handle.tx.send(data.clone());
            });
        }
    }
}
```

#### New Way ✅

```rust
use simple_engine::prelude::*;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Params {
    param1: String,
    param2: i32,
}

#[derive(Debug, Default)]
pub struct State {}

pub struct MyTask {
    base: BaseTask<Params, State>,
}

impl MyTask {
    pub fn create(id: String, params: Value) -> Result<Box<dyn Task>> {
        Ok(Box::new(Self {
            base: BaseTask::new(id, params)?,
        }))
    }
}

#[async_trait]
impl Task for MyTask {
    fn name(&self) -> &str {
        "MyTask"
    }

    async fn execute(&self, ctx: Arc<TaskContext>) -> Result<()> {
        let mut input = ctx.input("in")?;
        let output = ctx.output("out")?;
        
        while ctx.is_running() {
            let data = input.recv().await?;
            // Process...
            output.send(result)?;
        }
        Ok(())
    }
}
```

**Key Differences**:
- No more `task!` macro - explicit implementation
- No more `Wiring` - use `TaskContext` instead
- No more `unwrap()` - proper error handling with `?`
- No more `Option<&str>` - context provides clean API
- No more manual channel subscription

---

### 2. Workflow Building

#### Old Way ❌

```rust
let mut workflow = Workflow::new(
    "workflow-id".to_string(),
    Some("Workflow Name".to_string()),
    Some("Description".to_string()),
    tasks,
);

workflow.load();
workflow.execute();
```

#### New Way ✅

```rust
let workflow = WorkflowBuilder::new("workflow-id")
    .name("Workflow Name")
    .add_task(task_config1)
    .add_task(task_config2)
    .build()?;

workflow.start()?;
```

**Key Differences**:
- Builder pattern for clearer API
- Validation at `build()` time
- No separate `load()` step
- Explicit error handling

---

### 3. Task Configuration

#### Old Way ❌

```rust
use crate::cfg::{Component, Config};

let component = Component {
    id: "task1".to_string(),
    kind: Tasks::CsvReader,
    params: json!({
        "filename": "data.csv"
    }),
    dependencies: vec!["upstream".to_string()],
    handles: {
        let mut map = HashMap::new();
        map.insert("out".to_string(), vec!["downstream".to_string()]);
        map
    },
};
```

#### New Way ✅

```rust
use simple_engine::prelude::*;

let config = TaskConfig::new(
    "task1",
    Box::new(CsvReader::create),
    json!({
        "filename": "data.csv"
    })
)
.with_dependency("upstream")
.with_output("out", vec!["downstream".to_string()]);
```

**Key Differences**:
- Builder pattern for task config
- Factory function instead of enum
- Clearer method names

---

### 4. Channel Management

#### Old Way ❌

```rust
// Exposed internal channel details
let out_txs: DashMap<String, Vec<HandleSource>> = ...;
let in_rxs: DashMap<String, HandleTarget> = ...;

// Manual subscription
let mut rx = handle.tx.subscribe();
while let Ok(msg) = rx.recv().await { ... }

// Manual broadcast
out.iter().for_each(|h| {
    let _ = h.tx.send(data.clone());
});
```

#### New Way ✅

```rust
// Clean abstractions
let mut input = ctx.input("in")?;
let output = ctx.output("out")?;

// Simple API
let data = input.recv().await?;
output.send(result)?;
```

**Key Differences**:
- No exposure of broadcast channels
- No manual subscription management
- No iteration over handles
- Automatic error handling

---

## Step-by-Step Migration

### Step 1: Update Dependencies

**Old `Cargo.toml`:**
```toml
[dependencies]
engine = { path = "../engine" }
```

**New `Cargo.toml`:**
```toml
[dependencies]
simple_engine = { path = "../simple_engine" }
tokio = { version = "1", features = ["full"] }
async-trait = "0.1"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
```

### Step 2: Update Imports

**Old:**
```rust
use engine::tasks::{Task, Wiring, Worker};
use engine::wf::Workflow;
use engine::cfg::{Component, Config};
```

**New:**
```rust
use simple_engine::prelude::*;
use simple_engine::tasks::{CsvReader, CsvWriter, ...};
```

### Step 3: Migrate Each Task

For each task, follow this pattern:

1. **Extract parameters into a `Params` struct**
2. **Create a `State` struct (even if empty)**
3. **Wrap in `BaseTask`**
4. **Implement `Task` trait**
5. **Create factory function**

**Template:**
```rust
#[derive(Debug, Deserialize)]
pub struct Params {
    // Your parameters
}

#[derive(Debug, Default)]
pub struct State {
    // Your state (use AtomicUsize, etc. for concurrent access)
}

pub struct YourTask {
    base: BaseTask<Params, State>,
}

impl YourTask {
    pub fn create(id: String, params: Value) -> Result<Box<dyn Task>> {
        Ok(Box::new(Self {
            base: BaseTask::new(id, params)?,
        }))
    }
}

#[async_trait]
impl Task for YourTask {
    fn name(&self) -> &str {
        "YourTask"
    }

    async fn execute(&self, ctx: Arc<TaskContext>) -> Result<()> {
        // Your logic here
        Ok(())
    }
}
```

### Step 4: Update Workflow Construction

**Old:**
```rust
let tasks = vec![
    Component { ... },
    Component { ... },
];

let mut workflow = Workflow::new(id, name, desc, tasks);
workflow.load();
workflow.execute();
```

**New:**
```rust
let workflow = WorkflowBuilder::new(id)
    .name(name)
    .add_task(TaskConfig::new("task1", Box::new(Task1::create), params1)
        .with_output("out", vec!["task2".to_string()]))
    .add_task(TaskConfig::new("task2", Box::new(Task2::create), params2)
        .with_dependency("task1"))
    .build()?;

workflow.start()?;
```

### Step 5: Update Error Handling

**Old:**
```rust
// Panics everywhere
let value = map.get("key").unwrap();
let _ = sender.send(value);
```

**New:**
```rust
// Proper error handling
let value = map.get("key")
    .ok_or_else(|| EngineError::config("Key not found"))?;
sender.send(value)?;
```

---

## Common Patterns

### Pattern 1: Source Task (No Inputs)

**Old:**
```rust
async fn execute(&self, _id: Option<&str>) {
    let wiring = self.wiring();
    let running = self.running();
    
    while running.load(Relaxed) {
        let data = generate();
        let out = wiring.out_txs.get("out").unwrap();
        out.iter().for_each(|h| { let _ = h.tx.send(data.clone()); });
    }
}
```

**New:**
```rust
async fn execute(&self, ctx: Arc<TaskContext>) -> Result<()> {
    let output = ctx.output("out")?;
    
    while ctx.is_running() {
        let data = generate();
        output.send(data)?;
    }
    Ok(())
}
```

### Pattern 2: Processing Task (Inputs + Outputs)

**Old:**
```rust
async fn execute(&self, id: Option<&str>) {
    let wiring = self.wiring();
    let id = id.unwrap();
    let mut rx = wiring.in_rxs.get(id).unwrap().tx.subscribe();
    
    while let Ok(data) = rx.recv().await {
        let result = process(data);
        let out = wiring.out_txs.get("out").unwrap();
        out.iter().for_each(|h| { let _ = h.tx.send(result.clone()); });
    }
}
```

**New:**
```rust
async fn execute(&self, ctx: Arc<TaskContext>) -> Result<()> {
    let mut input = ctx.input("in")?;
    let output = ctx.output("out")?;
    
    while ctx.is_running() {
        let data = input.recv().await?;
        let result = process(data);
        output.send(result)?;
    }
    Ok(())
}
```

### Pattern 3: Sink Task (No Outputs)

**Old:**
```rust
async fn execute(&self, id: Option<&str>) {
    let wiring = self.wiring();
    let id = id.unwrap();
    let mut rx = wiring.in_rxs.get(id).unwrap().tx.subscribe();
    
    while let Ok(data) = rx.recv().await {
        store(data);
    }
}
```

**New:**
```rust
async fn execute(&self, ctx: Arc<TaskContext>) -> Result<()> {
    let mut input = ctx.input("in")?;
    
    while ctx.is_running() {
        let data = input.recv().await?;
        store(data)?;
    }
    Ok(())
}
```

### Pattern 4: Multiple Inputs

**Old:**
```rust
async fn execute(&self, id: Option<&str>) {
    let wiring = self.wiring();
    let id = id.unwrap();
    
    // Manual channel management
    let mut rx1 = wiring.in_rxs.get("input1").unwrap().tx.subscribe();
    let mut rx2 = wiring.in_rxs.get("input2").unwrap().tx.subscribe();
    
    // Complex select logic...
}
```

**New:**
```rust
async fn execute(&self, ctx: Arc<TaskContext>) -> Result<()> {
    let mut input1 = ctx.input("input1")?;
    let mut input2 = ctx.input("input2")?;
    
    while ctx.is_running() {
        tokio::select! {
            Ok(data1) = input1.recv() => process1(data1)?,
            Ok(data2) = input2.recv() => process2(data2)?,
        }
    }
    Ok(())
}
```

### Pattern 5: Stateful Task

**Old:**
```rust
pub struct State {
    counter: AtomicUsize,
}

async fn execute(&self, id: Option<&str>) {
    // Access self.state
    self.state.counter.fetch_add(1, Relaxed);
}
```

**New:**
```rust
pub struct State {
    counter: AtomicUsize,
}

async fn execute(&self, ctx: Arc<TaskContext>) -> Result<()> {
    // Access self.base.state
    self.base.state.counter.fetch_add(1, Ordering::Relaxed);
    Ok(())
}
```

---

## Checklist

Use this checklist to ensure complete migration:

- [ ] Updated `Cargo.toml` dependencies
- [ ] Changed imports to `simple_engine::prelude::*`
- [ ] Migrated all task implementations to new `Task` trait
- [ ] Added factory functions for all tasks
- [ ] Updated workflow construction to use `WorkflowBuilder`
- [ ] Replaced `unwrap()` with proper error handling
- [ ] Updated channel access to use `Input`/`Output` abstractions
- [ ] Tested all workflows
- [ ] Updated documentation
- [ ] Removed old engine dependency

---

## Troubleshooting

### Issue: "Input 'xyz' not found"

**Cause**: Task configuration doesn't match task code.

**Solution**: Ensure task dependencies match the input IDs you're requesting:
```rust
// Configuration
.with_dependency("upstream_task")

// Code
ctx.input("upstream_task")?  // Must match!
```

### Issue: "Output 'xyz' not found"

**Cause**: Output label not configured.

**Solution**: Add output configuration:
```rust
.with_output("out", vec!["downstream_task".to_string()])
```

### Issue: "Circular dependency detected"

**Cause**: Tasks depend on each other in a cycle.

**Solution**: Review your workflow graph. Use `WorkflowBuilder::build()` - it will detect cycles at build time.

### Issue: Task never receives data

**Cause**: Channel names don't match.

**Solution**: 
- Upstream task outputs to channel named after upstream task ID
- Downstream task depends on upstream task ID
- They should match!

---

## Benefits of Migration

✅ **Cleaner Code**: Less boilerplate, clearer intent  
✅ **Better Errors**: No panics, helpful error messages  
✅ **Type Safety**: Catch errors at compile time  
✅ **Easier Testing**: Mockable contexts  
✅ **Better Performance**: Optimized channel usage  
✅ **Documentation**: Extensive docs and examples  
✅ **Maintainability**: Simpler architecture  

---

## Need Help?

1. Check the [README.md](README.md) for examples
2. Review the [ARCHITECTURE.md](ARCHITECTURE.md) for design details
3. Look at built-in tasks in `src/tasks/` for reference
4. Run examples: `cargo run --example simple_workflow`

---

**Happy migrating! 🚀**