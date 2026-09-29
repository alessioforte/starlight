# Sequential Jobs/Phases Implementation Audit

**Project:** Starlight  
**Reviewed state:** branch `jobs`, commit `09fd10b`  
**Review date:** 2026-07-19  
**Scope:** the complete tracked workspace plus project configuration and example workflows; engine, server API, CLI/TUI, built-in tasks, simulator models, JSON helper, tests, and dependency lockfile

## Executive assessment

The jobs/phases direction is architecturally sensible, and the main happy path is implemented: legacy tasks normalize into one job, job DAGs are isolated, jobs run in declaration order, artifacts provide a declared cross-job handoff, and checkpoints record workflow progress. The code also has substantial tests around validation and sequential execution.

The implementation is not production-ready. Lifecycle control is split between the workflow driver and an ephemeral active-job command channel, which creates confirmed pause/stop/start races. Checkpoint semantics are not tied to a configuration version and mishandle continued failures. Resources and artifacts are only partially integrated. Most importantly, the unauthenticated server binds to all interfaces while allowing callers to save, mount, and execute configs with filesystem and network side effects.

**Verdict:** good prototype and a viable foundation, but unsafe for an untrusted or exposed deployment. Fix the critical security boundary and redesign workflow-level lifecycle/checkpoint semantics before treating sequential jobs as reliable.

## What is well designed

- **Backwards compatibility:** `Config::normalized_jobs()` turns legacy top-level tasks into a `default` job, keeping old workflow files operational (`crates/eng/src/cfg.rs:177-199`).
- **Clear execution unit:** each `JobRuntime` owns its task graph, channels, command broadcaster, contexts, and join set. Only one job is active at a time (`crates/eng/src/wf.rs:70-315`).
- **Job-local graph validation:** unresolved dependencies and cycles are checked per job; shared channels may intentionally have multiple producers, and cross-job channels are rejected in favor of explicit artifacts (`crates/eng/src/cfg.rs:449-477`, `crates/eng/src/wf.rs:1210-1438`).
- **Declared data handoff:** workflow/job resource scopes and prior-job artifact references make phase boundaries visible in configuration (`crates/eng/src/cfg.rs:24-141`, `crates/eng/src/resource.rs`).
- **Progress visibility:** `WorkflowInfo` exposes current, completed, and failed jobs, and the checkpoint store persists that summary (`crates/eng/src/wf.rs:42-52`, `crates/eng/src/checkpoint.rs:8-88`).
- **Test investment:** the engine has broad unit/integration coverage for config validation, job ordering, error policies, resource loading, artifacts, lifecycle basics, and checkpoint persistence.

## Architecture as implemented

1. `Engine::add()` validates the full config, loads an optional checkpoint, and invokes `WorkflowBuilder` (`crates/eng/src/eng.rs:81-111`).
2. The builder normalizes jobs, loads workflow-scoped resources synchronously, derives checkpoint progress, and prepares only the first incomplete job (`crates/eng/src/wf.rs:1008-1151`).
3. `Workflow::start()` starts or recreates the sequential driver. For each job, the driver loads job resources, merges them with workflow resources, spawns its task DAG, publishes an active-job view, sends `Start`, and waits for every task runner (`crates/eng/src/wf.rs:467-553`, `770-919`).
4. A completed job is checkpointed and the driver advances. A task failure applies the job's `on_error` policy. Stop terminates the active job. When the last job is handled, the workflow becomes `Completed`.
5. Artifact declarations from earlier configured jobs are made available to later artifact resource references; the resource loader then reads the declared path (`crates/eng/src/wf.rs:921-949`, `crates/eng/src/resource.rs:61-101`).

The separation between config, driver, and per-job runtime is a useful shape. The principal design flaw is that lifecycle intent belongs only to the current job's transient command channel, not to the workflow driver as durable state.

## Findings

Severity reflects the current default server behavior. If Starlight is strictly bound to a trusted local environment, some security severities fall, but the code does not enforce that assumption.

### Critical

#### SEC-01 — Unauthenticated remote workflow execution exposes host filesystem and network

The server binds `0.0.0.0` (`src/main.rs:38`) and installs no authentication or authorization middleware. The CLI sends `x-api-key`, but the server never checks it. Any reachable client can save, mount, start, stop, and delete workflows. Workflow-controlled operations include arbitrary CSV file reads/writes, HTTP requests, HTTP resource loading/caching, and a caller-supplied generation `base_url` (`src/api/generate.rs:131-159`).

An attacker can therefore use the process's permissions to overwrite files, read CSV-shaped local data and send it over HTTP, perform SSRF against internal services, and exhaust CPU/memory/network resources. This is the dominant system risk.

**Recommendation:** bind loopback by default; add mandatory authentication and per-operation authorization before supporting remote use; sandbox workflow paths under explicit roots; restrict outbound hosts/schemes/IP ranges; apply body, response, retry, rate, and concurrency limits; run task workers with minimal OS privileges.

#### SEC-02 — Workflow-file path traversal before config validation

`POST /workflows/files` deserializes `Config` but never calls `Config::validate()`. It directly builds `format!("{}.yaml", config.id)` and joins it to the workflows directory (`src/api/workflows.rs:229-256`). An ID such as `../../outside/name` escapes that directory and can write or overwrite a `.yaml` file in an existing directory writable by the server user. Lookup routes similarly build paths from raw route IDs (`src/api/workflows.rs:32-42`). The public checkpoint store also derives nested paths from an unchecked string (`crates/eng/src/checkpoint.rs:63-88`).

The engine's ID validation does reject separators, but it runs later and is bypassed by the file-save endpoint and direct public APIs.

**Recommendation:** use one validated newtype for workflow/job/resource/artifact/task IDs at every boundary; reject path components before joining; canonicalize and verify the parent remains under the configured root; save atomically; never rely on a later engine validation for filesystem safety.

### High

#### LIFE-01 — Pausing an interval timer can complete the active job

The timer sleeps, then checks `ctx.is_running()` and breaks when it is false (`crates/eng/src/tasks/timer.rs:206-215`; the cron path has the same pattern). `is_running()` is false while paused, so the task exits instead of waiting. `JobRuntime::wait()` treats every non-`Failed` task result as normal and declares the job completed unless the explicit stop flag is set (`crates/eng/src/wf.rs:291-315`).

A focused probe started a two-tick interval timer, paused it during the sleep, and observed workflow status change from `Paused` to `Completed` about 150 ms later.

**Recommendation:** after sleeps, call the blocking `ctx.running().await` gate; make task completion distinct from lifecycle stop; have the job runtime verify workflow intent before converting task exits to completion. Add interval and cron pause/resume regression tests.

#### LIFE-02 — Immediate stop/start is acknowledged but fails with a closed command channel

`Workflow::start()` returns `Ok(())` for `Stopped`/`Failed` while `driver_running` is still true rather than waiting for termination or scheduling a restart (`crates/eng/src/wf.rs:467-510`). The active job command broadcaster can close during this window.

A focused probe called start, stop, and start without delay. Both commands returned success; status was immediately `Stopped` and then settled as:

```text
Failed("Workflow error: Workflow 'restart_race_probe' failed to start: channel closed")
```

The driver also unconditionally writes `Running` when starting a new job, so pause/stop at a job boundary can be overwritten (`crates/eng/src/wf.rs:824-826`).

**Recommendation:** give the workflow one serialized control loop with durable desired state; acknowledge commands only after the transition is accepted; join/retire the old driver before restart; check desired state before and after resource loading, job creation, and job start.

#### STATE-01 — Checkpoints can skip or repeat the wrong jobs

Checkpoint restoration computes the next job as the first ID absent from `completed_jobs`; it ignores the stored `current_job`, configuration identity, ordering version, and artifact state (`crates/eng/src/wf.rs:958-1005`). A job that fails under `continue` is appended only to `failed_jobs`. On remount it is therefore the first incomplete job and is reopened.

A focused two-job probe completed with `completed_jobs = ["success"]` and `failed_jobs = ["allowed_failure"]`. Remounting the same config produced `Stopped` with `current_job = "allowed_failure"`.

Editing or reordering a workflow under the same ID can likewise apply stale completed IDs to a new graph. Deleting a workflow file does not delete its checkpoint, which amplifies this risk.

**Recommendation:** checkpoint a schema version and deterministic config/job-plan digest; persist an ordered terminal outcome for every handled job; validate a prefix against the current plan; explicitly define resume behavior for `continue`; verify required artifacts before skipping producers; provide checkpoint reset/migration APIs.

#### POLICY-01 — Error policies cover only task execution and two policies are indistinguishable

Job resource-load, task-construction/spawn, and initial-start errors are converted directly to fatal job/workflow failure before the `on_error` match (`crates/eng/src/wf.rs:809-842`). Only failures returned from `JobRuntime::wait()` use the policy. `continue` and `continue_with_warnings` share one match arm and produce no different status, event, or API warning (`crates/eng/src/wf.rs:874-909`).

**Recommendation:** define a job outcome model covering setup, execution, teardown, artifact validation, and cancellation; apply policy once to that outcome; add durable warnings to `WorkflowInfo`/checkpoints/events or remove the unsupported policy.

#### RES-01 — Blocking, unbounded resource work can stall the server and engine

Resource loading uses blocking `reqwest::blocking`, synchronous filesystem calls, `std::thread::sleep` retry backoff, and whole-body reads (`crates/eng/src/resource.rs:103-155`, `257-339`). Workflow resources load in `Engine::add()`, and API mount/auto-load invokes that while holding the global `Arc<Mutex<Engine>>` (`src/api/workflows.rs:139-151`). User-controlled timeouts/retries/body sizes have no useful upper bounds.

One slow or malicious resource can block a Tokio worker and serialize unrelated API calls; large resources can exhaust memory or disk.

**Recommendation:** make resource loading async or use a bounded blocking pool outside the engine lock; cap response/file/cache size, timeout, retries, delay, redirects, and concurrency; stage resources before committing the workflow to the registry.

#### ART-01 — Artifacts and `uses` are declarations, not enforceable data contracts

All artifacts from every earlier configured job are considered available regardless of that job's actual outcome (`crates/eng/src/wf.rs:921-949`). The engine does not verify existence, type, freshness, ownership, producer task, or checksum at job completion. A later resource discovers a missing artifact only when it tries to load the path.

Likewise, `uses` only validates visibility. Every task receives the complete workflow-plus-job resource map (`crates/eng/src/wf.rs:218-225`), and built-ins such as `csv_reader` still use an independent `params.filename`. A config can declare one resource while reading another path.

**Recommendation:** model artifact publication as a checked job output; publish only after successful existence/format/integrity validation; carry actual produced handles into later jobs; inject only resources listed by `uses`; let built-ins consume typed resource handles rather than duplicate paths.

#### VALID-01 — Public construction paths enforce different invariants

`Config::validate()` performs workflow-wide job/resource/artifact validation. Direct `WorkflowBuilder` use does not equivalently reject duplicate job IDs, artifact references to later/missing jobs, artifact format mismatches, top-level artifact resources, or all unsafe IDs; some errors are deferred until a later job (`crates/eng/src/cfg.rs:204-494`, `crates/eng/src/wf.rs:1008-1151`). Even `Config::validate()` does not reject empty job, task, channel, resource, or artifact IDs. `WorkflowStatus` is also part of public `WorkflowInfo` but is not re-exported from `eng::lib`, making the public type awkward to name externally (`crates/eng/src/lib.rs:25-33`).

**Recommendation:** make one validated plan the only input to runtime construction; keep low-level builders crate-private or require validated types; re-export all public field types.

#### DOS-01 — Missing numeric validation permits panics and tight loops

Examples include:

- simulator random `stddev <= 0` reaches `Normal::new(...).unwrap()` and can panic during workflow mount (`crates/simulator/src/models/random.rs:16-19`)
- negative random-walk volatility creates an invalid `random_range` at runtime (`crates/simulator/src/models/random_walk.rs:27-30`)
- zero interval values can create very hot source/timer loops
- aggregator accepts `window_count: 0` or `window_ms: 0`; a zero time window repeatedly sleeps for zero duration (`crates/eng/src/tasks/aggregator.rs:285-294`)
- collection/group cardinality and task/channel counts are not bounded

With unauthenticated workflow submission these are remote availability risks.

**Recommendation:** validate semantic ranges in every factory; establish global workflow quotas; replace panicking constructors with errors; add adversarial boundary tests.

### Medium

#### WIRE-01 — One channel assigned to two named ports is wired incorrectly

The consumer list records the task twice, while a reverse `HashMap<(task, channel), port>` stores only one of the two port names (`crates/eng/src/wf.rs:130-188`). Both receivers then land on the last stored port. Validation does not reject this shape.

**Recommendation:** represent each consumer edge as `(task, port)` from the outset, or reject duplicate channel use across a task's ports.

#### API-01 — API responses hide errors and can mutate state on failed requests

Lifecycle handlers map every engine error to 404 and return hard-coded target states instead of the actual state (`src/api/workflows.rs:86-116`). Mount checks the route ID for an existing workflow but adds the config's potentially different ID (`src/api/workflows.rs:123-153`). Delete unmounts first, then checks for a file; asking to delete a missing file can still unload a running workflow (`src/api/workflows.rs:282-305`). Engine removal aborts the driver/task futures without orderly stop hooks or a checkpoint update (`crates/eng/src/eng.rs:114-123`, `crates/eng/src/wf.rs:682-697`). Synchronous directory parsing happens while holding the engine lock, and invalid files are silently omitted.

**Recommendation:** define typed API errors/status codes, require route/config ID equality, validate before mutation, delete only after resolving the exact target, and return actual workflow info.

#### CLI-01 — Lifecycle commands can report success when nothing happened

The CLI `send_command` discards the HTTP result and always returns `Ok(())` (`crates/cli/src/api.rs:97-102`). Other client calls do not consistently call `error_for_status`. The CLI models workflow status as `String`, but server-side `Failed(String)` serializes as an object, so listing a failed workflow can fail deserialization. Job progress is not represented in CLI views.

The TUI tracks cursor positions in characters but passes them to `String::insert`/`remove` as byte offsets; non-ASCII editing can panic (`crates/cli/src/tui.rs:781-799`).

**Recommendation:** use a shared API schema, propagate non-2xx errors, parse the status enum, expose job progress, and use byte-safe grapheme/character editing.

#### TASK-01 — HTTP sender can silently lose concurrent request failures

Concurrent mode ignores errors when enqueueing completed outputs and ignores `JoinSet` results (`crates/eng/src/tasks/http_sender.rs:388-403`). A job may report success despite lost requests/results. Invalid configured headers are silently skipped and non-JSON response bodies become `null`.

**Recommendation:** define delivery/error semantics, aggregate request failures into task/job outcomes when configured, and surface counters for attempted/succeeded/failed/dropped requests.

#### TASK-02 — CSV support is not CSV-compliant

The reader uses `split(delimiter)`, so quoted delimiters, escaped quotes, and multiline fields are unsupported (`crates/eng/src/tasks/csv_reader.rs:121-129`, `176-188`). The writer decides quoting using a hard-coded comma even when a custom delimiter is configured (`crates/eng/src/tasks/csv_writer.rs:90-119`); headers also need escaping. Both defects already have ignored tests.

**Recommendation:** use the Rust `csv` crate for parsing/writing and unignore the conformance tests.

#### TASK-03 — Aggregator average is mathematically wrong for sparse/non-numeric fields

`avg` divides the numeric sum by the total messages in the window, not the number of numeric values found for that field (`crates/eng/src/tasks/aggregator.rs:175-193`). Missing or non-numeric values bias results downward.

**Recommendation:** track per-column valid counts and define behavior for empty numeric sets.

#### OBS-01 — Job history loses task outcomes and metrics

`WorkflowInfo.task_count` and `Workflow::state()` read only the active-job view. When a job completes, that view is cleared and its task metrics/status disappear (`crates/eng/src/wf.rs:384-459`). Completed workflows therefore show no task detail, and there is no per-job timing, warning, artifact, or failure history beyond ID lists and one workflow failure string.

**Recommendation:** persist immutable `JobRunInfo` records with start/end time, outcome, task summaries, artifacts, warnings, and retry/resume provenance.

#### GEN-01 — The generation prompt does not describe jobs/resources/artifacts

`src/api/prompt.md` still documents only the legacy top-level `tasks` schema. The model cannot intentionally generate sequential jobs or resource/artifact handoffs and may continually repair toward an obsolete shape.

**Recommendation:** version the prompt with the config schema and include job-based examples, error policies, resource scopes, artifact ordering, and `uses` behavior.

#### DEP-01 — The locked dependency graph has known advisories

`cargo audit` found **7 vulnerabilities** across 366 locked dependencies:

- `bytes 1.10.1`: RUSTSEC-2026-0007; upgrade to `>=1.11.1`
- `quick-xml 0.38.4`: RUSTSEC-2026-0194 and RUSTSEC-2026-0195; upgrade to `>=0.41.0` (CLI transitive dependency)
- `rustls-webpki 0.103.3`: RUSTSEC-2026-0049, -0098, -0099, and -0104; upgrade to at least the advisory-specific fixed `0.103.x` releases

It also reported 8 allowed warnings for unmaintained or unsound transitive packages: `bincode`, `paste`, `proc-macro-error2`, `yaml-rust`, `anyhow`, `lru`, and two locked `rand` versions.

**Recommendation:** update the lockfile/direct dependency constraints, verify the transitive CLI/TUI stack, rerun all tests and `cargo audit`, and add advisory checking to CI.

### Low / engineering hygiene

- Strict Clippy fails. Normal Clippy exposed 39 library warnings plus test lint failures, including derivable defaults, type complexity, suspicious identical branches, and ignored I/O amounts.
- `Cargo.toml` has no license value and the root `README.md` contains only a heading.
- Some engine/CLI commands are declared as placeholders; CLI context helpers are dead code.
- Workflow file writes and checkpoint writes lack full durability guarantees (no directory `fsync`; config writes are not atomic). The checkpoint temp name is predictable.
- CSV values beginning with spreadsheet formula markers are written unchanged; exporting untrusted data for spreadsheet use can cause formula injection.

## Verification performed

| Check | Result |
|---|---|
| Repository inventory and source review | All tracked Rust, manifest, docs, config, and example workflow files reviewed; generated `target/` excluded |
| `cargo fmt --all -- --check` | Passed |
| `cargo test --workspace --all-targets` | Passed outside the filesystem/socket sandbox: **345 passed, 2 ignored** |
| Ignored tests | Quoted-delimiter CSV reader; custom-delimiter CSV writer |
| `cargo clippy --workspace --all-targets --all-features -- -D warnings` | Failed |
| Focused lifecycle/checkpoint probes | Confirmed timer-pause completion, immediate stop/start channel-closed failure, and continued-job remount regression; temporary probes removed |
| Targeted committed-secret scan | No credential found; only `.starlight/cli/config.yaml` placeholder `your_api_key_here` matched |
| `cargo audit` | Failed policy: **7 vulnerabilities**, **8 warnings** across 366 dependencies |

The first sandboxed test run could not bind loopback sockets and produced seven environment-caused failures. The same complete suite passed after running in an environment that permits local listeners.

## Recommended remediation order

1. **Contain the security boundary:** loopback default, authentication/authorization, validated IDs, rooted filesystem access, outbound-network policy, and resource/task quotas.
2. **Fix workflow control:** one serialized workflow-level control loop, explicit desired state, correct command acknowledgements, and regression tests at every job boundary.
3. **Version checkpoint semantics:** plan digest, terminal result per job, continued-failure behavior, artifact verification, reset/migration support.
4. **Make job contracts real:** typed/published artifacts, restricted resource injection, policy coverage for the entire job lifecycle, durable warnings/history.
5. **Unify validation:** a validated execution plan shared by Engine, builder, API, CLI, and generator.
6. **Harden tasks/resources:** async bounded I/O, numeric/range checks, no panics, correct CSV/aggregation, explicit HTTP delivery failures.
7. **Repair clients and observability:** shared status schema, accurate HTTP errors, job/task history, generator prompt update.
8. **Restore quality gates:** resolve dependency advisories, make strict Clippy pass, unignore CSV tests, and add race/adversarial CI coverage.

## Release recommendation

Do not expose this branch's server to untrusted clients or depend on checkpoints for exactly-once phase execution. It is suitable for local development and continued design work after clearly documenting those limits. A production milestone should require all critical/high findings above to be fixed or explicitly accepted with compensating controls.
