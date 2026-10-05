# Distributed execution

`@promin/workflow/distributed` runs workflow steps on a fleet of workers. A
**distributed runner** (the coordinator) drives each run exactly like the
in-process runner — DAG waves, run lock, retries, compensation, deadlines —
but hands step bodies to a **step queue**; **workers** claim tasks, run the
handler they registered for the step, and write the outcome to the workflow
storage. Sleep and signal waits are resumed by **scanners**.

```
           ┌────────────────────────────┐
 run() ───►│ DistributedWorkflowRunner  │  holds the run lock, fenced writes,
           │ (coordinator, any number)  │  sleep / signal / child steps in-process
           └──────┬──────────────▲──────┘
          enqueue │              │ step rows (storage)
           ┌──────▼──────┐  ┌────┴─────────────┐
           │  StepQueue  │  │ WorkflowStorage  │
           └──────┬──────┘  └────▲─────────────┘
            claim │              │ saveStepResult / saveStepFailure
           ┌──────▼──────────────┴──┐
           │ workers (createWorker) │  handlers by step name, capabilities, versions
           └────────────────────────┘
```

## Setup

```typescript
import { InMemoryWorkflowStorage, workflow } from "@promin/workflow";
import {
  createDistributedWorkflowRunner,
  createWorker,
  InMemoryStepQueue,
  MapStepRegistry,
} from "@promin/workflow/distributed";
import { succeed } from "@spilne/perfect-core";

// Shared by every process: one storage and one queue (Postgres, Redis, SQLite, or remote).
const storage = new InMemoryWorkflowStorage();
const stepQueue = new InMemoryStepQueue();

// The definition. Under the distributed runner the step bodies here are not
// called: each ordinary step becomes a queue task named after the step.
const processVideo = workflow<{ videoId: string }>({ name: "process-video", version: "1" })
  .step("download", ({ input }) => succeed({ path: `/tmp/${input.videoId}.mp4` }))
  .step("transcribe", ({ prev }) => succeed({ text: `text of ${prev.path}` }), { needs: ["gpu"] })
  .build();

// Coordinator process.
const coordinator = createDistributedWorkflowRunner({ storage, stepQueue });
void coordinator.startLoop(); // leader-elected sweep: recovery + dead-worker requeue

// Worker process (GPU box): hosts both steps, offers the "gpu" capability.
const registry = new MapStepRegistry();
registry.register({
  stepName: "download",
  handler: async (ctx) => ({ path: `/tmp/${(ctx.input as { videoId: string }).videoId}.mp4` }),
});
registry.register({
  stepName: "transcribe",
  handler: async (ctx) => ({ text: `text of ${(ctx.deps["download"] as { path: string }).path}` }),
  retry: { maxRetries: 2 },
});
const worker = createWorker({
  storage,
  stepQueue,
  registry,
  capabilities: ["gpu"],
  supportedVersions: ["1"],
  concurrency: 4,
});
void worker.start(); // resolves only once the worker stops

const result = await coordinator.run({
  workflow: processVideo,
  workflowId: "video-abc",
  input: { videoId: "abc" },
});
console.log(result); // { text: "text of /tmp/abc.mp4" }

await worker.stop({ timeoutMs: 10_000 });
await coordinator.stopLoop();
```

The same `Workflow` runs unchanged on `createWorkflowRunner` (in-process,
the step bodies run) or `createDistributedWorkflowRunner` (the registered
handlers run). To dispatch only some steps, keep the in-process runner and
pass `stepExecutor: new RoutingStepExecutor({ remote: new StepQueueExecutor({ stepQueue, storage }), remoteSteps: ["transcribe"], storage })`.

### What runs where

- **On the coordinator:** the orchestration (waves, run lock and heartbeat,
  workflow retry, compensation, deadlines, cancel checks) and the
  `.sleep()`, `.waitForSignal()` and `.subworkflow()` steps: sleep and
  signal steps only record their suspension, and a subworkflow step drives
  its child run there (the child's ordinary steps go to the queue).
  `compensate` callbacks run here, from the definition.
- **On workers:** every other step, including `.journaled()`, `.guard()`,
  `.tripwire()`, parallel branches (task `"<block>.<label>"`) and `.map()`
  transforms (`"<head>.map"`). The worker runs the handler registered under
  that name; a step with no registered handler on any worker stays pending
  until its wait times out.
- **Step options split.** For a dispatched step, `retry`, `timeoutMs` and
  `onFailure` come from the worker side — the registration
  (`register({ retry, onFailure })`) and middleware (`timeoutMiddleware`) —
  not from the definition's `StepOptions`. `needs`, `priority`, `queue`,
  `skipWhen` and `compensate`, the workflow-level `retry` and the attempt
  count stay with the coordinator.
- **Routing** happens inside the queue's claim: a worker only claims tasks
  whose step name is in its registry, whose `needs` (from `StepOptions.needs`)
  are a subset of its `capabilities`, and whose workflow version is in its
  `supportedVersions` (unversioned tasks always pass). A task a worker can't
  run is never claimed, so it never blocks the tasks behind it.
- **Order:** higher `priority` first (0–10, default 5), FIFO within a
  priority. **Concurrency keys** (`queue` on the workflow or a step) cap how
  many tasks with the same `(scope, key)` run at once across every claimer.

## Delivery guarantees

**Step bodies are executed at least once.** The queue hands a pending task to
one claimer at a time, and every write a worker makes about a task is fenced
by its claim token, but a handler can still run more than once for the same
step:

| How a step runs twice                                                                                                            | Why                                                                                                                                                                                          |
| -------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| The worker crashes, stalls or is partitioned after the handler's side effect and before the outcome is written                   | The task has no fresh heartbeat for `workerTimeoutMs` (or its worker is declared dead); the coordinator's sweep requeues it and another worker runs it again.                                |
| The handler is still running when the claim is lost                                                                              | The heartbeat returns `false`: the worker aborts `ctx.signal` (`TaskLeaseLostError`) and skips the commit — but work the handler already did stays done, and the new claimant does it again. |
| The step row is written but the queue `complete()` fails                                                                         | The task stays claimed, goes stale and is redelivered; the step's row is already visible to the coordinator, but the handler runs again.                                                     |
| `worker.stop({ timeoutMs })` gives unfinished tasks back                                                                         | They are `release()`d and claimed by another worker at once; their handlers are aborted, not undone.                                                                                         |
| A worker-side retry (`register({ retry })`, `retryMiddleware`), a workflow retry, `force`, or a resume after a coordinator crash | The handler runs again (a workflow retry enqueues the step again with the next attempt number).                                                                                              |
| A step wait times out while its task is still `pending`                                                                          | The step fails with `StepWaitTimeoutError`, but the task stays queued (a pending task can't be recalled) and may still run later.                                                            |

Make handlers idempotent (an idempotency key derived from
`ctx.workflowId` + `ctx.stepName`, an upsert, a check-before-write) and
pass `ctx.signal` to everything that accepts one.

What fencing does guarantee:

- **One live claim per task.** `claim` hands a pending task to a single
  claimer and mints a fresh `claimToken`; `heartbeat`, `complete`, `fail` and
  `release` are rejected (return `false`) for any other token, so a stale
  worker can't settle a task someone else now owns.
- **No phantom completions.** A worker commits in the order fenced heartbeat
  → storage write → queue `complete` / `fail`. The queue never says
  `completed` while storage has nothing; a storage write that fails leaves
  the task claimed, so it is redelivered once its lease goes stale.
- **Narrow stale-write window.** The worker's step-row write itself is not
  fenced by the run lock; it is guarded by the heartbeat immediately before
  it. A worker that loses its claim between that heartbeat and the write can
  still land one stale step row. The coordinator, which holds the run lock,
  writes every run-level transition fenced (see
  [Storage → Fencing](../durable/storage/README.md#fencing)).
- **Poison tasks are dead-lettered.** Each claim counts a delivery
  (`release` does not). Once a task has been delivered `maxDeliveries` times
  (default 10), the next `requeueStuck` marks it failed with
  `poisoned: exceeded N deliveries` instead of requeueing it, and the
  coordinator fails the step.
- **Bounded waits.** The coordinator waits for a queued step's outcome for at
  most `stepWaitTimeoutMs` (default 24 h, `Infinity` to disable). After that
  the step fails with `StepWaitTimeoutError` and a still-running task is
  failed in the queue, so its worker loses the claim (a still-pending task
  stays queued). A deleted or terminal run ends the wait with
  `StepWaitAbandonedError`.
- **Enqueue is idempotent** on `(workflowId, stepName)` while a task for the
  pair is pending or running, so two coordinators that both decide a step is
  ready create one task.

## Step queue (contract v2)

```typescript
import { InMemoryStepQueue } from "@promin/workflow/distributed";

const queue = new InMemoryStepQueue({ maxDeliveries: 5 });

await queue.enqueue({
  workflowId: "wf-1",
  stepName: "send-email",
  input: { to: "a@example.com" },
  prevResults: {},
  needs: ["smtp"],
  priority: 8,
  attempt: 1,
  concurrencyScope: "send-email",
  concurrencyKey: "tenant-42",
  concurrencyLimit: 2, // at most 2 running per tenant, across all workers
});

const [task] = await queue.claim({
  workerId: "mailer-1", // recorded as claimedBy; dead-worker requeue keys on it
  limit: 10,
  capabilities: ["smtp"],
  stepNames: ["send-email"],
  versions: ["2"],
});
if (task) {
  const claim = { taskId: task.id, claimToken: task.claimToken ?? "" };
  const settled = await queue.complete({ ...claim, result: { sent: true }, durationMs: 12 });
  if (!settled) console.log("claim lost: another worker owns the task now");
}

const { requeued, deadLettered } = await queue.requeueStuck({ mode: "stale", olderThanMs: 30_000 });
await queue.requeueStuck({ mode: "worker", workerId: "mailer-1" });
await queue.purge({ completedBefore: new Date(Date.now() - 86_400_000) });
console.log(requeued, deadLettered, await queue.get("missing-id"));
```

| Method                                                                                 | Semantics                                                                                                                                                                                                                |
| -------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `enqueue(params)`                                                                      | Idempotent on `(workflowId, stepName)` while pending/running. Carries `attempt`, `needs`, `priority`, `version`, `metadata` and the concurrency key.                                                                     |
| `claim({ workerId, limit, capabilities?, stepNames?, versions? })`                     | Routing runs inside the claim (SQL `WHERE`, Lua, or in-process). Each claimed task is `running`, has a fresh `claimToken`, `claimedBy = workerId` and `deliveries + 1`. Tasks whose concurrency key is full are skipped. |
| `heartbeat` / `complete` / `fail`                                                      | Fenced by `claimToken`; return `false` when the claim is no longer current.                                                                                                                                              |
| `release({ taskId, claimToken })`                                                      | Back to `pending` in place, no delivery counted, concurrency slot freed.                                                                                                                                                 |
| `requeueStuck({ mode: "worker", workerId } \| { mode: "stale", olderThanMs }, lease?)` | Returns `{ requeued, deadLettered }`. With a `lease`, the sweep rejects with `StaleLeaseError` (and changes nothing) unless the lease is still current, checked in the same transaction or script.                       |
| `get(taskId)` / `purge({ completedBefore })` / `metrics({ since })`                    | Inspect one task (claim, outcome, dead-letter fields); delete terminal tasks; windowed counts and latency.                                                                                                               |

Implementations: `InMemoryStepQueue`, `PgStepQueue` (`@promin/postgres`),
`RedisStepQueue` (`@promin/redis`), `SqliteStepQueue` (`@promin/sqlite`) and
`RemoteStepQueue` (`@promin/workflow-remote`, the worker subset over HTTP).
The in-memory, Postgres, Redis and SQLite queues run `stepQueueTestSuite`
from `@promin/workflow/testing`.

## Workers

`createWorker(config)` returns a `WorkflowWorker` (`start()`, `stop()`,
`workerId`).

- **Claim loop.** Claims up to `concurrency` tasks per poll
  (`pollIntervalMs`). When a claim fills every free slot, each settling task
  wakes the loop to claim again at once. A failing claim is reported to
  `onError({ phase: "claim" })` and retried with capped exponential backoff
  (`maxErrorBackoffMs`, default 30 s); it never ends the loop.
- **Handler context.** `ctx.input` is the workflow input; `ctx.deps` holds
  the results of every step the run has completed so far, by step name (not
  only the step's declared dependencies); `ctx.prev` is the one result when
  exactly one step has completed, the input when none has, and otherwise the
  same record as `ctx.deps` — so read dependencies by name from `ctx.deps`.
  `ctx.attempt`, `ctx.workflowId`, `ctx.stepName` and `ctx.signal` complete it.
- **Outcome, then commit.** The handler (with its `retry`, middleware and
  `onFailure`) settles first; then the worker commits as described above.
  Every outcome — completed, failed, skipped, fallback — writes an attempt row
  when the storage has `stepAttempts`.
- **Heartbeats and lease loss.** Each running task is heartbeated every
  `heartbeatIntervalMs` (default 5 s). A heartbeat that returns `false` aborts
  `ctx.signal` with `TaskLeaseLostError`, stops heartbeating, fires
  `hooks.onLeaseLost` and skips the commit. A heartbeat that throws is only
  reported (`phase: "heartbeat"`); the next one may succeed.
- **Graceful stop.** `stop()` stops claiming and waits for running tasks.
  `stop({ timeoutMs })` waits at most that long, then `release()`s the
  unfinished tasks (another worker claims them at once) and aborts their
  handlers with `WorkerStoppingError`; their outcomes are not written.
- **Errors never crash the process.** Commit, hook, release and task errors
  are reported through `onError` (`phase`: `commit`, `hook`, `release`,
  `task`).
- `taskFilter` is an extra post-claim check; rejected tasks are released.
  Prefer `capabilities`, the registry and `supportedVersions`, which route
  inside the claim.

```typescript
import { InMemoryWorkflowStorage } from "@promin/workflow";
import {
  createWorker,
  InMemoryStepQueue,
  loggingMiddleware,
  MapStepRegistry,
  timeoutMiddleware,
  type WorkerMiddleware,
} from "@promin/workflow/distributed";

declare function fetchReport(url: string, signal: AbortSignal): Promise<string>;

const registry = new MapStepRegistry();
registry.register({
  stepName: "fetch-report",
  handler: (ctx) => fetchReport(String(ctx.input), ctx.signal), // abort on lease loss / stop
  retry: { maxRetries: 3, baseDelayMs: 500 },
  onFailure: { fallback: () => "" },
});

const tracing: WorkerMiddleware = async ({ task, ctx, next }) => {
  const started = performance.now();
  try {
    return await next(ctx);
  } finally {
    console.log(task.stepName, performance.now() - started);
  }
};

const worker = createWorker({
  storage: new InMemoryWorkflowStorage(),
  stepQueue: new InMemoryStepQueue(),
  registry,
  middleware: [timeoutMiddleware({ ms: 30_000 }), loggingMiddleware(), tracing],
  hooks: { onLeaseLost: (task) => console.warn("lost", task.id) },
  onError: (event) => console.error(event.phase, event.error),
});

process.on("SIGTERM", () => {
  void worker.stop({ timeoutMs: 15_000 }).then(() => process.exit(0));
});
```

Hooks (`beforeStep`, `afterStep`, `onError`, `onLeaseLost`) observe;
middleware (`timeoutMiddleware`, `retryMiddleware`, `loggingMiddleware`,
`metricsMiddleware`, your own) wraps the handler and may change the outcome;
per-step `retry` / `onFailure` belong to one registration. Worker retries
use the shared retry policy defaults (3 retries, 250 ms doubling, jitter off,
budget from the first failure) on the worker's `clock`.

## The coordinator

`createDistributedWorkflowRunner(config)` returns a `DistributedWorkflowRunner`:
a `WorkflowRunner` (`run`, `runSafe`, `start`, `handle`, `resume`,
`subscribe`, `getStatus`, `recover`) plus `submit`, `waitForResult`,
`startLoop` and `stopLoop`.

- **Many coordinators.** Any instance can `run` / `submit` a workflow; the
  run lock decides who drives it. `run()` on a run another instance drives,
  and `waitForResult()` through suspensions, are settled by one shared result
  poller (`resultPollIntervalMs`).
- **Leader election.** Only the leader runs the background sweep. The default
  `SingleLeader` always wins (one coordinator). With several, pass a
  `LeaseLeaderElection` over a `LeaderLeaseStore` with key
  `coordinatorLeaderKey({ namespace })`; its lease **fences the sweep's queue
  writes** (`requeueStuck({ lease })`), so an instance that lost leadership
  while paused cannot requeue anything. A stale lease makes the leader stand
  down; `stopLoop()` releases it.
- **Dead workers.** Every `pollIntervalMs` the leader requeues tasks of
  workers the `workerRegistry` reports dead (no heartbeat for
  `workerTimeoutMs`) and tasks with no activity for `workerTimeoutMs`.
- **Orphan-only recovery.** On becoming leader, and then every
  `recoveryIntervalMs` (default 60 s), the leader adopts runs nobody drives:
  `pending`, `running` or `compensating` runs whose lock is free or expired
  and that have not been updated for `orphanGraceMs`. It prefers the
  definition from `registry`, else rebuilds a stub from the DAG stored with
  the run (`buildStubWorkflow`), whose sleep / signal steps can only resume.
  Suspended runs are never adopted — the scanners resume them when due.
- **Errors.** A failing sweep, adoption or storage check is reported to
  `onError({ source })` and backed off; nothing there stops the coordinator.

```typescript
import { InMemoryWorkflowStorage } from "@promin/workflow";
import {
  coordinatorLeaderKey,
  createDistributedWorkflowRunner,
  InMemoryStepQueue,
  InMemoryWorkerRegistry,
} from "@promin/workflow/distributed";
import { InMemoryLeaderLeases, LeaseLeaderElection } from "@promin/workflow/scheduler";

const leases = new InMemoryLeaderLeases(); // PgLeaderLeaseStore / RedisLeaderLeaseStore / SqliteLeaderLeaseStore in production
const coordinator = createDistributedWorkflowRunner({
  storage: new InMemoryWorkflowStorage(),
  stepQueue: new InMemoryStepQueue({ leaderLeases: leases }),
  workerRegistry: new InMemoryWorkerRegistry(),
  leaderElection: new LeaseLeaderElection({
    store: leases,
    key: coordinatorLeaderKey({ namespace: "prod" }),
    instanceId: "coordinator-a",
    ttlMs: 10_000,
  }),
  pollIntervalMs: 1_000,
  workerTimeoutMs: 30_000,
  stepWaitTimeoutMs: 6 * 60 * 60 * 1000,
  onError: (event) => console.error(event.source, event.error),
});
void coordinator.startLoop();
```

## Scanners

`createSleepScanner` resumes runs whose sleep (or signal deadline) is due;
`createSignalScanner` resumes runs whose awaited signal has been delivered.
They query storage with keyset paging (`listDueTimers`,
`listSignalWakeups`), resume with bounded concurrency
(`resumeConcurrency`, default 10), skip runs they are already resuming,
report a workflow name `resolveWorkflow` doesn't know once, and back off on
storage errors. With several instances, gate each scanner with a
`LeaseLeaderElection` on `scannerLeaderKey({ scanner: "sleep" | "signal", namespace })`;
without one every instance scans (safe — the run lock admits one driver —
but wasteful).

```typescript
import { createWorkflowRunner, InMemoryWorkflowStorage, workflow } from "@promin/workflow";
import { createSignalScanner, createSleepScanner } from "@promin/workflow/distributed";
import { succeed } from "@spilne/perfect-core";

const storage = new InMemoryWorkflowStorage();
const runner = createWorkflowRunner({ storage });
const reminder = workflow<{ userId: string }>({ name: "reminder" })
  .sleep("wait", 3_600_000)
  .step("remind", ({ input }) => succeed(input.userId))
  .build();
const definitions = new Map([[reminder.name, reminder]]);

const sleeps = createSleepScanner({
  storage,
  runner,
  resolveWorkflow: (name) => definitions.get(name),
  scanIntervalMs: 5_000,
});
const signals = createSignalScanner({
  storage,
  runner,
  resolveWorkflow: (name) => definitions.get(name),
});
void sleeps.start();
void signals.start();
// on shutdown:
await Promise.all([sleeps.stop(), signals.stop()]);
```

The signal scanner is also what wakes a parent parked on a child workflow:
the child's end is delivered as a reserved signal (see
[child wake-up](../durable/README.md#subworkflows-and-child-wake-up)).

## Time

Workers, the coordinator, its step waits and result poller, the scanners,
the worker registry, the start queue and every poll loop run on the
injected `clock` (`WallClock`). Tests pass one `FakeWallClock` to all of
them and drive heartbeats, stale sweeps and scans with `clock.advance(ms)`,
waiting on `clock.pendingCount()` or an observable condition before each
advance — never on a fixed real-time sleep.

## Also here

- `InMemoryWorkerRegistry` (and the Postgres / SQLite / remote registries):
  worker rows with heartbeats, `detectDead`, `gc`.
- Workflow advertisements and the start queue
  (`InMemoryWorkflowAdvertisementRegistry`, `InMemoryWorkflowStartQueue`, and
  their Postgres / SQLite versions): workers advertise the workflows they can
  run; starts are claimed with a `claimToken`, kept alive with
  `heartbeat({ id, claimToken })` and completed with a fenced
  `complete({ id, claimToken })`, so a stale worker can't delete the new
  claimant's record.
- `StepQueueExecutor` / `RoutingStepExecutor`: queue-backed steps from an
  in-process runner.
