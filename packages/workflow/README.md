# @promin/workflow

Durable workflows for TypeScript: typed DAG steps on [perfect](https://github.com/spilne/perfect) `Eff`, checkpoint and resume, journaled steps, durable sleeps and signals, child workflows, sagas, distributed workers, schedulers and state machines. Storage is pluggable: in-memory here, Postgres, Redis, SQLite and HTTP in their own packages.

## Install

```bash
bun add @promin/workflow @spilne/perfect-core
```

## Quick start

```typescript
import { createWorkflowRunner, InMemoryWorkflowStorage, workflow } from "@promin/workflow";
import { succeed, TaggedError, tryPromise } from "@spilne/perfect-core";

class HttpError extends TaggedError("HttpError")<{ message: string }>() {}

const onboard = workflow<{ userId: string }>({ name: "onboard-user" })
  .step(
    "fetch",
    ({ input }) =>
      tryPromise(
        () =>
          fetch(`https://api.example.com/users/${input.userId}`).then(
            (r) => r.json() as Promise<{ name: string }>,
          ),
        (e) => new HttpError({ message: String(e) }),
      ),
    { retry: { maxRetries: 3 } },
  )
  .step("greet", ({ prev }) => succeed(`Welcome ${prev.name}!`))
  .build(); // Workflow<{ userId: string }, string, HttpError>

const runner = createWorkflowRunner({ storage: new InMemoryWorkflowStorage() });
const { data, error } = await runner.runSafe({
  workflow: onboard,
  workflowId: "onboard-u42",
  input: { userId: "u42" },
});
```

Every step result is checkpointed, so running the same `workflowId` again
after a crash resumes from the last completed step, and a completed run is
answered from storage instead of running twice. For durable state, replace
`InMemoryWorkflowStorage` with `PostgresWorkflowStorage`
([`@promin/postgres`](../postgres/README.md)), `RedisWorkflowStorage`
([`@promin/redis`](../redis/README.md)), `SqliteWorkflowStorage`
([`@promin/sqlite`](../sqlite/README.md)) or `RemoteWorkflowStorage`
([`@promin/workflow-remote`](../workflow-remote/README.md)).

## Fluent authoring

A step function returns an `Eff` (typed failures, retried per policy), a
Promise or a plain value, so plain async code needs no wrapping. Reusable
runs of steps are fragments, and a bound workflow carries its runner:

```typescript
import { createWorkflowApp, InMemoryWorkflowStorage, step, workflow } from "@promin/workflow";

declare const profiles: { lookup(name: string): Promise<{ email: string }> };
declare const permissions: { lookup(name: string): Promise<string[]> };

// A fragment: annotate the first step's context, chain with `.andThen()`.
const normalizeUser = step("trim", ({ input }: { input: { name: string } }) =>
  input.name.trim(),
).andThen("upper", async ({ prev }) => prev.toUpperCase());

const onboardUser = workflow<{ name: string }>({ name: "onboard-user" })
  .use(normalizeUser) // adds "trim" and "upper"
  .parallel("enrich", {
    profile: ({ prev }) => profiles.lookup(prev),
    permissions: ({ prev }) => permissions.lookup(prev),
  })
  .approval("manager") // waits for the "manager-approved" signal: { approved: boolean }
  .step("finish", ({ prev }) => ({ approved: prev.approved }), {
    timeoutMs: 30_000,
    retry: { maxRetries: 3, baseDelayMs: 500 },
  })
  .build();

const app = createWorkflowApp({ storage: new InMemoryWorkflowStorage() });
const onboard = app.workflow(onboardUser); // or bindWorkflow({ runner, workflow })
await onboard.start({ workflowId: "onboard-1", input: { name: " Ada " } });
await onboard
  .handle("onboard-1")
  .signal({ signalName: "manager-approved", payload: { approved: true } });
```

- Every step body (linear, DAG and `mapOver` element) receives `ctx.signal`,
  one `AbortSignal` per attempt. It aborts when the attempt times out
  (`timeoutMs`, reason `StepTimeoutError`) or fails (so a retry does not
  overlap leftovers), and with the run's own signal: a lost lock, or the
  `signal` passed to `runner.run`. Pass it to `fetch` or an SDK call to stop
  in-flight work; a body that ignores it behaves as before. A cancel made
  through storage (`cancelWorkflow`) is seen mid-step too: each lock heartbeat
  (default 30s) checks the run's status (piggybacked on the extension when the
  storage's `heartbeat` returns it, otherwise one `loadWorkflowStatus` read per
  beat) and aborts the signal with a `WorkflowCancelledError`, so a cancel
  takes effect within one beat.
- `compensate: { onCancel: "rollback" }` rolls the completed steps back when
  the run is cancelled (same order, retries and durable ledger as a failure
  rollback, `onComplete` fired), then the run ends cancelled, not failed.
  Default `"none"`. The driver that sees the cancel reopens the run as
  `compensating` (with a ledger-capable storage) and rolls back from there, so
  the rollback is crash-safe like a failure's: if the driver crashes or loses
  the lock, recovery (or the next driver) finishes it, skips the steps already
  rolled back, and the run ends cancelled. A run cancelled while no driver
  holds it (e.g. suspended) is not rolled back: the decision needs the
  workflow definition, which `cancelWorkflow` does not have. Without a ledger
  the rollback still runs but is not resumable.
- A step that never settles holds its run forever unless it has a timeout:
  set `defaultStepTimeoutMs` on `workflow()` or `createWorkflowRunner` (a
  step's `timeoutMs` wins, `timeoutMs: null` opts out) and `lock.maxHoldMs` to
  cap how long one driver holds a run. Both are off by default and
  recommended in production; see the durable README.
- `.parallel(name, branches)` forks the head into one DAG step per branch
  and joins them into `{ [branch]: result }`;
  `.parallel(name, { dependsOn }, branches)` forks from named steps.
- `.approval(name, { signalName?, timeoutMs?, dependsOn? })` is
  `.waitForSignal()` with the defaults for a human decision.
- `use(fragment)` checks at compile time that the workflow's input and head
  value fit the fragment, and adds exactly the fragment's step names.
- `bindWorkflow` / `createWorkflowApp().workflow()` give `run`, `runSafe`,
  `start`, `resume` and `handle` typed by the workflow's input and output.
- The JSON `WorkflowSchema` that `compileWorkflow` compiles has the same node
  kinds: `step`, `map`, `sleep`, `signal`, `approval`, `branch`, `parallel`.

## Tracing

`tracer` on `createWorkflowRunner`, `createDistributedWorkflowRunner` and
`createWorker` receives span-shaped lifecycle events: run start / end (outcome,
and the `WorkflowFailure` of a failed run), step attempts (attempt, duration,
outcome, whether a retry follows), sleeps and signal waits, child workflows (a
child's `runStart` names its `parentWorkflowId`), compensation (start, each
`compensate` attempt, end), run lock acquired / lost, `recover()` reclaims, and
`update` (a journaled step applied, failed or rejected an update; inline
execution only).
Every callback is optional and can never change a run: one that throws or
rejects is reported to `onTracerError` (default `console.error`). Without a
tracer the engine builds no events.

```ts
import * as api from "@opentelemetry/api";
import { createWorkflowRunner } from "@promin/workflow";
import { createOpenTelemetryWorkflowTracer } from "@promin/workflow/otel";

const tracer = createOpenTelemetryWorkflowTracer({
  tracer: api.trace.getTracer("my-app"),
  api, // builds child contexts and extracts `traceparent`
});

const runner = createWorkflowRunner({ storage, tracer });
await runner.run({
  workflow,
  workflowId,
  input,
  traceparent: req.headers.get("traceparent") ?? undefined,
});
```

The adapter makes one root span per orchestration of a run with a child span per
step attempt, wait and compensation, and a child workflow's root under its
parent's. Spans carry an allowlist of ids, names, tags, counts and durations;
inputs, results and error messages never reach them, and failures set `ERROR`
status with the error tag. Workflow, namespace, executor, worker and task ids are
hashed (SHA-256 prefix, optional `identifierSalt`) unless `includeIdentifiers: true`.
`filterAttribute` drops or rewrites any attribute and `parentContext` picks a root
span's parent.

Trace context crosses process boundaries as an opaque W3C `traceparent`:

- `run({ traceparent })` is passed to `runStart`; a run created by that call
  keeps it in `metadata.traceparent`, so a resume after a sleep or signal wait
  (another process, hours later) joins the same trace.
- The distributed runner stores the step attempt's `traceparent` in
  `StepTask.metadata.traceparent` (an existing slot, no schema change). A worker
  given the same tracer reports `workerTaskStart` / `workerTaskEnd` with it, and
  the adapter parents the worker's span under the attempt. `injectTraceparent:
false` turns the hand-off off.

Limits: step attempts are reported when the step settles (each event carries its
own start time, so spans are accurate, but a retried attempt's end arrives after
the next attempt's start); a run resumed without a `traceparent` starts a new
trace; a failed run always opens a `workflow.compensation` span, empty when no
step had a `compensate`.

## Entry points

| Import                         | What                                                                                                                                                                                                                                                        |
| ------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `@promin/workflow`             | `workflow()`, `flow()`, `createWorkflowRunner`, recovery, `trigger` / `webhookTrigger`, step executors, signals and streams, state machines, the `WorkflowStorage` contract and capabilities, errors, in-memory storage and version registry, `WallClock`   |
| `@promin/workflow/distributed` | `createDistributedWorkflowRunner`, `createWorker`, `MapStepRegistry`, the `StepQueue` contract and `InMemoryStepQueue`, worker registry, leader election keys, sleep / signal scanners, worker middleware, `StepQueueExecutor`, advertisements, start queue |
| `@promin/workflow/scheduler`   | `InMemoryScheduler`, `DurableScheduler`, `SchedulerStorage`, leader leases (`LeaseLeaderElection`, `InMemoryLeaderLeases`), schedule math, `validateScheduleConfig`                                                                                         |
| `@promin/workflow/discovery`   | `WorkflowScanner`, `ScheduleScanner`, `applyDiscoveredSchedules` (needs a Node-compatible runtime)                                                                                                                                                          |
| `@promin/workflow/sql-models`  | `compileSqlProject`: dbt-style SQL model DAGs                                                                                                                                                                                                               |
| `@promin/workflow/storage-kit` | helpers for storage backend authors                                                                                                                                                                                                                         |
| `@promin/workflow/otel`        | `createOpenTelemetryWorkflowTracer`: spans for runs, step attempts, waits, compensation and queue workers (structural types, no OpenTelemetry dependency)                                                                                                   |
| `@promin/workflow/testing`     | conformance suites (`storageTestSuite`, `stepQueueTestSuite`, `zombieWorkerTestSuite`, …) for `bun:test`                                                                                                                                                    |
| `@promin/workflow/dev`         | non-determinism instrumentation for journaled bodies                                                                                                                                                                                                        |

The root entry loads in any JavaScript runtime: nothing reachable from it
imports a Node built-in or `bun:test`.

## Guides

- **[Durable execution](./src/lib/durable/README.md)** — the builder (`step`, `mapOver`, `parallel`, `branch`, `match`, loops, `journaled`, `sleep`, `waitForSignal`, `subworkflow`), step options, retry defaults, typed errors, the run lifecycle, compensation, idempotency, recovery, `WallClock`.
- **[Storage](./src/lib/durable/storage/README.md)** — the storage contract, capabilities per backend, fencing, signals, writing a backend.
- **[Distributed execution](./src/lib/distributed/README.md)** — coordinator, workers, step queue, delivery guarantees, scanners.
- **[Scheduler](./src/lib/scheduler/README.md)** — cron / RRULE / interval schedules, at-least-once ticks, fenced leader leases, partitions.
- **[SQL models](./src/lib/sql-models/README.md)** — SQL transformation DAGs.
- **[Versioning](./versioning.md)** — strict / drain / `ctx.patched()` / rolling worker deploys, with runnable examples in [`examples/versioning/`](./examples/versioning).

## Guarantees in one place

- **Steps are at-least-once.** A step body whose result was not checkpointed
  (crash, `CheckpointError`, lost lock, redelivered queue task) runs again.
  `mapOver` elements are at-least-once per element. Journaled activities
  journal their result, so a completed activity (that no compensation rolled
  back) never runs twice; one
  interrupted mid-flight fails with `AmbiguousActivityOutcome` unless it is
  marked `idempotent`.
- **Writes are fenced.** Every write a run's driver makes carries its lock's
  fence token and is checked atomically with the write; an expired lock
  fences nothing.
- **Cancel wins** over a late completion; an ended run is never executed
  again unless `force: true`.
- **Schedule ticks are at-least-once** with a stable `tickNumber`, so run ids
  derived from the tick deduplicate.
- **All time goes through `WallClock`** (`SystemWallClock` by default,
  `FakeWallClock` in tests).
