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

## Entry points

| Import                         | What                                                                                                                                                                                                                                                        |
| ------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `@promin/workflow`             | `workflow()`, `flow()`, `createWorkflowRunner`, recovery, `trigger` / `webhookTrigger`, step executors, signals and streams, state machines, the `WorkflowStorage` contract and capabilities, errors, in-memory storage and version registry, `WallClock`   |
| `@promin/workflow/distributed` | `createDistributedWorkflowRunner`, `createWorker`, `MapStepRegistry`, the `StepQueue` contract and `InMemoryStepQueue`, worker registry, leader election keys, sleep / signal scanners, worker middleware, `StepQueueExecutor`, advertisements, start queue |
| `@promin/workflow/scheduler`   | `InMemoryScheduler`, `DurableScheduler`, `SchedulerStorage`, leader leases (`LeaseLeaderElection`, `InMemoryLeaderLeases`), schedule math, `validateScheduleConfig`                                                                                         |
| `@promin/workflow/discovery`   | `WorkflowScanner`, `ScheduleScanner`, `applyDiscoveredSchedules` (needs a Node-compatible runtime)                                                                                                                                                          |
| `@promin/workflow/sql-models`  | `compileSqlProject`: dbt-style SQL model DAGs                                                                                                                                                                                                               |
| `@promin/workflow/storage-kit` | helpers for storage backend authors                                                                                                                                                                                                                         |
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
