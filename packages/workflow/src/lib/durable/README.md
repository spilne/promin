# Durable Execution

A workflow is a DAG of named steps. Each step's result is checkpointed to a
`WorkflowStorage`, so a run that crashes, is cancelled mid-way or is retried
resumes from what is stored instead of starting over. Workflows are plain
data: `workflow()` builds a `Workflow<Input, Output, E>`, and a
`WorkflowRunner` runs it against a storage.

```typescript
import { createWorkflowRunner, InMemoryWorkflowStorage, workflow } from "@promin/workflow";
import { succeed } from "@spilne/perfect-core";

const onboard = workflow<{ userId: string }>({ name: "onboard-user" })
  .step("fetch", ({ input }) => succeed({ id: input.userId, email: `${input.userId}@example.com` }))
  .step("provision", async ({ prev }) => ({ accountId: `acct-${prev.id}` }))
  .step("welcome", ({ prev }) => succeed(`welcome ${prev.accountId}`))
  .build();

const runner = createWorkflowRunner({ storage: new InMemoryWorkflowStorage() });
const message = await runner.run({
  workflow: onboard,
  workflowId: "onboard-u42",
  input: { userId: "u42" },
});
```

`message` is typed as `string`: the runner's result is the last step's output.
Swap `InMemoryWorkflowStorage` for `PostgresWorkflowStorage`,
`RedisWorkflowStorage`, `SqliteWorkflowStorage` or `RemoteWorkflowStorage` to
make it survive a process restart (see [Storage](./storage/README.md)).

For one-shot, non-durable use (scripts, request handlers) `flow()` builds the
same chain and `.execute(input)` runs it on a throwaway in-memory storage:

```typescript
import { flow } from "@promin/workflow";
import { succeed } from "@spilne/perfect-core";

const total = await flow<{ prices: number[] }>("sum")
  .step("sum", ({ input }) => succeed(input.prices.reduce((a, b) => a + b, 0)))
  .execute({ prices: [1, 2, 3] });
```

## Step bodies

A step function (`StepBody`) returns one of:

- a perfect `Eff<A, Throws<E>>` (`StepEff<A, E>`). The typed failure `E` must
  be a tagged error; it is what `retry` and `onFailure` act on, and it is
  collected into the workflow's error type.
- a `Promise<A>` (an `async` function). A rejection is a **defect**: it fails
  the step without retry or `onFailure`. A Promise that resolves to an `Eff`
  runs that `Eff`, so its typed failures stay typed.
- a plain value `A`.

A synchronous throw is a defect too. Every step kind that takes a function
accepts the same three shapes:

| Method                             | Body                         | Failure                                                        |
| ---------------------------------- | ---------------------------- | -------------------------------------------------------------- |
| `.step(name, fn, options?)`        | `StepBody`                   | typed `E` from an `Eff`; a throw or rejection is a defect      |
| `.mapOver`                         | `StepBody` per element       | as above, per element                                          |
| `.dowhile` / `.dountil`            | `StepBody` per iteration     | as above, per iteration                                        |
| `.branch` / `.match` / `.parallel` | `StepBody` per branch / case | as above; a throw from `condition` / `on` / `when` is a defect |

```typescript
import { createWorkflowRunner, InMemoryWorkflowStorage, workflow } from "@promin/workflow";
import { TaggedError, tryPromise } from "@spilne/perfect-core";

class PaymentDeclined extends TaggedError("PaymentDeclined")<{
  message: string;
  retryable: boolean;
}>() {}

declare function chargeCard(orderId: string): Promise<{ chargeId: string }>;
declare function sendReceipt(chargeId: string): Promise<void>;

const checkout = workflow<{ orderId: string }>({ name: "checkout" })
  .step(
    "charge",
    ({ input }) =>
      tryPromise(
        () => chargeCard(input.orderId),
        (e) => new PaymentDeclined({ message: String(e), retryable: true }),
      ),
    { retry: { maxRetries: 3, when: (e) => e._tag === "PaymentDeclined" } },
  )
  .step("receipt", async ({ prev }) => {
    await sendReceipt(prev.chargeId);
    return prev.chargeId;
  })
  .build();

const runner = createWorkflowRunner({ storage: new InMemoryWorkflowStorage() });
const { data, error } = await runner.runSafe({
  workflow: checkout,
  workflowId: "order-1",
  input: { orderId: "1" },
});
if (error instanceof PaymentDeclined) console.log("declined", error.message);
else console.log("charged", data);
```

## Composing steps

### Linear and DAG steps

A step without `dependsOn` follows the previous step and receives its result
as `prev`. A step with `dependsOn` receives the named results as `deps`
(its `prev` for option callbacks is the first dependency). Steps whose
dependencies are all done run in the same wave, concurrently.

```typescript
import { workflow } from "@promin/workflow";
import { succeed } from "@spilne/perfect-core";

const analyze = workflow<{ text: string }>({ name: "analyze" })
  .step("parse", ({ input }) => succeed(input.text.split(" ")))
  .step("count", { dependsOn: ["parse"] }, ({ deps }) => succeed(deps.parse.length))
  .step("longest", { dependsOn: ["parse"] }, ({ deps }) =>
    succeed(deps.parse.reduce((a, b) => (b.length > a.length ? b : a), "")),
  )
  .step("report", { dependsOn: ["count", "longest"] }, ({ deps }) =>
    succeed({ words: deps.count, longest: deps.longest }),
  )
  .build();
```

`dependsOn` names are checked at compile time. `.map(fn)` adds a pure
transform step `"<head>.map"`; the head step keeps its own (unmapped) row,
codec and options.

### mapOver — fan-out with per-element resume

```typescript
import { workflow } from "@promin/workflow";
import { succeed, tryPromise, TaggedError } from "@spilne/perfect-core";

class FetchError extends TaggedError("FetchError")<{ message: string }>() {}

const crawl = workflow<{ urls: string[] }>({ name: "crawl" })
  .step("urls", ({ input }) => succeed(input.urls))
  .mapOver(
    "fetch-all",
    { array: "urls", concurrency: 5 },
    (url) =>
      tryPromise(
        () => fetch(url).then((r) => r.status),
        (e) => new FetchError({ message: String(e) }),
      ),
    {
      element: { retry: { maxRetries: 2 }, timeoutMs: 5_000 }, // each element on its own
      retry: { maxRetries: 1 }, // the map step as a whole
    },
  )
  .build();
```

Each finished element is saved as a task row (encoded with `element.codec`).
When the map step runs again — a step retry, a workflow retry, a resume after
a crash — only the elements without a saved result run. Element bodies are
therefore at-least-once **per element**: an element that finished but was not
yet saved when the process died runs again. A typed element failure past
`element.retry` is written as a failed task row and fails the map step. The
step-level options (`codec`, `onFailure`, `skipValue`, `compensate`, `cache`)
apply to the `T[]` result.

### parallel, branch, match

```typescript
import { workflow } from "@promin/workflow";
import { succeed } from "@spilne/perfect-core";

const route = workflow<{ id: string; total: number; kind: "express" | "standard" }>({
  name: "route",
})
  .parallel(
    "enrich",
    {
      user: ({ input }) => succeed({ userId: input.id }),
      risk: ({ input }) => succeed(input.total > 1_000 ? 0.8 : 0.1),
    },
    { retry: { maxRetries: 2 }, branches: { risk: { onFailure: { fallback: () => 0.5 } } } },
  )
  .branch("review", {
    condition: (e) => e.risk > 0.5,
    ifTrue: ({ prev }) => succeed({ ...prev, reviewed: true }),
    ifFalse: ({ prev }) => succeed({ ...prev, reviewed: false }),
  })
  .match("ship", {
    on: (order) => (order.reviewed ? "manual" : "auto"),
    cases: {
      manual: () => succeed("queued for review"),
      auto: () => succeed("shipped"),
    },
  })
  .build();
```

- `parallel` forks into one DAG step per branch (`"enrich.user"`,
  `"enrich.risk"`), each retried, cached and distributed on its own, then
  joins them into a keyed record. Block-level `timeoutMs` / `retry` /
  `needs` / `priority` / `queue` / `cache` are defaults for every branch;
  `branches` sets per-branch options; `codec` encodes the joined record.
  `.parallel(name, { dependsOn }, branches)` forks from the named steps
  instead of the head. (`ctx.parallel()` inside a `.journaled()` body is a
  different thing: in-process activities within one step.)
- `branch` and `match` are one DAG node each. `.branch(name, { dependsOn,
condition, ifTrue, ifFalse })` runs after the named steps. A selector with no matching case
  and no `default` fails with the typed `MatchError`. A throw from
  `condition`, `on` or `when` is a defect.

### Loops

```typescript
import { workflow } from "@promin/workflow";
import { succeed } from "@spilne/perfect-core";

declare function pollStatus(jobId: string): Promise<"pending" | "ready">;

const wait = workflow<{ jobId: string }>({ name: "wait-ready" })
  .dowhile(
    "drain",
    (_ctx, iter) => succeed(iter * 10),
    (processed) => processed < 30,
  )
  .dountil(
    "ready",
    ({ input }) => pollStatus(input.jobId),
    (s) => s === "ready",
    {
      maxIterations: 60,
    },
  )
  .build();
```

Each iteration is its own step row (`"<name>.iter.<n>"`); a resumed loop
replays the stored iterations and continues with the first missing one.
Exceeding `maxIterations` (default 100) fails with `LoopLimitExceededError`.

### guard and tripwire

`.guard(name, predicate)` fails fast with `GuardError` (no retry).
`.tripwire(name, { when, reason })` ends the run with status `tripwire` — a
deliberate short-circuit, not a failure. `run()` rejects with
`WorkflowTripwireError` and `handle.status()` reports the stored `reason`.

## Step options

Every step kind accepts the options it can honour, and an option it cannot
honour is a compile error.

| Option                       | step / branch / match | mapOver             | parallel                     | journaled | subworkflow | loops | tripwire |
| ---------------------------- | --------------------- | ------------------- | ---------------------------- | --------- | ----------- | ----- | -------- |
| `codec`                      | yes                   | array (+ `element`) | joined record (+ per branch) | yes       | yes         | yes   | yes      |
| `retry`, `timeoutMs`         | yes                   | yes (+ `element`)   | default + per branch         | `retry`   | `retry`     | yes   | –        |
| `onFailure`, `compensate`    | yes                   | yes                 | per branch                   | yes       | yes         | yes   | –        |
| `skipWhen`, `skipValue`      | yes                   | yes                 | per branch                   | yes       | yes         | yes   | –        |
| `needs`, `priority`, `queue` | yes                   | yes                 | default + per branch         | yes       | yes         | yes   | –        |
| `cache`                      | yes                   | yes                 | default + per branch         | –         | –           | –     | –        |

- `timeoutMs` is per attempt: the attempt is interrupted and fails with the
  typed `StepTimeoutError`, so `retry` / `onFailure` see it. The timer runs on
  the runner's `WallClock`.
- **Hung handlers.** A step without `timeoutMs` that never settles keeps the
  run locked and heartbeating forever, so recovery never reclaims it. Set
  `defaultStepTimeoutMs` on `workflow({ ... })` or on the runner to time out
  every plain, `branch`, `match`, `map` and `parallel` step that sets no
  `timeoutMs` (sleeps, signal waits, children, journaled steps and loops are
  skipped). Precedence: step `timeoutMs`, then the workflow default, then the
  runner default; `timeoutMs: null` on a step, or `defaultStepTimeoutMs: null`
  on a workflow, opts out. It is off by default for backward compatibility;
  **setting it on the runner (with `lock.maxHoldMs`, below) is recommended
  for production.** The default is per attempt and covers the step as a
  whole, so size it for the slowest legitimate step, or opt that one out.
- `onFailure`: `"fail"` (default), `"skip"` (continue with `undefined`) or
  `{ fallback: (error) => value }`. A skipped or fallen-back step is not
  compensated.
- `retry` and `onFailure` never act on engine control flow: suspension,
  continue-as-new, tripwire, non-determinism, an ambiguous activity outcome or
  a lost lock pass straight through.
- `cache` keys entries as `${namespace}:${stepName}:${key(ctx)}`; a hit skips
  the body. Cache errors fall through to a miss.
- `needs`, `priority` and `queue` (concurrency key) only matter when a
  `StepQueueExecutor` dispatches the step to workers. A dispatched step's
  `retry`, `timeoutMs` and `onFailure` come from the worker's registration
  instead (see [Distributed](../distributed/README.md#what-runs-where)).

## Retry defaults

Every retry in the package — step `retry`, `element.retry`, workflow `retry`,
activity `retry`, `compensate.retry`, checkpoint writes, the state machine's
retry middleware and the distributed worker — uses one policy
(`RetryPolicy`, defaults in `RETRY_POLICY_DEFAULTS`):

| Field          | Default                                                  |
| -------------- | -------------------------------------------------------- |
| `maxRetries`   | 3                                                        |
| `baseDelayMs`  | 250, doubling per retry (`baseDelayMs * 2^retry`)        |
| `maxDelayMs`   | no cap (`0` also means no cap)                           |
| `jitter`       | off; `true` spreads each delay over `delay × (1 ± 0.25)` |
| `timeBudgetMs` | none; measured from the **first failure**                |
| `when`         | every typed failure                                      |

Backoff waits run on the injected `WallClock`. The workflow-level retry and
the compensation retry only exist once a policy is set; the workflow-level
retry never retries control flow, a spent deadline (`WorkflowDeadlineError`)
or a cancel, and retries defects only with `retryDefects: true`.

## Running workflows

`createWorkflowRunner({ storage, clock?, registry?, hooks?, stepExecutor?, executorId?, lock?, defaultStepTimeoutMs? })`
returns a `WorkflowRunner`:

| Method                                         | What it does                                                                               |
| ---------------------------------------------- | ------------------------------------------------------------------------------------------ |
| `run({ workflow, workflowId, input })`         | runs to the end; resolves with the output, rejects with `WorkflowRunError<E>`              |
| `runSafe(...)`                                 | same, as `{ data, error }`                                                                 |
| `start(...)`                                   | resolves once the run holds its lock; returns a `WorkflowHandle<Output, E>`                |
| `handle(workflowId)`                           | a handle for a run driven elsewhere (`status`, `signal`, `result`, `cancel`, `events`)     |
| `resume({ workflow, workflowId, fromStep })`   | rewinds a run to `fromStep` (and everything downstream) and continues                      |
| `subscribe({ workflowId })` / `getStatus(...)` | live events (push when the storage has `runEvents`, polling otherwise) / a status snapshot |
| `recover(strategy)`                            | startup sweep: terminate stale runs, resume orphaned ones (needs a `registry`)             |

`run({ name, version?, ... })` resolves the definition through the runner's
`registry` instead. `namespace`, `idempotencyKey` / `idempotencyKeyTTL` and
`force` are accepted by every form.

### Typed errors

`build()` returns `Workflow<Input, Output, E>`, where `E` is the union of the
typed failures the steps declared plus the kinds' own errors (`GuardError`,
`MatchError`, `LoopLimitExceededError`, `StepError` for a failed child, …).
Suspension is never part of `E`. `run()` rejects, and `runSafe()` returns as
`error`, a `WorkflowRunError<E>` — one of `E` (the step's error, not a
wrapper), an engine error, or a defect. Read `E` with `WorkflowErrorOf<typeof wf>`.

A run may execute in another process, so `handle.result()` reads the failure
back from storage: it rejects with `WorkflowFailedError` whose `errorTag` is
the `_tag` of the error that failed the run, or with `WorkflowCancelledError`
/ `WorkflowTripwireError`.

#### One failure shape: `WorkflowFailure`

Every surface reports a failed run as the same `WorkflowFailure`
(`{ tag, message, stepName?, attempt?, cause? }`):

| Surface                              | How                                                                     |
| ------------------------------------ | ----------------------------------------------------------------------- |
| `runSafe()` result                   | `failure` beside `error` (the typed error stays `error`)                |
| `onWorkflowFailure` hook, DLQ record | `failure` beside the `error` string                                     |
| `handle.status()` / `getStatus()`    | `failure` beside `error` / `errorTag`                                   |
| Stored `WorkflowState` (any backend) | `workflowFailure(state)`: derived from `error`, `errorTag`, failed step |
| `handle.result()`                    | `WorkflowFailedError` with `errorTag`, `stepName`, `attempt`            |

### Budgets and usage

A `.journaled()` step reports what it spent, and the workflow caps it:

```ts
const wf = workflow({
  name: "research",
  budget: {
    maxTokens: 200_000,
    maxCostUsd: 2,
    perStep: { maxCostUsd: 1 },
    steps: { draft: { maxCostUsd: 1.5 } },
  },
})
  .journaled("draft", function* (ctx) {
    const reply = yield* ctx.activity("llm", () => model.complete(ctx.input.prompt));
    yield* ctx.reportUsage({ inputTokens: reply.in, outputTokens: reply.out, costUsd: reply.cost });
    return reply.text;
  })
  .build();
```

- `ctx.reportUsage` writes a journal entry (activity `__usage`), so a retry, replay or crash recovery
  re-reads it and never counts it twice. It returns the step's running total. Fields default to 0 and must be
  finite and >= 0 (`RangeError` otherwise). Spend of an activity that failed cannot be reported.
- When a step settles (completed or failed) its total is stored in its row's `metadata.usage`, so usage is
  derived from stored rows on every backend: no column, no migration. `handle.status()` / `getStatus()` carry
  `usage` (run total) and `steps[name].usage`; `runUsage(state.steps)` / `stepUsage(step)` do the same from a
  stored `WorkflowState`. A step suspended on a sleep or signal has not stored its usage yet (it is re-derived
  from the journal when the step resumes).
- Past a cap (strictly above it) the run fails with `WorkflowBudgetExceededError` (`scope`, `limit`, `max`,
  `used`, `stepName?`): the failure tag is `WorkflowBudgetExceededError`, compensation runs as for any failure,
  and neither step `retry`, workflow `retry` nor `onFailure` (`skip` / `fallback`) absorbs it. It is checked at
  each report (the run total is the step's own plus the other steps' stored usage) and after every wave of
  steps, and once more when a run resumes, so lowering a budget stops a suspended run at its next orchestration.
- The tracer's `stepAttemptEnd` (the attempt that settled the step) and `runEnd` carry an additive `usage`.
- Limits: the budget lives on the definition (not stored with the run, so there is no per-run override); a child
  workflow has its own budget and its usage is not rolled up into the parent; steps running in parallel on a
  worker see each other's usage only after the wave.

### What a run is waiting on

`handle.status()` / `getStatus()` carry `waitingOn`: what the live run is
parked on or busy with, derived from the stored run (no extra read). Each is
`{ kind, stepName, since, until?, signalName?, childWorkflowId?, groupName?, reason?, holder?, attempt? }`
with `kind` one of `sleep` (`until` = wake time), `signal` (`until` = timeout),
`approval` (an `approve:<id>` signal wait), `child` (`until` = the child's next
wake), `activity` (a step body is executing: `attempt`, `since` = when the
attempt began, `holder` = the worker id, or the runner's `executorId`, when known),
`retry-backoff` (attempt `attempt` failed and the next one starts at `until`)
or `step-queue` (a queued step its rate-limit / concurrency group holds back:
`groupName`, `reason`, `until` = when the rate window frees a slot; read from the
step executor / queue, not a step row, so only `getStatus` on a coordinator over a
queue that implements `throttled` reports it — see
[rate-limit groups](../distributed/README.md#rate-limit-and-concurrency-groups-with-fairness)). `waitingOnSummary` is a one-liner, e.g. `waiting for signal
manager-approved (step approve, times out in 2h)`. Empty for a run with nothing
parked and for ended runs.

`activity` and `retry-backoff` come from the optional storage capability
`stepProgress` (`saveStepProgress`, in memory, Postgres, SQLite, Redis and
`workflow-remote`): the runner records, per step, that an attempt started
(on a queue executor: when a worker claimed the task, with its id) or that a
failed attempt is waiting for its retry. An attempt is recorded once it has
run for a second on the run's clock (`STEP_PROGRESS_DELAY_MS`), so a step that
settles sooner costs no write; a retry backoff is recorded at once. The records
are advisory: written in
the background (a failed write is dropped and never fails a step, and one is
settled before the step's own write), kept one per step and read with the
run, so a status call costs no extra read and stays bounded by the step count.
Postgres keeps them in `wf_workflows.step_progress` (jsonb), added to an
existing database by the incremental migration `0001_step_progress` (run by
`migrate()`); SQLite adds the same column to a table that lacks it on open.
A record counts only while it still describes the step: not for a settled,
earlier-run or parked step, and a failed or parked step shows it only when it
is newer than the failure or the park (a workflow retry, a re-execution after a
wake). Limits: a storage without the capability lists neither kind; only the
step body is covered, not a retry inside `ctx.activity`; a task queued for a
worker nobody has claimed yet shows nothing (apart from one held back by a step
group, above, which replaces an older record); a worker that dies mid-step
leaves its `activity` until the run is re-driven.
Zorya's `GET /api/runs/:id` returns the same as `waitingOn` (ISO timestamps) and
`waitingOnSummary`; the run page shows the summary beside the status badge and
one chip per wait.

`tag` is the failing error's `_tag` (`"WorkflowCancelledError"` for a cancel,
`"UnknownError"` for an untagged defect). `cause` is the original error and
exists only in-process (`runSafe`, hook, DLQ); a failure derived from storage
has none, since an error does not survive a storage round trip. Nothing new is
stored, so no backend or migration changes: `error` / `errorTag` stay as they
were. To turn a stored failure back into a typed error, call
`rehydrateFailure({ workflowId, failure, errors })`: a tag in `errors` (your
own typed errors) or a built-in engine error whose fields a failure holds
(`WorkflowCancelledError`, `WorkflowError`, `WorkflowTimeoutError`,
`StepError`, `ChildWorkflowGoneError`) gives that error, any other tag a
`WorkflowFailedError`. It is opt-in: `handle.result()` still rejects with
`WorkflowFailedError`.

### Run lifecycle

- **Terminal gate.** A run that already ended is not executed again.
  Re-running a `completed` run answers with its stored result; a `failed`,
  cancelled or `tripwire` run rejects with `WorkflowFailedError` (carrying the
  stored `errorTag`), `WorkflowCancelledError` or `WorkflowTripwireError`.
  `force: true` archives the ended run and starts a fresh one under the same
  id; `runner.resume({ fromStep })` re-drives it from one step.
- **Cancel wins.** `handle.cancel()` (or `storage.cancelWorkflow`) marks the
  run failed with `errorTag: "WorkflowCancelledError"`. The runner checks the
  status between waves (and in the same write as each step checkpoint where
  the storage has `stepCheckpoint`), stops, and rejects with
  `WorkflowCancelledError`: no retry, no compensation. Terminal writes are
  conditional, so a late completion never overwrites a cancel.
- **Deadline.** `workflow({ timeoutMs })` runs from the persisted start time,
  across sleeps, signal waits and resumes, and fails with
  `WorkflowDeadlineError`.
- **Locks.** A run is driven under a lease lock with a fence token (see
  [Storage](./storage/README.md#fencing)); a second driver gets
  `WorkflowLockError` (or joins the run, per `idempotency.onInFlight`). If
  heartbeats are fenced out or none succeeds for a lock duration, the run
  stops at the next wave with `WorkflowLockLostError`. The lease is
  `lock: { durationMs, heartbeatIntervalMs }` on the runner (default 120 s /
  30 s): a dead driver's run is free for another driver that long after its
  last heartbeat. `lock.maxHoldMs` (off by default) bounds how long one driver
  may hold a run: once reached, the heartbeat stops extending the lease and the
  run stops at its next safe point with `WorkflowLockLostError` whose `cause`
  is `WorkflowMaxHoldExceededError`; the lease then expires and recovery can
  reclaim the run even if a handler never settles (its fenced writes are
  rejected). Set it above your longest expected run: a legitimately long run is
  stopped the same way and re-driven from its checkpoints. `run({ signal })` abandons a run: once the signal aborts,
  it stops at its next safe point the same way, releases the lock and stays
  in flight for another driver.
- **Children held elsewhere.** A parent step whose child run is locked by
  another driver (a wake-up resumed the child on its own) parks on the
  child like on a suspended one; the child's end wakes it.
- **Checkpoint failures.** Checkpoint and terminal writes are retried (3
  retries, 250 ms base). One that still fails rejects with `CheckpointError`
  and the run stops as it stands — no compensation, no `failWorkflow` — so
  recovery picks it up later. The step whose result was not saved runs again:
  step bodies are **at-least-once**.
- **Hooks** (`onStepComplete`, `onStepFailure`, `onWorkflowComplete`,
  `onWorkflowFailure`, `onWorkflowTripwire`) are observers. A throwing hook is
  reported to `hooks.onHookError` (default `console.error`) and never changes
  the outcome.
- **Parallel failures.** Every step of a wave settles on its own. A failing
  step gets its own failed row; its siblings finish, stay completed (and are
  compensated if compensation runs) and are not re-run by a workflow retry.

### Idempotency and singleflight

```typescript
import { createWorkflowRunner, InMemoryWorkflowStorage, workflow } from "@promin/workflow";
import { succeed } from "@spilne/perfect-core";

const processOrder = workflow<{ orderId: string }>({ name: "process-order" })
  .step("charge", ({ input }) => succeed({ chargeId: `ch-${input.orderId}` }))
  .build({
    idempotency: {
      ttl: { success: 3_600_000, failure: 10_000 }, // reuse the outcome this long
      onInFlight: "join", // a concurrent start joins the running execution ("reject" throws WorkflowLockError)
      onExpiry: "fresh-run", // after the TTL: a fresh run ("replay" re-enters the stored run)
    },
  });

const runner = createWorkflowRunner({ storage: new InMemoryWorkflowStorage() });
await runner.run({ workflow: processOrder, workflowId: "order-42", input: { orderId: "42" } });
// Within the TTL: answered from storage, nothing re-executes.
await runner.run({ workflow: processOrder, workflowId: "order-42", input: { orderId: "42" } });
```

When the caller cannot derive a stable `workflowId`, pass `idempotencyKey`
(with `idempotencyKeyTTL`): a live `(namespace, workflow name, key)` match
redirects the call to that run.

### Recovery at startup

```typescript
import {
  createWorkflowRunner,
  InMemoryWorkflowStorage,
  InMemoryWorkflowVersionRegistry,
  RecoveryStrategy,
} from "@promin/workflow";

const registry = new InMemoryWorkflowVersionRegistry();
const runner = createWorkflowRunner({ storage: new InMemoryWorkflowStorage(), registry });

const { resumed, settled } = await runner.recover(
  RecoveryStrategy.builder()
    .failStale({ olderThanMs: 24 * 60 * 60 * 1000 })
    .resumeRecent({ concurrent: 10 })
    .build(),
);
await settled; // every resumed run has finished (or suspended again)
console.log(`resumed ${resumed}`);
```

`resumeRecent` lists runs nobody is driving — `pending`, `running` and
`compensating`, with a free or expired lock — keyset-paged through
`listOrphanedRuns`, and resumes at most `concurrent` at once. A
`compensating` run finishes its rollback.

### Retention

Ended runs, their steps, journals, signals, compensation ledgers, step
progress and attempts stay until something deletes them. Declare how long on
the definition and let a `RetentionSweeper` (or the distributed runner's
`retention` option) carry it out:

```typescript
import { RetentionSweeper, workflow } from "@promin/workflow";

const audited = workflow<{ id: string }>({
  name: "audited",
  retention: { completed: 7 * DAY, failed: null, keepAtLeast: 50 }, // failures: forever
}).build();

const sweeper = new RetentionSweeper({
  storage,
  workflows: [audited], // and/or `registry`
  retention: { completed: DAY, failed: 14 * DAY, cancelled: DAY, stepQueue: DAY },
  stepQueue, // what `retention.stepQueue` purges
  batchSize: 500, // runs per delete
  perSweepBudgetMs: 30_000, // a sweep stops starting batches after this
  tracer, // `retentionSweep`
  onSweep: (report) => metrics.record(report),
});
await sweeper.sweep({ dryRun: true }); // counts, deletes nothing
void sweeper.start(); // sweep every intervalMs (default 1 h); stop() ends it
```

A definition's policy wins field by field over the sweeper's default (`null`
keeps a class forever). Classes: `completed` (including tripwired), `failed`,
`cancelled` (settled cancels only). A sweep pages ended runs oldest first with
a keyset cursor and, per run, keeps it when: it is younger than its class's
age; it is one of the newest `keepAtLeast` of its workflow; it is a cancelled
run still owing its rollback; its parent still runs or owes a rollback; or one
of its children has not ended. The rest are deleted in one guarded
`purgeRuns` call per batch, which re-checks each run atomically, so a run
resumed or ended again since it was listed survives, and any number of
sweepers over one storage delete each run exactly once. When the budget ends a
sweep (`report.complete === false`) the cursor is kept and the next sweep
resumes there. Limits: the parent / child checks are snapshots (a parent
resumed in the instant after its child is deleted is not protected), the
step queue is not counted in a dry run, and the dead-letter sink
(`workflow({ dlq })`) is user-owned and not swept. With the distributed runner,
pass `retention: { completed: DAY, ... }` and the leader sweeps in the
background on its sweep loop (`onError` source `"retention"`).

## Compensation (sagas)

```typescript
import { workflow } from "@promin/workflow";
import { succeed } from "@spilne/perfect-core";

declare const bank: {
  debit(account: string, amount: number): Promise<{ txId: string }>;
  credit(account: string, amount: number): Promise<{ txId: string }>;
  refund(txId: string): Promise<void>;
};

const transfer = workflow<{ from: string; to: string; amount: number }>({
  name: "transfer",
  retry: { maxRetries: 2 },
  compensate: {
    trigger: "after-retries", // default; "immediate" skips the workflow retries
    retry: { maxRetries: 2 },
    onComplete: async ({ compensatedSteps, failedCompensations }) => {
      console.log("rolled back", compensatedSteps, failedCompensations);
    },
  },
})
  .step("debit", ({ input }) => bank.debit(input.from, input.amount), {
    compensate: ({ result }) => bank.refund(result.txId),
  })
  .step("credit", ({ input }) => bank.credit(input.to, input.amount))
  .step("notify", ({ prev }) => succeed(prev.txId))
  .build();
```

When the run fails for good (after workflow retries, or at once with
`trigger: "immediate"`), every completed step with a `compensate` is rolled
back:

- **Durable.** With a storage that has the `compensationLedger` capability
  (every bundled backend), the run first moves to status `compensating`
  (storing the failure), then records each step's rollback outcome as it
  settles. A process that dies mid-rollback leaves the run `compensating`;
  recovery (or the coordinator) finishes it, skipping steps already
  ledgered, then fails the run with the stored error. A storage without the
  capability compensates in memory only.
- **Order.** Latest `completedAt` first, ties broken by definition order
  (later first), and never before a completed dependent — the DAG wins over
  clock skew.
- `compensate` receives the step result decoded through the step's codec, the
  workflow input and the `workflowId`. A failing compensation is retried per
  `compensate.retry` and does not block the others; `onComplete` gets the
  lists of compensated and failed steps.
- Skipped steps, `onFailure` fallbacks, cancelled runs and runs stopped by a
  `CheckpointError` or lost lock are not compensated.

Inside a `.journaled()` step, `ActivityOptions.compensate` gives intra-step
compensation: activities are rolled back in reverse when a later activity of
the same body throws.

## Journaled steps

A `.journaled()` step's body is a generator. Every side effect goes through
`yield* ctx.activity(name, fn)`, which journals its result; when the body runs
again (resume after a sleep or signal, a crash, a retry) journaled activities
return their recorded value without running.

```typescript
import { createWorkflowRunner, InMemoryWorkflowStorage, workflow } from "@promin/workflow";
import { succeed } from "@spilne/perfect-core";

declare const api: {
  createUser(email: string): Promise<{ id: string }>;
  deleteUser(id: string): Promise<void>;
  sendEmail(id: string): Promise<void>;
};

const signup = workflow<{ email: string }>({ name: "signup" })
  .step("normalize", ({ input }) => succeed(input.email.toLowerCase()))
  .journaled("create", function* (ctx, email) {
    const user = yield* ctx.activity("create-user", () => api.createUser(email), {
      compensate: (u) => api.deleteUser(u.id),
    });
    yield* ctx.sleep(60_000); // durable; the run suspends here
    const confirmed = yield* ctx.signal<boolean>("email-confirmed", { timeout: 86_400_000 });
    if (!confirmed.ok) return { user, confirmed: false };
    yield* ctx.activity("welcome", () => api.sendEmail(user.id), { idempotent: true });
    return { user, confirmed: confirmed.value };
  })
  .build();

const runner = createWorkflowRunner({ storage: new InMemoryWorkflowStorage() });
const handle = await runner.start({
  workflow: signup,
  workflowId: "signup-1",
  input: { email: "A@x.io" },
});
```

The context also has `ctx.parallel([...])` (concurrent activities, nested
parallels allowed), `ctx.child(workflow, { input, workflowId })`,
`ctx.dowhile` / `ctx.dountil` (journaled iterations), `ctx.validatedSignal`
/ `ctx.approval` (schema-checked signals), `ctx.proxy({...})` (bind a record
of activity functions), `ctx.patched(name)` (see [Versioning](../../../versioning.md)),
`ctx.metadata.set / merge`, `ctx.setQueryHandler` and `ctx.continueAsNew(input)`.

Semantics:

- **`ctx.waitForEvent(name | SignalType, { key?, where?, timeout? })`** parks
  until an event addressed `(name, key)` arrives; `deliverEvent({ storage,
workflowId, event, key?, payload, eventId? })` sends one. Unlike a signal
  (one last-wins value per name), events are **buffered**: one delivered
  before the wait starts is not lost, and several delivered in a row satisfy
  one wait each, oldest first (delivery order, ties by event id). The same
  `eventId` delivered twice stores one event (`duplicate: true`), and a retry
  of a half-finished delivery still wakes the wait. Which event a wait took
  is journaled, so replay returns the same payload. `where` takes only events
  whose payload it accepts (the others stay buffered for later waits); with a
  `timeout` the result is `{ ok, value } | { ok: false, error: "timeout" }`
  with the deadline fixed at the first call. A wait without `key` matches only
  unkeyed events. The buffer is per run and each step body reads it from the
  start, so two steps waiting on one address both see the event: use distinct
  keys to route one event to one step (likewise for concurrent waits on one
  address inside one step). It rides on the signal store, so every backend
  supports it with no schema change; a parked wait shows up in `waitingOn` as
  `kind: "event"` (`eventName`, `eventKey`) and in the tracer's `waitStart` /
  `waitEnd` events as `kind: "event"`.
- **`ctx.metadata` write errors.** `set` / `merge` update the in-process
  snapshot at once and write to storage in the background; the step waits
  for its writes before its outcome is checkpointed. A write that fails with
  `FenceTokenMismatchError` (the run's lock was lost) fails the step with it,
  like a journal write under a lost lock: the stale holder stops and the new
  holder re-drives the step. Any other storage error does not fail the step,
  because metadata is a dashboard surface and replay re-fires the writes: it
  is reported to the `onMetadataWriteError({ workflowId, stepName, error,
keys })` hook (default `console.warn` with the error). The snapshot keeps
  the value, so later steps of that execution see it though storage does not.
  A step run by a queue worker has no hook and uses the `console.warn`
  default.
- **Ambiguous outcomes.** An activity journals a pending row, runs, then
  completes the row. If the process dies in between, the engine cannot know
  whether the side effect happened: by default the step fails with
  `AmbiguousActivityOutcome` for an operator to inspect; `idempotent: true`
  re-runs the activity instead.
- **Failures.** An activity that fails past its own `retry` is journaled.
  Inside the same step attempt, replay rethrows an error of the same kind
  (engine errors as their class, other errors with their `_tag`, `name` and
  fields). Once a failure escapes the body, the attempt's recorded failures and
  the activities its compensations rolled back are discarded, so the next
  attempt runs them again; successful activities that were not rolled back
  replay and never run twice.
- **Compensation unwind** runs only for genuine failures. Suspension,
  continue-as-new, tripwire, `JournalNonDeterminismError`,
  `AmbiguousActivityOutcome` and lock loss propagate untouched.
- **Signal vs timeout.** A delivery and the timeout race for the same journal
  entry; whichever completes it first wins, and the live run and every replay
  take that outcome.
- **Children.** `ctx.child` runs a separate durable workflow (default id
  `"<parentId>.<step>.<slot>"`). A child that suspends parks the parent until
  the child's own wake time or until the child ends, whichever is first (see
  [child wake-up](#subworkflows-and-child-wake-up)). Pass
  `{ name, load }` (`LazyChildWorkflow`) instead of a workflow when building
  the child takes async work: `load` runs only when the child has to run (first
  run, or re-attaching to a child left in flight), never on a replay of its
  journaled outcome.
- **Determinism.** A replayed body must yield the same activities in the same
  order; a mismatch throws `JournalNonDeterminismError`. Use
  `workflow({ payloadHash: true })` with the 3-arg
  `ctx.activity(name, input, fn)` form to also catch changed inputs.
- `.journaled()` takes no `cache` and no `timeoutMs` (the body cannot be
  interrupted); time out activities or the workflow instead.

## Sleep and signals

```typescript
import { createWorkflowRunner, InMemoryWorkflowStorage, workflow } from "@promin/workflow";
import { succeed } from "@spilne/perfect-core";

const approval = workflow<{ requestId: string }>({ name: "approval" })
  .step("submit", ({ input }) => succeed(input.requestId))
  .sleep("cool-off", 60_000)
  .waitForSignal<{ approved: boolean }, "decision">("decision", {
    signalName: "manager-decision",
    timeoutMs: 86_400_000,
  })
  .step("apply", ({ prev }) => succeed(prev.approved ? "approved" : "rejected"))
  .build();

const runner = createWorkflowRunner({ storage: new InMemoryWorkflowStorage() });
const handle = await runner.start({
  workflow: approval,
  workflowId: "req-7",
  input: { requestId: "7" },
});
await handle.signal({ signalName: "manager-decision", payload: { approved: true } });
```

- `.approval(name, { signalName?, timeoutMs? })` is `.waitForSignal()` with
  the defaults for a human decision: signal `"<name>-approved"`, payload
  `{ approved: boolean }`.
- `.sleep()`, `.waitForSignal()` and `.approval()` take `dependsOn` to start
  after named steps instead of the head.
- `.sleep()` passes its predecessor's value through. The wake time and a
  signal's deadline are computed once, on the step's first execution, and
  stored; resuming early never moves them. A signal wait past its deadline
  fails with `WorkflowTimeoutError`.
- **Signals are named values on the run, last delivery wins.** Delivering a
  name again replaces the payload; a delivery before the wait is picked up at
  once; reads never consume it, so every wait on one name is satisfied by one
  delivery; a fresh run (`force`, continue-as-new) starts with none. Use
  distinct names (`approve-1`, `approve-2`) for distinct events.
- **Who resumes a suspended run?** Nothing, by itself: `handle.signal()` /
  `storage.deliverSignal` only record the signal, and `handle.result()` only
  polls storage. Run the sleep and signal scanners (`createSleepScanner`,
  `createSignalScanner` from `@promin/workflow/distributed`), which resume due
  sleeps and delivered signals from any process, or resume a run yourself by
  running it again under the same `workflowId` (it continues from the
  suspension; a sleep that is not yet due suspends again).

## Updates

An update is a named, typed request to a running run that comes back with the
handler's validated response (a signal that talks back). A journaled body
registers handlers; callers use `handle.update(...)`.

```typescript
import { defineUpdate, s, workflow } from "@promin/workflow";

const SetLimit = defineUpdate({
  name: "setLimit",
  input: s.object({ limit: s.integer() }),
  output: s.object({ previous: s.integer() }),
});

const batch = workflow<{ limit: number }>({ name: "batch" })
  .journaled("main", function* (ctx) {
    let limit = ctx.input.limit;
    ctx.setUpdateHandler(
      SetLimit,
      ({ limit: next }) => {
        const previous = limit;
        limit = next;
        return { previous };
      },
      { validate: ({ limit: next }) => (next < 1 ? "limit must be positive" : undefined) },
    );
    yield* ctx.signal<string>("go");
    return { limit };
  })
  .build();

const handle = runner.handle("batch-1", { workflow: batch });
const { previous } = await handle.update({ update: SetLimit, args: { limit: 20 } }); // typed
```

**Ordering.** A body applies updates only at update points: between its own
top-level yields, right after a `yield*` settles (a value, or the suspension of
a sleep or signal wait) and before the generator resumes. Never during an
activity, never inside a `ctx.parallel` branch, never two handlers at once.
Pending updates run in delivery order (`deliveredAt`, then update id), at most
`MAX_UPDATES_PER_POINT` (100) per point; the rest wait for the next point.
Updates arriving after the body's last yield are answered
`UpdateRejectedError` (`not_applied`): the step has finished.

**Durability.** Each applied update is one journal entry (an `activity` named
`workflow.update:<id>`, so no backend schema change) in the slot after the yield
that preceded it. A replay re-runs the handlers at the same points, in the same
order, to rebuild the body's state, and keeps the recorded outcome; only past
the recorded journal does it look at new requests. Handlers must be
deterministic in their args and the body's state, must not call `ctx`, and may
be async but not yield. A handler runs at least once (journaled pending before,
completed after, like an activity). Rejections (unknown name, schema or
`validate` failure) run no handler and write no journal entry. A handler that
throws is recorded as failed (`UpdateFailedError`).

**Idempotency.** The request is the signal `workflow.update:<updateId>` (value
semantics: a repeat is the same signal) and the answer is
`workflow.update-result:<updateId>`. Repeating `update` / `submitUpdate` with the
same `updateId` and request returns the recorded outcome without applying it
again; the same id with another request is `id_conflict`. Without `updateId` a
random one is used.

**Suspended runs.** The signal scanner also resumes runs holding an unanswered
update request (`listUpdateWakeups`, capability `updateWakeups`, in every bundled
backend), so a run parked on a sleep or signal wait applies the update and
suspends again; run `createSignalScanner` as for signals. Zorya runs it for you.

**Rejections the caller sees** (`UpdateRejectedError.reason`): `run_not_found`,
`run_terminal` (also when the run ended before applying it), `no_handlers`,
`unknown_update`, `validation` (the schema, checked by the caller from the
handler list the body published, and by the body again), `id_conflict`,
`not_applied`. `UpdateTimeoutError` when no outcome arrives within `timeoutMs`
(default 60 s, polled every `intervalMs`, 100 ms).

Limits. Handlers are served while the registering journaled step runs or is
suspended; when it finishes, pending requests are rejected `not_applied` and the
published handler list is emptied. Register handlers before the first `yield*`
(a caller racing the very first run start can see `no_handlers`). With several
journaled steps, the executing one's handlers are the served set. Args and
results must be JSON-safe. The tracer's `update` event comes from inline
execution; queue workers do not report it. Read-only queries stay
`ctx.setQueryHandler` (process-local, not journaled).

`handle.submitUpdate()` delivers without waiting and `handle.updateOutcome(id)` /
`awaitUpdate()` read the answer; `bindWorkflow(...).update({ workflowId, ... })`
is the bound form. `submitUpdate`, `awaitUpdate` and `executeUpdate` take any
`WorkflowStorage` (including `RemoteWorkflowStorage`), so a process without the
workflow code can send them.

## Subworkflows and child wake-up

```typescript
import { workflow } from "@promin/workflow";
import { succeed } from "@spilne/perfect-core";

const enrich = workflow<{ userId: string }>({ name: "enrich" })
  .step("score", ({ input }) => succeed({ userId: input.userId, score: 42 }))
  .build();

const onboard = workflow<{ userId: string }>({ name: "onboard" })
  .step("create", ({ input }) => succeed({ id: input.userId }))
  .subworkflow(
    "enrich",
    enrich,
    { input: (prev) => ({ userId: prev.id }), workflowId: (prev) => `enrich-${prev.id}` },
    { retry: { maxRetries: 1 } },
  )
  .step("notify", ({ prev }) => succeed(`score ${prev.score}`))
  .build();
```

- The child is its own durable run, created idempotently with the child's
  `version` and `parentWorkflowId`, and driven with the parent runner's
  storage, clock, step executor and hooks.
- A failed child fails the step with a typed `StepError`, so `retry`
  re-drives the child (a child that failed by itself is re-run fresh for a
  retried parent step) and `onFailure` / `compensate` apply. A child whose
  failure was a decision — cancelled, or marked failed by an operator
  (`failWorkflow({ errorTag: MARKED_FAILED_ERROR_TAG })`, see
  `isFinalFailedRun`) — is never re-run: the parent step observes its stored
  failure.
- **Child wake-up.** When the child suspends, the parent step parks as a
  signal wait on a reserved signal
  `workflow.child-ended:<childId>#<childRun>`, with the child's earliest wake
  time as its timeout. When the child run ends — completed, failed, tripwire
  or cancelled — the runner delivers that signal after releasing the child's
  lock, and the signal scanner resumes the parent. Both halves are ordinary
  storage writes, so this works across processes on every backend. The wake
  is best effort; the parent still wakes at the child's own wake time.
- `storage.cancelWorkflow({ workflowId, cascade: true })` cancels children too;
  `listWorkflows({ parentId })` lists them.

## Time: `WallClock`

Every piece of engine time math — step and workflow deadlines, retry backoff,
lock heartbeats, sleeps and signal deadlines, poll loops, scheduler cadence —
reads and schedules through a `WallClock` (`currentTimeMs`, `now`,
`setTimeout`, `setInterval`). Runners, workers, coordinators, scanners,
schedulers and the bundled storages take a `clock` option defaulting to
`SystemWallClock`. Tests pass a `FakeWallClock` and drive time with
`advance(ms)`, which fires due timers synchronously:

```typescript
import {
  createWorkflowRunner,
  FakeWallClock,
  InMemoryWorkflowStorage,
  workflow,
} from "@promin/workflow";
import { fail, succeed, TaggedError } from "@spilne/perfect-core";

class NotYet extends TaggedError("NotYet")<{ message: string }>() {}

const clock = FakeWallClock.create(0);
const storage = new InMemoryWorkflowStorage({ clock });
const runner = createWorkflowRunner({ storage, clock });

const wf = workflow<number>({ name: "retrying" })
  .step(
    "flaky",
    ({ attempt }) => (attempt < 3 ? fail(new NotYet({ message: "not yet" })) : succeed(attempt)),
    {
      retry: { maxRetries: 3 }, // waits 250 ms, then 500 ms, on `clock`
    },
  )
  .build();

let settled = false;
const done = runner
  .runSafe({ workflow: wf, workflowId: "w1", input: 0 })
  .finally(() => (settled = true));
while (!settled) {
  // Advance only once the run has parked on a backoff timer.
  if (clock.pendingCount() > 0) clock.advance(1_000);
  await new Promise((r) => setTimeout(r, 0)); // yield to the engine
}
console.log(await done); // { data: 3, error: null }
```

Wait on `pendingCount()` (or another observable predicate) before each
`advance()`; a fixed real-time sleep before advancing races the engine under
load. Inside a step, read time from the activity's arguments or the step
context, never from `Date.now()`, so replay stays deterministic.

## More

- **Triggers.** `trigger({ workflow, runner, toInput, toWorkflowId, concurrency, onDuplicate })`
  is a perfect `Pipe` from any `Stream` of events to `WorkflowResult`s;
  `onDuplicate: "skip"` skips ids that already exist. `webhookTrigger` turns
  HTTP requests (with optional HMAC verification) into runs.
- **Dead-letter queue.** `workflow({ dlq })` publishes a `FailedWorkflowRecord`
  to any `Sinkable` after retries and compensation.
- **Visual editor schema.** `compileWorkflow({ schema, registry })` builds a
  `Workflow` from JSON (validated with `validateWorkflowSchema`) and a
  `MapActivityRegistry` of activity factories.
- **DAG export.** `builder.toJSON()`, `dagToMermaid(dag)`, `dagToDot(dag)`.
- **Versioning.** `version`, `onVersionMismatch: "drain"`, `patches` and the
  version registry: see [Versioning](../../../versioning.md).
- **Distributed execution** and the scanners: see
  [Distributed](../distributed/README.md). **Schedules**: see
  [Scheduler](../scheduler/README.md).
