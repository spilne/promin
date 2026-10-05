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
  .stepAsync("provision", async ({ prev }) => ({ accountId: `acct-${prev.id}` }))
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

`.step()` takes a function returning a perfect `Eff<A, Throws<E>>`
(`StepEff<A, E>`). The typed failure `E` must be a tagged error; it is what
`retry` and `onFailure` act on, and it is collected into the workflow's error
type. Anything else that goes wrong (a throw, a rejected promise) is a
**defect**: it fails the step without retry.

| Method                                  | Body returns                  | Failure                                                         |
| --------------------------------------- | ----------------------------- | --------------------------------------------------------------- |
| `.step(name, fn, options?)`             | `Eff` (or a Promise of one)   | typed `E`; a throw is a defect                                  |
| `.stepAsync(name, fn, options?)`        | `Promise<A>`                  | a rejection is a defect                                         |
| `.mapOver` / `.mapOverAsync`            | `Eff` / `Promise` per element | as above, per element                                           |
| `.dowhile` / `.dountil`                 | `Eff` per iteration           | typed `E`                                                       |
| `.dowhileAsync` / `.dountilAsync`       | value or `Promise`            | a rejection is a defect                                         |
| `.branch` / `.match` / `.parallelSteps` | `Eff` per branch / case       | typed `E`; a throw from `condition` / `on` / `when` is a defect |

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
  .stepAsync("receipt", async ({ prev }) => {
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

### parallelSteps, branch, match

```typescript
import { workflow } from "@promin/workflow";
import { succeed } from "@spilne/perfect-core";

const route = workflow<{ id: string; total: number; kind: "express" | "standard" }>({
  name: "route",
})
  .parallelSteps(
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

- `parallelSteps` forks into one DAG step per branch (`"enrich.user"`,
  `"enrich.risk"`), each retried, cached and distributed on its own, then
  joins them into a keyed record. Block-level `timeoutMs` / `retry` /
  `needs` / `priority` / `queue` / `cache` are defaults for every branch;
  `branches` sets per-branch options; `codec` encodes the joined record.
- `branch` and `match` are one DAG node each. A selector with no matching case
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
  .dountilAsync(
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

| Option                       | step / stepAsync / branch / match | mapOver             | parallelSteps                | journaled | subworkflow | loops | tripwire |
| ---------------------------- | --------------------------------- | ------------------- | ---------------------------- | --------- | ----------- | ----- | -------- |
| `codec`                      | yes                               | array (+ `element`) | joined record (+ per branch) | yes       | yes         | yes   | yes      |
| `retry`, `timeoutMs`         | yes                               | yes (+ `element`)   | default + per branch         | `retry`   | `retry`     | yes   | –        |
| `onFailure`, `compensate`    | yes                               | yes                 | per branch                   | yes       | yes         | yes   | –        |
| `skipWhen`, `skipValue`      | yes                               | yes                 | per branch                   | yes       | yes         | yes   | –        |
| `needs`, `priority`, `queue` | yes                               | yes                 | default + per branch         | yes       | yes         | yes   | –        |
| `cache`                      | yes                               | yes                 | default + per branch         | –         | –           | –     | –        |

- `timeoutMs` is per attempt: the attempt is interrupted and fails with the
  typed `StepTimeoutError`, so `retry` / `onFailure` see it. The timer runs on
  the runner's `WallClock`.
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

`createWorkflowRunner({ storage, clock?, registry?, hooks?, stepExecutor?, executorId? })`
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
  stops at the next wave with `WorkflowLockLostError`.
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
  .stepAsync("debit", ({ input }) => bank.debit(input.from, input.amount), {
    compensate: ({ result }) => bank.refund(result.txId),
  })
  .stepAsync("credit", ({ input }) => bank.credit(input.to, input.amount))
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
  [child wake-up](#subworkflows-and-child-wake-up)).
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
  re-drives the child (a failed child is re-run fresh for a retried parent
  step) and `onFailure` / `compensate` apply.
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
