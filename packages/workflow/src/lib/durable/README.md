# Durable Execution

DAG-based workflows with checkpoint/resume, type-safe steps, and structural concurrency.

## Quick Start

### `flow()` — non-durable (scripts, request handlers, compositions)

No storage, no workflowId. Same composition API as `workflow()`.

```typescript
import { flow } from "@promin/workflow";
import { succeed } from "@spilne/perfect-core";

// Simple linear chain
const result = await flow<{ userId: string }>("process-user")
  .step("fetch", ({ input }) => api.get(`/users/${input.userId}`, UserSchema))
  .stepAsync("enrich", async ({ prev }) => enrichUser(prev))
  .step("format", ({ prev }) => succeed(`${prev.name} (${prev.score})`))
  .execute({ userId: "u_42" });

// DAG with auto-parallel
const report = await flow<{ text: string }>("analyze")
  .step("parse", ({ input }) => succeed(input.text))
  .step("summarize", { dependsOn: ["parse"] }, ({ deps }) => summarize(deps.parse))
  .step("keywords", { dependsOn: ["parse"] }, ({ deps }) => extractKeywords(deps.parse))
  .step("publish", { dependsOn: ["summarize", "keywords"] }, ({ deps }) =>
    succeed({ summary: deps.summarize, keywords: deps.keywords }),
  )
  .execute({ text: "..." });

// Error handling
const { data, error } = await flow<{ url: string }>("fetch")
  .step("download", ({ input }) => httpClient.get(input.url, Schema))
  .executeSafe({ url: "https://example.com" });
```

To make it durable later, change `flow("name")` to `workflow({ name }).bind(storage)` and `.execute(input)` to `.run({ workflowId, input })`.

### `workflow()` — durable (survives crashes, resumes from checkpoints)

```typescript
import { workflow } from "@promin/workflow";
import { succeed } from "@spilne/perfect-core";
import { migrate, PostgresWorkflowStorage } from "@promin/postgres";

await migrate(db);
const storage = await PostgresWorkflowStorage.create({ db });

const result = await workflow<{ userId: string }>({
  name: "onboard-user",
  type: "onboarding",
  metadata: { team: "growth" },
})
  .step("fetch", ({ input }) => api.get(`/users/${input.userId}`, UserSchema))
  .step("provision", ({ prev }) => api.post("/accounts", AccountSchema, { json: prev }))
  .stepAsync("notify", async ({ prev }) => {
    await mailer.send(prev.email, "Welcome!");
    return { notified: true };
  })
  .bind(storage)
  .run({ workflowId: "onboard-123", input: { userId: "u_42" } });
```

## Features (implemented)

### Linear & DAG Steps

```typescript
// Linear — each step depends on the previous
.step("a", fn)
.step("b", fn)  // b depends on a

// DAG — explicit dependencies, auto-parallel
.step("scrape", fn)
.step("summarize", { dependsOn: ["scrape"] }, fn)
.step("keywords", { dependsOn: ["scrape"] }, fn)    // runs parallel with summarize
.step("publish", { dependsOn: ["summarize", "keywords"] }, fn)
```

### stepAsync — Promise convenience

```typescript
.stepAsync("fetch", async ({ input }) => {
  const response = await fetch(`/api/users/${input.id}`);
  return response.json();
})
```

### mapOver — Fan-out with per-element retry

```typescript
.step("get-urls", ({ input }) => succeed(input.urls))
.mapOver("fetch-all", { array: "get-urls", concurrency: 5 }, (url, ctx) =>
  succeed(`Response from ${url}`),
  { element: { retry: { maxRetries: 3 }, timeoutMs: 5_000 } }, // per element
)
```

`element` options (codec, timeoutMs, retry) apply to each element on its own;
the other options apply to the map step and its array result. Each completed
element is saved as a task row, so when the map step runs again (a step or
workflow retry, a resume after a crash) only the elements without a saved
result run.

### map — Pure transform of `prev`

```typescript
.step("load", ({ input }) => succeed({ id: input.id, total: 42 }))
.map((order) => order.total) // the next step's prev is 42
```

`.map(fn)` adds a step `"<head>.map"` that applies `fn` to the head's result
and checkpoints the mapped value with the workflow codec. The head step is
unchanged: its row, codec, `compensate` and `dependsOn: ["load"]` see the
unmapped value, and its `skipValue` / `onFailure` fallback are mapped too.

### branch — Conditional paths

```typescript
.branch("classify", {
  condition: (n) => n > 10,
  ifTrue: ({ prev }) => succeed(`big: ${prev}`),
  ifFalse: ({ prev }) => succeed(`small: ${prev}`),
})
```

### sleep & waitForSignal — Durable timers & external events

```typescript
.sleep("wait-24h", 86_400_000)

.waitForSignal<{ approved: boolean }>("approval", {
  signalName: "manager-approved",
  timeoutMs: 86_400_000,
})

// External system delivers signal:
await storage.deliverSignal(workflowId, "manager-approved", { approved: true });
```

- `.sleep()` passes its predecessor's value through: the step after it gets
  the pre-sleep value as `prev`.
- Wake time and signal timeout are computed once, on the step's first
  execution, and stored. Resuming early (e.g. `handle.result()` polling) does
  not move them.
- A signal is a named value on the run, not a queued event. Re-delivering a
  name replaces the payload (last wins), signals are cleared when a fresh run
  starts, and they are not consumed: every `waitForSignal` on the same
  `signalName` is satisfied by one delivery. Use distinct names (e.g.
  `approve-1`, `approve-2`) to wait for distinct events.

### build + trigger — Stream → Workflow

```typescript
import { workflow, trigger, WorkflowResult } from "@promin/workflow";

const analyzeArticle = workflow<{ url: string }>({ name: "analyze" })
  .step("scrape", ({ input }) => scraper.get(input.url))
  .step("summarize", ({ prev }) => ai.summarize(prev))
  .build()
  .bind(storage);

// Trigger from any perfect Stream — trigger() returns a Pipe
await eventStream
  .through(
    trigger({
      workflow: analyzeArticle,
      runner,
      storage,
      toInput: (event) => ({ url: event.data }),
      toWorkflowId: (event) => `analyze-${event.id}`,
      concurrency: 5, // results stay in input order
      onDuplicate: "skip",
    }),
  )
  .filter(WorkflowResult.isCompleted)
  .tap((r) => log(r.result))
  .drain()
  .run();
```

### Step Failure Strategies

```typescript
.step("fetch", fn, {
  retry: { maxRetries: 3, baseDelayMs: 1000 },  // exponential backoff

  onFailure: "fail",                              // default — fail the workflow
  // OR
  onFailure: "skip",                              // skip, continue with undefined
  // OR
  onFailure: { fallback: (error) => defaults },   // use fallback value
})
```

Retry and `onFailure` handle typed failures only. A throw from a synchronous
callback (`.branch()` `condition`, `.match()` `on`/`when`, `.subworkflow()`
`input`/`workflowId`) is a defect and fails the step without them; return a
failed `Eff` for a recoverable error. Suspension (sleep, signal waits) is never
retried or skipped.

Every step kind applies the options its options type accepts. `.step()`,
`.branch()` and `.match()` accept all of them; `.mapOver()` adds per-element
`element` options; `.parallelSteps()` takes block-wide defaults plus per-branch
`branches` options; `.journaled()` and `.subworkflow()` have no `cache` or
`timeoutMs`; loops have no `cache`; `.tripwire()` takes only `codec`. An option a
kind cannot honour is a compile error.

### Workflow-level Retry

Re-runs from the failed step — completed steps are checkpointed and skipped.

```typescript
workflow<Input>({
  name: "resilient",
  retry: { maxRetries: 3, baseDelayMs: 5000 },
})
  .step("step-1", fn) // runs once, checkpointed
  .step("step-2", fn) // if this fails, workflow retries from here
  .bind(storage);
```

### Idempotency & Singleflight

Prevent duplicate executions and control what happens when a workflow is called again.

```typescript
import { workflow } from "@promin/workflow";

const processOrder = workflow<{ orderId: string }>({
  name: "process-order",
})
  .step("charge", ({ input }) => payments.charge(input.orderId))
  .step("fulfill", ({ prev }) => warehouse.ship(prev.chargeId))
  .build({
    idempotency: {
      ttl: 60_000, // cache result for 60s — re-calls return cached result
      onInFlight: "join", // concurrent calls join the running execution (singleflight)
      onExpiry: "fresh-run", // after TTL: re-execute with fresh run counter
    },
  })
  .bind(storage);

// First call — executes the workflow
const result1 = await processOrder.run({ workflowId: "order-42", input: { orderId: "42" } });

// Second call within TTL — returns cached result instantly (no re-execution)
const result2 = await processOrder.run({ workflowId: "order-42", input: { orderId: "42" } });

// Force re-execution regardless of TTL
const result3 = await processOrder.run({
  workflowId: "order-42",
  input: { orderId: "42" },
  force: true,
});
```

**TTL options:**

```typescript
// Same TTL for success and failure
idempotency: { ttl: 60_000 }

// Different TTLs — cache success longer, retry failures sooner
idempotency: { ttl: { success: 3_600_000, failure: 10_000 } }
```

**Behavior on concurrent calls (`onInFlight`):**

- `"join"` (default) — caller waits for the in-flight execution to finish and gets the same result (singleflight pattern)
- `"reject"` — throws `WorkflowLockError` immediately

**Behavior after TTL expires (`onExpiry`):**

- `"fresh-run"` (default) — increments the run counter and re-executes all steps from scratch. Previous run history is preserved.
- `"replay"` — re-enters the engine and replays from checkpointed state (skips completed steps)

### Saga Compensation

When a step fails, automatically undo completed steps in reverse order.

```typescript
import { workflow } from "@promin/workflow";
import { tryPromise } from "@spilne/perfect-core";

workflow<{ from: string; to: string; amount: number }>({
  name: "transfer",
  retry: { maxRetries: 2, baseDelayMs: 5000 },
  compensate: {
    trigger: "after-retries", // compensate after all workflow retries exhausted (default)
    // trigger: "immediate",      // compensate on first failure, skip workflow retries
    retry: { maxRetries: 2 }, // retry failing compensation functions
    onComplete: ({ input, error, compensatedSteps, failedCompensations }) =>
      tryPromise(
        () => audit.log("rollback", { compensatedSteps, error }),
        (e) => e,
      ),
  },
})
  .step("debit", ({ input }) => bankClient.debit(input.from, input.amount), {
    compensate: ({ result }) => bankClient.refund(result.txId),
  })
  .step("credit", ({ input }) => bankClient.credit(input.to, input.amount), {
    compensate: ({ result }) => bankClient.reverseCredit(result.txId),
  })
  .step("notify", ({ prev }) => emailClient.send(prev.receipt))
  .bind(storage)
  .run({ workflowId: "transfer-1", input: { from: "A", to: "B", amount: 100 } });
```

**Full failure cascade:**

```
Step fails
  → Step retries (exponential backoff)
    → Exhausted → StepFailureStrategy ("fail" / "skip" / fallback)
      → "fail" → Workflow fails
        → Workflow retries (re-run from failed step)
          → Exhausted → Compensation cascade (reverse order)
            → compensate.onComplete callback
              → Workflow marked failed
```

- `trigger: "immediate"` — skips workflow retries, compensates right away
- `trigger: "after-retries"` (default) — retries the workflow first, compensates only as last resort
- Compensation failures don't block other compensations
- `compensate.retry` retries individual compensation functions
- `onFailure: "skip"` or `{ fallback }` prevents compensation (workflow continues)

**Retry defaults.** Every retry (step `retry`, workflow `retry`, activity `retry`,
`compensate.retry`, the state machine's `retryMiddleware`, the distributed worker) runs on
one loop (`retryAsync` / `retryWithPolicy` in `shared/retry-policy.ts`): 3 retries, 250ms
base delay doubling per retry, no delay cap (`maxDelayMs: 0` is no cap), jitter off, the
time budget measured from the first failure, all waits on the injected clock. Workflow-level
and compensation retries only happen once a policy is set. The workflow-level retry never
retries control flow, a spent deadline or a cancel, and retries defects only with
`retryDefects: true`.

**Run lifecycle.**

- A run that already ended is not executed again: re-running a `completed` workflow returns
  its result; a `failed`, cancelled or `tripwire` one rejects with `WorkflowFailedError`
  (carrying the stored `errorTag`), `WorkflowCancelledError` or `WorkflowTripwireError`.
  `force: true` archives the ended run and starts a fresh one; `runner.resume({ fromStep })`
  re-drives a failed run from a step.
- `handle.cancel()` during a run wins: the run stops at the next wave boundary and rejects with
  `WorkflowCancelledError`; terminal writes are conditional, so a late completion never
  overwrites the cancel. Completed steps are not compensated.
- The `timeoutMs` deadline runs from the run's persisted start, across sleeps and signal waits.
- Hooks are observers: a throwing hook is reported to `hooks.onHookError` (default
  `console.error`) and never changes the outcome.
- A durable write the run depends on (step checkpoint, terminal status) is retried; if it
  still fails the run rejects with `CheckpointError` and stops as it stands (no compensation,
  no `failWorkflow`). Recovery re-drives it later, so a step whose result was not saved runs
  again (at-least-once). A lost lock stops the run at the next wave with
  `WorkflowLockLostError`.

### Observability

```typescript
// Lifecycle hooks
workflow<Input>({
  name,
  hooks: {
    onStepComplete: ({ stepName, result, durationMs }) => metrics.record(durationMs),
    onStepFailure: ({ stepName, error }) => alerting.notify(error),
    onWorkflowComplete: ({ workflowId, durationMs }) => log.info("done", { durationMs }),
    onWorkflowFailure: ({ workflowId, error }) => log.error("failed", { error }),
  },
}).bind(storage);

// Query workflows
await storage.listWorkflows({ status: "failed", type: "onboarding", limit: 10 });
await storage.cancelWorkflow(workflowId);

// DAG visualization
const dag = builder.toJSON();
const mermaid = dagToMermaid(dag); // graph LR ...
const dot = dagToDot(dag); // digraph "name" { ... }
```

### Visual Editor Schema

Compile JSON workflows from a node-based UI into executable WorkflowDefinitions:

```typescript
import { compileWorkflow, MapActivityRegistry } from "@promin/workflow";
import { succeed, tryPromise } from "@spilne/perfect-core";

const registry = new MapActivityRegistry({
  "http.get": (config) => () => httpClient.get({ url: config?.url as string }),
  "transform.uppercase": () => (ctx) => succeed(String(ctx.prev).toUpperCase()),
  "db.insert": (config) => (ctx) => tryPromise(() => db.insert(config?.table, ctx.prev), toDbError),
});

const definition = compileWorkflow({
  schema: {
    version: 1,
    name: "fetch-and-store",
    steps: [
      {
        type: "step",
        name: "fetch",
        dependsOn: [],
        activityRef: "http.get",
        config: { url: "https://api.example.com" },
      },
      { type: "step", name: "transform", dependsOn: ["fetch"], activityRef: "transform.uppercase" },
      {
        type: "step",
        name: "store",
        dependsOn: ["transform"],
        activityRef: "db.insert",
        config: { table: "results" },
      },
    ],
  },
  storage,
  registry,
});

await definition.run({ workflowId: "wf-1", input: {} });
```

Validate untrusted schema JSON from APIs:

```typescript
import { validateWorkflowSchema } from "@promin/workflow";

const schema = validateWorkflowSchema(req.body); // throws ZodError on invalid
```

### Subworkflows

Child workflow composition with parent-child tracking.

```typescript
import { workflow } from "@promin/workflow";
import { succeed } from "@spilne/perfect-core";

const enrichUser = workflow<{ userId: string }>({ name: "enrich" })
  .step("fetch", ({ input }) => api.get(`/profiles/${input.userId}`))
  .build()
  .bind(storage);

// .subworkflow() — builder sugar
workflow<{ userId: string }>({ name: "onboard" })
  .step("create", ({ input }) => api.post("/accounts", { json: input }))
  .subworkflow("enrich", enrichUser, {
    input: (prev) => ({ userId: prev.id }),
    workflowId: (prev) => `enrich-${prev.id}`,
  })
  .step("notify", ({ prev }) => succeed(`Score: ${prev.score}`))
  .bind(storage)
  .run({ workflowId: "onboard-1", input: { userId: "u_42" } });

// .invoke() — primitive for use inside any step
.step("enrich", ({ prev }) =>
  enrichUser.invoke({
    workflowId: `enrich-${prev.id}`,
    input: { userId: prev.id },
  })
)

// Fan-out — mapOver + invoke
.mapOver("process-all", { array: "get-items", concurrency: 10 }, (itemId) =>
  processItem.invoke({ workflowId: `item-${itemId}`, input: { itemId } })
)

// Parent-child tracking
const children = await storage.listWorkflows({ parentId: "onboard-1" });
await storage.cancelWorkflow("onboard-1", { cascade: true }); // cancels children too
```

### Dead Letter Queue

Failed workflows (after all retries + compensation) are published to a configurable DLQ. Works with any `Sinkable<FailedWorkflowRecord>` — an object with a `codec` and `publish(record): Promise<void>`, e.g. an adapter over a perfect-postgres `PgQueue`.

```typescript
import { workflow } from "@promin/workflow";
import { PgQueue } from "@spilne/perfect-postgres";

const queue = await PgQueue.create<FailedWorkflowRecord>(db, "workflow-dlq");
const dlq = { codec: queue.codec, publish: (record) => queue.publish(record).orDie().run() };

workflow<{ orderId: string }>({
  name: "process-order",
  retry: { maxRetries: 3 },
  dlq,
})
  .step("charge", fn)
  .step("fulfill", fn)
  .bind(storage)
  .run({ workflowId: "order-1", input: { orderId: "ord_42" } });

// Failed workflow record includes:
// - workflowId, workflowName, input, error, failedAt
// - step states (which steps completed, which failed)
// - compensatedSteps, failedCompensations
// - metadata

// Replay from DLQ
await dlq.subscribeAck().forEach(async (envelope) => {
  const failed = envelope.value;
  await processOrder.run({
    workflowId: `${failed.workflowId}-retry`,
    input: failed.input as { orderId: string },
  });
  await envelope.ack();
});
```

### Version Registry

Run multiple workflow versions simultaneously. New workflows use the latest version; existing workflows resume with the version they started on.

```typescript
import { workflow, WorkflowVersionRegistry } from "@promin/workflow";

const registry = new WorkflowVersionRegistry({ storage });

// Register versioned definitions (must have a version)
const v1 = workflow({ name: "order", version: "1" })
  .step("validate", ({ input }) => validateV1(input))
  .step("charge", ({ prev }) => chargeV1(prev))
  .build();

const v2 = workflow({ name: "order", version: "2" })
  .step("verify", ({ input }) => verifyV2(input))
  .step("charge", ({ prev }) => chargeV2(prev))
  .step("notify", ({ prev }) => notifyV2(prev))
  .build();

registry.register(v1);
registry.register(v2);
```

**Running workflows through the registry:**

```typescript
// New workflow -> uses latest (v2)
await registry.run({ workflowId: "order-new", name: "order", input: { orderId: "42" } });

// Existing v1 workflow -> resumes with v1 definition
await registry.run({ workflowId: "order-old", name: "order", input: { orderId: "7" } });
```

The registry checks storage for the workflow's version, then resolves the matching definition. If the stored version is no longer registered, it throws with a clear error listing available versions.

**Monitoring drain progress:**

Before deregistering an old version, check that all its workflows have finished:

```typescript
const counts = await registry.countByVersion({ name: "order", storage });
// Map { "1" => { running: 3, completed: 150, failed: 1, tripwire: 0 },
//       "2" => { running: 12, completed: 40, failed: 0, tripwire: 2 } }

if (counts.get("1")!.running === 0) {
  // Safe to remove v1 from the registry
}
```

**Inspecting the registry:**

```typescript
registry.names(); // ["order", "payment"]
registry.versions("order"); // ["1", "2"]
registry.latest("order"); // "2"
registry.resolve("order", "1"); // Workflow for v1
registry.resolve("order"); // Workflow for latest
```

### RRULE Support

Complex calendar recurrence via iCalendar RRULE (RFC 5545). Three trigger types: `cron`, `rrule`, `intervalMs`.

```typescript
// Biweekly on Tuesday at 10am
scheduler.register({
  id: "biweekly-standup",
  rrule: "FREQ=WEEKLY;INTERVAL=2;BYDAY=TU;BYHOUR=10",
});

// Quarterly on the 1st at 9am
scheduler.register({
  id: "quarterly-review",
  rrule: "FREQ=MONTHLY;INTERVAL=3;BYMONTHDAY=1;BYHOUR=9",
});

// Every second Monday
scheduler.register({
  id: "sprint-planning",
  rrule: "FREQ=WEEKLY;INTERVAL=2;BYDAY=MO;BYHOUR=9;BYMINUTE=30",
});
```
