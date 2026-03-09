# Durable Execution

DAG-based workflows with checkpoint/resume, type-safe steps, and structural concurrency.

## Quick Start

### `flow()` — non-durable (scripts, request handlers, compositions)

No storage, no workflowId. Same composition API as `workflow()`.

```typescript
import { flow, Pipeline } from "@promin/core";

// Simple linear chain
const result = await flow<{ userId: string }>("process-user")
  .step("fetch", ({ input }) => api.get(`/users/${input.userId}`, UserSchema))
  .stepAsync("enrich", async ({ prev }) => enrichUser(prev))
  .step("format", ({ prev }) => Pipeline.succeed(`${prev.name} (${prev.score})`))
  .execute({ userId: "u_42" });

// DAG with auto-parallel
const report = await flow<{ text: string }>("analyze")
  .step("parse", ({ input }) => Pipeline.succeed(input.text))
  .step("summarize", { dependsOn: ["parse"] }, ({ deps }) => summarize(deps.parse))
  .step("keywords", { dependsOn: ["parse"] }, ({ deps }) => extractKeywords(deps.parse))
  .step("publish", { dependsOn: ["summarize", "keywords"] }, ({ deps }) =>
    Pipeline.succeed({ summary: deps.summarize, keywords: deps.keywords }),
  )
  .execute({ text: "..." });

// Error handling
const { data, error } = await flow<{ url: string }>("fetch")
  .step("download", ({ input }) => httpClient.get(input.url, Schema))
  .executeSafe({ url: "https://example.com" });
```

To make it durable later, change `flow("name")` to `workflow({ name, storage })` and `.execute(input)` to `.run({ workflowId, input })`.

### `workflow()` — durable (survives crashes, resumes from checkpoints)

```typescript
import { workflow, Pipeline } from "@promin/core";
import { migrate, PostgresWorkflowStorage } from "@promin/postgres";

await migrate(db);
const storage = await PostgresWorkflowStorage.create({ db });

const result = await workflow<{ userId: string }>({
  name: "onboard-user",
  storage,
  type: "onboarding",
  metadata: { team: "growth" },
})
  .step("fetch", ({ input }) => api.get(`/users/${input.userId}`, UserSchema))
  .step("provision", ({ prev }) => api.post("/accounts", AccountSchema, { json: prev }))
  .stepAsync("notify", async ({ prev }) => {
    await mailer.send(prev.email, "Welcome!");
    return { notified: true };
  })
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
.step("get-urls", ({ input }) => Pipeline.succeed(input.urls))
.mapOver("fetch-all", { array: "get-urls", concurrency: 5 }, (url, ctx) =>
  Pipeline.succeed(`Response from ${url}`)
)
```

### branch — Conditional paths

```typescript
.branch("classify", {
  condition: (n) => n > 10,
  ifTrue: ({ prev }) => Pipeline.succeed(`big: ${prev}`),
  ifFalse: ({ prev }) => Pipeline.succeed(`small: ${prev}`),
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

### build + trigger — Stream → Workflow

```typescript
const analyzeArticle = workflow<{ url: string }>({ name: "analyze", storage })
  .step("scrape", ({ input }) => scraper.get(input.url))
  .step("summarize", ({ prev }) => ai.summarize(prev))
  .build();

// Trigger from any stream
await eventStream
  .through(
    trigger({
      workflow: analyzeArticle,
      toInput: (event) => ({ url: event.data }),
      toWorkflowId: (event) => `analyze-${event.id}`,
      concurrency: 5,
      onDuplicate: "skip",
    }),
  )
  .filter(WorkflowResult.isCompleted)
  .forEach((r) => log(r.result));
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
  // OR
  onFailure: { handler: (error) => "retry" | "skip" | "fail" },  // dynamic decision
})
```

### Workflow-level Retry

Re-runs from the failed step — completed steps are checkpointed and skipped.

```typescript
workflow<Input>({
  name: "resilient",
  storage,
  retry: { maxRetries: 3, baseDelayMs: 5000 },
})
  .step("step-1", fn) // runs once, checkpointed
  .step("step-2", fn); // if this fails, workflow retries from here
```

### Saga Compensation

When a step fails, automatically undo completed steps in reverse order.

```typescript
workflow<{ from: string; to: string; amount: number }>({
  name: "transfer",
  storage,
  retry: { maxRetries: 2, baseDelayMs: 5000 },
  compensate: {
    trigger: "after-retries", // compensate after all workflow retries exhausted (default)
    // trigger: "immediate",      // compensate on first failure, skip workflow retries
    retry: { maxRetries: 2 }, // retry failing compensation functions
    onComplete: ({ input, error, compensatedSteps, failedCompensations }) =>
      Pipeline.fromPromise(() => audit.log("rollback", { compensatedSteps, error })),
  },
})
  .step("debit", ({ input }) => bankClient.debit(input.from, input.amount), {
    compensate: ({ result }) => bankClient.refund(result.txId),
  })
  .step("credit", ({ input }) => bankClient.credit(input.to, input.amount), {
    compensate: ({ result }) => bankClient.reverseCredit(result.txId),
  })
  .step("notify", ({ prev }) => emailClient.send(prev.receipt))
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

### Observability

```typescript
// Lifecycle hooks
workflow<Input>({
  name,
  storage,
  hooks: {
    onStepComplete: ({ stepName, result, durationMs }) => metrics.record(durationMs),
    onStepFailure: ({ stepName, error }) => alerting.notify(error),
    onWorkflowComplete: ({ workflowId, durationMs }) => log.info("done", { durationMs }),
    onWorkflowFailure: ({ workflowId, error }) => log.error("failed", { error }),
  },
});

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
import { compileWorkflow, MapActivityRegistry, Pipeline } from "@promin/core";

const registry = new MapActivityRegistry({
  "http.get": (config) => () => httpClient.get({ url: config?.url as string }),
  "transform.uppercase": () => (ctx) => Pipeline.succeed(String(ctx.prev).toUpperCase()),
  "db.insert": (config) => (ctx) => Pipeline.fromPromise(() => db.insert(config?.table, ctx.prev)),
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
import { validateWorkflowSchema } from "@promin/core";

const schema = validateWorkflowSchema(req.body); // throws ZodError on invalid
```

### Subworkflows

Child workflow composition with parent-child tracking.

```typescript
const enrichUser = workflow<{ userId: string }>({ name: "enrich", storage })
  .step("fetch", ({ input }) => api.get(`/profiles/${input.userId}`))
  .build();

// .subworkflow() — builder sugar
workflow<{ userId: string }>({ name: "onboard", storage })
  .step("create", ({ input }) => api.post("/accounts", { json: input }))
  .subworkflow("enrich", enrichUser, {
    input: (prev) => ({ userId: prev.id }),
    workflowId: (prev) => `enrich-${prev.id}`,
  })
  .step("notify", ({ prev }) => Pipeline.succeed(`Score: ${prev.score}`))
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

Failed workflows (after all retries + compensation) are published to a configurable DLQ. Works with any `Sinkable<FailedWorkflowRecord>` — PgQueue, PgmqQueue, or custom.

```typescript
import { PgQueue } from "@promin/postgres";

const dlq = await PgQueue.create<FailedWorkflowRecord>(db, "workflow-dlq");

workflow<{ orderId: string }>({
  name: "process-order",
  storage,
  retry: { maxRetries: 3 },
  dlq,
})
  .step("charge", fn)
  .step("fulfill", fn)
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
