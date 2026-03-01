# Durable Execution

DAG-based workflows with checkpoint/resume, type-safe steps, and structural concurrency.

## Quick Start

### `flow()` — non-durable (scripts, request handlers, compositions)

No storage, no workflowId. Same composition API as `workflow()`.

```typescript
import { flow, Pipeline } from "@ts-backend/core";

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
import { workflow, Pipeline } from "@ts-backend/core";
import { migrate, PostgresWorkflowStorage } from "@ts-backend/postgres";

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

## Planned: Subworkflows

Ergonomic child workflow composition with parent-child tracking.

### Layer 1: `invoke()` — the primitive

`WorkflowDefinition.invoke()` returns a `Pipeline` directly — no `Pipeline.fromPromise()` wrapping needed:

```typescript
const enrichUser = workflow<{ userId: string }>({ name: "enrich", storage })
  .step("fetch-profile", ({ input }) => api.get(`/profiles/${input.userId}`))
  .step("fetch-scores", ({ prev }) => api.get(`/scores/${prev.id}`))
  .build();

// Use inside any step — invoke() returns Pipeline<Output, E>
workflow<{ userId: string }>({ name: "onboard", storage })
  .step("create-account", ({ input }) => api.post("/accounts", { json: input }))
  .step("enrich", ({ prev }) =>
    enrichUser.invoke({
      workflowId: `enrich-${prev.accountId}`,
      input: { userId: prev.userId },
    })
  )
  .run({ ... });
```

`invoke()` vs `run()`:

- `run()` returns `Promise<Output>` — for imperative use
- `invoke()` returns `Pipeline<Output, E>` — composable, retryable, can chain `.timeout()`, `.retry()`, etc.
- `invoke()` automatically sets `parentWorkflowId` for tracking

### Layer 2: `.subworkflow()` — builder sugar

For the common "call a child workflow with data from the previous step" pattern:

```typescript
workflow<{ userId: string }>({ name: "onboard", storage })
  .step("create-account", ({ input }) => api.post("/accounts", { json: input }))
  .subworkflow("enrich", enrichUser, {
    input: (prev) => ({ userId: prev.userId }),
    workflowId: (prev) => `enrich-${prev.accountId}`,
  })
  .step("notify", ({ prev }) => Pipeline.succeed(`Enriched: ${prev.score}`))
  .run({ ... });
```

Equivalent to a `.step()` that calls `.invoke()` — but less boilerplate.

### Layer 3: Fan-out — mapOver + invoke

No new API needed. `invoke()` returns Pipeline, `mapOver` accepts Pipeline:

```typescript
const processItem = workflow<{ itemId: string }>({ name: "process-item", storage })
  .step("fetch", ({ input }) => api.get(`/items/${input.itemId}`))
  .step("transform", ({ prev }) => Pipeline.succeed(transform(prev)))
  .build();

workflow<{ items: string[] }>({ name: "batch", storage })
  .step("get-items", ({ input }) => Pipeline.succeed(input.items))
  .mapOver("process-all", { array: "get-items", concurrency: 10 }, (itemId) =>
    processItem.invoke({
      workflowId: `item-${itemId}`,
      input: { itemId },
    })
  )
  .run({ ... });
```

Each child workflow is independently durable — if the parent crashes, completed children don't re-run.

### Parent-Child Tracking

```typescript
// WorkflowState gains parentWorkflowId
interface WorkflowState {
  parentWorkflowId?: string;
  // ... existing fields
}

// Query children of a workflow
const children = await storage.listWorkflows({ parentId: "onboard-123" });

// Cascade cancel — cancel parent + all children
await storage.cancelWorkflow("onboard-123", { cascade: true });
```

### SubworkflowAsync — Promise convenience

```typescript
.subworkflowAsync("enrich", enrichUser, {
  input: async (prev) => {
    const extra = await fetchExtra(prev.id);
    return { userId: prev.userId, extra };
  },
  workflowId: (prev) => `enrich-${prev.accountId}`,
})
```

## Planned: Step Failure Strategies & Error Recovery

### The Problem

Currently, any step failure immediately fails the entire workflow. There's no way to:

- Retry a step at the workflow layer (re-execute from checkpoint)
- Skip a failed step and continue
- Run a fallback step
- Make a decision about what to do on failure
- Send to DLQ on terminal failure vs retry on transient failure

### Current Workarounds

You can handle errors **inside** the Pipeline using existing operators:

```typescript
// Retry + timeout + partial recovery inside the step
.step("fetch-user", ({ input }) =>
  api.get(`/users/${input.userId}`, UserSchema)
    .retry(3)
    .timeout(10_000)
    .recover(
      (err) => err._tag === "HttpStatusError" && err.status === 404,
      () => null,  // 404 → null, other errors propagate
    )
)

// DLQ via hooks (notification only — can't change outcome)
workflow<Input>({
  name: "process-order",
  storage,
  hooks: {
    onStepFailure: async ({ workflowId, stepName, error }) => {
      await monitoring.recordStepFailure({ workflowId, stepName, error });
    },
    onWorkflowFailure: async ({ workflowId, error }) => {
      await dlq.send({ workflowId, error, failedAt: new Date() });
      await slack.alert(`Workflow ${workflowId} failed: ${error}`);
    },
  },
})
```

### Planned: StepOptions.onFailure — per-step failure strategy

```typescript
.step("fetch-user", ({ input }) =>
  api.get(`/users/${input.userId}`, UserSchema),
  {
    // Retry the entire step (re-execute from storage)
    retry: { maxAttempts: 3, backoffMs: 1000 },

    // What to do when retries are exhausted
    onFailure: "fail",  // default — fail the whole workflow
    // OR
    onFailure: "skip",  // skip this step, continue with undefined
    // OR
    onFailure: { fallback: (error) => ({ id: 0, name: "Unknown" }) },
    // OR
    onFailure: { handler: async (error, ctx) => {
      if (isTransient(error)) return "retry";
      await dlq.send({ step: ctx.stepName, error });
      return "skip";
    }},
  },
)
```

### Planned: Error Decision Hooks — control flow on failure

```typescript
workflow<Input>({
  name: "resilient-pipeline",
  storage,
  hooks: {
    // Decision hook: return what to do (not just notify)
    onStepError: async ({ workflowId, stepName, error, attempt }) => {
      if (isTransient(error) && attempt < 3) {
        return { action: "retry", delayMs: attempt * 1000 };
      }
      if (isNonCritical(stepName)) {
        await monitoring.warn(`Skipping ${stepName}`, { error });
        return { action: "skip" };
      }
      await dlq.send({ workflowId, stepName, error });
      await pagerduty.alert({ workflowId, error });
      return { action: "fail" };
    },
  },
});
```

### Planned: Compensation — rollback on failure

Saga pattern: when a step fails, run compensating actions for completed steps in reverse:

```typescript
workflow<{ orderId: string }>({ name: "place-order", storage })
  .step("reserve-inventory", ({ input }) =>
    api.post("/inventory/reserve", { json: { orderId: input.orderId } }),
    { compensate: (result) => api.post("/inventory/release", { json: { reservationId: result.id } }) },
  )
  .step("charge-payment", ({ prev }) =>
    api.post("/payments/charge", { json: { amount: prev.total } }),
    { compensate: (result) => api.post("/payments/refund", { json: { paymentId: result.id } }) },
  )
  .step("send-confirmation", ({ prev }) =>
    mailer.send(prev.email, "Order confirmed!"),
    // No compensation needed for emails
  )
  .run({ ... });

// If "charge-payment" fails:
// 1. "send-confirmation" never ran — nothing to compensate
// 2. "reserve-inventory" succeeded — compensate() runs → inventory released
// 3. Workflow marked as failed with compensation log
```

### Planned: DLQ Integration

First-class dead letter queue support, not just hook side-effects:

```typescript
import { PgmqQueue } from "@ts-backend/postgres";

const dlq = await PgmqQueue.create(db, "workflow-dlq");

workflow<Input>({
  name: "process-events",
  storage,
  dlq, // failed workflows automatically sent here
  dlqOptions: {
    // What gets sent: full state, input, error, step history
    include: ["input", "error", "steps"],
    // When: after all retries exhausted (not on first failure)
    when: "terminal-failure",
  },
});
```

Query and replay from DLQ:

```typescript
// Read failed workflows from DLQ
await StreamPipeline.fromAck(dlq).forEach(async (envelope) => {
  const failed = envelope.value;
  // Fix and retry
  await processEvents.run({
    workflowId: `${failed.workflowId}-retry`,
    input: failed.input,
  });
  await envelope.ack();
});
```

### Planned: RRULE Support for Complex Recurrence

Currently `ScheduleConfig` supports `cron` (5/6/7-field) and `intervalMs`. These can't express:

- **Biweekly** — every 2 weeks on Tuesday
- **Every N weeks/months** — every 3rd month on the 1st
- **Every other weekday** — every other Monday

Google Calendar, Outlook, and iCalendar use [RRULE (RFC 5545)](https://datatracker.ietf.org/doc/html/rfc5545#section-3.3.10) for these patterns. Plan is to add `rrule` field to `ScheduleConfig` using the [rrule](https://github.com/jkbrzt/rrule) library:

```typescript
scheduler.register({
  id: "biweekly-standup",
  rrule: "FREQ=WEEKLY;INTERVAL=2;BYDAY=TU;BYHOUR=10", // iCalendar RRULE
  timezone: "America/New_York",
});

scheduler.register({
  id: "quarterly-review",
  rrule: "FREQ=MONTHLY;INTERVAL=3;BYMONTHDAY=1;BYHOUR=9",
});
```

Three trigger types in `ScheduleConfig`:

- `cron` — standard cron (simple schedules)
- `rrule` — iCalendar RRULE (complex calendar patterns)
- `intervalMs` — fixed interval (heartbeats, polling)
