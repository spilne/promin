# @promin/workflow

Durable workflow execution with DAG steps, compensation, distributed workers, and state machines.

## Install

```bash
bun add @promin/workflow
```

## Quick Example

```typescript
import { createWorkflowRunner, workflow } from "@promin/workflow";
import { Pipeline } from "@promin/core";
import { migrate, PostgresWorkflowStorage } from "@promin/postgres";

await migrate(db);
const storage = await PostgresWorkflowStorage.create({ db });
const runner = createWorkflowRunner({ storage });

const onboardUser = workflow<{ userId: string }>({ name: "onboard-user" })
  .step("fetch", ({ input }) => api.get(`/users/${input.userId}`, UserSchema))
  .step("enrich", { dependsOn: ["fetch"] }, ({ deps }) => enrichUser(deps.fetch))
  .step("notify", { dependsOn: ["enrich"] }, ({ deps }) =>
    Pipeline.succeed(`Welcome ${deps.enrich.name}!`),
  )
  .build();

const result = await runner.run({
  workflow: onboardUser,
  workflowId: "wf_1",
  input: { userId: "u_42" },
});
```

Steps with independent dependencies run in parallel automatically. Each step is checkpointed — if the process crashes, the workflow resumes from the last completed step.

For non-durable use cases (scripts, request handlers), use `flow()` with the same API but no storage requirement.

## Fluent Authoring

```typescript
import { createWorkflowApp, step, stepOptions, workflow } from "@promin/workflow";

const normalizeUser = step<{ name: string }>("trim", async ({ input }) =>
  input.name.trim(),
).andThen("upper", async ({ prev }) => prev.toUpperCase());

const onboardUser = workflow<{ name: string }>({ name: "onboard-user" })
  .use(normalizeUser)
  .parallel("enrich", {
    profile: async ({ prev }) => profiles.lookup(prev),
    permissions: async ({ prev }) => permissions.lookup(prev),
  })
  .approval("manager", { signalName: "manager-approved" })
  .step(
    "finish",
    async ({ prev }) => ({ approved: prev.approved }),
    stepOptions().timeout(30_000).retry({ maxRetries: 3, baseDelayMs: 500 }).build(),
  )
  .build();

const app = createWorkflowApp({ storage });
const onboard = app.workflow(onboardUser);
await onboard.run({ workflowId: "onboard-1", input: { name: " Ada " } });
```

## Features

- **DAG scheduling** — declare step dependencies, auto-parallel execution
- **Checkpoint/resume** — survives crashes, resumes from last completed step
- **Compensation** — rollback completed steps on failure
- **Distributed workers** — run workflow steps across multiple instances
- **State machines** — model complex stateful processes
- **Type-safe** — full TypeScript inference across steps and dependencies

## Proxy-style activity binding

For journaled steps with a static set of activities, `ctx.proxy()` collapses the per-call `ctx.activity("name", () => fn(args))` boilerplate. The proxy returns a typed binder where each method names itself from the property key and forwards arguments into a journal-recorded call:

```typescript
.journaled("checkout", function* (ctx) {
  const acts = ctx.proxy({
    validate: (orderId: number) => api.validate(orderId),
    charge:   (validated: Order)  => api.charge(validated),
    ship:     (charged: Charge)   => api.ship(charged),
  });

  const order   = yield* acts.validate(ctx.input.orderId);
  const charged = yield* acts.charge(order);
  return yield* acts.ship(charged);
});
```

Each call expands to `ctx.activity(<key>, () => fn(...args))` — same journal entries, same replay semantics, same retry/compensation surface. Pass `defaultOptions` for retry/codec/idempotent that should apply across the proxy, or `optionsByName` for per-key overrides:

```typescript
const acts = ctx.proxy(
  { fetch: api.fetch, mutate: api.mutate },
  {
    defaultOptions: { idempotent: true },
    optionsByName: { mutate: { idempotent: false } },
  },
);
```

The longhand `ctx.activity(...)` form stays available for cases the proxy doesn't fit — dynamic activity names, the 3-arg form for payload-hash determinism checks.

## Documentation

- **[Versioning guide](./versioning.md)** — strict / drain / `ctx.patched()` / rolling worker deploys, with runnable examples in [`examples/versioning/`](./examples/versioning)
- Full docs: [packages/workflow](https://github.com/spilne/promin/tree/main/packages/workflow)
