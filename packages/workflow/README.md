# @promin/workflow

Durable workflow execution with DAG steps, compensation, distributed workers, and state machines.

## Install

```bash
bun add @promin/workflow
```

## Quick Example

```typescript
import { workflow } from "@promin/workflow";
import { succeed } from "@spilne/perfect-core";
import { migrate, PostgresWorkflowStorage } from "@promin/postgres";

await migrate(db);
const storage = await PostgresWorkflowStorage.create({ db });

const result = await workflow<{ userId: string }>({
  name: "onboard-user",
  storage,
})
  .step("fetch", ({ input }) => api.get(`/users/${input.userId}`, UserSchema))
  .step("enrich", { dependsOn: ["fetch"] }, ({ deps }) => enrichUser(deps.fetch))
  .step("notify", { dependsOn: ["enrich"] }, ({ deps }) => succeed(`Welcome ${deps.enrich.name}!`))
  .run({ workflowId: "wf_1", input: { userId: "u_42" } });
```

Steps with independent dependencies run in parallel automatically. Each step is checkpointed — if the process crashes, the workflow resumes from the last completed step.

For non-durable use cases (scripts, request handlers), use `flow()` with the same API but no storage requirement.

## Step bodies

`.step()` takes a function returning a [perfect](https://github.com/spilne/perfect) `Eff<A, Throws<E>>`. Typed failures (`E`) are what step `retry` and `onFailure` act on. `.stepAsync()` takes a Promise-returning function instead; its rejections are defects, so they fail the step without being retried.

```typescript
import { tryPromise } from "@spilne/perfect-core";

wf.step("charge", (ctx) => tryPromise(() => stripe.charge(ctx.input), toPaymentError), {
  retry: { maxRetries: 3, when: isTransient },
});
wf.stepAsync("notify", async (ctx) => {
  await mailer.send(ctx.input.email);
});
```

`runner.run()` rejects with the step's own error (or the thrown value for a defect) — never a wrapper.

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

## Journaled failures and signal outcomes

- An activity that still fails after its own `retry` is journaled. While the step attempt is in flight (suspended on `ctx.sleep` / `ctx.signal`, or re-driven after a crash), replay rethrows an error of the same kind: `TerminalError` and the other engine errors come back as their class, other tagged errors keep their `_tag`, `name` and fields.
- When a failure escapes the body, the attempt is over: after the compensation unwind, recorded failures and rolled-back activities are discarded, so the next attempt (a retry or a resume) runs them again. Successful activities that were not rolled back replay and never run twice.
- A signal delivery and the signal's timeout race for the same journal entry; the first to complete it wins, and the live run and every replay take that outcome. `completeSignal` returns `false` when it lost.

## Documentation

- **[Versioning guide](./versioning.md)** — strict / drain / `ctx.patched()` / rolling worker deploys, with runnable examples in [`examples/versioning/`](./examples/versioning)
- Full docs: [packages/workflow](https://github.com/spilne/promin/tree/main/packages/workflow)
