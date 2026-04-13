# @promin/workflow

Durable workflow execution with DAG steps, compensation, distributed workers, and state machines.

## Install

```bash
bun add @promin/workflow
```

## Quick Example

```typescript
import { workflow, Pipeline } from "@promin/core";
import { migrate, PostgresWorkflowStorage } from "@promin/postgres";

await migrate(db);
const storage = await PostgresWorkflowStorage.create({ db });

const result = await workflow<{ userId: string }>({
  name: "onboard-user",
  storage,
})
  .step("fetch", ({ input }) => api.get(`/users/${input.userId}`, UserSchema))
  .step("enrich", { dependsOn: ["fetch"] }, ({ deps }) => enrichUser(deps.fetch))
  .step("notify", { dependsOn: ["enrich"] }, ({ deps }) =>
    Pipeline.succeed(`Welcome ${deps.enrich.name}!`),
  )
  .run({ workflowId: "wf_1", input: { userId: "u_42" } });
```

Steps with independent dependencies run in parallel automatically. Each step is checkpointed — if the process crashes, the workflow resumes from the last completed step.

For non-durable use cases (scripts, request handlers), use `flow()` with the same API but no storage requirement.

## Features

- **DAG scheduling** — declare step dependencies, auto-parallel execution
- **Checkpoint/resume** — survives crashes, resumes from last completed step
- **Compensation** — rollback completed steps on failure
- **Distributed workers** — run workflow steps across multiple instances
- **State machines** — model complex stateful processes
- **Type-safe** — full TypeScript inference across steps and dependencies

## Documentation

Full docs and examples: [packages/workflow](https://github.com/spilne/promin/tree/main/packages/workflow)
