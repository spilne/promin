# Versioning workflows

How to evolve workflow code without breaking in-flight workflows.

All code in this doc is backed by working examples in
[`examples/versioning/`](./examples/versioning) — typechecked on every
commit and runnable via `bun run <path>`.

## The core problem

Durable workflows can live for minutes, days, or months. If you deploy new
code while a workflow is partway through execution, the new code must either:

1. **Refuse to resume** the old workflow — require operator action
2. **Continue with the old code** — let in-flight workflows drain
3. **Adapt in place** — one code file, two paths keyed on stored version

Promin supports all three via opt-in primitives.

## Primitives at a glance

| Primitive                                                 | What it does                                                                     | When to use                                          |
| --------------------------------------------------------- | -------------------------------------------------------------------------------- | ---------------------------------------------------- |
| `version: "2"` on workflow config                         | Stamps every new workflow row with this version                                  | Every production workflow                            |
| `onVersionMismatch: "strict"` (default)                   | Throws `WorkflowVersionMismatchError` on resume when stored version != current   | Safe default — makes drift impossible to miss        |
| `onVersionMismatch: "drain"` + `previousVersions: [v1]`   | Delegates resume to the stored version's definition                              | Letting in-flight workflows finish on old code       |
| `patches: ["X"]` + `ctx.patched("X")` in a journaled body | Inline branches in the same code file, keyed on the currently-running definition | Small code tweaks that don't need a full v1/v2 split |
| `InMemoryWorkflowVersionRegistry` + `run({ name })`       | Central place holding many versions of workflows, with drain events              | Long-lived workflows with 3+ coexisting versions     |
| `supportedVersions: ["1", "2"]` on `createWorker`         | Worker claims only tasks whose version is in the allow-list                      | Rolling distributed deploys                          |

## Pattern 1 — Strict policy (default)

Add `version` to your workflow config. Attempts to resume an older-version row
fail loudly with `WorkflowVersionMismatchError`.

```typescript
import { createWorkflowRunner, InMemoryWorkflowStorage, workflow } from "@promin/workflow";
import { succeed } from "@spilne/perfect-core";

const billing = workflow<{ amount: number }>({ name: "billing", version: "1" })
  .step("charge", ({ input }) => succeed({ charged: input.amount }))
  .build();

const runner = createWorkflowRunner({ storage: new InMemoryWorkflowStorage() });
await runner.run({ workflow: billing, workflowId: "invoice-001", input: { amount: 100 } });
```

If you deploy v2 and run it (`runner.run({ workflow: v2, workflowId })`) on an
existing v1 row, you get:

```
Workflow "invoice-001" was created with version "1" but current code is
version "2". To resume this workflow, either use `onVersionMismatch:
"drain"` + `previousVersions: [v1]` on the workflow config, or register
both versions in a WorkflowVersionRegistry.
```

The error message points at the fix. You have three options:

- Upgrade this specific workflow manually (rare; use `force: true`)
- Switch to drain policy (see Pattern 2 / 3)
- Wait for the v1 workflow to complete naturally under v1 code

**Full example**: [`examples/versioning/01-strict-policy.ts`](./examples/versioning/01-strict-policy.ts)

## Pattern 2 — Drain with inline `previousVersions`

For a 2-version deploy (the common case), v2's config lists v1 in
`previousVersions`. The engine delegates resumes of v1 rows to v1's
definition while using v2's code for fresh workflows.

```typescript
import { workflow } from "@promin/workflow";
import { succeed } from "@spilne/perfect-core";

const v1 = workflow<{ amount: number }>({ name: "billing", version: "1" })
  .step("charge", ({ input }) => succeed({ charged: input.amount, v: "1" }))
  .build();

const v2 = workflow<{ amount: number }>({
  name: "billing",
  version: "2",
  onVersionMismatch: "drain",
  previousVersions: [v1],
})
  .step("charge", ({ input }) => succeed({ charged: input.amount * 1.1, v: "2" }))
  .build();

// runner.run({ workflow: v2, ... }) against an existing v1 row → runs v1's code
// runner.run({ workflow: v2, ... }) against a new workflowId   → runs v2's code
```

v1's definition stays in the codebase until all v1 workflows complete. Use
a version registry's `countByVersion({ name, storage })` to monitor drain progress.

**Full example**: [`examples/versioning/02-drain-inline.ts`](./examples/versioning/02-drain-inline.ts)

## Pattern 3 — Registry for 3+ coexisting versions

When you have more than 2-3 coexisting versions, managing `previousVersions`
arrays gets unwieldy. Register every version in an
`InMemoryWorkflowVersionRegistry` (or the Postgres-backed
`PostgresWorkflowVersionRegistry`), give it to the runner, and run by name:
the registry resolves fresh runs to the latest version and resumes to the
stored one, and fires `onDrained` when a version's in-flight count reaches
zero.

```typescript
import {
  createWorkflowRunner,
  InMemoryWorkflowStorage,
  InMemoryWorkflowVersionRegistry,
  ScopedWorkflowVersionRegistry,
  workflow,
} from "@promin/workflow";
import { succeed } from "@spilne/perfect-core";

const job = (version: string) =>
  workflow<{ x: number }>({ name: "job", version })
    .step("run", ({ input }) => succeed(`v${version}-${input.x}`))
    .build();

const registry = new InMemoryWorkflowVersionRegistry({
  autoDeregister: true,
  onDrained: (_name, version) => console.log(`version "${version}" has drained`),
});
await registry.register(job("1"));
await registry.register(job("2"));
await registry.register(job("3"));

const storage = new InMemoryWorkflowStorage();
const runner = createWorkflowRunner({ storage, registry });

// New workflows use the latest (v3). Resumes delegate to the stored version.
await runner.run({ name: "job", workflowId: "j-42", input: { x: 1 } });

// Operational monitoring:
const counts = await registry.countByVersion({ name: "job", storage });
// → Map<version, { running, completed, failed, tripwire }>

// One workflow's versions only: a scoped view with a fluent register.
const jobs = new ScopedWorkflowVersionRegistry({ registry, name: "job" });
console.log(await jobs.versions(), counts);
```

`autoDeregister: true` removes drained versions from the registry automatically,
except the latest registered and the promoted active version. `onDrained` fires
once per drain (again if new runs appear and drain later). Terminal runs
(`completed`, `failed`, `tripwire`) never hold a version open.

**Full example**: [`examples/versioning/03-drain-registry.ts`](./examples/versioning/03-drain-registry.ts)

## Pattern 4 — Inline branches with `ctx.patched()`

Drain lets two versions coexist, but each version has its own code file.
For SMALL changes — a tweak to post-activity logic, a conditional on the
output — you can keep one code file and branch on `ctx.patched()` inside
a journaled step body.

```typescript
import { workflow, type JournaledStepBody } from "@promin/workflow";

type Input = { amount: number };

const calculateTotal: JournaledStepBody<Input, Input, number> = function* (ctx, prev) {
  const rate = yield* ctx.activity("fetch-rate", async () => 1.0);
  if (ctx.patched("new-pricing")) {
    return yield* ctx.activity("apply-new", async () => prev.amount * rate * 1.1);
  }
  return yield* ctx.activity("apply-legacy", async () => prev.amount * rate);
};

// v1 — patches empty, takes the legacy branch
const v1 = workflow<Input>({ name: "billing", version: "1", patches: [] })
  .journaled("calculate", calculateTotal)
  .build();

// v2 — patch active, takes the new branch; v1 rows drain on v1's definition
const v2 = workflow<Input>({
  name: "billing",
  version: "2",
  onVersionMismatch: "drain",
  previousVersions: [v1],
  patches: ["new-pricing"],
})
  .journaled("calculate", calculateTotal)
  .build();
```

### How `ctx.patched()` decides

`ctx.patched(name)` returns `patches.includes(name)` on the **currently-
running definition**. Drain ensures each stored version runs under its own
definition with its own patch list — no cross-version comparison logic.

- v1 workflow resumes → drain delegates to v1 def → `patches: []` → returns `false`
- v2 workflow (fresh) → runs v2 def → `patches: ["new-pricing"]` → returns `true`

### When `patched()` is insufficient

Structural changes (add/remove/rename an activity, change a step body's
yield order) break replay even with patches. The journal determinism check
throws `JournalNonDeterminismError`. For structural changes, use drain with
a separate definition (Pattern 2 or 3).

Rule of thumb:

- Post-activity logic change → `patched` works
- Add/remove/rename a yielded activity → needs drain with separate definition
- Add/remove/rename a DAG step → needs drain with separate definition

### Typo detection

`ctx.patched("unkown-name")` returns `false` silently — this is load-bearing
for the cross-version pattern (v1 with `patches: []` MUST return false for
every patch name). Keep patch names in shared constants to avoid typos.

### Escape hatch for custom comparison

`ctx.workflowVersion` exposes the stored workflow version. If you need
semver or date-based comparison logic, build it in user space:

```typescript
import type { JournaledContext } from "@promin/workflow";

declare const semver: { gte(a: string, b: string): boolean };

function introducedIn(ctx: JournaledContext<unknown, unknown>, version: string): boolean {
  return ctx.workflowVersion != null && semver.gte(ctx.workflowVersion, version);
}
```

**Full example**: [`examples/versioning/04-ctx-patched.ts`](./examples/versioning/04-ctx-patched.ts)

## Pattern 5 — Versioned workers (rolling distributed deploy)

In a distributed deploy, the coordinator enqueues step tasks and workers
claim them. During a rolling deploy, some workers support v1, some v2, and
some both. Every task is tagged with its workflow's version; workers declare
their `supportedVersions` allow-list.

```typescript
import { createWorker, InMemoryStepQueue, MapStepRegistry } from "@promin/workflow/distributed";

const registry = new MapStepRegistry();
registry.register({ stepName: "process", handler: async (ctx) => `handled ${ctx.workflowId}` });

const worker = createWorker({
  stepQueue: new InMemoryStepQueue(),
  registry,
  supportedVersions: ["1", "2"], // handle both during the drain window
});
void worker.start();
```

Tasks whose version isn't in `supportedVersions` are left pending for a
compatible worker. Unversioned tasks (from workflows without a `version`) are
always accepted.

### Rolling deploy recipe

1. Deploy v2 workers with `supportedVersions: ["1", "2"]`. They handle both
   in-flight v1 workflows and fresh v2 ones.
2. Monitor `registry.countByVersion({ name, storage })` until v1 reports
   `running: 0` (pending, running, suspended and compensating runs all count
   as running).
3. Next deploy drops `supportedVersions` to `["2"]`. Any straggler v1 tasks
   stay pending (safe by construction) until you clean them up or they expire.

**Full example**: [`examples/versioning/05-versioned-workers.ts`](./examples/versioning/05-versioned-workers.ts)

## The freeze-drain-deploy runbook

Standard upgrade sequence for a production workflow:

1. **Freeze** — stop starting new workflows on v1 (feature flag, deploy
   gate, or just stop the code path that creates them).
2. **Drain** — keep v1 definition in the codebase. New workflows run v2.
   In-flight v1 workflows resume on v1 code (via drain policy or registry).
3. **Monitor** — watch `countByVersion` / `onDrained` until v1's in-flight
   count hits zero.
4. **Deploy** — remove v1 definition. Next deploy ships only v2.

Total time: weeks for long-running workflows (regulatory cooling-off,
daily ETLs, etc.), minutes for short ones. Drain is the normal state during
this window; there's no "emergency" about it.

## Troubleshooting errors

### `WorkflowVersionMismatchError`

```
Workflow "abc123" was created with version "1" but current code is version "2".
To resume this workflow, either use `onVersionMismatch: "drain"` +
`previousVersions: [v1]` on the workflow config, or register both versions
in a WorkflowVersionRegistry.
```

Your code's version moved ahead and you haven't told the engine how to
bridge the gap. Add drain policy or register both versions.

### `JournalNonDeterminismError`

```
journaled step "calculate" diverged at activity 2: expected "fetch-rate",
got "lookup-rate". This usually means workflow code changed between runs.
Either bump the workflow `version` (strict policy throws cleanly) or use
`onVersionMismatch: "drain"` + `previousVersions` to let in-flight
workflows finish on their original code.
```

A journaled step body changed structurally (activity rename, reorder,
insertion) between the stored run and the current replay. Bump `version`
and add drain policy — don't try to patch around it. Structural journal
changes need a version split.

## What's NOT supported

Honest about limits:

- **State machine versioning.** `StateMachineInstance` has a `version` field but no drain policy or patches equivalent.
- **Pluggable version comparison.** Equality only — the `patched()` design doesn't need comparison. Escape hatch: `ctx.workflowVersion` + user-space helper.
- **Content-addressed version auto-derivation.** You set `version` explicitly; framework doesn't hash your code.
- **Cross-workflow version pinning.** `.subworkflow(child)` runs the child definition the parent was built with (its `version` is stamped on the child row); an existing child row with another version follows the child's own `onVersionMismatch`. There is no separate pin between parent and child versions.
- **Typo detection for `ctx.patched()`.** Unknown patch names return `false` silently (load-bearing for the pattern).
- **Per-worker heartbeat of `supportedVersions`.** Coordinator can't detect "nobody claims v3 tasks" gaps.

## FAQ

**Q: Is "strict" or "drain" the right default?**
Strict. Unexpected version drift should fail loudly on your first deploy,
not silently corrupt running workflows. Switch to drain deliberately when
you're ready to manage multiple coexisting versions.

**Q: When should I bump the version?**
Any time you change workflow code that affects its DAG shape or journal
structure. For tiny non-structural changes (log message tweaks, constant
adjustments) you can skip the bump — but be sure it's actually
non-structural.

**Q: How do I version activities inside a journaled step?**
You don't — individual activities aren't versioned. The workflow's version
covers the whole body. For inline branches within one body, use `patches`.

**Q: Can I have multiple in-flight versions at once?**
Yes. Drain policy + `previousVersions` (or registry) is exactly this.
There's no artificial limit — v1 through v7 can all be running
simultaneously if you keep their definitions registered.

**Q: What happens to orphaned v1 tasks after I remove v1 workers?**
They stay pending in the step queue. Depending on your operational
tolerance, either (a) re-add v1 worker support temporarily, (b) manually
mark them failed via the storage API, or (c) wait for them to hit their
retry/timeout limits and fail naturally.
