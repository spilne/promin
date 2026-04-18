// ---------------------------------------------------------------------------
// Example 04 — ctx.patched() for inline version branches
//
// Sometimes you want the SAME code file to behave differently across
// versions — e.g., "v2 doubles prices, v1 uses the original formula." Instead
// of maintaining two separate definitions, declare a patch name on the v2
// config and branch on `ctx.patched()` inside the journaled body.
//
// With drain + patches:
// - v1 workflows run under v1 code (patches: []) → ctx.patched() returns false
// - v2 workflows run under v2 code (patches: ["new-pricing"]) → returns true
//
// Same body, different behavior per stored version. Works because drain
// ensures each workflow runs under ITS OWN definition's patch list.
//
// Run: bun run packages/workflow/examples/versioning/04-ctx-patched.ts
// ---------------------------------------------------------------------------

import { Pipeline } from "@promin/core";
import { workflow, InMemoryWorkflowStorage, type JournaledContext } from "@promin/workflow";

async function main(): Promise<void> {
  const storage = new InMemoryWorkflowStorage();

  // Shared body — no separate v1/v2 files. The patch list on each version
  // drives the branch.
  const calculateTotal = function* (
    ctx: JournaledContext<{ amount: number }, { amount: number }>,
    prev: { amount: number },
  ) {
    const base = yield* ctx.activity("fetch-rate", async () => 1.0);
    if (ctx.patched("new-pricing")) {
      return yield* ctx.activity("apply-new-pricing", async () => ({
        total: prev.amount * base * 1.1,
        version: "2",
      }));
    } else {
      return yield* ctx.activity("apply-legacy-pricing", async () => ({
        total: prev.amount * base,
        version: "1",
      }));
    }
  };

  // v1: patches list is empty.
  const v1 = workflow<{ amount: number }>({
    name: "billing-journaled",
    storage,
    version: "1",
    patches: [],
  })
    .step("load", ({ input }) => Pipeline.succeed(input))
    .journaled("calculate", calculateTotal)
    .build();

  // v2: patch "new-pricing" is active. Drain policy delegates v1 rows to v1.
  const v2 = workflow<{ amount: number }>({
    name: "billing-journaled",
    storage,
    version: "2",
    onVersionMismatch: "drain",
    previousVersions: [v1],
    patches: ["new-pricing"],
  })
    .step("load", ({ input }) => Pipeline.succeed(input))
    .journaled("calculate", calculateTotal);

  // Start a v1 workflow — runs v1 code, patches: [], takes the else branch.
  const v1Result = await v1.run({ workflowId: "bill-A", input: { amount: 100 } });
  console.log("v1 result (legacy pricing):", v1Result);

  // Fresh v2 workflow — runs v2 code, patches: ["new-pricing"], takes the if branch.
  const v2Result = await v2.run({ workflowId: "bill-B", input: { amount: 100 } });
  console.log("v2 result (new pricing):", v2Result);

  // ctx.workflowVersion is also available for custom logic (e.g. semver
  // comparisons in user space). See the framework docs for details.
}

if (import.meta.main) {
  await main();
}
