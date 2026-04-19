// ---------------------------------------------------------------------------
// Example 02 — Drain policy with inline previousVersions
//
// When you deploy v2, existing v1 workflows shouldn't silently run under v2
// code (strict) but also shouldn't be force-restarted. The drain policy
// lets each workflow run under the definition it was started with. v2 code
// holds a reference to v1's definition via `previousVersions` and delegates
// v1 resumes automatically.
//
// This is the simplest pattern for a 2-version deploy. For many versions,
// see the registry example (03).
//
// Run: bun run packages/workflow/examples/versioning/02-drain-inline.ts
// ---------------------------------------------------------------------------

import { Pipeline } from "@promin/core";
import { workflow, InMemoryWorkflowStorage } from "@promin/workflow";

async function main(): Promise<void> {
  const storage = new InMemoryWorkflowStorage();

  // Build v1 and keep the `.build()` result as a reference we can pass later.
  const v1 = workflow<{ amount: number }>({ name: "billing", version: "1" })
    .step("charge", ({ input }) => Pipeline.succeed({ charged: input.amount, v: "1" }))
    .build();

  // Start a v1 workflow. This row is stamped `version: "1"` and runs v1 code.
  await v1.bind(storage).run({ workflowId: "invoice-A", input: { amount: 100 } });

  // "Deploy" v2. Note: the same v1 instance above is listed in previousVersions.
  // New workflows will use v2's code; resumes of existing v1 rows delegate to v1.
  const v2 = workflow<{ amount: number }>({
    name: "billing",
    version: "2",
    onVersionMismatch: "drain",
    previousVersions: [v1],
  })
    .step("charge", ({ input }) => Pipeline.succeed({ charged: input.amount * 1.1, v: "2" }))
    .bind(storage);

  // Re-running the v1 workflow under v2 code — drain delegates to v1.
  const resumed = await v2.run({ workflowId: "invoice-A", input: { amount: 100 } });
  console.log("Resumed v1 workflow:", resumed);

  // Starting a fresh workflow — v2's code runs (new rows get version "2").
  const fresh = await v2.run({ workflowId: "invoice-B", input: { amount: 200 } });
  console.log("Fresh v2 workflow:", fresh);

  // Takeaway: v1 definitions live in the codebase until drain completes.
  // Remove them once `countByVersion` shows zero v1 in-flight workflows.
}

if (import.meta.main) {
  await main();
}
