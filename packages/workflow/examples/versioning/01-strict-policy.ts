// ---------------------------------------------------------------------------
// Example 01 — Strict policy (default)
//
// Every workflow definition with a `version` stamps its workflowId rows with
// that version. When you deploy v2 code and try to re-run a v1 row, the
// engine throws `WorkflowVersionMismatchError`. This is the SAFE default —
// version drift can't silently break your workflows.
//
// Run: bun run packages/workflow/examples/versioning/01-strict-policy.ts
// ---------------------------------------------------------------------------

import { Pipeline } from "@promin/core";
import {
  workflow,
  InMemoryWorkflowStorage,
  WorkflowVersionMismatchError,
  createWorkflowRunner,
} from "@promin/workflow";

async function main(): Promise<void> {
  const storage = new InMemoryWorkflowStorage();
  const runner = createWorkflowRunner({ storage });

  // v1 of the workflow — stamps new rows with version "1".
  const v1 = workflow<{ amount: number }>({ name: "billing", version: "1" })
    .step("charge", ({ input }) => Pipeline.succeed({ charged: input.amount }))
    .build();

  // Start a v1 workflow — succeeds.
  const v1Result = await runner.run({
    workflow: v1,
    workflowId: "invoice-001",
    input: { amount: 100 },
  });
  console.log("v1 result:", v1Result);

  // Now "deploy" v2 of the workflow — same workflowId, different definition.
  // Strict policy (the default) refuses to resume.
  const v2 = workflow<{ amount: number }>({ name: "billing", version: "2" })
    .step("charge", ({ input }) => Pipeline.succeed({ charged: input.amount * 1.1, v: "2" }))
    .build();

  try {
    await runner.run({ workflow: v2, workflowId: "invoice-001", input: { amount: 100 } });
  } catch (err) {
    if ((err as { _tag?: string })?._tag === "WorkflowVersionMismatchError") {
      const e = err as InstanceType<typeof WorkflowVersionMismatchError>;
      console.log(`Strict policy rejected resume: ${e.actual} → ${e.expected}`);
    } else {
      throw err;
    }
  }

  // Takeaway: version drift is loud. To bridge a version gap, either use
  // `onVersionMismatch: "drain"` with `previousVersions` (example 02), or
  // register all versions in a WorkflowVersionRegistry (example 03).
}

if (import.meta.main) {
  await main();
}
