/**
 * Workflow version registry — deploy new code while old workflows finish safely.
 *
 * Problem: You deploy v2 of your workflow (renamed steps, changed logic).
 * But there are in-flight workflows still running v1. Without the registry,
 * resuming v1 workflows with v2 code fails — step names don't match.
 *
 * Solution: Register both versions. The registry picks the right definition
 * based on the version stored when the workflow was created.
 */

import { workflow, WorkflowVersionRegistry, InMemoryWorkflowStorage } from "@promin/workflow";

const storage = new InMemoryWorkflowStorage();
const registry = new WorkflowVersionRegistry();

// ---------------------------------------------------------------------------
// 1. Define v1 — the original workflow
// ---------------------------------------------------------------------------

const orderV1 = workflow({ name: "order", storage, version: "1" })
  .stepAsync("validate", async (input: any) => {
    console.log("[v1] Validating order...");
    return { ...input, validated: true };
  })
  .stepAsync("charge", async (ctx: any) => {
    console.log("[v1] Charging payment...");
    return { ...ctx, charged: true };
  })
  .build();

// ---------------------------------------------------------------------------
// 2. Define v2 — renamed "validate" to "verify", added fraud check
// ---------------------------------------------------------------------------

const orderV2 = workflow({ name: "order", storage, version: "2" })
  .stepAsync("verify", async (input: any) => {
    console.log("[v2] Verifying order + fraud check...");
    return { ...input, verified: true, fraudScore: 0.1 };
  })
  .stepAsync("charge", async (ctx: any) => {
    console.log("[v2] Charging payment (new provider)...");
    return { ...ctx, charged: true };
  })
  .build();

// ---------------------------------------------------------------------------
// 3. Register both versions
// ---------------------------------------------------------------------------

registry.register(orderV1);
registry.register(orderV2);

console.log("Registered versions:", registry.versions("order")); // ["1", "2"]
console.log("Latest:", registry.latest("order")); // "2"

// ---------------------------------------------------------------------------
// 4. New workflows use the latest version (v2)
// ---------------------------------------------------------------------------

const newResult = await registry.run({
  workflowId: "order-new",
  name: "order",
  input: { items: ["Widget"] },
});
console.log("New order result:", newResult);
// [v2] Verifying order + fraud check...
// [v2] Charging payment (new provider)...

// ---------------------------------------------------------------------------
// 5. Old v1 workflow resumes with v1 code
// ---------------------------------------------------------------------------

// Simulate: a v1 workflow was created before deployment, needs to resume
await orderV1.run({ workflowId: "order-legacy", input: { items: ["Gadget"] } });
// Creates with version "1" in storage

// Start a fresh run (simulating resume after crash/restart)
await storage.startFreshRun("order-legacy");

// Registry picks v1 definition for this workflow (stored version = "1")
const legacyResult = await registry.run({
  workflowId: "order-legacy",
  name: "order",
  input: { items: ["Gadget"] },
});
console.log("Legacy order result:", legacyResult);
// [v1] Validating order...    ← used v1 code, not v2!
// [v1] Charging payment...

// ---------------------------------------------------------------------------
// 6. Monitor drain progress — when can we deregister v1?
// ---------------------------------------------------------------------------

const counts = await registry.countByVersion({ name: "order", storage });
for (const [version, stats] of counts) {
  console.log(`Version ${version}: ${stats.running} running, ${stats.completed} completed`);
}
// Version 1: 0 running, 1 completed  ← safe to deregister
// Version 2: 0 running, 1 completed
