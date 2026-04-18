// ---------------------------------------------------------------------------
// Example 05 — Versioned workers + rolling deploy
//
// In a distributed deployment, the coordinator enqueues step tasks and
// workers claim them. When you roll out v2 code, some workers may only
// support v1, others only v2, and some both during the transition.
//
// `StepTask.version` carries each task's workflow version. Workers declare
// `supportedVersions` and the framework skips tasks they can't handle —
// those tasks stay pending for a compatible worker to pick up.
//
// This example shows a v2 worker that declares support for BOTH versions
// during the rolling-deploy window, then drops v1 once drained.
//
// Run: bun run packages/workflow/examples/versioning/05-versioned-workers.ts
// ---------------------------------------------------------------------------

import { Pipeline } from "@promin/core";
import {
  workflow,
  InMemoryWorkflowStorage,
  InMemoryStepQueue,
  MapStepRegistry,
  createWorker,
} from "@promin/workflow";

async function main(): Promise<void> {
  const storage = new InMemoryWorkflowStorage();
  const stepQueue = new InMemoryStepQueue();

  // Worker-side: register handlers. In this example the handler logic is
  // the same for v1 and v2 — real deployments might have version-specific
  // handlers, or use ctx.patched inside journaled steps.
  const registry = new MapStepRegistry();
  registry.register("process", ({ input }) =>
    Pipeline.succeed(`handled-${(input as { id: string }).id}`),
  );

  // Rolling-deploy window: this worker declares it handles v1 and v2.
  // After v1 drains (countByVersion shows 0 in-flight), update this to ["2"]
  // on the next deploy.
  const worker = createWorker({
    storage,
    stepQueue,
    registry,
    queues: ["default"],
    pollIntervalMs: 25,
    supportedVersions: ["1", "2"],
  });
  void worker.start();

  // v1 in-flight workflow.
  await workflow<{ id: string }>({
    name: "order",
    storage,
    version: "1",
    dispatch: { stepQueue, routing: { process: "default" }, pollIntervalMs: 25 },
  })
    .step("process", ({ input }) => Pipeline.succeed(`v1-${input.id}`))
    .run({ workflowId: "order-A", input: { id: "abc" } });

  // Fresh v2 workflow.
  await workflow<{ id: string }>({
    name: "order",
    storage,
    version: "2",
    dispatch: { stepQueue, routing: { process: "default" }, pollIntervalMs: 25 },
  })
    .step("process", ({ input }) => Pipeline.succeed(`v2-${input.id}`))
    .run({ workflowId: "order-B", input: { id: "def" } });

  await worker.stop();

  // Both workflows completed because the worker handled both versions.
  console.log(
    "v1 status:",
    (await storage.loadWorkflow("order-A"))?.status,
    "v2 status:",
    (await storage.loadWorkflow("order-B"))?.status,
  );

  // If you want to simulate "v1 drained, v2-only worker": after your metrics
  // show v1 in-flight = 0, redeploy with `supportedVersions: ["2"]`. Any
  // remaining v1 tasks (there shouldn't be any) stay pending rather than
  // failing — safe by construction.
}

if (import.meta.main) {
  await main();
}
