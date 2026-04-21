// ---------------------------------------------------------------------------
// Premium worker — claims steps with needs=["premium"].
// Handles: execute-premium-job
//
// concurrency=2 — dedicated pool for premium tenants (low wait time).
// Standard tenant work never competes here.
// ---------------------------------------------------------------------------

import { MapStepRegistry, createWorker } from "@promin/workflow";
import { buildStack } from "./shared.ts";
import { executePremiumJobHandler } from "./workflow.ts";

const { storage, stepQueue, close } = await buildStack();

const registry = new MapStepRegistry();
registry.register("execute-premium-job", executePremiumJobHandler);

const worker = createWorker({
  storage,
  stepQueue,
  registry,
  capabilities: ["premium"],
  concurrency: 2,
  metadata: { role: "premium-worker" },
});

console.log(`[worker-premium] starting (id=${worker.workerId}, concurrency=2)`);
await worker.start();

const shutdown = async (): Promise<void> => {
  console.log("[worker-premium] shutting down");
  await worker.stop();
  await close();
  process.exit(0);
};
process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
