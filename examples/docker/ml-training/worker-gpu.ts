// ---------------------------------------------------------------------------
// GPU worker — claims steps with needs=["gpu"].
// Handles: train-model (both v1 and v2)
//
// concurrency=1 simulates a single GPU — only one training job at a time.
// ---------------------------------------------------------------------------

import { MapStepRegistry, createWorker } from "@promin/workflow/distributed";
import { buildStack } from "./shared.ts";
import { trainModelHandler } from "./workflow.ts";

const { stepQueue, close } = await buildStack();

const registry = new MapStepRegistry();
registry.register({ stepName: "train-model", handler: trainModelHandler });

const worker = createWorker({
  stepQueue,
  registry,
  capabilities: ["gpu"],
  concurrency: 1,
  metadata: { role: "gpu-worker" },
});

console.log(`[worker-gpu] starting (id=${worker.workerId}, concurrency=1)`);
await worker.start();

const shutdown = async (): Promise<void> => {
  console.log("[worker-gpu] shutting down");
  await worker.stop();
  await close();
  process.exit(0);
};
process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
