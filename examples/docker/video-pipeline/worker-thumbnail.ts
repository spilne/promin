// ---------------------------------------------------------------------------
// CPU worker — claims tasks from the "cpu" queue. Handles thumbnail
// extraction (parallelizable, CPU-bound in real life).
// ---------------------------------------------------------------------------

import { MapStepRegistry, createWorker } from "@promin/workflow";
import { buildStack } from "./shared.ts";
import { thumbnailHandler } from "./workflow.ts";

const { storage, stepQueue, close } = await buildStack();

const registry = new MapStepRegistry();
registry.register("thumbnail", thumbnailHandler);

const worker = createWorker({
  storage,
  stepQueue,
  registry,
  queues: ["cpu"],
  concurrency: 4,
  metadata: { role: "cpu-worker" },
});

console.log(`[worker-thumbnail] starting (id=${worker.workerId})`);
await worker.start();

const shutdown = async (): Promise<void> => {
  console.log("[worker-thumbnail] shutting down");
  await worker.stop();
  await close();
  process.exit(0);
};
process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
