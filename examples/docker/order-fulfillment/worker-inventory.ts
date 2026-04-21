// ---------------------------------------------------------------------------
// Inventory worker — claims tasks needing capability "inventory".
// Handles the reserve-inventory step (and its compensation path when the
// workflow unwinds after a later failure).
// ---------------------------------------------------------------------------

import { MapStepRegistry, createWorker } from "@promin/workflow";
import { buildStack } from "./shared.ts";
import { reserveInventoryHandler } from "./workflow.ts";

const { storage, stepQueue, close } = await buildStack();

const registry = new MapStepRegistry();
registry.register("reserve-inventory", reserveInventoryHandler);

const worker = createWorker({
  storage,
  stepQueue,
  registry,
  capabilities: ["inventory"],
  concurrency: 4,
  metadata: { role: "inventory-worker" },
});

console.log(`[worker-inventory] starting (id=${worker.workerId})`);
await worker.start();

const shutdown = async (): Promise<void> => {
  console.log("[worker-inventory] shutting down");
  await worker.stop();
  await close();
  process.exit(0);
};
process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
