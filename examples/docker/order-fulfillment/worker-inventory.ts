// ---------------------------------------------------------------------------
// Inventory worker — claims tasks needing capability "inventory".
// Handles the reserve-inventory step (and its compensation path when the
// workflow unwinds after a later failure).
// ---------------------------------------------------------------------------

import { MapStepRegistry, createWorker } from "@promin/workflow/distributed";
import { buildStack } from "./shared.ts";
import { reserveInventoryHandler } from "./workflow.ts";

const { stepQueue, close } = await buildStack();

const registry = new MapStepRegistry();
registry.register({ stepName: "reserve-inventory", handler: reserveInventoryHandler });

const worker = createWorker({
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
