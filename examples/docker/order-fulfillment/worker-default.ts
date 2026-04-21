// ---------------------------------------------------------------------------
// Default worker — handles steps without a capability requirement.
// In this workflow: notify-warehouse + mark-delivered. Everything else
// routes to a specialized worker via `needs`.
// ---------------------------------------------------------------------------

import { MapStepRegistry, createWorker } from "@promin/workflow";
import { buildStack } from "./shared.ts";
import { notifyWarehouseHandler, markDeliveredHandler } from "./workflow.ts";

const { storage, stepQueue, close } = await buildStack();

const registry = new MapStepRegistry();
registry.register("notify-warehouse", notifyWarehouseHandler);
registry.register("mark-delivered", markDeliveredHandler);

const worker = createWorker({
  storage,
  stepQueue,
  registry,
  // No capabilities — claims tasks with empty `needs` only.
  concurrency: 4,
  metadata: { role: "default-worker" },
});

console.log(`[worker-default] starting (id=${worker.workerId})`);
await worker.start();

const shutdown = async (): Promise<void> => {
  console.log("[worker-default] shutting down");
  await worker.stop();
  await close();
  process.exit(0);
};
process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
