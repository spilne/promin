// ---------------------------------------------------------------------------
// Shipping worker — claims tasks needing capability "shipping".
// Handles generate-label (carrier label API integration in real life).
// ---------------------------------------------------------------------------

import { MapStepRegistry, createWorker } from "@promin/workflow";
import { buildStack } from "./shared.ts";
import { generateLabelHandler } from "./workflow.ts";

const { storage, stepQueue, close } = await buildStack();

const registry = new MapStepRegistry();
registry.register("generate-label", generateLabelHandler);

const worker = createWorker({
  storage,
  stepQueue,
  registry,
  capabilities: ["shipping"],
  concurrency: 3,
  metadata: { role: "shipping-worker" },
});

console.log(`[worker-shipping] starting (id=${worker.workerId})`);
await worker.start();

const shutdown = async (): Promise<void> => {
  console.log("[worker-shipping] shutting down");
  await worker.stop();
  await close();
  process.exit(0);
};
process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
