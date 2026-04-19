// ---------------------------------------------------------------------------
// Default worker — catches everything that didn't get routed to "gpu" or
// "cpu": decode, metadata, notify. These are the lighter, IO-ish steps;
// in a real deployment you'd scale this replica count to match your
// inbound rate, keeping the expensive GPU workers focused on transcode.
// ---------------------------------------------------------------------------

import { MapStepRegistry, createWorker } from "@promin/workflow";
import { buildStack } from "./shared.ts";
import { decodeHandler, metadataHandler, notifyHandler } from "./workflow.ts";

const { storage, stepQueue, close } = await buildStack();

const registry = new MapStepRegistry();
registry.register("decode", decodeHandler);
registry.register("metadata", metadataHandler);
registry.register("notify", notifyHandler);

const worker = createWorker({
  storage,
  stepQueue,
  registry,
  queues: ["default"],
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
