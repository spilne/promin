// ---------------------------------------------------------------------------
// Ingest worker — claims steps with needs=["ingest"].
// Handles: ingest-dataset
// ---------------------------------------------------------------------------

import { MapStepRegistry, createWorker } from "@promin/workflow";
import { buildStack } from "./shared.ts";
import { ingestDatasetHandler } from "./workflow.ts";

const { storage, stepQueue, close } = await buildStack();

const registry = new MapStepRegistry();
registry.register("ingest-dataset", ingestDatasetHandler);

const worker = createWorker({
  storage,
  stepQueue,
  registry,
  capabilities: ["ingest"],
  concurrency: 4,
  metadata: { role: "ingest-worker" },
});

console.log(`[worker-ingest] starting (id=${worker.workerId})`);
await worker.start();

const shutdown = async (): Promise<void> => {
  console.log("[worker-ingest] shutting down");
  await worker.stop();
  await close();
  process.exit(0);
};
process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
