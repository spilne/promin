// ---------------------------------------------------------------------------
// CPU worker — claims steps with needs=["cpu"].
// Handles: prepare-data, evaluate, optimize (v2 only)
// ---------------------------------------------------------------------------

import { MapStepRegistry, createWorker } from "@promin/workflow/distributed";
import { buildStack } from "./shared.ts";
import { prepareDataHandler, evaluateHandler, optimizeHandler } from "./workflow.ts";

const { storage, stepQueue, close } = await buildStack();

const registry = new MapStepRegistry();
registry.register({ stepName: "prepare-data", handler: prepareDataHandler });
registry.register({ stepName: "evaluate", handler: evaluateHandler });
registry.register({ stepName: "optimize", handler: optimizeHandler });

const worker = createWorker({
  storage,
  stepQueue,
  registry,
  capabilities: ["cpu"],
  concurrency: 4,
  metadata: { role: "cpu-worker" },
});

console.log(`[worker-cpu] starting (id=${worker.workerId})`);
await worker.start();

const shutdown = async (): Promise<void> => {
  console.log("[worker-cpu] shutting down");
  await worker.stop();
  await close();
  process.exit(0);
};
process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
