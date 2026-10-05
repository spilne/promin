// ---------------------------------------------------------------------------
// Loader worker — claims steps with needs=["loader"].
// Handles: reduce-results
// ---------------------------------------------------------------------------

import { MapStepRegistry, createWorker } from "@promin/workflow/distributed";
import { buildStack } from "./shared.ts";
import { reduceResultsHandler } from "./workflow.ts";

const { stepQueue, close } = await buildStack();

const registry = new MapStepRegistry();
registry.register({ stepName: "reduce-results", handler: reduceResultsHandler });

const worker = createWorker({
  stepQueue,
  registry,
  capabilities: ["loader"],
  concurrency: 2,
  metadata: { role: "loader-worker" },
});

console.log(`[worker-loader] starting (id=${worker.workerId})`);
await worker.start();

const shutdown = async (): Promise<void> => {
  console.log("[worker-loader] shutting down");
  await worker.stop();
  await close();
  process.exit(0);
};
process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
