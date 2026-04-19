// ---------------------------------------------------------------------------
// GPU worker — claims tasks from the "gpu" queue. Only registers the
// `transcode` handler. If a task for any other step ended up in the gpu
// queue (it shouldn't — the coordinator's routing pins transcode →
// gpu), the registry lookup would miss and the task would stay claimable
// by another worker.
//
// Scale by `docker compose up --scale worker-transcode=N`.
// ---------------------------------------------------------------------------

import { MapStepRegistry, createWorker } from "@promin/workflow";
import { buildStack } from "./shared.ts";
import { transcodeHandler } from "./workflow.ts";

const { storage, stepQueue, close } = await buildStack();

const registry = new MapStepRegistry();
registry.register("transcode", transcodeHandler);

const worker = createWorker({
  storage,
  stepQueue,
  registry,
  capabilities: ["gpu"],
  concurrency: 2,
  metadata: { role: "gpu-worker" },
});

console.log(`[worker-transcode] starting (id=${worker.workerId})`);
await worker.start();

const shutdown = async (): Promise<void> => {
  console.log("[worker-transcode] shutting down");
  await worker.stop();
  await close();
  process.exit(0);
};
process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
