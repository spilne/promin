// ---------------------------------------------------------------------------
// Coordinator entry point — orchestrates the DAG and routes each step to
// the queue its handler lives on. Workers claim from their queues; the
// coordinator watches for completions and enqueues the next ready steps.
//
// Routing:
//   transcode → "gpu"  (worker-transcode)
//   thumbnail → "cpu"  (worker-thumbnail)
//   decode / metadata / notify → default queue (worker-metadata and any
//     other default-queue workers). In production you'd scale each one
//     independently; here the "default" worker handles all three.
// ---------------------------------------------------------------------------

import { createCoordinator } from "@promin/workflow";
import { buildStack } from "./shared.ts";

const { storage, stepQueue, close } = await buildStack();

const coordinator = createCoordinator({
  storage,
  stepQueue,
  routing: {
    transcode: "gpu",
    thumbnail: "cpu",
  },
  // Everything unlisted (decode, metadata, notify) goes to "default".
  pollIntervalMs: 500,
});

console.log("[coordinator] starting");
await coordinator.start();
console.log("[coordinator] running — Ctrl+C to stop");

const shutdown = async (): Promise<void> => {
  console.log("[coordinator] shutting down");
  await coordinator.stop();
  await close();
  process.exit(0);
};
process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
