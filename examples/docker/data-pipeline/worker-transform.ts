// ---------------------------------------------------------------------------
// Transform worker — claims steps with needs=["transform"].
// Handles: transform-shard-a, transform-shard-b, transform-shard-c
//
// Scale this pool to run shards in parallel across replicas:
//   docker compose up --scale worker-transform=3
// ---------------------------------------------------------------------------

import { MapStepRegistry, createWorker } from "@promin/workflow/distributed";
import { buildStack } from "./shared.ts";
import {
  transformShardAHandler,
  transformShardBHandler,
  transformShardCHandler,
} from "./workflow.ts";

const { stepQueue, close } = await buildStack();

const registry = new MapStepRegistry();
registry.register({ stepName: "transform-shard-a", handler: transformShardAHandler });
registry.register({ stepName: "transform-shard-b", handler: transformShardBHandler });
registry.register({ stepName: "transform-shard-c", handler: transformShardCHandler });

const worker = createWorker({
  stepQueue,
  registry,
  capabilities: ["transform"],
  concurrency: 4,
  metadata: { role: "transform-worker" },
});

console.log(`[worker-transform] starting (id=${worker.workerId})`);
await worker.start();

const shutdown = async (): Promise<void> => {
  console.log("[worker-transform] shutting down");
  await worker.stop();
  await close();
  process.exit(0);
};
process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
