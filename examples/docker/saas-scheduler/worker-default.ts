// ---------------------------------------------------------------------------
// Default worker — claims steps with no needs requirement.
// Handles: validate-job, execute-standard-job, store-result, notify-webhook
//
// concurrency=4 — shared pool serving all tenants for non-premium steps.
// ---------------------------------------------------------------------------

import { MapStepRegistry, createWorker } from "@promin/workflow/distributed";
import { buildStack } from "./shared.ts";
import {
  validateJobHandler,
  executeStandardJobHandler,
  storeResultHandler,
  notifyWebhookHandler,
} from "./workflow.ts";

const { storage, stepQueue, close } = await buildStack();

const registry = new MapStepRegistry();
registry.register({ stepName: "validate-job", handler: validateJobHandler });
registry.register({ stepName: "execute-standard-job", handler: executeStandardJobHandler });
registry.register({ stepName: "store-result", handler: storeResultHandler });
registry.register({ stepName: "notify-webhook", handler: notifyWebhookHandler });

const worker = createWorker({
  storage,
  stepQueue,
  registry,
  capabilities: [],
  concurrency: 4,
  metadata: { role: "default-worker" },
});

console.log(`[worker-default] starting (id=${worker.workerId}, concurrency=4)`);
await worker.start();

const shutdown = async (): Promise<void> => {
  console.log("[worker-default] shutting down");
  await worker.stop();
  await close();
  process.exit(0);
};
process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
