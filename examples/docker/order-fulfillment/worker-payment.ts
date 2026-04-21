// ---------------------------------------------------------------------------
// Payment worker — claims tasks needing capability "payment".
// The charge-payment handler is flaky on the first attempt to show the
// retry-with-backoff path; the workflow-level retry config kicks in.
// ---------------------------------------------------------------------------

import { MapStepRegistry, createWorker } from "@promin/workflow";
import { buildStack } from "./shared.ts";
import { chargePaymentHandler } from "./workflow.ts";

const { storage, stepQueue, close } = await buildStack();

const registry = new MapStepRegistry();
registry.register("charge-payment", chargePaymentHandler);

const worker = createWorker({
  storage,
  stepQueue,
  registry,
  capabilities: ["payment"],
  // Payment is usually latency-bound on the gateway — keep concurrency
  // modest so a single slow gateway doesn't cascade into heap pressure.
  concurrency: 2,
  metadata: { role: "payment-worker" },
});

console.log(`[worker-payment] starting (id=${worker.workerId})`);
await worker.start();

const shutdown = async (): Promise<void> => {
  console.log("[worker-payment] shutting down");
  await worker.stop();
  await close();
  process.exit(0);
};
process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
