// ---------------------------------------------------------------------------
// Coordinator entry point — drives the DAG + submits a fresh demo order
// every SUBMIT_INTERVAL_MS + auto-delivers the "shipped" signal a few
// seconds after each order reaches the wait-for-shipped step.
//
// Workers claim steps by capability (inventory / payment / shipping);
// the default worker picks up notify-warehouse + mark-delivered.
// ---------------------------------------------------------------------------

import { createCoordinator, WorkflowVersionRegistry } from "@promin/workflow";
import { buildStack } from "./shared.ts";
import { buildOrderWorkflow, type OrderInput, type ShippedSignal } from "./workflow.ts";

const SUBMIT_INTERVAL_MS = Number(process.env["SUBMIT_INTERVAL_MS"] ?? 6_000);
const SHIP_AFTER_MS = Number(process.env["SHIP_AFTER_MS"] ?? 3_000);

const { storage, stepQueue, close } = await buildStack();

const registry = new WorkflowVersionRegistry();
registry.register(buildOrderWorkflow());

const coordinator = createCoordinator({
  storage,
  stepQueue,
  registry,
  pollIntervalMs: 500,
});

console.log("[coordinator] starting");
coordinator.startLoop().catch((err) => {
  console.error("[coordinator] loop crashed", err);
  process.exit(1);
});

let counter = 0;
const submitDemo = async (): Promise<void> => {
  counter += 1;
  const orderId = `demo-order-${counter}`;
  const workflowId = `${orderId}-${Math.random().toString(36).slice(2, 8)}`;
  const input: OrderInput = {
    orderId,
    userId: `user-${counter % 7}`,
    items: [
      { sku: "WIDGET-42", qty: 2 },
      { sku: "GADGET-7", qty: 1 },
    ],
    amountCents: 4999,
  };
  try {
    await coordinator.submit<OrderInput>({ name: "order-fulfillment", workflowId, input });
    console.log(`[coordinator] submitted ${workflowId} (#${counter})`);
  } catch (err) {
    console.error(`[coordinator] submit ${workflowId} failed`, err);
    return;
  }

  // Simulate the warehouse posting a "shipped" webhook after a delay.
  // In prod this would be a separate webhook handler reading from the
  // carrier's event feed. The signal landing resumes the waiting workflow.
  setTimeout(() => {
    const payload: ShippedSignal = {
      trackingNumber: `1Z${orderId.replace(/[^A-Z0-9]/gi, "").toUpperCase()}`,
      shippedAt: new Date().toISOString(),
    };
    storage
      .deliverSignal(workflowId, "shipped", payload)
      .then(() => console.log(`[coordinator] signal shipped → ${workflowId}`))
      .catch((err) => console.error(`[coordinator] deliverSignal ${workflowId} failed`, err));
  }, SHIP_AFTER_MS);
};

await submitDemo();
const submitTimer = setInterval(() => {
  submitDemo().catch(() => undefined);
}, SUBMIT_INTERVAL_MS);

console.log(
  `[coordinator] running — new order every ${SUBMIT_INTERVAL_MS}ms, shipped signal fires ${SHIP_AFTER_MS}ms after submit (Ctrl+C to stop)`,
);

const shutdown = async (): Promise<void> => {
  console.log("[coordinator] shutting down");
  clearInterval(submitTimer);
  await coordinator.stopLoop();
  await close();
  process.exit(0);
};
process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
