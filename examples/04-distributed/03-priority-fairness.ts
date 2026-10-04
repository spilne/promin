/**
 * Priority queue — how tasks are ordered and routed at claim time.
 *
 * - Higher `priority` is claimed first; FIFO within the same priority.
 * - `stepNames` / `versions` / `capabilities` are applied inside the claim,
 *   so tasks a worker can't run never block the ones behind them.
 * - `concurrencyKey` caps how many tasks per key run at once, across every
 *   claimer.
 */

import { InMemoryStepQueue } from "@promin/workflow/distributed";

const queue = new InMemoryStepQueue();

// ---------------------------------------------------------------------------
// 1. Enqueue tasks with different priorities
// ---------------------------------------------------------------------------

// Premium customer — high priority
await queue.enqueue({
  workflowId: "order-premium-1",
  stepName: "process",
  input: { customer: "premium" },
  prevResults: {},
  priority: 10, // highest priority
});

// Standard customers — normal priority
for (let i = 0; i < 5; i++) {
  await queue.enqueue({
    workflowId: `order-standard-${i}`,
    stepName: "process",
    input: { customer: "standard" },
    prevResults: {},
    priority: 5, // default priority
  });
}

// Background task — low priority, a different step
await queue.enqueue({
  workflowId: "cleanup-1",
  stepName: "gc",
  input: {},
  prevResults: {},
  priority: 1, // lowest priority
});

// ---------------------------------------------------------------------------
// 2. Priority order — premium first, then FIFO within a priority
// ---------------------------------------------------------------------------

const first = await queue.claim({ workerId: "worker-1", limit: 3 });
console.log(
  "Priority order:",
  first.map((t) => `${t.workflowId} (p=${t.priority})`),
);
// ["order-premium-1 (p=10)", "order-standard-0 (p=5)", "order-standard-1 (p=5)"]

// ---------------------------------------------------------------------------
// 3. Routing inside the claim — a gc-only worker skips the backlog ahead
// ---------------------------------------------------------------------------

const gcOnly = await queue.claim({ workerId: "janitor", limit: 1, stepNames: ["gc"] });
console.log(
  "gc worker:",
  gcOnly.map((t) => t.workflowId),
);
// ["cleanup-1"] — the three pending "process" tasks ahead of it didn't block it

// ---------------------------------------------------------------------------
// 4. Concurrency keys — at most 2 per tenant, however many workers claim
// ---------------------------------------------------------------------------

const queue2 = new InMemoryStepQueue();
for (let i = 0; i < 6; i++) {
  await queue2.enqueue({
    workflowId: `email-${i}`,
    stepName: "send",
    input: {},
    prevResults: {},
    concurrencyKey: "tenant-a",
    concurrencyScope: "send-email",
    concurrencyLimit: 2,
  });
}
const claims = await Promise.all(
  ["w-1", "w-2", "w-3"].map((workerId) => queue2.claim({ workerId, limit: 5 })),
);
console.log("Running for tenant-a:", claims.flat().length); // 2

// ---------------------------------------------------------------------------
// 5. Queue metrics
// ---------------------------------------------------------------------------

const metrics = await queue.metrics({ since: new Date(Date.now() - 60_000) });
console.log("Queue metrics:", metrics);
// { pending: 3, running: 4, completed: 0, failed: 0, ... }
