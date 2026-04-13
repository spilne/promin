/**
 * Priority queue with fairness policies — control how tasks are dequeued.
 *
 * Three policies:
 * - strict-priority: highest priority first (default)
 * - round-robin: interleave across workflows (prevents starvation)
 * - weighted: probabilistic priority (higher = more likely, not guaranteed)
 */

import { InMemoryStepQueue } from "@promin/workflow";

const queue = new InMemoryStepQueue();

// ---------------------------------------------------------------------------
// 1. Enqueue tasks with different priorities
// ---------------------------------------------------------------------------

// Premium customer — high priority
await queue.enqueue({
  workflowId: "order-premium-1",
  stepName: "process",
  queue: "default",
  input: { customer: "premium" },
  prevResults: {},
  priority: 10, // highest priority
});

// Standard customers — normal priority
for (let i = 0; i < 5; i++) {
  await queue.enqueue({
    workflowId: `order-standard-${i}`,
    stepName: "process",
    queue: "default",
    input: { customer: "standard" },
    prevResults: {},
    priority: 5, // default priority
  });
}

// Background task — low priority
await queue.enqueue({
  workflowId: "cleanup-1",
  stepName: "gc",
  queue: "default",
  input: {},
  prevResults: {},
  priority: 1, // lowest priority
});

// ---------------------------------------------------------------------------
// 2. Strict priority — premium always first
// ---------------------------------------------------------------------------

const strictTasks = await queue.claim({
  queues: ["default"],
  limit: 3,
  fairness: "strict-priority",
});
console.log(
  "Strict priority:",
  strictTasks.map((t) => `${t.workflowId} (p=${t.priority})`),
);
// ["order-premium-1 (p=10)", "order-standard-0 (p=5)", "order-standard-1 (p=5)"]
// Premium always first, then FIFO within same priority

// ---------------------------------------------------------------------------
// 3. Round-robin — fair across workflows
// ---------------------------------------------------------------------------

// Re-enqueue for demo
const queue2 = new InMemoryStepQueue();
for (const wfId of ["wf-A", "wf-B", "wf-C"]) {
  for (let i = 0; i < 3; i++) {
    await queue2.enqueue({
      workflowId: wfId,
      stepName: `step-${i}`,
      queue: "default",
      input: {},
      prevResults: {},
    });
  }
}

const rrTasks = await queue2.claim({
  queues: ["default"],
  limit: 6,
  fairness: "round-robin",
});
console.log(
  "Round-robin:",
  rrTasks.map((t) => `${t.workflowId}:${t.stepName}`),
);
// Interleaved: ["wf-A:step-0", "wf-B:step-0", "wf-C:step-0", "wf-A:step-1", ...]
// Each workflow gets equal share — no starvation

// ---------------------------------------------------------------------------
// 4. Weighted — probabilistic priority
// ---------------------------------------------------------------------------

const queue3 = new InMemoryStepQueue();
for (let i = 0; i < 10; i++) {
  await queue3.enqueue({
    workflowId: `wf-${i}`,
    stepName: "work",
    queue: "default",
    input: {},
    prevResults: {},
    priority: i < 3 ? 10 : 2, // 3 high priority, 7 low priority
  });
}

const weightedTasks = await queue3.claim({
  queues: ["default"],
  limit: 5,
  fairness: "weighted",
});
console.log(
  "Weighted:",
  weightedTasks.map((t) => `${t.workflowId} (p=${t.priority})`),
);
// High priority tasks are MORE LIKELY to be picked, but not guaranteed
// Some low-priority tasks may appear — that's the fairness

// ---------------------------------------------------------------------------
// 5. Queue metrics
// ---------------------------------------------------------------------------

const metrics = await queue.metrics();
console.log("Queue metrics:", metrics);
// { default: { pending: 4, running: 3, completed: 0, failed: 0 } }
