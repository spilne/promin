/**
 * Stream triggers — bridge StreamPipeline to workflow execution
 *
 * Business flow:
 * 1. A stream of incoming order events is received (from a queue, API, or file)
 * 2. Each order event triggers an order-processing workflow (validate, then charge)
 * 3. Multiple orders are processed concurrently up to a configurable limit
 * 4. Duplicate order IDs are detected and skipped to prevent double-processing
 * 5. Results are collected and categorized as completed, failed, or skipped
 *
 * Connects event streams to workflow execution with built-in concurrency control and dedup.
 */

import {
  workflow,
  Pipeline,
  StreamPipeline,
  InMemoryWorkflowStorage,
  trigger,
  WorkflowResult,
} from "@promin/core";

const storage = new InMemoryWorkflowStorage();

// Define a workflow to trigger
const processOrder = workflow<{ orderId: string; amount: number }>({
  name: "process-order",
  storage,
})
  .step("validate", ({ input }) => Pipeline.succeed({ ...input, valid: true }))
  .step("charge", ({ prev }) => Pipeline.succeed({ chargeId: `ch_${prev.orderId}` }))
  .build();

// Trigger from a stream of events
async function streamTrigger() {
  const events = [
    { orderId: "ord_1", amount: 100 },
    { orderId: "ord_2", amount: 250 },
    { orderId: "ord_3", amount: 50 },
  ];

  const results = await StreamPipeline.fromIterable(events)
    .through(
      trigger({
        workflow: processOrder,
        toInput: (event) => event,
        toWorkflowId: (event) => `order-${event.orderId}`,
        concurrency: 3,
        onDuplicate: "skip",
      }),
    )
    .collect();

  for (const r of results) {
    if (WorkflowResult.isCompleted(r)) {
      console.log(`Order ${r.workflowId} completed:`, r.result);
    } else if (WorkflowResult.isFailed(r)) {
      console.log(`Order ${r.workflowId} failed:`, r.error);
    } else if (WorkflowResult.isSkipped(r)) {
      console.log(`Order ${r.workflowId} skipped (duplicate)`);
    }
  }
}

// Filter and process only completed workflows
async function filterResults() {
  const events = [
    { orderId: "ord_4", amount: 100 },
    { orderId: "ord_5", amount: 200 },
  ];

  const completed = await StreamPipeline.fromIterable(events)
    .through(
      trigger({
        workflow: processOrder,
        toInput: (e) => e,
        toWorkflowId: (e) => `order-${e.orderId}`,
      }),
    )
    .filter(WorkflowResult.isCompleted)
    .map((r) => (r as WorkflowResult.Completed<{ chargeId: string }>).result)
    .collect();

  console.log("Completed:", completed);
}

export { streamTrigger, filterResults };
