/**
 * Dead letter queue — capture failed workflows for replay
 *
 * Failed workflows (after all retries + compensation) are published
 * to a configurable DLQ. Works with any Sinkable<FailedWorkflowRecord>.
 */

import { Data } from "effect";
import {
  workflow,
  Pipeline,
  InMemoryWorkflowStorage,
  type FailedWorkflowRecord,
} from "@promin/core";
import type { Sinkable, Codec } from "@promin/core";
import { JsonCodec } from "@promin/core";

class ProcessingError extends Data.TaggedError("ProcessingError")<{
  readonly message: string;
}> {}

// Simple in-memory DLQ for demonstration
class InMemoryDlq implements Sinkable<FailedWorkflowRecord> {
  readonly messages: FailedWorkflowRecord[] = [];
  readonly codec: Codec<FailedWorkflowRecord> = JsonCodec as Codec<FailedWorkflowRecord>;

  async publish(value: FailedWorkflowRecord): Promise<void> {
    this.messages.push(value);
  }
}

const storage = new InMemoryWorkflowStorage();

// DLQ captures failed workflows
async function dlqCapture() {
  const dlq = new InMemoryDlq();

  const processOrder = workflow<{ orderId: string }>({
    name: "process-order",
    storage,
    retry: { maxRetries: 2, baseDelayMs: 10 },
    dlq,
    metadata: { team: "payments" },
  })
    .step("validate", ({ input }) => Pipeline.succeed({ orderId: input.orderId, valid: true }))
    .step("charge", () => Pipeline.fail(new ProcessingError({ message: "payment gateway down" })))
    .build();

  // Run and fail
  await processOrder.runSafe({ workflowId: "order-1", input: { orderId: "ord_42" } });

  // Inspect DLQ
  const record = dlq.messages[0]!;
  console.log("DLQ record:");
  console.log("  workflowId:", record.workflowId);
  console.log("  error:", record.error);
  console.log("  input:", record.input);
  console.log("  steps:", Object.keys(record.steps));
  console.log("  metadata:", record.metadata);
  console.log("  failedAt:", record.failedAt);
}

// Replay from DLQ
async function dlqReplay() {
  const dlq = new InMemoryDlq();
  let shouldFail = true;

  const processOrder = workflow<{ orderId: string }>({
    name: "process-order",
    storage,
    dlq,
  })
    .step("charge", ({ input }) => {
      if (shouldFail) return Pipeline.fail(new ProcessingError({ message: "down" }));
      return Pipeline.succeed({ chargeId: `ch_${input.orderId}` });
    })
    .build();

  // First run fails → goes to DLQ
  await processOrder.runSafe({ workflowId: "order-2", input: { orderId: "ord_43" } });
  console.log("DLQ has", dlq.messages.length, "messages");

  // Fix the issue
  shouldFail = false;

  // Replay from DLQ record
  const failed = dlq.messages[0]!;
  const result = await processOrder.run({
    workflowId: `${failed.workflowId}-retry`,
    input: failed.input as { orderId: string },
  });
  console.log("Replay result:", result);
}

// DLQ with compensation info
async function dlqWithCompensation() {
  const dlq = new InMemoryDlq();

  await workflow<string>({
    name: "with-comp",
    storage,
    dlq,
  })
    .step("step-1", () => Pipeline.succeed("ok"), {
      compensate: () => Pipeline.succeed(undefined as void),
    })
    .step("fail", () => Pipeline.fail(new ProcessingError({ message: "boom" })))
    .runSafe({ workflowId: "comp-dlq-1", input: "x" });

  const record = dlq.messages[0]!;
  console.log("Compensated steps:", record.compensatedSteps);
  console.log("Failed compensations:", record.failedCompensations);
}

export { dlqCapture, dlqReplay, dlqWithCompensation };
