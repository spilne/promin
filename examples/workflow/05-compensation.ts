/**
 * Saga compensation — undo completed steps on failure
 *
 * When a step fails, the engine automatically runs compensation
 * functions for completed steps in reverse order. Compensation
 * only triggers after all retries (step + workflow) are exhausted.
 */

import { Data } from "effect";
import { workflow, Pipeline, InMemoryWorkflowStorage } from "@promin/core";

class PaymentError extends Data.TaggedError("PaymentError")<{
  readonly message: string;
}> {}

const storage = new InMemoryWorkflowStorage();

// Basic compensation — reverse order
async function basicCompensation() {
  const log: string[] = [];

  const { error: _error } = await workflow<{ from: string; to: string; amount: number }>({
    name: "transfer",
    storage,
  })
    .step(
      "debit",
      ({ input }) => {
        log.push(`debit $${input.amount} from ${input.from}`);
        return Pipeline.succeed({ txId: "tx_123" });
      },
      {
        compensate: ({ result }) => {
          log.push(`refund txId=${result.txId}`);
          return Pipeline.succeed(undefined as void);
        },
      },
    )
    .step(
      "credit",
      ({ input }) => {
        log.push(`credit $${input.amount} to ${input.to}`);
        return Pipeline.fail(new PaymentError({ message: "insufficient funds" }));
      },
      {
        compensate: ({ result }) => {
          log.push(`reverse credit`);
          return Pipeline.succeed(undefined as void);
        },
      },
    )
    .runSafe({ workflowId: "transfer-1", input: { from: "Alice", to: "Bob", amount: 100 } });

  console.log("Execution log:", log);
  // ["debit $100 from Alice", "credit $100 to Bob", "refund txId=tx_123"]
  // Note: credit.compensate doesn't run because credit never completed
}

// Compensation with trigger modes
async function immediateTrigger() {
  const log: string[] = [];

  // trigger: "immediate" — skip workflow retries, compensate right away
  const { error: _error } = await workflow<string>({
    name: "immediate",
    storage,
    retry: { maxRetries: 3 }, // would normally retry 3x
    compensate: { trigger: "immediate" }, // but immediate skips retries
  })
    .step("step-1", () => {
      log.push("step-1");
      return Pipeline.succeed("ok");
    }, {
      compensate: () => {
        log.push("compensate-step-1");
        return Pipeline.succeed(undefined as void);
      },
    })
    .step("step-2", () => {
      log.push("step-2-fail");
      return Pipeline.fail(new PaymentError({ message: "fail" }));
    })
    .runSafe({ workflowId: "immediate-1", input: "x" });

  console.log("Immediate trigger log:", log);
  // ["step-1", "step-2-fail", "compensate-step-1"]
  // No retries despite maxRetries: 3
}

// Compensation with retry
async function compensationRetry() {
  let compAttempts = 0;

  await workflow<string>({
    name: "comp-retry",
    storage,
    compensate: {
      retry: { maxRetries: 2, baseDelayMs: 10 },
    },
  })
    .step("step-1", () => Pipeline.succeed("ok"), {
      compensate: () => {
        compAttempts++;
        if (compAttempts < 3) throw new Error("compensation temporarily failed");
        return Pipeline.succeed(undefined as void);
      },
    })
    .step("fail", () => Pipeline.fail(new PaymentError({ message: "boom" })))
    .runSafe({ workflowId: "comp-retry-1", input: "x" });

  console.log(`Compensation succeeded after ${compAttempts} attempts`); // 3
}

// onComplete callback — audit trail after compensation
async function compensationAudit() {
  await workflow<string>({
    name: "audit",
    storage,
    compensate: {
      onComplete: ({ compensatedSteps, failedCompensations, error }) => {
        console.log("Compensation report:");
        console.log("  Compensated:", compensatedSteps);
        console.log("  Failed:", failedCompensations);
        console.log("  Original error:", error);
        return Pipeline.succeed(undefined as void);
      },
    },
  })
    .step("a", () => Pipeline.succeed("ok"), {
      compensate: () => Pipeline.succeed(undefined as void),
    })
    .step("b", () => Pipeline.succeed("ok"), {
      compensate: () => { throw new Error("comp failed"); },
    })
    .step("fail", () => Pipeline.fail(new PaymentError({ message: "boom" })))
    .runSafe({ workflowId: "audit-1", input: "x" });
}

// Full cascade: step retry → workflow retry → compensation
async function fullCascade() {
  const log: string[] = [];
  let step2Calls = 0;

  await workflow<string>({
    name: "cascade",
    storage,
    retry: { maxRetries: 1, baseDelayMs: 10 },
    compensate: {
      trigger: "after-retries", // default
      onComplete: ({ compensatedSteps }) => {
        log.push(`compensated: [${compensatedSteps.join(", ")}]`);
        return Pipeline.succeed(undefined as void);
      },
    },
  })
    .step("step-1", () => {
      log.push("step-1");
      return Pipeline.succeed("ok");
    }, {
      compensate: () => {
        log.push("undo-step-1");
        return Pipeline.succeed(undefined as void);
      },
    })
    .step("step-2", () => {
      step2Calls++;
      log.push(`step-2 (attempt ${step2Calls})`);
      return Pipeline.fail(new PaymentError({ message: "always fails" }));
    }, {
      retry: { maxRetries: 1 },
    })
    .runSafe({ workflowId: "cascade-1", input: "x" });

  console.log("Full cascade:", log);
  // ["step-1",
  //  "step-2 (attempt 1)", "step-2 (attempt 2)",     ← step retry
  //  "step-2 (attempt 3)", "step-2 (attempt 4)",     ← workflow retry + step retry
  //  "undo-step-1",                                   ← compensation
  //  "compensated: [step-1]"]                          ← onComplete
}

export { basicCompensation, immediateTrigger, compensationRetry, compensationAudit, fullCascade };
