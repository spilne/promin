import { describe, it, expect } from "bun:test";
import { Data } from "effect";
import { Pipeline } from "@promin/core";
import { JsonCodec } from "@promin/core";
import { workflow } from "../durable-pipeline.ts";
import { InMemoryWorkflowStorage } from "../in-memory-storage.ts";
import { createWorkflowRunner } from "../workflow-runner.ts";
import type { FailedWorkflowRecord } from "../workflow-state.ts";
import type { Sinkable } from "@promin/core";
import type { Codec } from "@promin/core";

// ---------------------------------------------------------------------------
// Test error
// ---------------------------------------------------------------------------

class TestError extends Data.TaggedError("TestError")<{
  readonly message: string;
}> {}

// ---------------------------------------------------------------------------
// In-memory DLQ for testing
// ---------------------------------------------------------------------------

class InMemoryDlq implements Sinkable<FailedWorkflowRecord> {
  readonly messages: FailedWorkflowRecord[] = [];
  readonly codec: Codec<FailedWorkflowRecord> = JsonCodec as Codec<FailedWorkflowRecord>;

  async publish(value: FailedWorkflowRecord): Promise<void> {
    this.messages.push(value);
  }
}

// ---------------------------------------------------------------------------
// DLQ basics
// ---------------------------------------------------------------------------

describe("Dead-letter queue — capture failed workflows for investigation", () => {
  it("order processing fails — full context is sent to the DLQ for debugging", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const dlq = new InMemoryDlq();

    const wf = workflow<{ userId: string }>({
      name: "dlq-basic",
      dlq,
    })
      .step("step-1", ({ input }) => Pipeline.succeed(input.userId))
      .step("step-2", () => Pipeline.fail(new TestError({ message: "boom" })))
      .build();

    await runner.runSafe({ workflow: wf, workflowId: "dlq-1", input: { userId: "u_42" } });

    expect(dlq.messages).toHaveLength(1);
    const record = dlq.messages[0]!;
    expect(record.workflowId).toBe("dlq-1");
    expect(record.workflowName).toBe("dlq-basic");
    expect(record.input).toEqual({ userId: "u_42" });
    expect(record.error).toContain("boom");
    expect(record.failedAt).toBeInstanceOf(Date);
  });

  it("DLQ record shows which steps succeeded and which failed — aids triage", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const dlq = new InMemoryDlq();

    const wf = workflow<string>({
      name: "dlq-steps",
      dlq,
    })
      .step("ok-step", () => Pipeline.succeed("done"))
      .step("fail-step", () => Pipeline.fail(new TestError({ message: "fail" })))
      .build();

    await runner.runSafe({ workflow: wf, workflowId: "dlq-2", input: "x" });

    const record = dlq.messages[0]!;
    expect(record.steps["ok-step"]).toBeDefined();
    expect(record.steps["ok-step"]!.status).toBe("completed");
    expect(record.steps["fail-step"]).toBeDefined();
    expect(record.steps["fail-step"]!.status).toBe("failed");
  });

  it("DLQ record tracks which rollbacks succeeded and which failed", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const dlq = new InMemoryDlq();

    const wf = workflow<string>({
      name: "dlq-comp",
      dlq,
    })
      .step("step-1", () => Pipeline.succeed("ok"), {
        compensate: () => Pipeline.succeed(undefined as void),
      })
      .step("step-2", () => Pipeline.succeed("ok"), {
        compensate: () => {
          throw new Error("comp-failed");
        },
      })
      .step("fail", () => Pipeline.fail(new TestError({ message: "boom" })))
      .build();

    await runner.runSafe({ workflow: wf, workflowId: "dlq-3", input: "x" });

    const record = dlq.messages[0]!;
    expect(record.compensatedSteps).toContain("step-1");
    expect(record.failedCompensations).toHaveLength(1);
    expect(record.failedCompensations[0]!.stepName).toBe("step-2");
    expect(record.failedCompensations[0]!.error).toBe("comp-failed");
  });

  it("team and priority metadata attached — route DLQ alerts to the right on-call", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const dlq = new InMemoryDlq();

    const wf = workflow<string>({
      name: "dlq-meta",
      dlq,
      metadata: { team: "growth", priority: "high" },
    })
      .step("fail", () => Pipeline.fail(new TestError({ message: "fail" })))
      .build();

    await runner.runSafe({ workflow: wf, workflowId: "dlq-4", input: "x" });

    const record = dlq.messages[0]!;
    expect(record.metadata).toEqual({ team: "growth", priority: "high" });
  });

  it("successful workflow does not clutter the DLQ", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const dlq = new InMemoryDlq();

    const wf = workflow<number>({
      name: "dlq-success",
      dlq,
    })
      .step("ok", ({ input }) => Pipeline.succeed(input * 2))
      .build();

    await runner.run({ workflow: wf, workflowId: "dlq-5", input: 5 });

    expect(dlq.messages).toHaveLength(0);
  });

  it("DLQ message sent only after all retry attempts are exhausted", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const dlq = new InMemoryDlq();
    let attempts = 0;

    const wf = workflow<string>({
      name: "dlq-retry",
      dlq,
      retry: { maxRetries: 2, baseDelayMs: 10 },
    })
      .step("fail", () => {
        attempts++;
        return Pipeline.fail(new TestError({ message: `fail-${attempts}` }));
      })
      .build();

    await runner.runSafe({ workflow: wf, workflowId: "dlq-6", input: "x" });

    // Published once after all 3 attempts (1 + 2 retries) exhausted
    expect(dlq.messages).toHaveLength(1);
    expect(attempts).toBe(3);
  });

  it("DLQ itself is down — original business error is still surfaced to the caller", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const failingDlq: Sinkable<FailedWorkflowRecord> = {
      codec: JsonCodec as Codec<FailedWorkflowRecord>,
      publish: async () => {
        throw new Error("DLQ is down");
      },
    };

    const wf = workflow<string>({
      name: "dlq-fail",
      dlq: failingDlq,
    })
      .step("fail", () => Pipeline.fail(new TestError({ message: "original" })))
      .build();

    const { error } = await runner.runSafe({ workflow: wf, workflowId: "dlq-7", input: "x" });

    expect(error).not.toBeNull();
    expect((error as any).message).toBe("original");
  });

  it("DLQ is optional — workflows run fine without one configured", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });

    // No dlq option — should work fine
    const wf = workflow<string>({ name: "no-dlq" })
      .step("fail", () => Pipeline.fail(new TestError({ message: "fail" })))
      .build();

    const { error } = await runner.runSafe({ workflow: wf, workflowId: "no-dlq-1", input: "x" });

    expect(error).not.toBeNull();
  });

  it("pre-built workflow definition retains DLQ configuration", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const dlq = new InMemoryDlq();

    const definition = workflow<string>({
      name: "dlq-build",
      dlq,
    })
      .step("fail", () => Pipeline.fail(new TestError({ message: "fail" })))
      .build();

    await runner.runSafe({ workflow: definition, workflowId: "dlq-8", input: "x" });

    expect(dlq.messages).toHaveLength(1);
  });
});

// ---------------------------------------------------------------------------
// DLQ replay pattern
// ---------------------------------------------------------------------------

describe("DLQ replay — re-process failed workflows after fixing the root cause", () => {
  it("transient issue resolved — replay the failed order from its DLQ record", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const dlq = new InMemoryDlq();
    let shouldFail = true;

    const wf = workflow<{ n: number }>({
      name: "replay-test",
      dlq,
    })
      .step("process", ({ input }) => {
        if (shouldFail) return Pipeline.fail(new TestError({ message: "transient" }));
        return Pipeline.succeed(input.n * 2);
      })
      .build();

    // First run fails → goes to DLQ
    await runner.runSafe({ workflow: wf, workflowId: "replay-1", input: { n: 5 } });
    expect(dlq.messages).toHaveLength(1);

    // Fix the issue
    shouldFail = false;

    // Replay from DLQ record with a new workflowId
    const record = dlq.messages[0]!;
    const result = await runner.run({
      workflow: wf,
      workflowId: `${record.workflowId}-retry`,
      input: record.input as { n: number },
    });

    expect(result).toBe(10);
  });
});
