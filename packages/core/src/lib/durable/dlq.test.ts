import { describe, it, expect } from "bun:test";
import { Data } from "effect";
import { Pipeline } from "../pipeline.ts";
import { JsonCodec } from "../typeclasses/codec.ts";
import { workflow } from "./durable-pipeline.ts";
import { InMemoryWorkflowStorage } from "./in-memory-storage.ts";
import type { FailedWorkflowRecord } from "./workflow-state.ts";
import type { Sinkable } from "../typeclasses/streamable.ts";
import type { Codec } from "../typeclasses/codec.ts";

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
    const dlq = new InMemoryDlq();

    await workflow<{ userId: string }>({
      name: "dlq-basic",
      storage,
      dlq,
    })
      .step("step-1", ({ input }) => Pipeline.succeed(input.userId))
      .step("step-2", () => Pipeline.fail(new TestError({ message: "boom" })))
      .runSafe({ workflowId: "dlq-1", input: { userId: "u_42" } });

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
    const dlq = new InMemoryDlq();

    await workflow<string>({
      name: "dlq-steps",
      storage,
      dlq,
    })
      .step("ok-step", () => Pipeline.succeed("done"))
      .step("fail-step", () => Pipeline.fail(new TestError({ message: "fail" })))
      .runSafe({ workflowId: "dlq-2", input: "x" });

    const record = dlq.messages[0]!;
    expect(record.steps["ok-step"]).toBeDefined();
    expect(record.steps["ok-step"]!.status).toBe("completed");
    expect(record.steps["fail-step"]).toBeDefined();
    expect(record.steps["fail-step"]!.status).toBe("failed");
  });

  it("DLQ record tracks which rollbacks succeeded and which failed", async () => {
    const storage = new InMemoryWorkflowStorage();
    const dlq = new InMemoryDlq();

    await workflow<string>({
      name: "dlq-comp",
      storage,
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
      .runSafe({ workflowId: "dlq-3", input: "x" });

    const record = dlq.messages[0]!;
    expect(record.compensatedSteps).toContain("step-1");
    expect(record.failedCompensations).toHaveLength(1);
    expect(record.failedCompensations[0]!.stepName).toBe("step-2");
    expect(record.failedCompensations[0]!.error).toBe("comp-failed");
  });

  it("team and priority metadata attached — route DLQ alerts to the right on-call", async () => {
    const storage = new InMemoryWorkflowStorage();
    const dlq = new InMemoryDlq();

    await workflow<string>({
      name: "dlq-meta",
      storage,
      dlq,
      metadata: { team: "growth", priority: "high" },
    })
      .step("fail", () => Pipeline.fail(new TestError({ message: "fail" })))
      .runSafe({ workflowId: "dlq-4", input: "x" });

    const record = dlq.messages[0]!;
    expect(record.metadata).toEqual({ team: "growth", priority: "high" });
  });

  it("successful workflow does not clutter the DLQ", async () => {
    const storage = new InMemoryWorkflowStorage();
    const dlq = new InMemoryDlq();

    await workflow<number>({
      name: "dlq-success",
      storage,
      dlq,
    })
      .step("ok", ({ input }) => Pipeline.succeed(input * 2))
      .run({ workflowId: "dlq-5", input: 5 });

    expect(dlq.messages).toHaveLength(0);
  });

  it("DLQ message sent only after all retry attempts are exhausted", async () => {
    const storage = new InMemoryWorkflowStorage();
    const dlq = new InMemoryDlq();
    let attempts = 0;

    await workflow<string>({
      name: "dlq-retry",
      storage,
      dlq,
      retry: { maxRetries: 2, baseDelayMs: 10 },
    })
      .step("fail", () => {
        attempts++;
        return Pipeline.fail(new TestError({ message: `fail-${attempts}` }));
      })
      .runSafe({ workflowId: "dlq-6", input: "x" });

    // Published once after all 3 attempts (1 + 2 retries) exhausted
    expect(dlq.messages).toHaveLength(1);
    expect(attempts).toBe(3);
  });

  it("DLQ itself is down — original business error is still surfaced to the caller", async () => {
    const storage = new InMemoryWorkflowStorage();
    const failingDlq: Sinkable<FailedWorkflowRecord> = {
      codec: JsonCodec as Codec<FailedWorkflowRecord>,
      publish: async () => {
        throw new Error("DLQ is down");
      },
    };

    const { error } = await workflow<string>({
      name: "dlq-fail",
      storage,
      dlq: failingDlq,
    })
      .step("fail", () => Pipeline.fail(new TestError({ message: "original" })))
      .runSafe({ workflowId: "dlq-7", input: "x" });

    expect(error).not.toBeNull();
    expect((error as any).message).toBe("original");
  });

  it("DLQ is optional — workflows run fine without one configured", async () => {
    const storage = new InMemoryWorkflowStorage();

    // No dlq option — should work fine
    const { error } = await workflow<string>({ name: "no-dlq", storage })
      .step("fail", () => Pipeline.fail(new TestError({ message: "fail" })))
      .runSafe({ workflowId: "no-dlq-1", input: "x" });

    expect(error).not.toBeNull();
  });

  it("pre-built workflow definition retains DLQ configuration", async () => {
    const storage = new InMemoryWorkflowStorage();
    const dlq = new InMemoryDlq();

    const definition = workflow<string>({
      name: "dlq-build",
      storage,
      dlq,
    })
      .step("fail", () => Pipeline.fail(new TestError({ message: "fail" })))
      .build();

    await definition.runSafe({ workflowId: "dlq-8", input: "x" });

    expect(dlq.messages).toHaveLength(1);
  });
});

// ---------------------------------------------------------------------------
// DLQ replay pattern
// ---------------------------------------------------------------------------

describe("DLQ replay — re-process failed workflows after fixing the root cause", () => {
  it("transient issue resolved — replay the failed order from its DLQ record", async () => {
    const storage = new InMemoryWorkflowStorage();
    const dlq = new InMemoryDlq();
    let shouldFail = true;

    const wf = workflow<{ n: number }>({
      name: "replay-test",
      storage,
      dlq,
    })
      .step("process", ({ input }) => {
        if (shouldFail) return Pipeline.fail(new TestError({ message: "transient" }));
        return Pipeline.succeed(input.n * 2);
      })
      .build();

    // First run fails → goes to DLQ
    await wf.runSafe({ workflowId: "replay-1", input: { n: 5 } });
    expect(dlq.messages).toHaveLength(1);

    // Fix the issue
    shouldFail = false;

    // Replay from DLQ record with a new workflowId
    const record = dlq.messages[0]!;
    const result = await wf.run({
      workflowId: `${record.workflowId}-retry`,
      input: record.input as { n: number },
    });

    expect(result).toBe(10);
  });
});
