import { describe, it, expect } from "bun:test";
import { TaggedError, succeed, fail } from "@spilne/perfect-core";
import { workflow } from "../workflow-builder.ts";
import { InMemoryWorkflowStorage } from "../in-memory-storage.ts";
import { createWorkflowRunner } from "../workflow-runner.ts";
import { WorkflowCancelledError, WorkflowFailedError } from "../durable-pipeline-error.ts";
import { rehydrateFailure, workflowFailure } from "../workflow-failure.ts";
import type { FailedWorkflowRecord, WorkflowFailure } from "../workflow-state.ts";
import type { Sinkable } from "../../shared/streamable.ts";
import { JsonCodec, type Codec } from "@spilne/perfect-core/connect";

class PaymentDeclined extends TaggedError("PaymentDeclined")<{ readonly message: string }>() {}

function setup() {
  const storage = new InMemoryWorkflowStorage();
  const hookFailures: (WorkflowFailure | undefined)[] = [];
  const dlq: FailedWorkflowRecord[] = [];
  const sink: Sinkable<FailedWorkflowRecord> = {
    codec: JsonCodec as Codec<FailedWorkflowRecord>,
    publish: async (v) => void dlq.push(v),
  };
  const runner = createWorkflowRunner({
    storage,
    hooks: { onWorkflowFailure: ({ failure }) => void hookFailures.push(failure) },
  });
  const wf = workflow<number>({ name: "wf-failure", dlq: sink })
    .step("reserve", ({ input }) => succeed(input))
    .step("charge", { dependsOn: ["reserve"] }, () =>
      fail(new PaymentDeclined({ message: "card declined" })),
    )
    .build();
  return { storage, runner, wf, hookFailures, dlq };
}

describe("WorkflowFailure — one shape for a failed run", () => {
  it("every surface reports the same tag, message, step and attempt", async () => {
    const { storage, runner, wf, hookFailures, dlq } = setup();

    const { error, failure } = await runner.runSafe({ workflow: wf, workflowId: "f1", input: 1 });

    // runSafe: typed error kept, structured failure beside it with the live cause
    expect(error).toBeInstanceOf(PaymentDeclined);
    expect(failure).toMatchObject({
      tag: "PaymentDeclined",
      message: "card declined",
      stepName: "charge",
      attempt: 1,
    });
    expect(failure!.cause).toBe(error);

    const expected = {
      tag: "PaymentDeclined",
      message: "card declined",
      stepName: "charge",
      attempt: 1,
    };
    // hook and DLQ: same shape (with cause)
    expect(hookFailures[0]).toMatchObject(expected);
    expect(hookFailures[0]!.cause).toBe(error);
    expect(dlq[0]!.failure).toMatchObject(expected);

    // storage state and handle status: derived, no cause, legacy fields kept
    const state = (await storage.loadWorkflow("f1"))!;
    expect(state.errorTag).toBe("PaymentDeclined");
    expect(workflowFailure(state)).toEqual(expected);
    const status = await runner.getStatus({ workflowId: "f1" });
    expect(status!.failure).toEqual(expected);
    expect(status!.error).toBe("card declined");
    expect(status!.errorTag).toBe("PaymentDeclined");

    // handle.result(): WorkflowFailedError carrying the same step and attempt
    const result = await runner
      .handle("f1")
      .result()
      .catch((e: unknown) => e);
    expect(result).toBeInstanceOf(WorkflowFailedError);
    expect(result).toMatchObject({ errorTag: "PaymentDeclined", stepName: "charge", attempt: 1 });
  });

  it("a run that did not fail has no failure", async () => {
    const { storage, runner } = setup();
    const ok = workflow<number>({ name: "ok" })
      .step("a", ({ input }) => succeed(input))
      .build();
    await runner.run({ workflow: ok, workflowId: "ok1", input: 1 });
    expect(workflowFailure((await storage.loadWorkflow("ok1"))!)).toBeUndefined();
    expect((await runner.getStatus({ workflowId: "ok1" }))!.failure).toBeUndefined();
  });

  it("an untagged defect is reported under the unknown tag", async () => {
    const { runner } = setup();
    const wf = workflow<number>({ name: "defect" })
      .step("boom", () => {
        throw new Error("kaput");
      })
      .build();
    const { failure } = await runner.runSafe({ workflow: wf, workflowId: "d1", input: 1 });
    expect(failure).toMatchObject({ tag: "UnknownError", message: "kaput", stepName: "boom" });
  });

  it("a cancelled run reads as WorkflowCancelledError and rehydrates", async () => {
    const { storage } = setup();
    await storage.createWorkflow({
      workflowId: "c1",
      workflowName: "wf-failure",
      input: 1,
    });
    await storage.cancelWorkflow({ workflowId: "c1" });
    const failure = workflowFailure((await storage.loadWorkflow("c1"))!)!;
    expect(failure.tag).toBe("WorkflowCancelledError");
    expect(rehydrateFailure({ workflowId: "c1", failure })).toBeInstanceOf(WorkflowCancelledError);
  });

  it("rehydrates a known tag to its typed error and an unknown tag to WorkflowFailedError", () => {
    const failure: WorkflowFailure = {
      tag: "PaymentDeclined",
      message: "card declined",
      stepName: "charge",
      attempt: 2,
    };
    const typed = rehydrateFailure({
      workflowId: "w",
      failure,
      errors: { PaymentDeclined: ({ failure: f }) => new PaymentDeclined({ message: f.message }) },
    });
    expect(typed).toBeInstanceOf(PaymentDeclined);

    const generic = rehydrateFailure({ workflowId: "w", failure });
    expect(generic).toBeInstanceOf(WorkflowFailedError);
    expect(generic).toMatchObject({ errorTag: "PaymentDeclined", stepName: "charge", attempt: 2 });
  });
});
