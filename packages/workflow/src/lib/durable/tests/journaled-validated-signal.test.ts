// ---------------------------------------------------------------------------
// ctx.validatedSignal + ctx.approval — typed durable signal waits.
//
// Same suspend/resume mechanics as ctx.signal, but keyed by a SignalType
// artifact (defineSignal({name, schema})). The schema's JSON Schema snapshot
// gets written onto step.signalJsonSchema at suspend time so the server's
// delivery path can validate inbound payloads against the shape this suspend
// point actually waited on — even if the SignalType definition later evolves.
// ---------------------------------------------------------------------------

import { describe, it, expect, beforeEach } from "bun:test";
import { workflow } from "../durable-pipeline.ts";
import { InMemoryWorkflowStorage } from "../in-memory-storage.ts";
import { createWorkflowRunner } from "../workflow-runner.ts";
import { runJournaledStep, completeSignal } from "../journaled-step.ts";
import type { JournaledContext } from "../journaled-step.ts";
import { WorkflowSuspendedError } from "../durable-pipeline-error.ts";
import { defineSignal, approvalSignal, ApprovalSchema } from "../../signals/define-signal.ts";
import { s } from "../../schema/builder.ts";

const ReviewSignal = defineSignal({
  name: "review",
  schema: s.object({
    approved: s.boolean(),
    reason: s.string().optional(),
  }),
});

describe("ctx.validatedSignal — typed signal wait", () => {
  let storage: InMemoryWorkflowStorage;

  beforeEach(async () => {
    storage = new InMemoryWorkflowStorage();
    // Need a workflow record before signalImpl can call suspendWorkflow.
    const wf = workflow({ name: "vs-test", inputSchema: undefined }).journaled(
      "gate",
      function* () {
        return undefined;
      },
    );
    const runner = createWorkflowRunner({ storage });
    await storage.createWorkflow({ workflowId: "wf-vs-1", workflowName: "vs-test", input: {} });
    void runner;
    void wf;
  });

  it("first run suspends and writes the schema snapshot onto the step", async () => {
    const body = function* (ctx: JournaledContext<unknown, unknown>) {
      const value = yield* ctx.validatedSignal(ReviewSignal);
      return value;
    };

    await expect(
      runJournaledStep({
        input: {},
        prev: {},
        workflowId: "wf-vs-1",
        stepName: "gate",
        storage,
        workflowStorage: storage,
        body,
      }),
    ).rejects.toBeInstanceOf(WorkflowSuspendedError);

    // The journal entry uses the SignalType's name (no `validated:` prefix —
    // wire format is unchanged).
    const journal = await storage.loadJournal("wf-vs-1", "gate");
    expect(journal[0]!.activityName).toBe("review");
    expect(journal[0]!.stepType).toBe("signal");

    // The schema snapshot lands on the StepState's signalJsonSchema field.
    const state = await storage.loadWorkflow("wf-vs-1");
    const step = state?.steps.gate;
    expect(step?.status).toBe("waiting_for_signal");
    expect(step?.signalName).toBe("review");
    expect(step?.signalJsonSchema).toEqual(ReviewSignal.schema.jsonSchema);
  });

  it("delivery resumes with the typed value", async () => {
    const body = function* (ctx: JournaledContext<unknown, unknown>) {
      const value = yield* ctx.validatedSignal(ReviewSignal);
      return value;
    };

    await expect(
      runJournaledStep({
        input: {},
        prev: {},
        workflowId: "wf-vs-1",
        stepName: "gate",
        storage,
        workflowStorage: storage,
        body,
      }),
    ).rejects.toBeInstanceOf(WorkflowSuspendedError);

    const delivered = await completeSignal({
      storage,
      workflowId: "wf-vs-1",
      stepName: "gate",
      signalName: "review",
      value: { approved: true, reason: "looks good" },
    });
    expect(delivered).toBe(true);

    const result = await runJournaledStep<unknown, unknown, { approved: boolean; reason?: string }>(
      {
        input: {},
        prev: {},
        workflowId: "wf-vs-1",
        stepName: "gate",
        storage,
        workflowStorage: storage,
        body: body as never,
      },
    );
    expect(result).toEqual({ approved: true, reason: "looks good" });
  });
});

describe("ctx.approval — preset sugar for canonical approvals", () => {
  let storage: InMemoryWorkflowStorage;

  beforeEach(async () => {
    storage = new InMemoryWorkflowStorage();
    await storage.createWorkflow({ workflowId: "wf-ap-1", workflowName: "ap-test", input: {} });
  });

  it("suspends on approve:<id> and stores the canonical ApprovalSchema", async () => {
    const body = function* (ctx: JournaledContext<unknown, unknown>) {
      const decision = yield* ctx.approval("call-xyz");
      return decision;
    };

    await expect(
      runJournaledStep({
        input: {},
        prev: {},
        workflowId: "wf-ap-1",
        stepName: "gate",
        storage,
        workflowStorage: storage,
        body,
      }),
    ).rejects.toBeInstanceOf(WorkflowSuspendedError);

    const state = await storage.loadWorkflow("wf-ap-1");
    const step = state?.steps.gate;
    expect(step?.signalName).toBe("approve:call-xyz");
    expect(step?.signalJsonSchema).toEqual(ApprovalSchema.jsonSchema);

    // approvalSignal is also reusable directly — same shape comes out.
    const sig = approvalSignal("call-xyz");
    expect(step?.signalJsonSchema).toEqual(sig.schema.jsonSchema);
  });

  it("delivery resumes with the ApprovalDecision shape", async () => {
    const body = function* (ctx: JournaledContext<unknown, unknown>) {
      const decision = yield* ctx.approval("call-xyz");
      return decision;
    };

    await expect(
      runJournaledStep({
        input: {},
        prev: {},
        workflowId: "wf-ap-1",
        stepName: "gate",
        storage,
        workflowStorage: storage,
        body,
      }),
    ).rejects.toBeInstanceOf(WorkflowSuspendedError);

    await completeSignal({
      storage,
      workflowId: "wf-ap-1",
      stepName: "gate",
      signalName: "approve:call-xyz",
      value: { approved: false, reason: "missing context" },
    });

    const result = await runJournaledStep<
      unknown,
      unknown,
      { approved: boolean; reason?: string; by?: string }
    >({
      input: {},
      prev: {},
      workflowId: "wf-ap-1",
      stepName: "gate",
      storage,
      workflowStorage: storage,
      body: body as never,
    });
    expect(result).toEqual({ approved: false, reason: "missing context" });
  });
});
