// ---------------------------------------------------------------------------
// agentAction — durability under crash and replay.
//
//   - A crash inside an LLM call resumes the turn by re-running the call
//     (think activities are idempotent), instead of halting on
//     AmbiguousActivityOutcome.
//   - A crash inside a non-idempotent tool does not re-run the tool: the
//     model gets an "outcome unknown" error result and the turn finishes.
//   - A crash inside an idempotent tool re-runs it with the same toolCallId.
//   - Replay does not re-emit events of recorded activities: an approval
//     resume sees no second approval.requested / turn.start.
//   - The tool-set snapshot is journaled per step, so a tool whose
//     `requireApproval` flips while the turn is suspended still replays
//     along the recorded path.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { z } from "zod";
import { completeSignal } from "@promin/workflow";
import { agentAction } from "../agent-action.ts";
import type { ToolRegistry } from "../tool-registry.ts";
import { tool, type AgentTool } from "../tool.ts";
import {
  collectingBus,
  crashAndRecover,
  deferred,
  freshRunner,
  lastMessage,
  reactiveLLM,
} from "./engine-harness.ts";

/** Calls `act` until the conversation has a tool result, then answers. */
function actThenAnswer(call: { name: string; input: unknown }) {
  return reactiveLLM((messages) =>
    lastMessage(messages)?.role === "tool"
      ? { content: "done", finishReason: "stop" }
      : {
          content: null,
          finishReason: "tool_calls",
          toolCalls: [{ id: "call-1", ...call }],
        },
  );
}

describe("agentAction — crash recovery", () => {
  it("re-runs an LLM call interrupted by a crash and finishes the turn", async () => {
    const gate = deferred();
    let llmCalls = 0;
    const llm = reactiveLLM(async () => {
      llmCalls++;
      if (llmCalls === 1) {
        await gate.promise;
        return { content: "from the crashed worker", finishReason: "stop" };
      }
      return { content: "recovered", finishReason: "stop" };
    });

    const { result } = await crashAndRecover({
      build: () => agentAction({ name: "think-crash", llm }),
      input: { task: "hi" },
      workflowId: "think-crash-1",
      stalled: () => llmCalls === 1,
      release: () => gate.resolve(),
    });

    expect(result.answer).toBe("recovered");
    expect(result.finishReason).toBe("stop");
    expect(llmCalls).toBe(2);
  });

  it("does not re-run a non-idempotent tool; the model hears the outcome is unknown", async () => {
    const gate = deferred();
    let runs = 0;
    const charge = tool({
      name: "charge",
      description: "charge a card",
      parameters: z.object({ amount: z.number() }),
      execute: async () => {
        runs++;
        await gate.promise;
        return "charged";
      },
    });
    const llm = actThenAnswer({ name: "charge", input: { amount: 5 } });

    const { result } = await crashAndRecover({
      build: () => agentAction({ name: "tool-crash", llm, tools: { charge } }),
      input: { task: "charge me" },
      workflowId: "tool-crash-1",
      stalled: () => runs === 1,
      release: () => gate.resolve(),
    });

    expect(runs).toBe(1);
    expect(result.answer).toBe("done");
    const toolResult = result.messages.find((m) => m.role === "tool");
    expect(toolResult).toMatchObject({ toolCallId: "call-1", isError: true });
    expect(toolResult?.role === "tool" && toolResult.content).toContain("Tool outcome unknown");
  });

  it("re-runs an idempotent tool with the same toolCallId", async () => {
    const gate = deferred();
    const seenCallIds: Array<string | undefined> = [];
    const upsert: AgentTool<{ key: string }, string> = {
      name: "upsert",
      description: "upsert a record",
      parameters: z.object({ key: z.string() }),
      idempotent: true,
      execute: async (_input, ctx) => {
        seenCallIds.push(ctx?.toolCallId);
        if (seenCallIds.length === 1) await gate.promise;
        return "stored";
      },
    };
    const llm = actThenAnswer({ name: "upsert", input: { key: "k" } });

    const { result } = await crashAndRecover({
      build: () => agentAction({ name: "idem-crash", llm, tools: { upsert } }),
      input: { task: "store it" },
      workflowId: "idem-crash-1",
      stalled: () => seenCallIds.length === 1,
      release: () => gate.resolve(),
    });

    expect(seenCallIds).toEqual(["call-1", "call-1"]);
    const toolResult = result.messages.find((m) => m.role === "tool");
    expect(toolResult).toMatchObject({ toolCallId: "call-1", content: "stored" });
    expect(result.answer).toBe("done");
  });
});

describe("agentAction — replay-safe events", () => {
  it("an approval resume emits only what happens after the resume", async () => {
    const { runner, storage } = freshRunner();
    const deploy = tool({
      name: "deploy",
      description: "deploy",
      parameters: z.object({ env: z.string() }),
      requireApproval: true,
      execute: async () => "deployed",
    });
    const llm = actThenAnswer({ name: "deploy", input: { env: "prod" } });

    const first = collectingBus();
    const firstRun = await runner.runSafe({
      workflow: agentAction({ name: "approve", llm, tools: { deploy }, bus: first.bus }),
      workflowId: "approve-1",
      input: { task: "ship it" },
    });
    expect(firstRun.error).toBeTruthy();
    const requested = first.events.find((e) => e.type === "approval.requested");
    expect(requested).toMatchObject({
      toolCallId: "call-1",
      toolName: "deploy",
      input: { env: "prod" },
    });

    const wf = await storage.loadWorkflow("approve-1");
    const waiting = Object.values(wf!.steps).find((s) => s.status === "waiting_for_signal")!;
    await completeSignal({
      storage,
      workflowId: "approve-1",
      stepName: waiting.stepName,
      signalName: "approve:call-1",
      value: { approved: true },
    });

    const second = collectingBus();
    const result = await runner.run({
      workflow: agentAction({ name: "approve", llm, tools: { deploy }, bus: second.bus }),
      workflowId: "approve-1",
      input: { task: "ship it" },
    });

    expect(result.answer).toBe("done");
    const types = second.events.map((e) => e.type);
    expect(types).not.toContain("turn.start");
    expect(types).not.toContain("approval.requested");
    expect(types).toContain("approval.decision");
    expect(types).toContain("tool.start");
    expect(types[types.length - 1]).toBe("turn.end");
    // The first think ran before the suspend; only the answering think is new.
    expect(types.filter((t) => t === "llm.call")).toHaveLength(1);
  });
});

describe("agentAction — tool-set snapshot", () => {
  it("replays the journaled approval path after the live tool stops requiring approval", async () => {
    const { runner, storage } = freshRunner();
    let requireApproval = true;
    let executions = 0;
    const registry: ToolRegistry = {
      getTools: () => ({
        deploy: {
          name: "deploy",
          description: "deploy",
          parameters: z.object({ env: z.string() }),
          requireApproval,
          execute: async () => {
            executions++;
            return "deployed";
          },
        },
      }),
      close: () => {},
    };
    const llm = actThenAnswer({ name: "deploy", input: { env: "prod" } });
    const build = () => agentAction({ name: "snapshot", llm, toolRegistry: registry });

    const firstRun = await runner.runSafe({
      workflow: build(),
      workflowId: "snapshot-1",
      input: { task: "ship" },
    });
    expect(firstRun.error).toBeTruthy();

    // Hot reload while suspended: the tool no longer asks for approval.
    requireApproval = false;
    const wf = await storage.loadWorkflow("snapshot-1");
    const waiting = Object.values(wf!.steps).find((s) => s.status === "waiting_for_signal")!;
    await completeSignal({
      storage,
      workflowId: "snapshot-1",
      stepName: waiting.stepName,
      signalName: "approve:call-1",
      value: { approved: true },
    });

    const result = await runner.run({
      workflow: build(),
      workflowId: "snapshot-1",
      input: { task: "ship" },
    });
    expect(result.answer).toBe("done");
    expect(executions).toBe(1);
  });
});

describe("agentAction — journaled approval verdicts", () => {
  it("keeps the approval path when autoApprove changes while the turn is suspended", async () => {
    const { runner, storage } = freshRunner();
    let runs = 0;
    const deploy = tool({
      name: "deploy",
      description: "deploy",
      parameters: z.object({ env: z.string() }),
      requireApproval: true,
      execute: async () => {
        runs++;
        return "deployed";
      },
    });
    let allow = false;
    const build = () =>
      agentAction({
        name: "auto-approve",
        llm: actThenAnswer({ name: "deploy", input: { env: "prod" } }),
        tools: { deploy },
        autoApprove: () => allow,
      });
    const first = await runner.runSafe({
      workflow: build(),
      workflowId: "auto-approve-1",
      input: { task: "ship" },
    });
    expect(first.error).toBeTruthy();

    // "Always allow" chosen in the approval UI while the turn waits.
    allow = true;
    const wf = await storage.loadWorkflow("auto-approve-1");
    const waiting = Object.values(wf!.steps).find((s) => s.status === "waiting_for_signal")!;
    await completeSignal({
      storage,
      workflowId: "auto-approve-1",
      stepName: waiting.stepName,
      signalName: "approve:call-1",
      value: { approved: true },
    });

    const result = await runner.run({
      workflow: build(),
      workflowId: "auto-approve-1",
      input: { task: "ship" },
    });
    expect(result.answer).toBe("done");
    expect(runs).toBe(1);
  });
});

describe("agentAction — crash inside hooks", () => {
  it("asks onApprovalRequired again after a crash inside it and finishes the turn", async () => {
    const gate = deferred();
    let asked = 0;
    const deploy = tool({
      name: "deploy",
      description: "deploy",
      parameters: z.object({ env: z.string() }),
      requireApproval: true,
      execute: async () => "deployed",
    });
    const llm = actThenAnswer({ name: "deploy", input: { env: "prod" } });
    const { result } = await crashAndRecover({
      build: () =>
        agentAction({
          name: "approval-crash",
          llm,
          tools: { deploy },
          onApprovalRequired: async () => {
            asked++;
            if (asked === 1) await gate.promise;
            return { approved: true };
          },
        }),
      input: { task: "ship" },
      workflowId: "approval-crash-1",
      stalled: () => asked === 1,
      release: () => gate.resolve(),
    });
    expect(asked).toBe(2);
    expect(result.answer).toBe("done");
  });

  it("runs afterTurn again after a crash inside it and finishes the turn", async () => {
    const gate = deferred();
    let calls = 0;
    const { result } = await crashAndRecover({
      build: () =>
        agentAction({
          name: "after-turn-crash",
          llm: reactiveLLM(() => ({ content: "hi", finishReason: "stop" })),
          hooks: {
            afterTurn: async () => {
              calls++;
              if (calls === 1) await gate.promise;
            },
          },
        }),
      input: { task: "hello" },
      workflowId: "after-turn-crash-1",
      stalled: () => calls === 1,
      release: () => gate.resolve(),
    });
    expect(calls).toBe(2);
    expect(result.answer).toBe("hi");
  });
});
