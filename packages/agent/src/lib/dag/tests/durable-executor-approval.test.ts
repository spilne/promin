// A suspended approval branch does not stop an independent agent sibling:
// the sibling can finish while the DAG remains suspended awaiting a decision.

import { describe, expect, it } from "bun:test";
import {
  FakeWallClock,
  InMemoryWorkflowStorage,
  completeSignal,
  createWorkflowRunner,
} from "@promin/workflow";
import { z } from "zod";

import { LocalAgent } from "../../agent/local-agent.ts";
import type { AgentTool } from "../../tool.ts";
import { listPendingSignals } from "../../approvals/list-pending-signals.ts";
import { InMemoryDagRegistry } from "../../versioned-registry/in-memory-registries.ts";
import { deferred, lastMessage, reactiveLLM, until } from "../../tests/engine-harness.ts";
import { createDagWorkflow } from "../durable-executor.ts";
import type { AgentResolver, AgenticDagRecipe } from "../types.ts";
import {
  SCOPE,
  invokeAgent,
  localAgents,
  makeResolver,
  stubAgent,
  taskOf,
  textOnly,
} from "./dag-test-kit.ts";

const routingDag: AgenticDagRecipe = {
  id: "approval-routing",
  version: "v1",
  nodes: [
    {
      id: "draft",
      kind: "agent",
      agentId: "draft",
      inputs: {},
    },
    {
      id: "gate",
      kind: "approval",
      prompt: "Publish this?",
      inputs: { draft: { kind: "node", nodeId: "draft", path: "" } },
    },
    {
      id: "publish",
      kind: "agent",
      agentId: "publish",
      inputs: {},
    },
    {
      id: "revise",
      kind: "agent",
      agentId: "revise",
      inputs: {},
    },
  ],
  edges: [
    { from: "draft", to: "gate" },
    {
      from: "gate",
      to: "publish",
      condition: { kind: "equals", path: "approved", value: true },
    },
    {
      from: "gate",
      to: "revise",
      condition: { kind: "equals", path: "approved", value: false },
    },
  ],
  entry: ["draft"],
  terminals: ["publish", "revise"],
};

async function suspendedRun(params: {
  readonly dag: AgenticDagRecipe;
  readonly resolver?: AgentResolver;
  readonly workflowId: string;
  readonly storage?: InMemoryWorkflowStorage;
  readonly clock?: FakeWallClock;
  readonly budget?: { readonly maxLlmCalls: number };
}) {
  const storage = params.storage ?? new InMemoryWorkflowStorage({ clock: params.clock });
  const registry = new InMemoryDagRegistry();
  await registry.register(params.dag);
  const runner = createWorkflowRunner({
    storage,
    ...(params.clock !== undefined && { clock: params.clock }),
  });
  const workflow = createDagWorkflow({
    registry,
    resolver: params.resolver ?? makeResolver({}),
    ...(params.clock !== undefined && { clock: params.clock }),
  });
  const first = await runner.runSafe({
    workflow,
    workflowId: params.workflowId,
    input: {
      dagId: params.dag.id,
      version: params.dag.version,
      initialInput: {},
      scope: SCOPE,
      ...(params.budget !== undefined && { budget: params.budget }),
    },
  });
  expect(first.error).toBeTruthy();
  expect((await storage.loadWorkflow(params.workflowId))?.status).toBe("suspended");
  return { storage, runner, workflow };
}

async function resume(params: {
  readonly storage: InMemoryWorkflowStorage;
  readonly runner: ReturnType<typeof createWorkflowRunner>;
  readonly workflow: ReturnType<typeof createDagWorkflow>;
  readonly workflowId: string;
  readonly payload: { readonly approved: boolean; readonly reason?: string; readonly by?: string };
}) {
  await params.storage.deliverSignal({
    workflowId: params.workflowId,
    signalName: "approve:gate",
    payload: params.payload,
  });
  await completeSignal({
    storage: params.storage,
    workflowId: params.workflowId,
    stepName: "execute-dag",
    signalName: "approve:gate",
    value: params.payload,
  });
  return params.runner.run({
    workflow: params.workflow,
    workflowId: params.workflowId,
    input: undefined,
    useStoredInput: true,
  });
}

function waitingAgentFixture(params: {
  readonly runner: ReturnType<typeof createWorkflowRunner>;
  readonly ran: string[];
}): AgentResolver {
  const guarded: AgentTool<{ value: string }, string> = {
    name: "guarded",
    description: "needs approval",
    parameters: z.object({ value: z.string() }),
    requireApproval: true,
    execute: async ({ value }) => {
      params.ran.push(value);
      return `ran ${value}`;
    },
  };
  const llm = reactiveLLM((messages) => {
    const last = lastMessage(messages);
    if (last?.role === "tool") {
      if (String(last.content).startsWith("Rejected:")) {
        return {
          content: null,
          finishReason: "tool_calls",
          toolCalls: [{ id: "invalid-output", name: "_respond", input: {} }],
        };
      }
      return { content: `done: ${String(last.content)}`, finishReason: "stop" };
    }
    const task = taskOf(messages);
    const item = /"item"\s*:\s*(\d+)/.exec(task)?.[1];
    const value = item === undefined ? "plain" : `item-${item}`;
    return {
      content: null,
      finishReason: "tool_calls",
      toolCalls: [{ id: `call-${value}`, name: "guarded", input: { value } }],
    };
  });
  return async ({ scope }) =>
    new LocalAgent({
      ...(scope.namespaceId !== undefined && { namespaceId: scope.namespaceId }),
      runner: params.runner,
      agent: { name: "worker", llm, tools: { guarded }, structuredOutputRepairs: 0 },
    });
}

function waitingAgentDag(params: {
  readonly id: string;
  readonly timeoutMs?: number;
  readonly forEach?: {
    readonly items: { readonly kind: "initial"; readonly path: string };
    readonly as: string;
    readonly maxItems: number;
    readonly concurrency?: number;
  };
}): AgenticDagRecipe {
  return {
    id: params.id,
    version: "v1",
    nodes: [
      {
        id: "n1",
        kind: "agent",
        agentId: "worker",
        approvals: "wait",
        inputs: {},
        ...(params.timeoutMs !== undefined && { timeoutMs: params.timeoutMs }),
        ...(params.forEach !== undefined && { forEach: params.forEach }),
      },
    ],
    edges: [],
    entry: ["n1"],
    terminals: ["n1"],
  };
}

async function decidePending(params: {
  readonly storage: InMemoryWorkflowStorage;
  readonly workflowId: string;
  readonly approved?: boolean;
}): Promise<void> {
  const pending = (await listPendingSignals(params.storage)).find(
    (signal) => signal.workflowId === params.workflowId,
  );
  expect(pending).toBeDefined();
  await params.storage.deliverSignal({
    workflowId: params.workflowId,
    signalName: pending!.signalName,
    payload: { approved: params.approved ?? true },
  });
  await completeSignal({
    storage: params.storage,
    workflowId: params.workflowId,
    stepName: pending!.stepName,
    signalName: pending!.signalName,
    value: { approved: params.approved ?? true },
  });
}

async function startWaitingRun(params: {
  readonly dag: AgenticDagRecipe;
  readonly workflowId: string;
  readonly initialInput?: Record<string, unknown>;
  readonly extraDags?: ReadonlyArray<AgenticDagRecipe>;
  readonly clock?: FakeWallClock;
}) {
  const storage = new InMemoryWorkflowStorage({ clock: params.clock });
  const registry = new InMemoryDagRegistry();
  for (const dag of params.extraDags ?? []) await registry.register(dag);
  await registry.register(params.dag);
  const runner = createWorkflowRunner({
    storage,
    ...(params.clock !== undefined && { clock: params.clock }),
  });
  const ran: string[] = [];
  const workflow = createDagWorkflow({
    registry,
    resolver: waitingAgentFixture({ runner, ran }),
    ...(params.clock !== undefined && { clock: params.clock }),
  });
  const first = await runner.runSafe({
    workflow,
    workflowId: params.workflowId,
    input: {
      dagId: params.dag.id,
      version: params.dag.version,
      initialInput: params.initialInput ?? {},
      scope: SCOPE,
    },
  });
  expect(first.error).toBeTruthy();
  return { storage, registry, runner, workflow, ran };
}

describe("createDagWorkflow — approval nodes", () => {
  it("lets an independent agent finish while an approval branch is suspended", async () => {
    const storage = new InMemoryWorkflowStorage();
    const registry = new InMemoryDagRegistry();
    const runner = createWorkflowRunner({ storage });
    const releaseSide = deferred();
    let sideFinished = false;
    const dag: AgenticDagRecipe = {
      id: "approval-parallel",
      version: "v1",
      nodes: [
        {
          id: "gate",
          kind: "approval",
          prompt: "Continue?",
          inputs: {},
        },
        {
          id: "side",
          kind: "agent",
          agentId: "side",
          inputs: {},
        },
      ],
      edges: [],
      entry: ["gate", "side"],
      terminals: ["gate", "side"],
    };
    await registry.register(dag);
    const workflow = createDagWorkflow({
      registry,
      resolver: makeResolver({
        side: invokeAgent(async () => {
          await releaseSide.promise;
          sideFinished = true;
          return textOnly("side done");
        }),
      }),
    });

    const suspended = await runner.runSafe({
      workflow,
      workflowId: "approval-parallel",
      input: {
        dagId: dag.id,
        version: dag.version,
        initialInput: {},
        scope: SCOPE,
      },
    });
    expect(suspended.error).toBeTruthy();
    expect((await storage.loadWorkflow("approval-parallel"))?.status).toBe("suspended");

    releaseSide.resolve();
    await until(() => sideFinished);

    expect(sideFinished).toBe(true);
    expect((await storage.loadWorkflow("approval-parallel"))?.status).toBe("suspended");
  });

  it("continues through an approved gate", async () => {
    let publishCalls = 0;
    const run = await suspendedRun({
      dag: routingDag,
      workflowId: "approval-yes",
      resolver: makeResolver({
        draft: stubAgent(() => "draft"),
        publish: stubAgent(() => {
          publishCalls++;
          return "published";
        }),
        revise: stubAgent(() => "revised"),
      }),
    });

    const result = await resume({
      ...run,
      workflowId: "approval-yes",
      payload: { approved: true, by: "ana" },
    });

    expect(result.nodeOutputs.gate).toEqual({
      approved: true,
      decidedBy: "ana",
      timedOut: false,
    });
    expect(result.nodeOutputs.publish).toBe("published");
    expect(publishCalls).toBe(1);
  });

  it("routes a rejection as an output rather than an error", async () => {
    const run = await suspendedRun({
      dag: routingDag,
      workflowId: "approval-no",
      resolver: makeResolver({
        draft: stubAgent(() => "draft"),
        publish: stubAgent(() => "published"),
        revise: stubAgent(() => "revised"),
      }),
    });

    const result = await resume({
      ...run,
      workflowId: "approval-no",
      payload: { approved: false, reason: "tone" },
    });

    expect(result.ok).toBe(true);
    expect(result.nodeOutputs.gate).toEqual({
      approved: false,
      reason: "tone",
      timedOut: false,
    });
    expect(result.nodeOutputs.revise).toBe("revised");
    expect(result.nodeOutputs.publish).toBeUndefined();
    expect(result.errors).toEqual({});
  });

  it("lists the approval prompt and resolved inputs", async () => {
    const run = await suspendedRun({
      dag: routingDag,
      workflowId: "approval-inbox",
      resolver: makeResolver({
        draft: stubAgent(() => "draft"),
        publish: stubAgent(),
        revise: stubAgent(),
      }),
    });

    expect(await listPendingSignals(run.storage)).toEqual([
      expect.objectContaining({
        workflowId: "approval-inbox",
        signalName: "approve:gate",
        isApproval: true,
        toolName: "dag-approval",
        toolInput: { prompt: "Publish this?", inputs: { draft: "draft" } },
      }),
    ]);
  });

  it("turns a timeout into a rejection by default", async () => {
    const clock = FakeWallClock.create(0);
    const dag: AgenticDagRecipe = {
      id: "approval-timeout-reject",
      version: "v1",
      nodes: [{ id: "gate", kind: "approval", inputs: {}, timeoutMs: 3_600_000 }],
      edges: [],
      entry: ["gate"],
      terminals: ["gate"],
    };
    const run = await suspendedRun({
      dag,
      workflowId: "approval-timeout-reject",
      clock,
    });

    clock.advance(3_600_000);
    const result = await run.runner.run({
      workflow: run.workflow,
      workflowId: "approval-timeout-reject",
      input: undefined,
      useStoredInput: true,
    });

    expect(result.nodeOutputs.gate).toEqual({ approved: false, timedOut: true });
  });

  it("can fail an approval node on timeout", async () => {
    const clock = FakeWallClock.create(0);
    const dag: AgenticDagRecipe = {
      id: "approval-timeout-fail",
      version: "v1",
      nodes: [
        {
          id: "gate",
          kind: "approval",
          inputs: {},
          timeoutMs: 3_600_000,
          onTimeout: "fail",
        },
      ],
      edges: [],
      entry: ["gate"],
      terminals: ["gate"],
    };
    const run = await suspendedRun({ dag, workflowId: "approval-timeout-fail", clock });

    clock.advance(3_600_000);
    const result = await run.runner.run({
      workflow: run.workflow,
      workflowId: "approval-timeout-fail",
      input: undefined,
      useStoredInput: true,
    });

    expect(result.errors.gate).toMatch(/^approval_timeout/);
  });

  it("does not produce a gate output when an independent sibling aborts before resume", async () => {
    const dag: AgenticDagRecipe = {
      id: "approval-abort",
      version: "v1",
      nodes: [
        {
          id: "boom",
          kind: "transform",
          inputs: { value: { kind: "literal", value: "not an array" } },
          from: "value",
          ops: [{ op: "flatten" }],
        },
        { id: "gate", kind: "approval", inputs: {} },
      ],
      edges: [],
      entry: ["boom", "gate"],
      terminals: ["boom", "gate"],
    };
    const storage = new InMemoryWorkflowStorage();
    const registry = new InMemoryDagRegistry();
    await registry.register(dag);
    const runner = createWorkflowRunner({ storage });
    const workflow = createDagWorkflow({
      registry,
      resolver: makeResolver({}),
    });

    const run = await runner.runSafe({
      workflow,
      workflowId: "approval-abort",
      input: {
        dagId: dag.id,
        version: dag.version,
        initialInput: {},
        scope: SCOPE,
      },
    });

    expect(run.error).toBeTruthy();
    await completeSignal({
      storage,
      workflowId: "approval-abort",
      stepName: "execute-dag",
      signalName: "approve:gate",
      value: { approved: true },
    });
    const result = await runner.run({
      workflow,
      workflowId: "approval-abort",
      input: undefined,
      useStoredInput: true,
    });
    expect(result.ok).toBe(false);
    expect(result.nodeOutputs.gate).toBeUndefined();
  });

  it("does not change a timed-out run when a decision arrives late", async () => {
    const clock = FakeWallClock.create(0);
    const dag: AgenticDagRecipe = {
      id: "approval-late",
      version: "v1",
      nodes: [{ id: "gate", kind: "approval", inputs: {}, timeoutMs: 60_000 }],
      edges: [],
      entry: ["gate"],
      terminals: ["gate"],
    };
    const run = await suspendedRun({ dag, workflowId: "approval-late", clock });
    clock.advance(60_000);
    await run.runner.run({
      workflow: run.workflow,
      workflowId: "approval-late",
      input: undefined,
      useStoredInput: true,
    });
    const before = (await run.storage.loadWorkflow("approval-late"))?.result;

    await run.storage.deliverSignal({
      workflowId: "approval-late",
      signalName: "approve:gate",
      payload: { approved: true },
    });

    expect((await run.storage.loadWorkflow("approval-late"))?.result).toEqual(before);
  });

  it("does not change a finished run when a second decision arrives", async () => {
    const run = await suspendedRun({
      dag: routingDag,
      workflowId: "approval-twice",
      resolver: makeResolver({
        draft: stubAgent(() => "draft"),
        publish: stubAgent(() => "published"),
        revise: stubAgent(() => "revised"),
      }),
    });
    await resume({
      ...run,
      workflowId: "approval-twice",
      payload: { approved: true },
    });
    const before = (await run.storage.loadWorkflow("approval-twice"))?.result;

    await run.storage.deliverSignal({
      workflowId: "approval-twice",
      signalName: "approve:gate",
      payload: { approved: false },
    });

    expect((await run.storage.loadWorkflow("approval-twice"))?.result).toEqual(before);
  });

  it("takes no share from the agents' LLM-call budget", async () => {
    const storage = new InMemoryWorkflowStorage();
    const llm = reactiveLLM(() => ({ content: "ok", finishReason: "stop" }));
    const agents = await localAgents({ llms: { worker: llm }, storage });
    const dag: AgenticDagRecipe = {
      id: "approval-budget",
      version: "v1",
      nodes: [
        { id: "before", kind: "agent", agentId: "worker", inputs: {} },
        { id: "gate", kind: "approval", inputs: {} },
        { id: "after", kind: "agent", agentId: "worker", inputs: {} },
      ],
      edges: [
        { from: "before", to: "gate" },
        { from: "gate", to: "after" },
      ],
      entry: ["before"],
      terminals: ["after"],
    };
    const run = await suspendedRun({
      dag,
      resolver: agents.boot().resolver,
      workflowId: "approval-budget",
      storage,
      budget: { maxLlmCalls: 2 },
    });

    const result = await resume({
      ...run,
      workflowId: "approval-budget",
      payload: { approved: true },
    });

    expect(result.ok).toBe(true);
    expect(llm.calls).toHaveLength(2);
    expect(result.spend.llmCalls).toBe(2);
  });
});

describe("createDagWorkflow — agent tool approvals", () => {
  it('waits for an approval when the agent node opts in with approvals: "wait"', async () => {
    const run = await startWaitingRun({
      dag: waitingAgentDag({ id: "agent-approval-wait" }),
      workflowId: "agent-approval-wait",
    });

    await decidePending({ storage: run.storage, workflowId: "agent-approval-wait.node-n1" });
    const result = await run.runner.run({
      workflow: run.workflow,
      workflowId: "agent-approval-wait",
      input: undefined,
      useStoredInput: true,
    });

    expect(result.ok).toBe(true);
    expect(run.ran).toEqual(["plain"]);
  });

  it("lists a waiting node under its child run with no agent thread", async () => {
    const run = await startWaitingRun({
      dag: waitingAgentDag({ id: "agent-approval-inbox" }),
      workflowId: "agent-approval-inbox",
    });

    const pending = (await listPendingSignals(run.storage)).find(
      (row) => row.workflowId === "agent-approval-inbox.node-n1",
    );
    expect(pending).toEqual(
      expect.objectContaining({
        workflowId: "agent-approval-inbox.node-n1",
        toolName: "guarded",
        toolInput: { value: "plain" },
      }),
    );
    expect(pending?.agentThread).toBeUndefined();
  });

  it("parks a parent DAG while an agent inside its sub-DAG waits", async () => {
    const child = waitingAgentDag({ id: "approval-child" });
    const parent: AgenticDagRecipe = {
      id: "approval-parent",
      version: "v1",
      nodes: [
        {
          id: "sub",
          kind: "dag",
          dagId: child.id,
          version: child.version,
          inputs: {},
        },
      ],
      edges: [],
      entry: ["sub"],
      terminals: ["sub"],
    };
    const run = await startWaitingRun({
      dag: parent,
      extraDags: [child],
      workflowId: "approval-parent",
    });
    expect((await run.storage.loadWorkflow("approval-parent"))?.status).toBe("suspended");
    expect((await run.storage.loadWorkflow("approval-parent.node-sub"))?.status).toBe("suspended");

    await decidePending({
      storage: run.storage,
      workflowId: "approval-parent.node-sub.node-n1",
    });
    const result = await run.runner.run({
      workflow: run.workflow,
      workflowId: "approval-parent",
      input: undefined,
      useStoredInput: true,
    });

    expect(result.ok).toBe(true);
    expect((await run.storage.loadWorkflow("approval-parent.node-sub"))?.status).toBe("completed");
  });

  it("waits separately for every forEach item and resumes only the approved item", async () => {
    const run = await startWaitingRun({
      dag: waitingAgentDag({
        id: "agent-approval-items",
        forEach: {
          items: { kind: "initial", path: "items" },
          as: "item",
          maxItems: 2,
        },
      }),
      workflowId: "agent-approval-items",
      initialInput: { items: [0, 1] },
    });
    expect(
      (await listPendingSignals(run.storage))
        .filter((row) => row.isApproval)
        .map((row) => row.workflowId)
        .sort(),
    ).toEqual(["agent-approval-items.node-n1.item-0", "agent-approval-items.node-n1.item-1"]);

    await decidePending({
      storage: run.storage,
      workflowId: "agent-approval-items.node-n1.item-0",
    });
    const partly = await run.runner.runSafe({
      workflow: run.workflow,
      workflowId: "agent-approval-items",
      input: undefined,
      useStoredInput: true,
    });

    expect(partly.error).toBeTruthy();
    expect(run.ran).toEqual(["item-0"]);
    expect((await run.storage.loadWorkflow("agent-approval-items.node-n1.item-0"))?.status).toBe(
      "completed",
    );
    expect((await run.storage.loadWorkflow("agent-approval-items.node-n1.item-1"))?.status).toBe(
      "suspended",
    );
  });

  it("includes approval wait time in node duration", async () => {
    const clock = FakeWallClock.create(0);
    const run = await startWaitingRun({
      dag: waitingAgentDag({ id: "agent-approval-duration" }),
      workflowId: "agent-approval-duration",
      clock,
    });
    clock.advance(60_000);
    await decidePending({
      storage: run.storage,
      workflowId: "agent-approval-duration.node-n1",
    });

    const result = await run.runner.run({
      workflow: run.workflow,
      workflowId: "agent-approval-duration",
      input: undefined,
      useStoredInput: true,
    });

    expect(result.nodeStats.n1?.durationMs).toBeGreaterThanOrEqual(60_000);
  });

  it("counts approval wait time toward the node timeout", async () => {
    const clock = FakeWallClock.create(0);
    const run = await startWaitingRun({
      dag: waitingAgentDag({ id: "agent-approval-timeout", timeoutMs: 60_000 }),
      workflowId: "agent-approval-timeout",
      clock,
    });

    clock.advance(60_000);
    const result = await run.runner.run({
      workflow: run.workflow,
      workflowId: "agent-approval-timeout",
      input: undefined,
      useStoredInput: true,
    });

    expect(result.errors.n1).toMatch(/^node_timeout/);
  });

  it("does not retry a failed turn after its approval was rejected", async () => {
    const dag: AgenticDagRecipe = {
      id: "agent-approval-rejected",
      version: "v1",
      nodes: [
        {
          id: "n1",
          kind: "agent",
          agentId: "worker",
          approvals: "wait",
          inputs: {},
          output: {
            schema: {
              type: "object",
              properties: { ok: { type: "boolean" } },
              required: ["ok"],
              additionalProperties: false,
            },
          },
          retry: { maxAttempts: 2 },
        },
      ],
      edges: [],
      entry: ["n1"],
      terminals: ["n1"],
    };
    const run = await startWaitingRun({ dag, workflowId: dag.id });
    await decidePending({ storage: run.storage, workflowId: `${dag.id}.node-n1`, approved: false });

    const result = await run.runner.run({
      workflow: run.workflow,
      workflowId: dag.id,
      input: undefined,
      useStoredInput: true,
    });

    expect(result.errors.n1).toMatch(/^output_schema_mismatch/);
    expect(result.nodeStats.n1?.attempts).toBeUndefined();
    expect(await run.storage.loadWorkflow(`${dag.id}.node-n1.try-2`)).toBeNull();
  });

  it("does not start a queued forEach item while the active item waits", async () => {
    const run = await startWaitingRun({
      dag: waitingAgentDag({
        id: "agent-approval-queued",
        forEach: {
          items: { kind: "initial", path: "items" },
          as: "item",
          maxItems: 2,
          concurrency: 1,
        },
      }),
      workflowId: "agent-approval-queued",
      initialInput: { items: [0, 1] },
    });

    expect(await run.storage.loadWorkflow("agent-approval-queued.node-n1.item-0")).toBeDefined();
    expect(await run.storage.loadWorkflow("agent-approval-queued.node-n1.item-1")).toBeNull();
  });
});
