// ---------------------------------------------------------------------------
// ZoryaDags defines the DAG workflow once, at boot, under its fixed name on
// the host's in-process workflows. So a DAG run whose process died is found
// by orphan recovery after a restart: the new process resumes it, re-attaches
// to the node that was running, and finishes the DAG without redoing the
// nodes that had finished.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import {
  DAG_WORKFLOW_NAME,
  InMemoryAgentRegistry,
  InMemoryDagRegistry,
  resolveLocalAgent,
  type AgenticDagRecipe,
  type DurableDagInput,
  type LLMProvider,
  type Message,
} from "@promin/agent";
import {
  FakeWallClock,
  InMemoryWorkflowStorage,
  createWorkflowRunner,
  type WorkflowState,
} from "@promin/workflow";
import { ZoryaAgents } from "../../agents/zorya-agents.ts";
import { UnknownWorkflowError } from "../../workflows/errors.ts";
import { LocalWorkflows } from "../../workflows/local-workflows.ts";
import { ZoryaDags } from "../zorya-dags.ts";

/** Past the in-memory lock lease and the orphan grace period. */
const PAST_LEASE_MS = 121_000;

const chain: AgenticDagRecipe = {
  id: "chain",
  version: "v1",
  nodes: [
    { id: "n1", agentId: "upper", inputs: { task: { kind: "initial", path: "task" } } },
    { id: "n2", agentId: "exclaim", inputs: { task: { kind: "node", nodeId: "n1", path: "" } } },
  ],
  edges: [{ from: "n1", to: "n2" }],
  entry: ["n1"],
  terminals: ["n2"],
};

function lastUser(messages: ReadonlyArray<Message>): string {
  const last = messages[messages.length - 1];
  return typeof last?.content === "string" ? last.content : "";
}

async function yieldUntil(condition: () => boolean | Promise<boolean>): Promise<void> {
  for (let i = 0; i < 5_000; i++) {
    if (await condition()) return;
    await new Promise((r) => setTimeout(r, 0));
  }
  throw new Error("condition never held");
}

describe("ZoryaDags — recovery", () => {
  it("resumes a DAG run after a restart through the workflow it defined at boot", async () => {
    const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
    const storage = new InMemoryWorkflowStorage({ clock });
    const agentRegistry = new InMemoryAgentRegistry({ clock });
    for (const id of ["upper", "exclaim"]) {
      await agentRegistry.register({
        id,
        backend: {
          type: "local",
          model: { provider: "test", id: "m" },
          role: { inline: { systemPrompt: null, tools: [] } },
        },
      });
    }
    const dagRegistry = new InMemoryDagRegistry({ clock });
    await dagRegistry.register({
      id: chain.id,
      version: chain.version,
      nodes: chain.nodes,
      edges: chain.edges,
      entry: chain.entry,
      terminals: chain.terminals,
    });
    const calls = { upper: 0, exclaim: 0 };
    let releaseStalled!: () => void;
    const stalled = new Promise<void>((r) => {
      releaseStalled = r;
    });
    const llms: Record<string, LLMProvider> = {
      upper: {
        chat: async (params) => {
          calls.upper++;
          return { content: lastUser(params.messages).toUpperCase(), finishReason: "stop" };
        },
      },
      exclaim: {
        chat: async (params) => {
          calls.exclaim++;
          // The first process dies while n2's LLM call is in flight.
          if (calls.exclaim === 1) await stalled;
          return { content: `${lastUser(params.messages)}!`, finishReason: "stop" };
        },
      },
    };

    /** One host process over the shared stores. */
    const boot = () => {
      const runner = createWorkflowRunner({ storage, clock });
      const workflows = new LocalWorkflows({
        storage,
        runner,
        definitions: {},
        clock,
        sleepScanIntervalMs: 0,
        signalScanIntervalMs: 0,
      });
      const agents = new ZoryaAgents({
        registry: agentRegistry,
        resolve: (recipe) =>
          resolveLocalAgent(recipe, { runner, llm: () => llms[recipe.id]!, tools: {} }),
      });
      const dags = new ZoryaDags({ registry: dagRegistry, workflows, agents });
      return { workflows, dags };
    };

    const first = boot();
    expect(first.dags.workflow.name).toBe(DAG_WORKFLOW_NAME);
    const input: DurableDagInput = {
      dagId: chain.id,
      version: chain.version,
      initialInput: { task: "hi" },
      scope: { namespaceId: "acme", resourceId: "r-1" },
    };
    const { workflowId } = await first.workflows.trigger(DAG_WORKFLOW_NAME, input, {
      internal: true,
    });
    await yieldUntil(() => calls.exclaim === 1);

    // Restart: the new process defines the DAG workflow at boot and its
    // orphan sweep resumes the run by name.
    // Time moves on without firing the dead process's heartbeats.
    clock.set(clock.currentTimeMs() + PAST_LEASE_MS);
    const second = boot();
    expect(await second.workflows.recoverOrphans()).toEqual({ resumed: 1 });
    let done: WorkflowState | null = null;
    await yieldUntil(async () => {
      done = await storage.loadWorkflow(workflowId);
      return done?.status === "completed" || done?.status === "failed";
    });
    releaseStalled();

    const run = done as WorkflowState | null;
    expect(run?.status).toBe("completed");
    expect((run?.result as { outputs?: Record<string, unknown> } | undefined)?.outputs).toEqual({
      n2: "HI!",
    });
    // n1 is not redone; n2's interrupted LLM call runs once more.
    expect(calls).toEqual({ upper: 1, exclaim: 2 });
    const children = await storage.listWorkflows({ parentId: workflowId });
    expect(children.map((c) => c.workflowId).sort()).toEqual([
      `${workflowId}.node-n1`,
      `${workflowId}.node-n2`,
    ]);
  });

  it("defines the DAG workflow as internal: generic triggers can't start it", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const workflows = new LocalWorkflows({
      storage,
      runner,
      definitions: {},
      sleepScanIntervalMs: 0,
    });
    const agents = new ZoryaAgents({
      registry: new InMemoryAgentRegistry(),
      resolve: () => {
        throw new Error("not used");
      },
    });
    new ZoryaDags({ registry: new InMemoryDagRegistry(), workflows, agents });

    expect(workflows.isInternal(DAG_WORKFLOW_NAME)).toBe(true);
    await expect(
      workflows.trigger(DAG_WORKFLOW_NAME, {
        dagId: "x",
        version: "v1",
        initialInput: {},
        scope: { namespaceId: "acme", resourceId: "r" },
      }),
    ).rejects.toBeInstanceOf(UnknownWorkflowError);
    expect(await storage.listWorkflows({ limit: 10 })).toEqual([]);
  });
});
