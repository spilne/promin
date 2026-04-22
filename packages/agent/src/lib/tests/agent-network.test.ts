import { describe, it, expect } from "bun:test";
import { agentNetwork } from "../agent-network.ts";
import type { AgentResult } from "../agent-action.ts";
import type { WorkflowRunner } from "@promin/workflow";

// ---- mock runner ----

function makeRunner(answer: string): WorkflowRunner {
  const handle = {
    workflowId: "mock-wf",
    async status() {
      return null;
    },
    async signal() {},
    async result(): Promise<AgentResult> {
      return { answer, messages: [], steps: 1 };
    },
  };
  return {
    storage: {} as WorkflowRunner["storage"],
    async start() {
      return handle;
    },
    async runSafe() {},
    async getStatus() {
      return null;
    },
  } as unknown as WorkflowRunner;
}

// biome-ignore lint/suspicious/noExplicitAny: test stub
const STUB_WORKFLOW = {} as any;

// ---- tests ----

describe("agentNetwork", () => {
  it("creates one tool per registered agent", () => {
    const network = agentNetwork({
      runner: makeRunner("done"),
      agents: {
        researcher: { workflow: STUB_WORKFLOW, description: "Researches topics." },
        coder: { workflow: STUB_WORKFLOW, description: "Writes code." },
      },
    });
    const tools = network.handoffTools();
    expect(Object.keys(tools)).toEqual(["researcher", "coder"]);
  });

  it("tool name matches agent name", () => {
    const network = agentNetwork({
      runner: makeRunner("done"),
      agents: {
        reviewer: { workflow: STUB_WORKFLOW, description: "Reviews output." },
      },
    });
    const tools = network.handoffTools();
    expect(tools["reviewer"]!.name).toBe("reviewer");
  });

  it("tool description matches the spec description", () => {
    const network = agentNetwork({
      runner: makeRunner("done"),
      agents: {
        researcher: {
          workflow: STUB_WORKFLOW,
          description: "Searches the web and summarises findings.",
        },
      },
    });
    const tools = network.handoffTools();
    expect(tools["researcher"]!.description).toBe("Searches the web and summarises findings.");
  });

  it("execute starts a workflow and returns the specialist answer", async () => {
    const network = agentNetwork({
      runner: makeRunner("research complete"),
      agents: {
        researcher: { workflow: STUB_WORKFLOW, description: "Researches topics." },
      },
    });
    const tools = network.handoffTools();
    const result = await tools["researcher"]!.execute({ task: "Find info on TypeScript runtimes" });
    expect(result).toBe("research complete");
  });

  it("passes the task to the workflow input", async () => {
    const startCalls: { task: string }[] = [];
    const runner = {
      storage: {} as WorkflowRunner["storage"],
      async start({ input }: { input: { task: string } }) {
        startCalls.push({ task: input.task });
        return {
          workflowId: "wf",
          async status() {
            return null;
          },
          async signal() {},
          async result(): Promise<AgentResult> {
            return { answer: "ok", messages: [], steps: 1 };
          },
        };
      },
      async runSafe() {},
      async getStatus() {
        return null;
      },
    } as unknown as WorkflowRunner;

    const network = agentNetwork({
      runner,
      agents: { coder: { workflow: STUB_WORKFLOW, description: "Writes code." } },
    });
    await network.handoffTools()["coder"]!.execute({ task: "Write a sort function" });
    expect(startCalls).toHaveLength(1);
    expect(startCalls[0]!.task).toBe("Write a sort function");
  });

  it("generates a unique workflowId per invocation", async () => {
    const workflowIds: string[] = [];
    const runner = {
      storage: {} as WorkflowRunner["storage"],
      async start({ workflowId }: { workflowId: string }) {
        workflowIds.push(workflowId);
        return {
          workflowId,
          async status() {
            return null;
          },
          async signal() {},
          async result(): Promise<AgentResult> {
            return { answer: "ok", messages: [], steps: 1 };
          },
        };
      },
      async runSafe() {},
      async getStatus() {
        return null;
      },
    } as unknown as WorkflowRunner;

    const network = agentNetwork({
      runner,
      agents: { worker: { workflow: STUB_WORKFLOW, description: "Does work." } },
    });
    const handoff = network.handoffTools()["worker"]!;
    await handoff.execute({ task: "task 1" });
    await handoff.execute({ task: "task 2" });
    expect(workflowIds).toHaveLength(2);
    expect(workflowIds[0]).not.toBe(workflowIds[1]);
    expect(workflowIds[0]).toMatch(/^worker-/);
  });
});
