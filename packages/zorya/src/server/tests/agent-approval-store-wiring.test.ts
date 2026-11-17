import { describe, expect, it } from "bun:test";
import { InMemoryAgentRegistry, InMemoryPendingApprovalStore } from "@promin/agent";
import { InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import { ZoryaServer } from "../server.ts";
import { ZoryaAgents, type ZoryaAgentsConfig } from "../services/agents/zorya-agents.ts";
import { LocalWorkflows } from "../services/workflows/local-workflows.ts";

function workflows(): LocalWorkflows {
  const storage = new InMemoryWorkflowStorage();
  return new LocalWorkflows({
    storage,
    runner: createWorkflowRunner({ storage }),
    definitions: {},
    sleepScanIntervalMs: 0,
  });
}

describe("agent approval-store wiring", () => {
  it("fails a secured server at construction when routes cannot reach the agent approval store", () => {
    const agents = new ZoryaAgents({
      registry: new InMemoryAgentRegistry(),
      resolve: () => {
        throw new Error("unused");
      },
    } as unknown as ZoryaAgentsConfig);

    expect(
      () => new ZoryaServer({ workflows: workflows(), agents, apiKeys: ["operator-key"] }),
    ).toThrow("PendingApprovalStore");
  });

  it("accepts a secured server when ZoryaAgents exposes the shared approval store", () => {
    const approvals = new InMemoryPendingApprovalStore();
    const agents = new ZoryaAgents({
      registry: new InMemoryAgentRegistry(),
      approvals,
      resolve: () => {
        throw new Error("unused");
      },
    });

    expect(
      () => new ZoryaServer({ workflows: workflows(), agents, apiKeys: ["operator-key"] }),
    ).not.toThrow();
  });
});
