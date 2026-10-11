// ---------------------------------------------------------------------------
// A clustered ZoryaServer refuses agents whose turn gate is the default
// in-process one: turns on one thread can reach any instance, so the gate's
// leases must be shared.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { AgentTurnGate, InMemoryAgentRegistry, InMemoryLeaseStore } from "@promin/agent";
import { createWorkflowRunner } from "@promin/workflow";
import { createSqliteZoryaStack } from "../sqlite-stack.ts";
import { SqliteLeaderLeaseStore } from "@promin/sqlite";
import { LocalWorkflows, ZoryaAgents, ZoryaClusterConfigError } from "../../index.ts";
import { ZoryaServer } from "../server.ts";

function clusteredServer(agents: ZoryaAgents): ZoryaServer {
  const stack = createSqliteZoryaStack({ path: ":memory:" });
  const workflows = new LocalWorkflows({
    storage: stack.storage,
    runner: createWorkflowRunner({ storage: stack.storage }),
    definitions: {},
    cluster: { instanceId: "a", leases: SqliteLeaderLeaseStore.make({ db: stack.db }) },
  });
  return new ZoryaServer({
    insecure: true,
    workflows,
    agents,
    remoteWorkers: { apiKeys: ["w"], workerRegistry: stack.workerRegistry },
  });
}

describe("clustered server — agent turn gate", () => {
  it("refuses the default in-process turn gate", () => {
    const agents = new ZoryaAgents({
      registry: new InMemoryAgentRegistry(),
      resolve: () => {
        throw new Error("unused");
      },
    });
    expect(() => clusteredServer(agents)).toThrow(ZoryaClusterConfigError);
    expect(() => clusteredServer(agents)).toThrow(/turn gate/);
  });

  it("accepts an explicitly configured gate", () => {
    const agents = new ZoryaAgents({
      registry: new InMemoryAgentRegistry(),
      resolve: () => {
        throw new Error("unused");
      },
      // Stands in for a shared (Postgres) lease store.
      turnGate: new AgentTurnGate({ leaseStore: new InMemoryLeaseStore() }),
    });
    expect(() => clusteredServer(agents)).not.toThrow(/turn gate/);
  });
});
