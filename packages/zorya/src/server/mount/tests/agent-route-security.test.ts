import { describe, expect, it } from "bun:test";
import { SystemWallClock } from "@promin/workflow";
import { Router, type RouteAccessGuard } from "../../router.ts";
import { mountAgents } from "../mount-agents.ts";

function mounted(accessGuard?: RouteAccessGuard): Router {
  const router = new Router({ ...(accessGuard !== undefined && { accessGuard }) });
  mountAgents({
    router,
    agents: {
      registry: {},
      resolve: () => ({}),
      memory: {},
      instances: { get: async () => undefined },
      roles: {},
      models: {},
      toolCatalog: {},
      retrievers: {},
      fragments: {},
      toolHistory: {},
      turnGate: undefined,
      workerId: undefined,
      authorizeScope: undefined,
      budgets: undefined,
    } as never,
    retainedSubagents: {} as never,
    skills: { registry: {} } as never,
    fragments: { registry: {} } as never,
    secrets: {} as never,
    mcp: {} as never,
    namespaces: {} as never,
    evals: { datasets: {} } as never,
    security: undefined,
    clock: SystemWallClock,
  });
  return router;
}

describe("agent route security declarations", () => {
  it("leaves no agent, thread, memory, instance, role or retained route uncovered", () => {
    expect(mounted().uncoveredRoutes()).toEqual([]);
  });

  it("rejects duplicated scope identity before the guard or handler can choose a value", async () => {
    let guarded = false;
    const router = mounted(() => {
      guarded = true;
      return new Response(null, { status: 403 });
    });
    const response = await router.handle(
      new Request(
        "http://zorya.test/api/agents/support/threads?namespaceId=team-a&namespaceId=team-b&resourceId=user",
      ),
    );
    expect(response.status).toBe(400);
    expect(guarded).toBe(false);
  });
});
