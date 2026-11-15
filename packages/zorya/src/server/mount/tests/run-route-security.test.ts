import { describe, expect, it } from "bun:test";
import { SystemWallClock } from "@promin/workflow";
import type { ResolvedRouteAccess, RouteAccessGuard } from "../../router.ts";
import { Router } from "../../router.ts";
import { mountRuns } from "../mount-runs.ts";

function mounted(
  params: {
    readonly accessGuard?: RouteAccessGuard;
    readonly loadWorkflow?: (workflowId: string) => Promise<unknown>;
  } = {},
): Router {
  const router = new Router({
    ...(params.accessGuard !== undefined && {
      accessGuard: params.accessGuard,
    }),
  });
  mountRuns({
    router,
    workflows: {
      trigger: async () => ({ workflowId: "run" }),
      resume: async () => undefined,
      rerun: async () => undefined,
      isInternal: () => false,
      checkStoredRunSecurity: async () => true,
    } as never,
    storage: {
      loadWorkflow: params.loadWorkflow ?? (async () => null),
    } as never,
    clock: SystemWallClock,
    definitions: undefined,
    advertisements: undefined,
    versionRegistry: undefined,
    runWatchers: {} as never,
    agentStreamHub: {} as never,
    workerWs: {} as never,
    workers: {} as never,
    metrics: {} as never,
    sampleInput: undefined,
    sseIntervalMs: undefined,
    signalTokenMaxTtlMs: undefined,
    publicBaseUrl: undefined,
    namespaces: undefined,
  });
  return router;
}

describe("run route security declarations", () => {
  it("declares every run/workflow/signal route and leaves only workers and metrics legacy", () => {
    expect(mounted().uncoveredRoutes()).toEqual(["GET /api/metrics", "GET /api/workers"]);
  });

  it("uses the approved permission and capability mapping for all 37 routes", async () => {
    const protectedRoutes = [
      ["GET", "/api/runs", "run", "read"],
      ["GET", "/api/workflows", "workflow", "read"],
      ["GET", "/api/runs/run-1", "run", "read"],
      ["GET", "/api/runs/run-1/signals", "run", "read"],
      ["GET", "/api/runs/run-1/attempts", "run", "read"],
      ["GET", "/api/runs/run-1/history", "run", "read"],
      ["GET", "/api/runs/run-1/children", "run", "read"],
      ["GET", "/api/runs/run-1/journal/step", "run", "read"],
      ["GET", "/api/signals", "run", "read"],
      ["POST", "/api/runs/run-1/mark-success", "run", "trigger"],
      ["POST", "/api/runs/run-1/mark-failed", "run", "trigger"],
      ["POST", "/api/runs/run-1/rerun", "run", "trigger"],
      ["POST", "/api/runs/run-1/resume", "run", "trigger"],
      ["POST", "/api/workflows/wf/runs", "run", "trigger"],
      ["GET", "/api/workflows/sparklines", "workflow", "read"],
      ["GET", "/api/workflows/definitions", "workflow", "read"],
      ["GET", "/api/workflows/wf/grid", "workflow", "read"],
      ["GET", "/api/workflows/wf/history", "workflow", "read"],
      ["GET", "/api/workflows/wf/definition", "workflow", "read"],
      ["POST", "/api/runs/run-1/cancel", "run", "cancel"],
      ["POST", "/api/runs/run-1/signal", "run", "signal"],
      ["POST", "/api/runs/run-1/updates", "run", "signal"],
      ["GET", "/api/runs/run-1/updates/update", "run", "read"],
      ["POST", "/api/runs/run-1/signals/ready/token", "run", "signal"],
      ["GET", "/api/runs/run-1/signal-tokens", "run", "signal"],
      ["GET", "/api/workflows/wf/versions", "workflow", "read"],
      ["GET", "/api/workflows/wf/versions/active", "workflow", "read"],
      ["POST", "/api/workflows/wf/versions/v1/promote", "workflow", "update"],
      ["POST", "/api/workflows/wf/rollback", "workflow", "update"],
      ["GET", "/api/runs/run-1/streams/output", "run", "read"],
      ["GET", "/api/runs/run-1/streams/output/chunks", "run", "read"],
      ["POST", "/api/runs/run-1/streams/input", "run", "signal"],
      ["GET", "/api/runs/run-1/events", "run", "read"],
      ["GET", "/api/runs/run-1/agent-stream", "run", "read"],
      ["POST", "/api/runs/run-1/query", "run", "signal"],
    ] as const;
    const signalTokenRoutes = [
      ["GET", "/api/signal-tokens/token/describe"],
      ["POST", "/api/signal-tokens/token/complete"],
    ] as const;
    let access: ResolvedRouteAccess | undefined;
    const router = mounted({
      loadWorkflow: async () => ({ namespace: "team-a" }),
      accessGuard: (request) => {
        access = request.access;
        return new Response(null, { status: 403 });
      },
    });

    for (const [method, path, resource, action] of protectedRoutes) {
      access = undefined;
      const response = await router.handle(
        new Request(`http://zorya.test${path}`, {
          method,
          ...(method === "POST" && {
            headers: { "content-type": "application/json" },
            body: "{}",
          }),
        }),
      );
      expect(response.status, `${method} ${path}`).toBe(403);
      expect(access, `${method} ${path}`).toMatchObject({
        kind: "protected",
        permission: { resource, action },
      });
    }

    for (const [method, path] of signalTokenRoutes) {
      access = undefined;
      const response = await router.handle(
        new Request(`http://zorya.test${path}`, {
          method,
          ...(method === "POST" && { body: "{}" }),
        }),
      );
      expect(response.status, `${method} ${path}`).toBe(403);
      expect(access as ResolvedRouteAccess | undefined, `${method} ${path}`).toEqual({
        kind: "signal-token",
      });
    }
  });

  it("targets point operations at the stored run namespace, never query input", async () => {
    let access: ResolvedRouteAccess | undefined;
    const router = mounted({
      loadWorkflow: async () => ({ namespace: "team-a" }),
      accessGuard: (request) => {
        access = request.access;
        return new Response(null, { status: 403 });
      },
    });

    const response = await router.handle(
      new Request("http://zorya.test/api/runs/run-1?namespace=team-b"),
    );

    expect(response.status).toBe(403);
    expect(access).toMatchObject({
      kind: "protected",
      permission: { resource: "run", action: "read" },
      target: { resource: { kind: "run", namespaceId: "team-a", id: "run-1" } },
    });
  });

  it("rejects duplicate namespace query values before authorization", async () => {
    let guarded = false;
    const router = mounted({
      accessGuard: () => {
        guarded = true;
        return new Response(null, { status: 403 });
      },
    });

    const response = await router.handle(
      new Request("http://zorya.test/api/runs?namespace=team-a&namespace=team-b"),
    );

    expect(response.status).toBe(400);
    expect(guarded).toBe(false);
  });
});
