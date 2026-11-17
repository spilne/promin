import { describe, expect, it } from "bun:test";
import { SystemWallClock } from "@promin/workflow";
import type { ResolvedRouteAccess } from "../../router.ts";
import { Router } from "../../router.ts";
import { mountProbes } from "../mount-lifecycle.ts";
import { mountRpc } from "../mount-rpc.ts";
import { mountStatic } from "../mount-static.ts";

function guarded(accesses: ResolvedRouteAccess[]): Router {
  return new Router({
    accessGuard: ({ access }) => {
      accesses.push(access);
      return new Response(null, { status: 403 });
    },
  });
}

describe("final route security declarations", () => {
  it("classifies every RPC and worker-protocol route as worker access", async () => {
    const accesses: ResolvedRouteAccess[] = [];
    const router = guarded(accesses);
    mountRpc({
      router,
      storage: {} as never,
      workerRegistry: {} as never,
      advertisements: {} as never,
      workflowStarts: {} as never,
      storageCaller: () => undefined,
    });

    const requests = [
      ["POST", "/rpc/storage"],
      ["POST", "/rpc/worker"],
      ["POST", "/api/advertisements"],
      ["DELETE", "/api/advertisements/worker-1"],
      ["GET", "/api/advertisements"],
      ["POST", "/api/worker-protocol/claim-starts"],
      ["POST", "/api/worker-protocol/heartbeat-start/start-1"],
      ["POST", "/api/worker-protocol/complete-start/start-1"],
      ["POST", "/api/worker-protocol/release-start/start-1"],
      ["GET", "/api/worker-protocol/starts"],
    ] as const;

    expect(router.uncoveredRoutes()).toEqual([]);
    for (const [method, path] of requests) {
      const response = await router.handle(new Request(`http://zorya.test${path}`, { method }));
      expect(response.status, `${method} ${path}`).toBe(403);
    }
    expect(accesses).toHaveLength(requests.length);
    expect(accesses.every((access) => access.kind === "worker")).toBe(true);
  });

  it("classifies probes and the UI fallback as explicit public surfaces", async () => {
    const accesses: ResolvedRouteAccess[] = [];
    const router = guarded(accesses);
    mountProbes({
      router,
      storage: {} as never,
      lifecycle: {} as never,
      clock: SystemWallClock,
      readinessTimeoutMs: 1_000,
      isStopping: () => false,
    });
    mountStatic({ router, uiDir: "/unused" });

    expect(router.uncoveredRoutes()).toEqual([]);
    for (const path of ["/api/health", "/api/ready", "/dashboard"]) {
      expect((await router.handle(new Request(`http://zorya.test${path}`))).status).toBe(403);
    }
    expect(accesses).toEqual([
      { kind: "public", purpose: "lifecycle" },
      { kind: "public", purpose: "lifecycle" },
      { kind: "public", purpose: "static-ui" },
    ]);
  });
});
