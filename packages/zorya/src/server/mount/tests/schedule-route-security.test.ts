import { describe, expect, it } from "bun:test";
import { SystemWallClock } from "@promin/workflow";
import type { ResolvedRouteAccess } from "../../router.ts";
import { Router } from "../../router.ts";
import { mountSchedules } from "../mount-schedules.ts";

function mounted(params: {
  readonly configured?: boolean;
  readonly access: ResolvedRouteAccess[];
}): Router {
  const router = new Router({
    accessGuard: ({ access }) => {
      params.access.push(access);
      return new Response(null, { status: 403 });
    },
  });
  mountSchedules({
    router,
    scheduler:
      params.configured === false
        ? undefined
        : ({
            storage: {
              loadSchedule: async (id: string) => ({
                id,
                intervalMs: 60_000,
                namespace: "team-stored",
              }),
            },
            defaultNamespace: "team-default",
            servesNamespace: () => true,
            assignedSecurityContext: async () => undefined,
            fireOnce: async () => null,
          } as never),
    storage: {} as never,
    clock: SystemWallClock,
    namespaces: undefined,
  });
  return router;
}

describe("schedule route security declarations", () => {
  it("protects the unconfigured discovery response", async () => {
    const access: ResolvedRouteAccess[] = [];
    const router = mounted({ configured: false, access });

    expect(router.uncoveredRoutes()).toEqual([]);
    expect((await router.handle(new Request("http://zorya.test/api/schedules"))).status).toBe(403);
    expect(access).toMatchObject([
      {
        kind: "protected",
        permission: { resource: "schedule", action: "read" },
        target: { resource: { kind: "schedule", namespaceId: "org" } },
      },
    ]);
  });

  it("maps all eight routes and scopes point operations from stored state", async () => {
    const access: ResolvedRouteAccess[] = [];
    const router = mounted({ access });
    const requests = [
      ["GET", "/api/schedules?namespace=team-list", undefined],
      ["POST", "/api/schedules", { id: "new", namespace: "team-create", intervalMs: 1 }],
      ["GET", "/api/schedules/nightly", undefined],
      ["PATCH", "/api/schedules/nightly", {}],
      ["DELETE", "/api/schedules/nightly", undefined],
      ["GET", "/api/schedules/nightly/history", undefined],
      ["GET", "/api/schedules/nightly/upcoming", undefined],
      ["POST", "/api/schedules/nightly/emit", {}],
    ] as const;

    expect(router.uncoveredRoutes()).toEqual([]);
    for (const [method, path, body] of requests) {
      const response = await router.handle(
        new Request(`http://zorya.test${path}`, {
          method,
          ...(body !== undefined && {
            headers: { "content-type": "application/json" },
            body: JSON.stringify(body),
          }),
        }),
      );
      expect(response.status, `${method} ${path}`).toBe(403);
    }

    expect(
      access.map((entry) => (entry.kind === "protected" ? entry.permission.action : "public")),
    ).toEqual(["read", "create", "read", "update", "delete", "read", "read", "update"]);
    expect(
      access.map((entry) =>
        entry.kind === "protected" ? entry.target.resource.namespaceId : undefined,
      ),
    ).toEqual([
      "team-list",
      "team-create",
      "team-stored",
      "team-stored",
      "team-stored",
      "team-stored",
      "team-stored",
      "team-stored",
    ]);
  });
});
