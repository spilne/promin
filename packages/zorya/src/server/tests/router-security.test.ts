import { describe, expect, it } from "bun:test";
import {
  Router,
  type ProtectedRouteAccess,
  type ResolvedRouteAccess,
  type RouteAccess,
} from "../router.ts";

type MismatchedProtectedAccess = {
  readonly kind: "protected";
  readonly permission: { readonly resource: "agent"; readonly action: "read" };
  readonly resolveTarget: () => {
    readonly resource: {
      readonly kind: "knowledge";
      readonly namespaceId: "team-a";
    };
  };
};

// This value fails to compile if the declaration union ever admits a
// permission and target resolver for different resource kinds.
const mismatchedAccessIsRejected: MismatchedProtectedAccess extends ProtectedRouteAccess
  ? true
  : false = false;

const quietLogger = { error: () => {} };

describe("Router security declarations", () => {
  it("keeps legacy registrations visible in deterministic coverage reports", () => {
    const router = new Router()
      .post("/z", () => Response.json({}))
      .get("/a", () => Response.json({}))
      .route({
        method: "GET",
        path: "/declared",
        access: { kind: "public", purpose: "lifecycle" },
        handler: () => Response.json({}),
      });

    expect(mismatchedAccessIsRejected).toBeFalse();
    expect(router.uncoveredRoutes()).toEqual(["GET /a", "POST /z"]);
    expect(() => router.assertSecurityCoverage()).toThrow(
      "Routes missing access declarations:\nGET /a\nPOST /z",
    );
  });

  it("tracks an undeclared static fallback and accepts an explicit static-ui fallback", async () => {
    const router = new Router().setStatic(() => new Response("legacy"));
    expect(router.uncoveredRoutes()).toEqual(["STATIC *"]);

    const metadata = { kind: "public", purpose: "static-ui" };
    router.setStaticRoute(metadata as unknown as RouteAccess, (_req, _params, access) => {
      expect(access).toEqual({ kind: "public", purpose: "static-ui" });
      expect(Object.isFrozen(access)).toBeTrue();
      return new Response("declared");
    });
    metadata.purpose = "login";
    expect(router.uncoveredRoutes()).toEqual([]);
    expect(() => router.assertSecurityCoverage()).not.toThrow();
    expect(await (await router.handle(new Request("http://zorya.test/dashboard"))).text()).toBe(
      "declared",
    );
  });

  it("updates declared static fallbacks atomically", async () => {
    const invalidAccess = { kind: "public", purpose: "because" } as unknown as RouteAccess;
    let rejectedHandlerCalled = false;
    const fresh = new Router();
    expect(() =>
      fresh.setStaticRoute(invalidAccess, () => {
        rejectedHandlerCalled = true;
        return new Response("rejected");
      }),
    ).toThrow("requires an approved purpose");
    expect(fresh.uncoveredRoutes()).toEqual([]);
    expect((await fresh.handle(new Request("http://zorya.test/fresh"))).status).toBe(404);
    expect(rejectedHandlerCalled).toBeFalse();

    const existing = new Router().setStaticRoute(
      { kind: "public", purpose: "static-ui" },
      () => new Response("original"),
    );
    expect(() =>
      existing.setStaticRoute(invalidAccess, () => {
        rejectedHandlerCalled = true;
        return new Response("replacement");
      }),
    ).toThrow("requires an approved purpose");
    expect(() =>
      existing.setStaticRoute({ kind: "public", purpose: "static-ui" }, undefined as never),
    ).toThrow("requires a handler");
    expect(existing.uncoveredRoutes()).toEqual([]);
    expect(await (await existing.handle(new Request("http://zorya.test/existing"))).text()).toBe(
      "original",
    );
    expect(rejectedHandlerCalled).toBeFalse();
  });

  it("tracks a custom not-found handler until it has an explicit declaration", async () => {
    const router = new Router().setNotFound(() => new Response("legacy", { status: 404 }));
    expect(router.uncoveredRoutes()).toEqual(["NOT_FOUND *"]);
    expect(() => router.assertSecurityCoverage()).toThrow("NOT_FOUND *");

    router.setNotFoundRoute({ kind: "public", purpose: "not-found" }, (_req, params, access) => {
      expect(Object.getPrototypeOf(params)).toBeNull();
      expect(access).toEqual({ kind: "public", purpose: "not-found" });
      expect(Object.isFrozen(access)).toBeTrue();
      return new Response("declared", { status: 404 });
    });
    expect(router.uncoveredRoutes()).toEqual([]);
    expect((await router.handle(new Request("http://zorya.test/missing"))).status).toBe(404);
  });

  it("updates declared not-found fallbacks atomically", async () => {
    const invalidAccess = { kind: "public", purpose: "because" } as unknown as RouteAccess;
    let rejectedHandlerCalled = false;
    const fresh = new Router();
    expect(() =>
      fresh.setNotFoundRoute(invalidAccess, () => {
        rejectedHandlerCalled = true;
        return new Response("rejected", { status: 404 });
      }),
    ).toThrow("requires an approved purpose");
    expect(fresh.uncoveredRoutes()).toEqual([]);
    expect(await (await fresh.handle(new Request("http://zorya.test/fresh"))).json()).toEqual({
      error: "not_found",
    });
    expect(rejectedHandlerCalled).toBeFalse();

    const existing = new Router().setNotFoundRoute(
      { kind: "public", purpose: "not-found" },
      () => new Response("original", { status: 404 }),
    );
    expect(() =>
      existing.setNotFoundRoute(invalidAccess, () => {
        rejectedHandlerCalled = true;
        return new Response("replacement", { status: 404 });
      }),
    ).toThrow("requires an approved purpose");
    expect(() =>
      existing.setNotFoundRoute({ kind: "public", purpose: "not-found" }, null as never),
    ).toThrow("requires a handler");
    expect(existing.uncoveredRoutes()).toEqual([]);
    expect(await (await existing.handle(new Request("http://zorya.test/existing"))).text()).toBe(
      "original",
    );
    expect(rejectedHandlerCalled).toBeFalse();
  });

  it("resolves a stored resource before dispatch and passes through the preloaded value", async () => {
    const events: string[] = [];
    const stored = new Map([["agent-1", { namespaceId: "team-a", name: "Support" }]]);
    const router = new Router().route({
      method: "GET",
      path: "/agents/:id",
      access: {
        kind: "protected",
        permission: { resource: "agent", action: "read" },
        resolveTarget: async ({ params }) => {
          events.push("resolve");
          const agent = stored.get(params.id!);
          if (!agent) throw new Error("agent missing");
          return {
            resource: {
              kind: "agent",
              namespaceId: agent.namespaceId,
              id: params.id,
            },
            value: agent,
          };
        },
      },
      handler: (_req, _params, access) => {
        events.push("handler");
        expect(access.kind).toBe("protected");
        if (access.kind !== "protected") throw new Error("unreachable");
        expect(Object.isFrozen(access)).toBeTrue();
        expect(Object.isFrozen(access.permission)).toBeTrue();
        expect(Object.isFrozen(access.target)).toBeTrue();
        expect(Object.isFrozen(access.target.resource)).toBeTrue();
        return Response.json(access.target.value);
      },
    });

    const response = await router.handle(new Request("http://zorya.test/agents/agent-1"));
    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({
      namespaceId: "team-a",
      name: "Support",
    });
    expect(events).toEqual(["resolve", "handler"]);
    expect(router.uncoveredRoutes()).toEqual([]);
  });

  it("clones protected metadata so caller mutation cannot change the admitted boundary", async () => {
    const events: string[] = [];
    const permission = { resource: "agent", action: "read" };
    const metadata = {
      kind: "protected",
      permission,
      resolveTarget: () => {
        events.push("resolve");
        return { resource: { kind: "agent", namespaceId: "team-a", id: "agent-1" } };
      },
    };
    const router = new Router().route({
      method: "GET",
      path: "/agents/agent-1",
      access: metadata as unknown as RouteAccess,
      handler: (_req, _params, access) => {
        expect(access.kind).toBe("protected");
        if (access.kind !== "protected") throw new Error("unreachable");
        expect(access.permission).toEqual({ resource: "agent", action: "read" });
        return new Response(null, { status: 204 });
      },
    });

    metadata.kind = "public";
    permission.resource = "knowledge";
    permission.action = "delete";
    expect(() => router.assertSecurityCoverage()).not.toThrow();
    expect((await router.handle(new Request("http://zorya.test/agents/agent-1"))).status).toBe(204);
    expect(events).toEqual(["resolve"]);
  });

  it("snapshots stateful access and permission getters exactly once", async () => {
    const reads = { kind: 0, permission: 0, resource: 0, action: 0, resolver: 0 };
    let goodResolverCalls = 0;
    const permission = {
      get resource() {
        reads.resource++;
        return reads.resource === 1 ? "agent" : "knowledge";
      },
      get action() {
        reads.action++;
        return reads.action === 1 ? "read" : "delete";
      },
    };
    const metadata = {
      get kind() {
        reads.kind++;
        return reads.kind === 1 ? "protected" : "public";
      },
      get permission() {
        reads.permission++;
        return permission;
      },
      get resolveTarget() {
        reads.resolver++;
        if (reads.resolver !== 1) {
          return () => ({ resource: { kind: "knowledge", namespaceId: "" } });
        }
        return () => {
          goodResolverCalls++;
          return { resource: { kind: "agent", namespaceId: "team-a", id: "agent-1" } };
        };
      },
    };
    const router = new Router().route({
      method: "GET",
      path: "/stateful-access",
      access: metadata as unknown as RouteAccess,
      handler: (_req, _params, access) => {
        expect(access.kind).toBe("protected");
        if (access.kind !== "protected") throw new Error("unreachable");
        expect(access.permission).toEqual({ resource: "agent", action: "read" });
        return new Response(null, { status: 204 });
      },
    });

    expect(reads).toEqual({ kind: 1, permission: 1, resource: 1, action: 1, resolver: 1 });
    expect((await router.handle(new Request("http://zorya.test/stateful-access"))).status).toBe(
      204,
    );
    expect(goodResolverCalls).toBe(1);

    let publicKindReads = 0;
    let purposeReads = 0;
    const publicMetadata = new Proxy<Record<string, unknown>>(
      {},
      {
        get: (_target, property) => {
          if (property === "kind") {
            publicKindReads++;
            return publicKindReads === 1 ? "public" : "protected";
          }
          if (property === "purpose") {
            purposeReads++;
            return purposeReads === 1 ? "lifecycle" : "because";
          }
          return undefined;
        },
      },
    );
    const publicRouter = new Router().route({
      method: "GET",
      path: "/stateful-public",
      access: publicMetadata as unknown as RouteAccess,
      handler: (_req, _params, access) => {
        expect(access).toEqual({ kind: "public", purpose: "lifecycle" });
        return new Response(null, { status: 204 });
      },
    });
    expect(publicKindReads).toBe(1);
    expect(purposeReads).toBe(1);
    expect(
      (await publicRouter.handle(new Request("http://zorya.test/stateful-public"))).status,
    ).toBe(204);

    let badActionReads = 0;
    expect(() =>
      new Router().route({
        method: "GET",
        path: "/bad-action",
        access: {
          kind: "protected",
          permission: {
            resource: "agent",
            get action() {
              badActionReads++;
              return badActionReads === 1 ? "delete" : "read";
            },
          },
          resolveTarget: () => ({ resource: { kind: "agent", namespaceId: "team-a" } }),
        } as unknown as RouteAccess,
        handler: () => Response.json({}),
      }),
    ).toThrow("permission is not in the approved vocabulary");
    expect(badActionReads).toBe(1);
  });

  it("snapshots stateful resolved resource fields before validation", async () => {
    const reads = { resource: 0, kind: 0, namespace: 0, id: 0, value: 0 };
    const resource = {
      get kind() {
        reads.kind++;
        return reads.kind === 1 ? "agent" : "knowledge";
      },
      get namespaceId() {
        reads.namespace++;
        return reads.namespace === 1 ? "team-a" : "";
      },
      get id() {
        reads.id++;
        return reads.id === 1 ? "agent-1" : "";
      },
    };
    const target = {
      get resource() {
        reads.resource++;
        return resource;
      },
      get value() {
        reads.value++;
        return reads.value === 1 ? "safe" : "changed";
      },
    };
    const router = new Router().route({
      method: "GET",
      path: "/stateful-target",
      access: {
        kind: "protected",
        permission: { resource: "agent", action: "read" },
        resolveTarget: () => target as never,
      },
      handler: (_req, _params, access) => {
        expect(access.kind).toBe("protected");
        if (access.kind !== "protected") throw new Error("unreachable");
        expect(access.target).toEqual({
          resource: { kind: "agent", namespaceId: "team-a", id: "agent-1" },
          value: "safe",
        });
        return new Response(null, { status: 204 });
      },
    });

    expect((await router.handle(new Request("http://zorya.test/stateful-target"))).status).toBe(
      204,
    );
    expect(reads).toEqual({ resource: 1, kind: 1, namespace: 1, id: 1, value: 1 });

    for (const resourceWithBadFirstSnapshot of [
      {
        get kind() {
          return "knowledge";
        },
        namespaceId: "team-a",
      },
      {
        kind: "agent",
        get namespaceId() {
          return "";
        },
      },
    ]) {
      let handled = false;
      const rejecting = new Router({ logger: quietLogger }).route({
        method: "GET",
        path: "/bad-target",
        access: {
          kind: "protected",
          permission: { resource: "agent", action: "read" },
          resolveTarget: () => ({ resource: resourceWithBadFirstSnapshot }) as never,
        },
        handler: () => {
          handled = true;
          return Response.json({});
        },
      });
      expect((await rejecting.handle(new Request("http://zorya.test/bad-target"))).status).toBe(
        500,
      );
      expect(handled).toBeFalse();
    }
  });

  it("fails closed before the handler when a resolver returns the wrong resource kind", async () => {
    let handled = false;
    const mismatched = {
      kind: "protected",
      permission: { resource: "agent", action: "read" },
      resolveTarget: () => ({
        resource: { kind: "knowledge", namespaceId: "team-a", id: "base-1" },
      }),
    } as unknown as RouteAccess;
    const router = new Router({ logger: quietLogger }).route({
      method: "GET",
      path: "/agents/:id",
      access: mismatched,
      handler: () => {
        handled = true;
        return Response.json({});
      },
    });

    const response = await router.handle(new Request("http://zorya.test/agents/agent-1"));
    expect(response.status).toBe(500);
    expect(handled).toBeFalse();
  });

  it("fails closed on empty or non-string resolved resource ids", async () => {
    for (const id of ["", 42, null]) {
      let handled = false;
      const router = new Router({ logger: quietLogger }).route({
        method: "GET",
        path: "/agents/current",
        access: {
          kind: "protected",
          permission: { resource: "agent", action: "read" },
          resolveTarget: () => ({
            resource: { kind: "agent", namespaceId: "team-a", id },
          }),
        } as unknown as RouteAccess,
        handler: () => {
          handled = true;
          return Response.json({});
        },
      });

      expect((await router.handle(new Request("http://zorya.test/agents/current"))).status).toBe(
        500,
      );
      expect(handled).toBeFalse();
    }
  });

  it("uses null-prototype params and rejects unsafe or duplicate parameter names", async () => {
    let observed: Record<string, string> | undefined;
    const router = new Router().route({
      method: "GET",
      path: "/safe/:id",
      access: { kind: "public", purpose: "lifecycle" },
      handler: (_req, params) => {
        observed = params;
        return new Response(null, { status: 204 });
      },
    });
    expect((await router.handle(new Request("http://zorya.test/safe/__proto__"))).status).toBe(204);
    expect(Object.getPrototypeOf(observed!)).toBeNull();
    expect(observed).toEqual({ id: "__proto__" });
    expect(Object.hasOwn(observed!, "id")).toBeTrue();

    for (const pattern of ["/:__proto__", "/:prototype", "/:constructor"]) {
      expect(() => new Router().get(pattern, () => Response.json({}))).toThrow(
        "Unsafe route parameter name",
      );
    }
    expect(() => new Router().get("/:id/:id", () => Response.json({}))).toThrow(
      "Duplicate route parameter name",
    );
    expect(() => new Router().get("/:not-valid", () => Response.json({}))).toThrow(
      "Invalid route parameter name",
    );
  });

  it("requires valid access metadata at runtime for untyped callers", () => {
    const base = {
      method: "GET",
      path: "/x",
      handler: () => Response.json({}),
    };
    expect(() => new Router().route({ ...base, access: undefined as never })).toThrow(
      "requires access metadata",
    );
    expect(() =>
      new Router().route({
        ...base,
        access: {
          kind: "protected",
          permission: { resource: "agent", action: "erase" },
          resolveTarget: () => ({
            resource: { kind: "agent", namespaceId: "team-a" },
          }),
        } as unknown as RouteAccess,
      }),
    ).toThrow("permission is not in the approved vocabulary");
    expect(() =>
      new Router().route({
        ...base,
        access: {
          kind: "public",
          purpose: "because",
        } as unknown as RouteAccess,
      }),
    ).toThrow("requires an approved purpose");
  });

  it("retains each explicit non-RBAC credential classification", async () => {
    const accesses: ResolvedRouteAccess[] = [];
    const router = new Router();
    const declarations = [
      ["/health", { kind: "public", purpose: "lifecycle" }],
      ["/rpc", { kind: "worker" }],
      ["/hook", { kind: "signed-webhook" }],
      ["/signal", { kind: "signal-token" }],
    ] as const;
    for (const [path, access] of declarations) {
      router.route({
        method: "GET",
        path,
        access,
        handler: (_req, _params, resolved) => {
          accesses.push(resolved);
          return new Response(null, { status: 204 });
        },
      });
      expect((await router.handle(new Request(`http://zorya.test${path}`))).status).toBe(204);
    }
    expect(accesses).toEqual(declarations.map(([, access]) => access));
    expect(() => router.assertSecurityCoverage()).not.toThrow();
  });
});
