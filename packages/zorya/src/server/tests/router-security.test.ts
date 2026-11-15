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

  it("tracks an undeclared static fallback and accepts an explicit static-ui fallback", () => {
    const router = new Router().setStatic(() => new Response("legacy"));
    expect(router.uncoveredRoutes()).toEqual(["STATIC *"]);

    router.setStaticRoute({ kind: "public", purpose: "static-ui" }, (_req, _params, access) => {
      expect(access).toEqual({ kind: "public", purpose: "static-ui" });
      return new Response("declared");
    });
    expect(router.uncoveredRoutes()).toEqual([]);
    expect(() => router.assertSecurityCoverage()).not.toThrow();
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
