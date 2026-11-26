import { describe, expect, it } from "bun:test";
import { InMemoryRetrieverRegistry } from "@promin/agent";
import { mountExtensionRoutes, registerExtensionRetrievers } from "../extensions.ts";
import { Router } from "../router.ts";

const protectedAgentRoute = (path: string) => ({
  method: "GET",
  path,
  access: {
    kind: "protected" as const,
    permission: { resource: "agent" as const, action: "read" as const },
    resolveTarget: () => ({
      resource: { kind: "agent" as const, namespaceId: "team-a", id: "helper" },
    }),
  },
  handler: () => Response.json({ ok: true }),
});

describe("Zorya extensions", () => {
  it("mounts protected routes through the normal router", async () => {
    let guarded = 0;
    const router = new Router({
      accessGuard: () => {
        guarded += 1;
        return undefined;
      },
    });
    mountExtensionRoutes(router, [(routes) => routes.route(protectedAgentRoute("/ext/helper"))]);

    const response = await router.handle(new Request("http://zorya.test/ext/helper"));
    expect(response.status).toBe(200);
    expect(guarded).toBe(1);
    expect(router.uncoveredRoutes()).toEqual([]);
  });

  it("rejects missing protection, asynchronous registration and route conflicts", () => {
    const router = new Router();
    router.route(protectedAgentRoute("/core"));
    expect(() =>
      mountExtensionRoutes(router, [
        (routes) => routes.route({ ...protectedAgentRoute("/public"), access: undefined } as never),
      ]),
    ).toThrow("protected action and resource");
    expect(() =>
      mountExtensionRoutes(router, [(routes) => routes.route(protectedAgentRoute("/core"))]),
    ).toThrow("conflicts with an existing route");
    expect(() => mountExtensionRoutes(router, [(() => Promise.resolve()) as never])).toThrow(
      "must be synchronous",
    );
  });

  it("registers external retrievers without replacing existing rows", () => {
    const registry = new InMemoryRetrieverRegistry([
      { id: "existing", retriever: { retrieve: async () => [] } },
    ]);
    registerExtensionRetrievers({
      registry,
      entries: [{ id: "external", retriever: { retrieve: async () => [] } }],
    });
    expect(registry.get({ id: "external" })?.id).toBe("external");
    expect(() =>
      registerExtensionRetrievers({
        registry,
        entries: [{ id: "existing", retriever: { retrieve: async () => [] } }],
      }),
    ).toThrow("retriever already registered");
    expect(() =>
      registerExtensionRetrievers({
        registry: undefined,
        entries: [{ id: "missing", retriever: { retrieve: async () => [] } }],
      }),
    ).toThrow("requires agents() with a retriever registry");
  });
});
