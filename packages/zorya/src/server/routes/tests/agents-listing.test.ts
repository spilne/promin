// ---------------------------------------------------------------------------
// `GET /api/agents` through `ZoryaServer.handle`: one row per agent (its
// active version + version count), filters on the active version, Designer
// drafts dropped before paging, and the `/versions` sub-resource.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import type { AgentListEntry, AgentsListResponse, AgentVersionsResponse } from "../agents.ts";
import { bootAgents, jsonOf, localBackend, req } from "./agent-gateway-harness.ts";

async function list(server: { handle(r: Request): Promise<Response> }, query = "") {
  const res = await server.handle(req("GET", `/api/agents${query}`));
  expect(res.status).toBe(200);
  return jsonOf<AgentsListResponse>(res);
}

function summary(a: AgentListEntry) {
  return [a.id, a.version, a.versionCount];
}

describe("GET /api/agents — one row per agent", () => {
  it("lists each agent once with its active version and version count", async () => {
    const { server, clock } = await bootAgents();
    clock.advance(1_000);
    const v2 = await server.handle(
      req("POST", "/api/agents", { id: "support", version: "v2", backend: localBackend }),
    );
    expect(v2.status).toBe(201);

    const body = await list(server);
    expect(body.total).toBe(2);
    expect(body.agents.map(summary)).toEqual([
      ["other", "v1", 1],
      ["support", "v2", 2],
    ]);
  });

  it("filters on the active version's capabilities and tags", async () => {
    const { server, clock } = await bootAgents();
    // support@v1 claims `chat` / `beta`; the newer v2 claims neither.
    expect((await list(server, "?capability=chat")).agents.map((a) => a.id)).toEqual(["support"]);
    clock.advance(1_000);
    await server.handle(
      req("POST", "/api/agents", { id: "support", version: "v2", backend: localBackend }),
    );
    expect(await list(server, "?capability=chat")).toEqual({ agents: [], total: 0 });
    expect(await list(server, "?tag=beta")).toEqual({ agents: [], total: 0 });
  });

  it("drops drafts before paging, so totals and pages are exact", async () => {
    const { server } = await bootAgents();
    for (let i = 0; i < 3; i++) {
      const draft = await server.handle(
        req("POST", "/api/agents/_draft", { backend: localBackend, sourceId: "support" }),
      );
      expect(draft.status).toBe(201);
    }
    for (const id of ["alpha", "beta"]) {
      await server.handle(req("POST", "/api/agents", { id, backend: localBackend }));
    }

    const first = await list(server, "?limit=2");
    expect(first.total).toBe(4);
    expect(first.agents.map((a) => a.id)).toEqual(["alpha", "beta"]);
    const second = await list(server, "?limit=2&offset=2");
    expect(second.total).toBe(4);
    expect(second.agents.map((a) => a.id)).toEqual(["other", "support"]);
    const past = await list(server, "?limit=2&offset=4");
    expect(past).toEqual({ agents: [], total: 4 });
  });

  it("rejects a malformed page", async () => {
    const { server } = await bootAgents();
    for (const q of ["?limit=0", "?limit=501", "?offset=-1", "?limit=abc"]) {
      const res = await server.handle(req("GET", `/api/agents${q}`));
      expect([q, res.status]).toEqual([q, 400]);
      expect((await jsonOf<{ error: string }>(res)).error).toBe("invalid_request");
    }
  });
});

describe("GET /api/agents/:id/versions", () => {
  it("lists every version, oldest first; 404 for an unknown agent", async () => {
    const { server, clock } = await bootAgents();
    clock.advance(1_000);
    await server.handle(
      req("POST", "/api/agents", { id: "support", version: "v2", backend: localBackend }),
    );
    const res = await server.handle(req("GET", "/api/agents/support/versions"));
    expect(res.status).toBe(200);
    const body = await jsonOf<AgentVersionsResponse>(res);
    expect(body.versions.map((v) => v.version)).toEqual(["v1", "v2"]);

    const missing = await server.handle(req("GET", "/api/agents/nope/versions"));
    expect(missing.status).toBe(404);
  });
});
