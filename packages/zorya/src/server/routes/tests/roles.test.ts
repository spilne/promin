// ---------------------------------------------------------------------------
// Role HTTP routes — end-to-end against a real ZoryaServer backed by an
// InMemoryRoleRegistry (wired through ZoryaAgents.roles). Pins the wire
// shape, validation, CRUD, and the extract-role ("Save as role") flow.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import {
  InMemoryAgentRegistry,
  InMemoryRoleRegistry,
  type RegisteredAgent,
  type RegisteredRole,
} from "@promin/agent";
import { InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import { LocalWorkflows, ZoryaAgents } from "../../../index.ts";
import { ZoryaServer } from "../../server.ts";

function boot() {
  const storage = new InMemoryWorkflowStorage();
  const runner = createWorkflowRunner({ storage });
  const workflows = new LocalWorkflows({
    storage,
    runner,
    definitions: {},
    sleepScanIntervalMs: 0,
  });
  const roles = new InMemoryRoleRegistry();
  const registry = new InMemoryAgentRegistry();
  const agents = new ZoryaAgents({
    registry,
    resolve: () => {
      throw new Error("roles route test should not resolve agents");
    },
    roles,
  });
  const server = new ZoryaServer({ workflows, agents });
  return { server, roles, registry };
}

function post(path: string, body: unknown): Request {
  return new Request(`http://test${path}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
}

const VALID = {
  id: "git-master",
  definition: {
    systemPrompt: { base: "you are a git master", layers: ["findings-table"] },
    tools: ["bash"],
    skills: [{ id: "structured-debugging" }],
    capabilities: ["chat"],
  },
  metadata: { description: "Git expert", tags: ["role"], suggestedSecrets: ["GH_TOKEN"] },
};

describe("roles HTTP routes — CRUD", () => {
  it("creates a role and returns 201 with the stored row", async () => {
    const { server } = boot();
    const res = await server.handle(post("/api/roles", VALID));
    expect(res.status).toBe(201);
    const row = (await res.json()) as RegisteredRole;
    expect(row.id).toBe("git-master");
    expect(row.version).toBe("v1");
    expect(row.definition.tools).toEqual(["bash"]);
    expect(row.metadata.suggestedSecrets).toEqual(["GH_TOKEN"]);
  });

  it("rejects a missing definition / tools", async () => {
    const { server } = boot();
    expect((await server.handle(post("/api/roles", { id: "x" }))).status).toBe(400);
    expect(
      (await server.handle(post("/api/roles", { id: "x", definition: { systemPrompt: "p" } })))
        .status,
    ).toBe(400);
  });

  it("rejects a `_`-prefixed id", async () => {
    const { server } = boot();
    const res = await server.handle(
      post("/api/roles", { id: "_x", definition: { systemPrompt: null, tools: [] } }),
    );
    expect(res.status).toBe(400);
  });

  it("lists, gets, versions, and deletes", async () => {
    const { server } = boot();
    await server.handle(post("/api/roles", VALID));
    // Distinct updatedAt so "latest" (get without version) is unambiguous.
    await new Promise((r) => setTimeout(r, 2));
    await server.handle(post("/api/roles", { ...VALID, version: "v2" }));

    const list = (await (await server.handle(new Request("http://test/api/roles"))).json()) as {
      roles: RegisteredRole[];
    };
    expect(list.roles.map((r) => r.id)).toContain("git-master");

    const got = (await (
      await server.handle(new Request("http://test/api/roles/git-master"))
    ).json()) as RegisteredRole;
    expect(got.version).toBe("v2"); // latest

    const versions = (await (
      await server.handle(new Request("http://test/api/roles/git-master/versions"))
    ).json()) as { versions: RegisteredRole[] };
    expect(versions.versions.map((v) => v.version)).toEqual(["v1", "v2"]);

    const del = await server.handle(
      new Request("http://test/api/roles/git-master?version=v1", { method: "DELETE" }),
    );
    expect(del.status).toBe(204);
    const after = (await (
      await server.handle(new Request("http://test/api/roles/git-master/versions"))
    ).json()) as { versions: RegisteredRole[] };
    expect(after.versions.map((v) => v.version)).toEqual(["v2"]);
  });

  it("PATCH merges — editing tags keeps the definition", async () => {
    const { server } = boot();
    await server.handle(post("/api/roles", VALID));
    const res = await server.handle(
      new Request("http://test/api/roles/git-master", {
        method: "PATCH",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ metadata: { description: "Git expert", tags: ["updated"] } }),
      }),
    );
    expect(res.status).toBe(200);
    const row = (await res.json()) as RegisteredRole;
    expect(row.metadata.tags).toEqual(["updated"]);
    expect(row.definition.tools).toEqual(["bash"]); // preserved
  });
});

describe("extract-role — Save as role", () => {
  it("lifts an agent's inline role into the registry and rebinds it to a ref", async () => {
    const { server, roles, registry } = boot();
    await registry.register({
      id: "my-bot",
      backend: {
        type: "local",
        model: { provider: "anthropic", id: "claude-sonnet-4-6" },
        role: { inline: { systemPrompt: "you help with git", tools: ["bash"] } },
      },
      metadata: { description: null, capabilities: [], tags: [] },
    });

    const res = await server.handle(
      post("/api/agents/my-bot/extract-role", { roleId: "git-helper" }),
    );
    expect(res.status).toBe(201);
    const body = (await res.json()) as { role: RegisteredRole; agent: RegisteredAgent };
    // Role registered from the inline definition.
    expect(body.role.id).toBe("git-helper");
    expect(body.role.definition.systemPrompt).toBe("you help with git");
    expect(await roles.get("git-helper")).not.toBeNull();
    // Agent rebound to a ref pinning the new role version.
    if (body.agent.backend.type === "local") {
      expect(body.agent.backend.role).toEqual({ ref: { id: "git-helper", version: "v1" } });
    }
  });

  it("400s when the agent has no inline role (already a ref)", async () => {
    const { server, registry } = boot();
    await registry.register({
      id: "ref-bot",
      backend: {
        type: "local",
        model: { provider: "anthropic", id: "claude-sonnet-4-6" },
        role: { ref: { id: "some-role" } },
      },
      metadata: { description: null, capabilities: [], tags: [] },
    });
    const res = await server.handle(post("/api/agents/ref-bot/extract-role", { roleId: "x" }));
    expect(res.status).toBe(400);
  });

  it("404s for an unknown agent", async () => {
    const { server } = boot();
    const res = await server.handle(post("/api/agents/ghost/extract-role", { roleId: "x" }));
    expect(res.status).toBe(404);
  });
});
