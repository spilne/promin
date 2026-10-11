// ---------------------------------------------------------------------------
// Fragment HTTP routes — end-to-end against a real ZoryaServer backed by an
// InMemoryFragmentRegistry (via ZoryaFragments). Pins CRUD, the _sources
// endpoint, the scanner-discovered file-managed flag, and the 409 on
// deleting a fragment roles or agents still layer. Sibling of skills.test.ts.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { mkdir, writeFile, rm } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import {
  InMemoryAgentRegistry,
  InMemoryFragmentRegistry,
  InMemoryRoleRegistry,
} from "@promin/agent";
import { InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import { LocalWorkflows, ZoryaAgents, ZoryaFragments } from "../../../index.ts";
import { ZoryaServer } from "../../server.ts";

function bootServer(scanRoot?: string) {
  const storage = new InMemoryWorkflowStorage();
  const runner = createWorkflowRunner({ storage });
  const workflows = new LocalWorkflows({
    storage,
    runner,
    definitions: {},
    sleepScanIntervalMs: 0,
  });
  const registry = new InMemoryFragmentRegistry();
  const fragments = new ZoryaFragments({
    registry,
    ...(scanRoot !== undefined && { scan: { root: scanRoot, intervalMs: 60_000 } }),
  });
  const server = new ZoryaServer({ insecure: true, workflows, fragments });
  return { server, fragments, registry };
}

function post(path: string, body: unknown): Request {
  return new Request(`http://test${path}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
}

describe("fragments HTTP routes — CRUD", () => {
  it("creates, lists, reads, updates, and deletes a fragment", async () => {
    const { server } = bootServer();

    const created = await server.handle(
      post("/api/fragments", { key: "review-checklist", content: "## Review\n- correctness" }),
    );
    expect(created.status).toBe(201);

    const list = (await (await server.handle(new Request("http://test/api/fragments"))).json()) as {
      fragments: Array<{ key: string }>;
    };
    expect(list.fragments.map((f) => f.key)).toEqual(["review-checklist"]);

    const got = (await (
      await server.handle(new Request("http://test/api/fragments/review-checklist"))
    ).json()) as { key: string; content: string };
    expect(got.content).toContain("correctness");

    const patched = await server.handle(
      new Request("http://test/api/fragments/review-checklist", {
        method: "PATCH",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ content: "## Review\n- correctness\n- edges" }),
      }),
    );
    expect(patched.status).toBe(200);

    const del = await server.handle(
      new Request("http://test/api/fragments/review-checklist", { method: "DELETE" }),
    );
    expect(del.status).toBe(204);
    const after = await server.handle(new Request("http://test/api/fragments/review-checklist"));
    expect(after.status).toBe(404);
  });

  it("rejects duplicate-on-create (409) and reserved keys (400)", async () => {
    const { server } = bootServer();
    await server.handle(post("/api/fragments", { key: "x", content: "x" }));
    const dup = await server.handle(post("/api/fragments", { key: "x", content: "y" }));
    expect(dup.status).toBe(409);
    const reserved = await server.handle(post("/api/fragments", { key: "_sources", content: "x" }));
    expect(reserved.status).toBe(400);
  });
});

describe("fragments HTTP routes — _sources + scanner", () => {
  it("returns [] when no scanner is configured", async () => {
    const { server } = bootServer();
    const res = await server.handle(new Request("http://test/api/fragments/_sources"));
    expect(res.status).toBe(200);
    const body = (await res.json()) as { fileManaged: string[] };
    expect(body.fileManaged).toEqual([]);
  });

  it("flags scanned fragments, not API-created ones", async () => {
    const root = join(tmpdir(), `frag-routes-${process.pid}-${Date.now()}`);
    await mkdir(root, { recursive: true });
    await writeFile(join(root, "from-file.md"), "## From file\nbody");
    try {
      const { server, fragments } = bootServer(root);
      await fragments.start(); // immediate scan tick
      await server.handle(post("/api/fragments", { key: "api-made", content: "x" }));
      const body = (await (
        await server.handle(new Request("http://test/api/fragments/_sources"))
      ).json()) as { fileManaged: string[] };
      expect(body.fileManaged).toContain("from-file");
      expect(body.fileManaged).not.toContain("api-made");
      await fragments.stop();
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
});

describe("fragments HTTP routes — referential integrity", () => {
  async function bootWithAgents() {
    const storage = new InMemoryWorkflowStorage();
    const workflows = new LocalWorkflows({
      storage,
      runner: createWorkflowRunner({ storage }),
      definitions: {},
      sleepScanIntervalMs: 0,
    });
    const registry = new InMemoryFragmentRegistry();
    const fragments = new ZoryaFragments({ registry });
    const agentRegistry = new InMemoryAgentRegistry();
    const roles = new InMemoryRoleRegistry();
    const agents = new ZoryaAgents({
      registry: agentRegistry,
      roles,
      resolve: () => {
        throw new Error("fragment tests never resolve an agent");
      },
    });
    const server = new ZoryaServer({ insecure: true, workflows, fragments, agents });
    for (const key of ["tone", "safety", "unused"]) {
      const res = await server.handle(post("/api/fragments", { key, content: key }));
      expect(res.status).toBe(201);
    }
    await roles.register({
      id: "reviewer",
      definition: { systemPrompt: { base: "Review.", layers: ["tone"] }, tools: [] },
    });
    await agentRegistry.register({
      id: "writer",
      backend: {
        type: "local",
        model: { provider: "anthropic", id: "claude-sonnet-4-6" },
        role: {
          inline: { systemPrompt: { base: "Write.", layers: ["tone", "safety"] }, tools: [] },
        },
      },
    });
    return { server, registry };
  }

  const del = (key: string, query = "") =>
    new Request(`http://test/api/fragments/${key}${query}`, { method: "DELETE" });

  it("refuses to delete a fragment a role or an agent layers, naming them", async () => {
    const { server, registry } = await bootWithAgents();
    const tone = await server.handle(del("tone"));
    expect(tone.status).toBe(409);
    expect(await tone.json()).toMatchObject({
      error: "fragment_in_use",
      agents: ["writer"],
      roles: ["reviewer"],
    });
    const safety = await server.handle(del("safety"));
    expect(safety.status).toBe(409);
    expect(await safety.json()).toMatchObject({ agents: ["writer"], roles: [] });
    expect(registry.get("tone")).toBe("tone");
    expect(registry.get("safety")).toBe("safety");
  });

  it("deletes an unreferenced fragment, and a referenced one with ?force=true", async () => {
    const { server, registry } = await bootWithAgents();
    expect((await server.handle(del("unused"))).status).toBe(204);
    expect((await server.handle(del("tone", "?force=true"))).status).toBe(204);
    expect(registry.get("unused")).toBeUndefined();
    expect(registry.get("tone")).toBeUndefined();
  });

  it("answers 404 for a fragment that does not exist", async () => {
    const { server } = await bootWithAgents();
    const res = await server.handle(del("nope"));
    expect(res.status).toBe(404);
    expect(((await res.json()) as { error: string }).error).toBe("fragment_not_found");
  });
});
