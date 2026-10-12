// ---------------------------------------------------------------------------
// Fragment HTTP routes — end-to-end against a real ZoryaServer backed by an
// InMemoryFragmentRegistry (via ZoryaFragments). Pins CRUD with versions
// (edit = new version, If-Match, promote), the _sources endpoint, the
// scanner-discovered file-managed flag, scan sync leaving operator
// fragments alone, and the 409 on deleting a fragment roles or agents
// still layer. Sibling of skills.test.ts.
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

function post(path: string, body: unknown, headers: Record<string, string> = {}): Request {
  return new Request(`http://test${path}`, {
    method: "POST",
    headers: { "content-type": "application/json", ...headers },
    body: JSON.stringify(body),
  });
}

function patch(path: string, body: unknown, ifMatch?: string): Request {
  return new Request(`http://test${path}`, {
    method: "PATCH",
    headers: {
      "content-type": "application/json",
      ...(ifMatch !== undefined && { "if-match": ifMatch }),
    },
    body: JSON.stringify(body),
  });
}

describe("fragments HTTP routes — CRUD", () => {
  it("creates, lists, reads, updates, and deletes a fragment", async () => {
    const { server } = bootServer();

    const created = await server.handle(
      post("/api/fragments", { id: "review-checklist", content: "## Review\n- correctness" }),
    );
    expect(created.status).toBe(201);
    expect(created.headers.get("etag")).toBe('W/"v1"');

    const list = (await (await server.handle(new Request("http://test/api/fragments"))).json()) as {
      fragments: Array<{ id: string }>;
    };
    expect(list.fragments.map((f) => f.id)).toEqual(["review-checklist"]);

    const got = (await (
      await server.handle(new Request("http://test/api/fragments/review-checklist"))
    ).json()) as { id: string; content: string };
    expect(got.content).toContain("correctness");

    const edit = { content: "## Review\n- correctness\n- edges" };
    expect((await server.handle(patch("/api/fragments/review-checklist", edit))).status).toBe(428);
    const patched = await server.handle(patch("/api/fragments/review-checklist", edit, 'W/"v1"'));
    expect(patched.status).toBe(200);
    expect(await patched.json()).toMatchObject({ version: "v2", active: true });
    // The edit-from tag is stale now.
    const stale = await server.handle(
      patch("/api/fragments/review-checklist", { content: "lost update" }, 'W/"v1"'),
    );
    expect(stale.status).toBe(412);

    // Roll back to v1.
    const promoted = await server.handle(
      post("/api/fragments/review-checklist/promote", { version: "v1" }),
    );
    expect(promoted.status).toBe(200);
    const versions = (await (
      await server.handle(new Request("http://test/api/fragments/review-checklist/versions"))
    ).json()) as { versions: Array<{ version: string; active: boolean }> };
    expect(versions.versions.map((v) => [v.version, v.active])).toEqual([
      ["v1", true],
      ["v2", false],
    ]);

    const del = await server.handle(
      new Request("http://test/api/fragments/review-checklist", { method: "DELETE" }),
    );
    expect(del.status).toBe(204);
    const after = await server.handle(new Request("http://test/api/fragments/review-checklist"));
    expect(after.status).toBe(404);
  });

  it("rejects duplicate-on-create (409), reserved ids and the version separator (400)", async () => {
    const { server } = bootServer();
    await server.handle(post("/api/fragments", { id: "x", content: "x" }));
    const dup = await server.handle(post("/api/fragments", { id: "x", content: "y" }));
    expect(dup.status).toBe(409);
    const reserved = await server.handle(post("/api/fragments", { id: "_sources", content: "x" }));
    expect(reserved.status).toBe(400);
    const pinned = await server.handle(post("/api/fragments", { id: "tone@v2", content: "x" }));
    expect(pinned.status).toBe(400);
  });

  it("two servers over one registry see each other's edits (no per-process copy)", async () => {
    const shared = new InMemoryFragmentRegistry();
    const boot = () => {
      const storage = new InMemoryWorkflowStorage();
      const workflows = new LocalWorkflows({
        storage,
        runner: createWorkflowRunner({ storage }),
        definitions: {},
        sleepScanIntervalMs: 0,
      });
      return new ZoryaServer({
        insecure: true,
        workflows,
        fragments: new ZoryaFragments({ registry: shared }),
      });
    };
    const a = boot();
    const b = boot();
    await a.handle(post("/api/fragments", { id: "tone", content: "Be terse." }));
    await b.handle(patch("/api/fragments/tone", { content: "Be warm." }, 'W/"v1"'));
    const seen = (await (await a.handle(new Request("http://test/api/fragments/tone"))).json()) as {
      content: string;
    };
    expect(seen.content).toBe("Be warm.");
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
      await server.handle(post("/api/fragments", { id: "api-made", content: "x" }));
      const body = (await (
        await server.handle(new Request("http://test/api/fragments/_sources"))
      ).json()) as { fileManaged: string[] };
      expect(body.fileManaged).toContain("from-file");
      expect(body.fileManaged).not.toContain("api-made");
      // File-managed fragments are edited in the file, not over HTTP.
      const edit = await server.handle(patch("/api/fragments/from-file", { content: "x" }, "*"));
      expect(edit.status).toBe(409);
      await fragments.stop();

      // File-managed is stored: a replica that never scanned agrees, at once.
      const storage = new InMemoryWorkflowStorage();
      const replica = new ZoryaServer({
        insecure: true,
        workflows: new LocalWorkflows({
          storage,
          runner: createWorkflowRunner({ storage }),
          definitions: {},
          sleepScanIntervalMs: 0,
        }),
        fragments: new ZoryaFragments({ registry: fragments.registry }),
      });
      const seen = (await (
        await replica.handle(new Request("http://test/api/fragments/_sources"))
      ).json()) as { fileManaged: string[] };
      expect(seen.fileManaged).toEqual(["from-file"]);
      const replicaEdit = await replica.handle(
        patch("/api/fragments/from-file", { content: "x" }, "*"),
      );
      expect(replicaEdit.status).toBe(409);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it("scan sync removes a fragment whose file is gone, never an operator's", async () => {
    const root = join(tmpdir(), `frag-sync-${process.pid}-${Date.now()}`);
    await mkdir(root, { recursive: true });
    await writeFile(join(root, "from-file.md"), "## From file");
    try {
      const storage = new InMemoryWorkflowStorage();
      const workflows = new LocalWorkflows({
        storage,
        runner: createWorkflowRunner({ storage }),
        definitions: {},
        sleepScanIntervalMs: 0,
      });
      const registry = new InMemoryFragmentRegistry();
      const fragments = new ZoryaFragments({
        registry,
        scan: { root, intervalMs: 60_000, sync: true },
      });
      const server = new ZoryaServer({ insecure: true, workflows, fragments });
      await fragments.start();
      await server.handle(post("/api/fragments", { id: "operator-made", content: "keep me" }));
      await rm(join(root, "from-file.md"));
      await writeFile(join(root, "other.md"), "## Other");
      await fragments.stop();
      await fragments.start(); // a fresh loop: its first tick scans again
      expect(await registry.get({ id: "from-file" })).toBeNull();
      expect((await registry.get({ id: "operator-made" }))?.content).toBe("keep me");
      expect(await registry.get({ id: "other" })).not.toBeNull();
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
    for (const id of ["tone", "safety", "unused"]) {
      const res = await server.handle(post("/api/fragments", { id, content: id }));
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
          inline: { systemPrompt: { base: "Write.", layers: ["tone", "safety@v1"] }, tools: [] },
        },
      },
    });
    return { server, registry };
  }

  const del = (id: string, query = "") =>
    new Request(`http://test/api/fragments/${id}${query}`, { method: "DELETE" });

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
    expect((await registry.get({ id: "tone" }))?.content).toBe("tone");
    expect((await registry.get({ id: "safety" }))?.content).toBe("safety");
  });

  it("deletes an unreferenced fragment, and a referenced one with ?force=true", async () => {
    const { server, registry } = await bootWithAgents();
    expect((await server.handle(del("unused"))).status).toBe(204);
    expect((await server.handle(del("tone", "?force=true"))).status).toBe(204);
    expect(await registry.get({ id: "unused" })).toBeNull();
    expect(await registry.get({ id: "tone" })).toBeNull();
  });

  it("answers 404 for a fragment that does not exist", async () => {
    const { server } = await bootWithAgents();
    const res = await server.handle(del("nope"));
    expect(res.status).toBe(404);
    expect(((await res.json()) as { error: string }).error).toBe("fragment_not_found");
  });
});
