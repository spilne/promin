// ---------------------------------------------------------------------------
// Skill HTTP routes — end-to-end against a real ZoryaServer backed by an
// InMemorySkillRegistry (via ZoryaSkills). Pins the wire shape, validation,
// CRUD, and the agent-editor catalog endpoint.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { mkdir, writeFile, rm } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { InMemorySkillRegistry, type RegisteredSkill } from "@promin/agent";
import { InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import { LocalWorkflows, ZoryaSkills } from "../../../index.ts";
import { ZoryaServer } from "../../server.ts";
import { listCatalogSkills } from "../agent-catalog.ts";

function bootServerWithSkills() {
  const storage = new InMemoryWorkflowStorage();
  const runner = createWorkflowRunner({ storage });
  const workflows = new LocalWorkflows({
    storage,
    runner,
    definitions: {},
    sleepScanIntervalMs: 0,
  });
  const registry = new InMemorySkillRegistry();
  const skills = new ZoryaSkills({ registry });
  const server = new ZoryaServer({ workflows, skills });
  return { server, registry };
}

function post(path: string, body: unknown): Request {
  return new Request(`http://test${path}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
}

const VALID = {
  id: "structured-debugging",
  description: "A disciplined debugging loop.",
  whenToUse: "A bug resists a quick fix.",
  body: "# Debug\nReproduce first.",
};

describe("skills HTTP routes — create", () => {
  it("creates a skill and returns 201 with the stored row", async () => {
    const { server } = bootServerWithSkills();
    const res = await server.handle(post("/api/skills", VALID));
    expect(res.status).toBe(201);
    const row = (await res.json()) as RegisteredSkill;
    expect(row.id).toBe("structured-debugging");
    expect(row.version).toBe("v1");
    expect(row.body).toContain("Reproduce first.");
  });

  it("rejects a missing description / body", async () => {
    const { server } = bootServerWithSkills();
    const noDesc = await server.handle(post("/api/skills", { id: "x", body: "b" }));
    expect(noDesc.status).toBe(400);
    const noBody = await server.handle(post("/api/skills", { id: "x", description: "d" }));
    expect(noBody.status).toBe(400);
  });

  it("rejects a reserved `_`-prefixed id", async () => {
    const { server } = bootServerWithSkills();
    const res = await server.handle(post("/api/skills", { ...VALID, id: "_catalog" }));
    expect(res.status).toBe(400);
  });
});

describe("skills HTTP routes — read / update / delete", () => {
  it("lists, gets, updates, and deletes a skill", async () => {
    const { server } = bootServerWithSkills();
    await server.handle(post("/api/skills", VALID));

    // list
    const list = await (await server.handle(new Request("http://test/api/skills"))).json();
    expect((list as { skills: RegisteredSkill[] }).skills.map((s) => s.id)).toEqual([
      "structured-debugging",
    ]);

    // get (includes body)
    const got = (await (
      await server.handle(new Request("http://test/api/skills/structured-debugging"))
    ).json()) as RegisteredSkill;
    expect(got.body).toContain("Reproduce first.");

    // partial update: toggle enabled without resending body
    const patched = await server.handle(
      new Request("http://test/api/skills/structured-debugging", {
        method: "PATCH",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ metadata: { capabilities: [], tags: [], enabled: false } }),
      }),
    );
    expect(patched.status).toBe(200);
    const patchedRow = (await patched.json()) as RegisteredSkill;
    expect(patchedRow.metadata.enabled).toBe(false);
    expect(patchedRow.body).toContain("Reproduce first."); // body preserved

    // delete
    const del = await server.handle(
      new Request("http://test/api/skills/structured-debugging", { method: "DELETE" }),
    );
    expect(del.status).toBe(204);
    const after = await server.handle(new Request("http://test/api/skills/structured-debugging"));
    expect(after.status).toBe(404);
  });
});

describe("GET /api/skills/_sources — file-managed tracking", () => {
  it("returns [] when no scanner is configured", async () => {
    const { server } = bootServerWithSkills();
    const res = await server.handle(new Request("http://test/api/skills/_sources"));
    expect(res.status).toBe(200);
    const body = (await res.json()) as { fileManaged: string[] };
    expect(body.fileManaged).toEqual([]);
  });

  it("marks scanned skills as file-managed, not operator-created ones", async () => {
    const root = join(tmpdir(), `skill-sources-${process.pid}-${Date.now()}`);
    await mkdir(root, { recursive: true });
    await writeFile(
      join(root, "from-file.md"),
      ["---", "name: from-file", "description: Scanned skill.", "---", "# Body"].join("\n"),
    );
    try {
      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });
      const workflows = new LocalWorkflows({
        storage,
        runner,
        definitions: {},
        sleepScanIntervalMs: 0,
      });
      const registry = new InMemorySkillRegistry();
      const skills = new ZoryaSkills({ registry, scan: { root, intervalMs: 60_000 } });
      const server = new ZoryaServer({ workflows, skills });
      await skills.start(); // runs an immediate scan tick

      // An operator-authored skill that has no file behind it.
      await server.handle(post("/api/skills", VALID));

      const body = (await (
        await server.handle(new Request("http://test/api/skills/_sources"))
      ).json()) as { fileManaged: string[] };
      expect(body.fileManaged).toContain("from-file");
      expect(body.fileManaged).not.toContain("structured-debugging");

      await skills.stop();
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
});

describe("listCatalogSkills handler", () => {
  it("returns catalog entries without bodies", async () => {
    const registry = new InMemorySkillRegistry();
    await registry.register({ ...VALID, metadata: { tags: ["eng"], capabilities: [] } });
    const res = await listCatalogSkills({ skills: registry })();
    const body = (await res.json()) as {
      skills: Array<{ id: string; description: string; tags: string[]; enabled: boolean }>;
    };
    expect(body.skills).toHaveLength(1);
    expect(body.skills[0]!.id).toBe("structured-debugging");
    expect(body.skills[0]!.tags).toEqual(["eng"]);
    expect(body.skills[0]!.enabled).toBe(true);
    // No body field in the catalog entry.
    expect((body.skills[0] as Record<string, unknown>).body).toBeUndefined();
  });

  it("excludes needs-review skills (trust gate)", async () => {
    const registry = new InMemorySkillRegistry();
    await registry.register({ ...VALID }); // trusted (default)
    await registry.register({
      id: "unreviewed",
      description: "third-party, not approved",
      whenToUse: "never until reviewed",
      body: "# x",
      metadata: { tags: [], capabilities: [], trust: "needs-review" },
    });
    const res = await listCatalogSkills({ skills: registry })();
    const body = (await res.json()) as { skills: Array<{ id: string }> };
    expect(body.skills.map((s) => s.id)).toEqual(["structured-debugging"]);
  });
});
