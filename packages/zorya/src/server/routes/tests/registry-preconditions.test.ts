// ---------------------------------------------------------------------------
// Optimistic concurrency through `ZoryaServer.handle`: agents, roles,
// skills, fragments and schedules answer GET / POST / PATCH with an `etag`,
// and a PATCH carrying a stale `If-Match` is refused with 412 and leaves the
// resource unchanged.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import {
  InMemoryAgentRegistry,
  InMemoryFragmentRegistry,
  InMemoryRoleRegistry,
  InMemorySkillRegistry,
} from "@promin/agent";
import { FakeWallClock, InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import { InMemorySchedulerStorage } from "@promin/workflow/scheduler";
import { LocalWorkflows, ZoryaAgents, ZoryaFragments, ZoryaSkills } from "../../../index.ts";
import { ZoryaScheduler } from "../../services/scheduler/index.ts";
import { ZoryaServer } from "../../server.ts";
import { localBackend } from "./agent-gateway-harness.ts";

function boot() {
  const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
  const storage = new InMemoryWorkflowStorage();
  const workflows = new LocalWorkflows({
    storage,
    runner: createWorkflowRunner({ storage }),
    definitions: {},
    sleepScanIntervalMs: 0,
    signalScanIntervalMs: 0,
  });
  const agents = new ZoryaAgents({
    registry: new InMemoryAgentRegistry({ clock }),
    roles: new InMemoryRoleRegistry({ clock }),
    resolve: () => {
      throw new Error("precondition tests never resolve an agent");
    },
  });
  const skills = new ZoryaSkills({ registry: new InMemorySkillRegistry({ clock }) });
  const fragments = new ZoryaFragments({ registry: new InMemoryFragmentRegistry() });
  const scheduler = new ZoryaScheduler({
    storage: new InMemorySchedulerStorage({ clock }),
    workflows,
    onError: () => {},
  });
  const server = new ZoryaServer({
    insecure: true,
    workflows,
    agents,
    skills,
    fragments,
    scheduler,
    clock,
  });
  const send = (params: {
    method: string;
    path: string;
    body?: unknown;
    ifMatch?: string;
  }): Promise<Response> =>
    server.handle(
      new Request(`http://test${params.path}`, {
        method: params.method,
        headers: {
          "content-type": "application/json",
          ...(params.ifMatch !== undefined && { "if-match": params.ifMatch }),
        },
        ...(params.body !== undefined && { body: JSON.stringify(params.body) }),
      }),
    );
  return { send, clock };
}

interface Resource {
  readonly name: string;
  readonly collection: string;
  readonly item: string;
  readonly create: unknown;
  /** Three distinct edits; each changes what a GET returns. */
  readonly edits: readonly [unknown, unknown, unknown];
  /** A field of the GET body that shows which edit is stored. */
  readonly probe: (body: Record<string, unknown>) => unknown;
}

const RESOURCES: Resource[] = [
  {
    name: "agent",
    collection: "/api/agents",
    item: "/api/agents/writer",
    create: { id: "writer", backend: localBackend },
    edits: [
      { metadata: { description: "one" } },
      { metadata: { description: "two" } },
      { metadata: { description: "three" } },
    ],
    probe: (b) => (b.metadata as { description: unknown }).description,
  },
  {
    name: "role",
    collection: "/api/roles",
    item: "/api/roles/reviewer",
    create: { id: "reviewer", definition: { systemPrompt: "Review.", tools: [] } },
    edits: [
      { metadata: { description: "one" } },
      { metadata: { description: "two" } },
      { metadata: { description: "three" } },
    ],
    probe: (b) => (b.metadata as { description: unknown }).description,
  },
  {
    name: "skill",
    collection: "/api/skills",
    item: "/api/skills/triage",
    create: { id: "triage", description: "Triage bugs", body: "# Triage" },
    edits: [{ body: "# one" }, { body: "# two" }, { body: "# three" }],
    probe: (b) => b.body,
  },
  {
    name: "fragment",
    collection: "/api/fragments",
    item: "/api/fragments/tone",
    create: { key: "tone", content: "Be brief." },
    edits: [{ content: "one" }, { content: "two" }, { content: "three" }],
    probe: (b) => b.content,
  },
  {
    name: "schedule",
    collection: "/api/schedules",
    item: "/api/schedules/nightly",
    create: { id: "nightly", intervalMs: 60_000 },
    edits: [{ name: "one" }, { name: "two" }, { name: "three" }],
    probe: (b) => b.name,
  },
];

for (const r of RESOURCES) {
  describe(`${r.name} — ETag / If-Match`, () => {
    it("tags create, read and edit responses; a stale If-Match is a 412", async () => {
      const { send, clock } = boot();
      const created = await send({ method: "POST", path: r.collection, body: r.create });
      expect(created.status).toBe(201);
      const createdTag = created.headers.get("etag");
      expect(createdTag).toMatch(/^W\/".+"$/);

      const read = await send({ method: "GET", path: r.item });
      expect(read.status).toBe(200);
      expect(read.headers.get("etag")).toBe(createdTag);

      clock.advance(1_000);
      const edited = await send({
        method: "PATCH",
        path: r.item,
        body: r.edits[0],
        ifMatch: createdTag!,
      });
      expect(edited.status).toBe(200);
      const editedTag = edited.headers.get("etag");
      expect(editedTag).not.toBe(createdTag);
      const reread = await send({ method: "GET", path: r.item });
      expect(reread.headers.get("etag")).toBe(editedTag);

      // A writer still holding the creation-time tag lost the race.
      const stale = await send({
        method: "PATCH",
        path: r.item,
        body: r.edits[1],
        ifMatch: createdTag!,
      });
      expect(stale.status).toBe(412);
      expect(((await stale.json()) as { error: string }).error).toBe("precondition_failed");
      const unchanged = await send({ method: "GET", path: r.item });
      expect(r.probe((await unchanged.json()) as Record<string, unknown>)).toBe(
        r.probe(r.edits[0] as Record<string, unknown>),
      );
      expect(unchanged.headers.get("etag")).toBe(editedTag);
    });

    it("applies an edit with If-Match: *, a tag list naming the current tag, or no If-Match", async () => {
      const { send, clock } = boot();
      await send({ method: "POST", path: r.collection, body: r.create });
      clock.advance(1_000);
      const star = await send({ method: "PATCH", path: r.item, body: r.edits[0], ifMatch: "*" });
      expect(star.status).toBe(200);
      clock.advance(1_000);
      const listed = await send({
        method: "PATCH",
        path: r.item,
        body: r.edits[1],
        ifMatch: `W/"nope", ${star.headers.get("etag")}`,
      });
      expect(listed.status).toBe(200);
      clock.advance(1_000);
      const bare = await send({ method: "PATCH", path: r.item, body: r.edits[2] });
      expect(bare.status).toBe(200);
      const final = await send({ method: "GET", path: r.item });
      expect(r.probe((await final.json()) as Record<string, unknown>)).toBe(
        r.probe(r.edits[2] as Record<string, unknown>),
      );
    });
  });
}
