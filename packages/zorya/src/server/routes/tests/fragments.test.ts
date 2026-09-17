// ---------------------------------------------------------------------------
// Fragment HTTP routes — end-to-end against a real ZoryaServer backed by an
// InMemoryFragmentRegistry (via ZoryaFragments). Pins CRUD, the _sources
// endpoint, and the scanner-discovered file-managed flag. Sibling of
// skills.test.ts.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { mkdir, writeFile, rm } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { InMemoryFragmentRegistry } from "@promin/agent";
import { InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import { LocalWorkflows, ZoryaFragments } from "../../../index.ts";
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
  const server = new ZoryaServer({ workflows, fragments });
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
