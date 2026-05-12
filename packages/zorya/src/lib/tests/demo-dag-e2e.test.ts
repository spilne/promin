// ---------------------------------------------------------------------------
// E2E — boots the demo server as a subprocess and exercises the DAG
// gateway + Designer UI catalog endpoints against the live HTTP surface.
//
// Validates the wiring shipped under the DAG gateway work (POST/GET/DELETE
// /api/dags/*, /api/dags/:id/run) and the agent designer endpoints
// (/api/agents/_catalog/models, /api/agents/_catalog/tools,
// /api/agents/:id/versions, PATCH /api/agents/:id, publish via POST).
//
// Why subprocess instead of in-process `server.handle()`:
//   The existing route tests assemble a bare ZoryaServer with stub deps.
//   The demo wires the full stack — sqlite registries, model catalog,
//   tool catalog, agent resolver, DAG registry — so spawning demo.ts
//   gives us a real "would this boot and respond?" gate that a unit
//   test can't replicate.
// ---------------------------------------------------------------------------

import { afterAll, beforeAll, describe, expect, it, setDefaultTimeout } from "bun:test";
import { spawn, type Subprocess } from "bun";
import path from "node:path";

setDefaultTimeout(60_000);

const DEMO_SCRIPT = path.resolve(import.meta.dir, "..", "..", "..", "examples", "demo.ts");

let proc: Subprocess<"ignore", "pipe", "pipe"> | undefined;
let baseUrl = "";
let stdoutTail = "";
let stderrTail = "";

async function pickFreePort(): Promise<number> {
  // Bind to 0 to let the kernel pick a port, then release it so the demo
  // subprocess can claim the same number. There's a small TOCTOU window
  // here, but the port range is large enough that collisions are rare.
  const s = Bun.serve({ port: 0, fetch: () => new Response() });
  const port = s.port;
  s.stop(true);
  return port;
}

async function drainToTail(
  stream: ReadableStream<Uint8Array> | undefined,
  setter: (s: string) => void,
): Promise<void> {
  if (!stream) return;
  const dec = new TextDecoder();
  const reader = stream.getReader();
  let buf = "";
  while (true) {
    const { value, done } = await reader.read();
    if (done) break;
    buf += dec.decode(value, { stream: true });
    if (buf.length > 8_000) buf = buf.slice(-8_000);
    setter(buf);
  }
}

async function waitForHealth(url: string, timeoutMs = 30_000): Promise<void> {
  const start = Date.now();
  let lastErr: unknown;
  while (Date.now() - start < timeoutMs) {
    try {
      const r = await fetch(`${url}/api/health`);
      if (r.ok) return;
    } catch (err) {
      lastErr = err;
    }
    await new Promise((r) => setTimeout(r, 250));
  }
  const stderrHint = stderrTail ? `\n--- stderr tail ---\n${stderrTail}` : "";
  const stdoutHint = stdoutTail ? `\n--- stdout tail ---\n${stdoutTail}` : "";
  throw new Error(
    `Demo did not become healthy at ${url} within ${timeoutMs}ms (last err: ${String(lastErr)})` +
      stderrHint +
      stdoutHint,
  );
}

describe("demo e2e — DAG gateway + Designer UI catalog endpoints", () => {
  beforeAll(async () => {
    const port = await pickFreePort();
    baseUrl = `http://127.0.0.1:${port}`;
    proc = spawn({
      cmd: ["bun", "--conditions=@promin/source", DEMO_SCRIPT],
      env: {
        ...process.env,
        PORT: String(port),
        ZORYA_DB: ":memory:",
        // Unreachable Ollama URL — the demo only calls it lazily on a
        // chat turn, so this is safe and avoids hanging on real probes.
        OLLAMA_URL: "http://127.0.0.1:1",
        // The demo prints quite a lot at boot; pipe stdout/stderr so we
        // can surface a tail on health-check failure rather than letting
        // bun:test interleave it with assertion output.
        NO_COLOR: "1",
      },
      stdout: "pipe",
      stderr: "pipe",
      stdin: "ignore",
    });
    // Detach drainers — we only need the most recent ~8KB on failure.
    void drainToTail(proc.stdout, (s) => (stdoutTail = s));
    void drainToTail(proc.stderr, (s) => (stderrTail = s));
    await waitForHealth(baseUrl);
  });

  afterAll(async () => {
    if (!proc) return;
    proc.kill();
    await proc.exited.catch(() => {});
  });

  describe("DAG gateway — /api/dags/*", () => {
    it("GET /api/dags lists the pre-registered research-synthesis recipe", async () => {
      const res = await fetch(`${baseUrl}/api/dags`);
      expect(res.status).toBe(200);
      const body = (await res.json()) as { dags: ReadonlyArray<{ id: string; version: string }> };
      expect(Array.isArray(body.dags)).toBe(true);
      const ids = body.dags.map((d) => d.id);
      expect(ids).toContain("research-synthesis");
    });

    it("GET /api/dags/research-synthesis returns the full recipe with nodes + edges", async () => {
      const res = await fetch(`${baseUrl}/api/dags/research-synthesis`);
      expect(res.status).toBe(200);
      const body = (await res.json()) as {
        id: string;
        version: string;
        nodes: ReadonlyArray<{ id: string; agentId: string }>;
        edges: ReadonlyArray<{ from: string; to: string }>;
        entry: ReadonlyArray<string>;
        terminals: ReadonlyArray<string>;
      };
      expect(body.id).toBe("research-synthesis");
      expect(body.nodes.length).toBeGreaterThanOrEqual(5);
      expect(body.edges.length).toBeGreaterThanOrEqual(5);
      expect(body.entry).toContain("planner");
      expect(body.terminals).toContain("synthesizer");
      const agentIds = new Set(body.nodes.map((n) => n.agentId));
      expect(agentIds.has("dag-planner")).toBe(true);
      expect(agentIds.has("dag-researcher")).toBe(true);
      expect(agentIds.has("dag-synthesizer")).toBe(true);
    });

    it("GET /api/dags/:id 404s for unknown id", async () => {
      const res = await fetch(`${baseUrl}/api/dags/does-not-exist`);
      expect(res.status).toBe(404);
    });

    it("GET /api/dags/research-synthesis/versions returns the registered version row", async () => {
      const res = await fetch(`${baseUrl}/api/dags/research-synthesis/versions`);
      expect(res.status).toBe(200);
      const body = (await res.json()) as {
        versions: ReadonlyArray<{ id: string; version: string }>;
      };
      expect(body.versions.length).toBeGreaterThanOrEqual(1);
      expect(body.versions.every((v) => v.id === "research-synthesis")).toBe(true);
    });

    it("POST /api/dags registers a fresh DAG and DELETE removes it", async () => {
      const dagId = `e2e-tiny-${Date.now()}`;
      const create = await fetch(`${baseUrl}/api/dags`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          id: dagId,
          version: "v1",
          nodes: [
            {
              id: "n1",
              agentId: "echo-bot",
              inputs: { task: { kind: "initial", path: "task" } },
            },
          ],
          edges: [],
          entry: ["n1"],
          terminals: ["n1"],
          metadata: { description: "e2e tiny DAG", tags: ["e2e"] },
        }),
      });
      expect(create.status).toBe(201);

      const fetched = await fetch(`${baseUrl}/api/dags/${dagId}`);
      expect(fetched.status).toBe(200);
      const fetchedBody = (await fetched.json()) as { id: string; nodes: ReadonlyArray<unknown> };
      expect(fetchedBody.id).toBe(dagId);
      expect(fetchedBody.nodes).toHaveLength(1);

      const del = await fetch(`${baseUrl}/api/dags/${dagId}`, { method: "DELETE" });
      expect(del.status).toBe(204);

      const gone = await fetch(`${baseUrl}/api/dags/${dagId}`);
      expect(gone.status).toBe(404);
    });

    it("POST /api/dags rejects an invalid graph with 400 + issue list", async () => {
      const res = await fetch(`${baseUrl}/api/dags`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          id: `e2e-bad-${Date.now()}`,
          version: "v1",
          // Cycle: a → b → a — the validator must reject.
          nodes: [
            { id: "a", agentId: "echo-bot", inputs: {} },
            { id: "b", agentId: "echo-bot", inputs: {} },
          ],
          edges: [
            { from: "a", to: "b" },
            { from: "b", to: "a" },
          ],
          entry: ["a"],
          terminals: ["b"],
        }),
      });
      expect(res.status).toBe(400);
      const body = (await res.json()) as { error: string; issues?: ReadonlyArray<string> };
      expect(body.error).toBe("invalid_dag");
      expect(Array.isArray(body.issues)).toBe(true);
      expect((body.issues ?? []).length).toBeGreaterThan(0);
    });

    it("POST /api/dags/:id/run executes a 1-node echo DAG end-to-end", async () => {
      const dagId = `e2e-run-${Date.now()}`;
      const create = await fetch(`${baseUrl}/api/dags`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          id: dagId,
          version: "v1",
          nodes: [
            {
              id: "echo",
              agentId: "echo-bot",
              inputs: { task: { kind: "initial", path: "topic" } },
            },
          ],
          edges: [],
          entry: ["echo"],
          terminals: ["echo"],
        }),
      });
      expect(create.status).toBe(201);

      const run = await fetch(`${baseUrl}/api/dags/${dagId}/run`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ initialInput: { topic: "hello dag" } }),
      });
      expect(run.status).toBe(200);
      const body = (await run.json()) as {
        workflowId: string;
        result: { ok: boolean; outputs: Record<string, unknown> };
      };
      expect(body.workflowId).toContain(dagId);
      expect(body.result.ok).toBe(true);
      const echoOutput = String(body.result.outputs["echo"] ?? "");
      // demo.ts's echo-bot template: "echo-bot says: I heard '{task}' (turn #{n})"
      expect(echoOutput).toContain("echo-bot says");
      expect(echoOutput).toContain("hello dag");
    });
  });

  describe("Designer UI metadata — /api/agents/_catalog/*", () => {
    it("GET /api/agents/_catalog/models exposes at least the ollama default", async () => {
      const res = await fetch(`${baseUrl}/api/agents/_catalog/models`);
      expect(res.status).toBe(200);
      const body = (await res.json()) as {
        models: ReadonlyArray<{ provider: string; id: string; displayName?: string }>;
      };
      expect(Array.isArray(body.models)).toBe(true);
      expect(body.models.length).toBeGreaterThanOrEqual(1);
      const providers = new Set(body.models.map((m) => m.provider));
      expect(providers.has("ollama")).toBe(true);
      // SerializedModelCatalogItem must not leak the runtime `llm` factory.
      for (const m of body.models) {
        expect((m as Record<string, unknown>).llm).toBeUndefined();
      }
    });

    it("GET /api/agents/_catalog/tools enumerates host + file tools", async () => {
      const res = await fetch(`${baseUrl}/api/agents/_catalog/tools`);
      expect(res.status).toBe(200);
      const body = (await res.json()) as {
        tools: ReadonlyArray<{ name: string; source: string }>;
      };
      expect(Array.isArray(body.tools)).toBe(true);
      expect(body.tools.length).toBeGreaterThanOrEqual(1);
      const names = new Set(body.tools.map((t) => t.name));
      // Host-supplied closure tool exposed under in-process source.
      expect(names.has("listWorkflows")).toBe(true);
    });

    it("GET /api/agents/_catalog/tools/health returns a recipes+orphans report", async () => {
      const res = await fetch(`${baseUrl}/api/agents/_catalog/tools/health`);
      expect(res.status).toBe(200);
      const body = (await res.json()) as {
        recipes: ReadonlyArray<unknown>;
        orphans: ReadonlyArray<unknown>;
      };
      expect(Array.isArray(body.recipes)).toBe(true);
      expect(Array.isArray(body.orphans)).toBe(true);
    });
  });

  describe("Agent versioning — PATCH + publish via POST", () => {
    it("GET /api/agents/echo-bot/versions returns the seeded version", async () => {
      const res = await fetch(`${baseUrl}/api/agents/echo-bot/versions`);
      expect(res.status).toBe(200);
      const body = (await res.json()) as { versions: ReadonlyArray<{ version: string }> };
      expect(body.versions.length).toBeGreaterThanOrEqual(1);
    });

    it("PATCH /api/agents/:id replaces in-place (same version) and POST publishes a new version", async () => {
      // Snapshot the current version count.
      const before = (await (await fetch(`${baseUrl}/api/agents/echo-bot/versions`)).json()) as {
        versions: ReadonlyArray<{ version: string }>;
      };
      const beforeCount = before.versions.length;
      const beforeVersion = before.versions[0]!.version;

      // PATCH replaces in-place — no new row.
      const patch = await fetch(`${baseUrl}/api/agents/echo-bot`, {
        method: "PATCH",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          metadata: {
            description: "Echoes — touched by e2e PATCH.",
            capabilities: ["chat"],
            tags: ["demo", "stable", "e2e-touched"],
          },
        }),
      });
      expect(patch.status).toBe(200);
      const patched = (await patch.json()) as { metadata: { tags: ReadonlyArray<string> } };
      expect(patched.metadata.tags).toContain("e2e-touched");

      const afterPatch = (await (
        await fetch(`${baseUrl}/api/agents/echo-bot/versions`)
      ).json()) as { versions: ReadonlyArray<{ version: string }> };
      expect(afterPatch.versions.length).toBe(beforeCount);

      // POST registers a brand-new version — row count grows by 1.
      const nextVersion = `${beforeVersion}-e2e`;
      const post = await fetch(`${baseUrl}/api/agents`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          id: "echo-bot",
          version: nextVersion,
          backend: {
            type: "local",
            model: { provider: "mock", id: "echo-v1" },
            systemPrompt: "Echo bot, e2e-published variant.",
            tools: [],
          },
          metadata: {
            description: "Echo — e2e published version.",
            capabilities: ["chat"],
            tags: ["e2e-published"],
          },
        }),
      });
      expect(post.status).toBe(201);

      const afterPost = (await (await fetch(`${baseUrl}/api/agents/echo-bot/versions`)).json()) as {
        versions: ReadonlyArray<{ version: string }>;
      };
      expect(afterPost.versions.length).toBe(beforeCount + 1);
      expect(afterPost.versions.map((v) => v.version)).toContain(nextVersion);
    });
  });
});
