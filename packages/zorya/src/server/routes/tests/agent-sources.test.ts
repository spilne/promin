// ---------------------------------------------------------------------------
// GET /api/agents/_sources — file-managed agent-recipe tracking. Mirror of
// the skills /api/skills/_sources test. The editor uses this to disable
// in-place Save on a file-scanned recipe so the next scan tick doesn't
// silently overwrite the change (parity with promin-q5px.16).
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { mkdir, writeFile, rm } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { InMemoryAgentRegistry } from "@promin/agent";
import { InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import { LocalWorkflows, ZoryaAgents } from "../../../index.ts";
import { ZoryaServer } from "../../server.ts";

function boot(scanRoot?: string) {
  const storage = new InMemoryWorkflowStorage();
  const runner = createWorkflowRunner({ storage });
  const workflows = new LocalWorkflows({
    storage,
    runner,
    definitions: {},
    sleepScanIntervalMs: 0,
  });
  const registry = new InMemoryAgentRegistry();
  const stub = { invoke: async () => ({ text: Promise.resolve("ok") }), withScope: () => stub };
  const agents = new ZoryaAgents({
    registry,
    resolve: () => stub as never,
    ...(scanRoot !== undefined && { scan: { root: scanRoot, intervalMs: 60_000 } }),
  });
  const server = new ZoryaServer({ workflows, agents });
  return { server, agents, registry };
}

describe("GET /api/agents/_sources — file-managed agent tracking", () => {
  it("returns [] when no scanner is configured", async () => {
    const { server } = boot();
    const res = await server.handle(new Request("http://test/api/agents/_sources"));
    expect(res.status).toBe(200);
    const body = (await res.json()) as { fileManaged: string[] };
    expect(body.fileManaged).toEqual([]);
  });

  it("marks scanned agents as file-managed, not API-created ones", async () => {
    const root = join(tmpdir(), `agent-sources-${process.pid}-${Date.now()}`);
    await mkdir(root, { recursive: true });
    await writeFile(
      join(root, "from-file.agent.ts"),
      `export const fromFile = { id: "from-file", backend: { type: "local", model: { provider: "anthropic", id: "x" }, systemPrompt: null, tools: [] } };`,
    );
    try {
      const { server, agents, registry } = boot(root);
      await agents.start(); // runs an immediate scan tick

      // An API-created agent that has no file behind it.
      await registry.register({
        id: "api-made",
        backend: {
          type: "local",
          model: { provider: "anthropic", id: "x" },
          systemPrompt: null,
          tools: [],
        },
      });

      const body = (await (
        await server.handle(new Request("http://test/api/agents/_sources"))
      ).json()) as { fileManaged: string[] };
      expect(body.fileManaged).toContain("from-file");
      expect(body.fileManaged).not.toContain("api-made");

      await agents.stop();
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
});
