// ---------------------------------------------------------------------------
// A keyed deployment end to end over a real port: a workflow-mode
// ZoryaWorker holding only a worker key advertises, claims a dashboard
// start, runs it through the scoped /rpc/storage plane, and keeps its
// control socket open; the trigger goes through /api with the operator key.
// ---------------------------------------------------------------------------

import { afterEach, describe, expect, it } from "bun:test";
import { InMemoryWorkflowStorage, workflow } from "@promin/workflow";
import { ZoryaClient, ZoryaWorker } from "@promin/zorya-client";
import { ZoryaServer } from "../../server/server.ts";
import type { Logger } from "../../server/server-config.ts";
import { QueuedWorkflows } from "../../server/services/workflows/index.ts";
import { InMemoryWorkflowAdvertisementRegistry } from "@promin/workflow/distributed";
import { InMemoryWorkflowStartQueue } from "@promin/workflow/distributed";

const OPERATOR_KEY = "op-key-e2e";
const WORKER_KEY = "worker-key-e2e";
const quietLogger: Logger = { log() {}, warn() {}, error() {} };

const cleanups: Array<() => Promise<void>> = [];
afterEach(async () => {
  for (const c of cleanups.splice(0).reverse()) await c();
});

async function until(predicate: () => boolean | Promise<boolean>, maxTurns = 1_000): Promise<void> {
  for (let i = 0; i < maxTurns; i++) {
    if (await predicate()) return;
    await new Promise((r) => setTimeout(r, 5));
  }
  throw new Error("condition not reached");
}

describe("keyed workflow-mode worker", () => {
  it("runs a dashboard-triggered workflow with only a worker key", async () => {
    const storage = new InMemoryWorkflowStorage();
    const server = new ZoryaServer({
      workflows: new QueuedWorkflows({
        storage,
        workflowStarts: new InMemoryWorkflowStartQueue(),
        advertisements: new InMemoryWorkflowAdvertisementRegistry(),
      }),
      apiKeys: [OPERATOR_KEY],
      remoteWorkers: { apiKeys: [WORKER_KEY] },
      logger: quietLogger,
    });
    const handle = server.listen({ port: 0, hostname: "127.0.0.1" });
    cleanups.push(() => handle.stop());
    const url = `http://127.0.0.1:${handle.port}`;

    const wf = workflow<{ n: number }>({ name: "keyed-e2e" })
      .stepAsync("double", async ({ input }) => input.n * 2)
      .build();
    const worker = new ZoryaWorker({
      client: new ZoryaClient({ url, apiKey: WORKER_KEY }),
      workflows: [wf],
      pollWorkflowStarts: { intervalMs: 10 },
      controlSocket: { reconnectDelayMs: 10 },
    });
    worker.start();
    cleanups.push(() => worker.stop());
    await worker.ready();

    // A worker key cannot trigger: /api is the operator plane.
    await expect(
      new ZoryaClient({ url, apiKey: WORKER_KEY }).startByName("keyed-e2e", { input: { n: 1 } }),
    ).rejects.toThrow(/401/);

    const operator = new ZoryaClient({ url, apiKey: WORKER_KEY, operatorApiKey: OPERATOR_KEY });
    const started = await operator.startByName("keyed-e2e", {
      input: { n: 21 },
      workflowId: "keyed-1",
    });
    expect(started.workflowId).toBe("keyed-1");

    await until(async () => (await storage.loadWorkflow("keyed-1"))?.status === "completed");
    expect((await storage.loadWorkflow("keyed-1"))?.steps.double?.result).toBe(42);
    await until(() => server.workerWs.connectedWorkers().length === 1);
  });
});
