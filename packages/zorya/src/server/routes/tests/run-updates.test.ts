// ---------------------------------------------------------------------------
// POST /api/runs/:id/updates and GET /api/runs/:id/updates/:updateId — an
// update sent to a suspended journaled run is applied by the signal scanner's
// resume, and answered over HTTP.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import {
  InMemoryWorkflowStorage,
  createWorkflowRunner,
  defineUpdate,
  s,
  workflow,
  type Workflow,
} from "@promin/workflow";
import { ZoryaServer } from "../../server.ts";
import { LocalWorkflows } from "../../services/workflows/index.ts";
import type { ApiError, UpdateResponse } from "../../api-types.ts";

const SetLimit = defineUpdate({
  name: "setLimit",
  input: s.object({ limit: s.integer() }),
  output: s.object({ previous: s.integer() }),
});

const batch = workflow<unknown>({ name: "batch" })
  .journaled("main", function* (ctx) {
    let limit = 1;
    ctx.setUpdateHandler(SetLimit, ({ limit: next }) => {
      const previous = limit;
      limit = next;
      return { previous };
    });
    yield* ctx.signal<string>("go");
    return { limit };
  })
  .build() as Workflow<unknown, unknown>;

async function until(predicate: () => boolean | Promise<boolean>): Promise<void> {
  for (let i = 0; i < 2_000; i++) {
    if (await predicate()) return;
    await new Promise((r) => setTimeout(r, 5));
  }
  throw new Error("condition never held");
}

async function setup() {
  const storage = new InMemoryWorkflowStorage();
  const workflows = new LocalWorkflows({
    storage,
    runner: createWorkflowRunner({ storage }),
    definitions: { batch },
    sleepScanIntervalMs: 0,
    signalScanIntervalMs: 20,
  });
  await workflows.start();
  const server = new ZoryaServer({ workflows, insecure: true });
  const call = (path: string, init?: RequestInit) =>
    server.handle(new Request(`http://x${path}`, init));
  const post = (path: string, body: unknown) =>
    call(path, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
  return { storage, workflows, post, call };
}

describe("run updates over HTTP", () => {
  it("applies an update to a suspended run, answers it, and is idempotent by updateId", async () => {
    const { storage, workflows, post, call } = await setup();
    try {
      await post("/api/workflows/batch/runs", { workflowId: "b-1" });
      await until(async () => (await storage.loadSignals("b-1")).length > 0);

      const waited = await post("/api/runs/b-1/updates", {
        updateName: "setLimit",
        args: { limit: 7 },
        updateId: "u-1",
        wait: true,
      });
      expect(waited.status).toBe(200);
      expect((await waited.json()) as UpdateResponse).toEqual({
        workflowId: "b-1",
        updateId: "u-1",
        outcome: { status: "completed", result: { previous: 1 } },
      });
      expect((await storage.loadWorkflow("b-1"))?.status).toBe("suspended");

      // Same id again: answered from the record, not applied twice.
      const again = await post("/api/runs/b-1/updates", {
        updateName: "setLimit",
        args: { limit: 7 },
        updateId: "u-1",
      });
      expect(again.status).toBe(200);
      expect(((await again.json()) as UpdateResponse).outcome).toEqual({
        status: "completed",
        result: { previous: 1 },
      });
      const conflict = await post("/api/runs/b-1/updates", {
        updateName: "setLimit",
        args: { limit: 8 },
        updateId: "u-1",
      });
      expect(conflict.status).toBe(409);
      expect(((await conflict.json()) as ApiError).error).toBe("id_conflict");

      // Without `wait`: 202, then the outcome by GET once the scanner has applied it.
      const queued = await post("/api/runs/b-1/updates", {
        updateName: "setLimit",
        args: { limit: 9 },
        updateId: "u-2",
      });
      expect(queued.status).toBe(202);
      await until(async () => (await call("/api/runs/b-1/updates/u-2")).status === 200);
      expect(
        ((await (await call("/api/runs/b-1/updates/u-2")).json()) as UpdateResponse).outcome,
      ).toEqual({ status: "completed", result: { previous: 7 } });
      expect((await call("/api/runs/b-1/updates/ghost")).status).toBe(404);

      // Bad args are refused before delivery; an unknown name and an unknown run too.
      const bad = await post("/api/runs/b-1/updates", {
        updateName: "setLimit",
        args: { limit: "x" },
      });
      expect(bad.status).toBe(400);
      expect(((await bad.json()) as ApiError).error).toBe("validation");
      expect((await post("/api/runs/b-1/updates", { updateName: "nope" })).status).toBe(409);
      expect((await post("/api/runs/ghost/updates", { updateName: "setLimit" })).status).toBe(404);

      // After the run ends: 409 run_terminal.
      await post("/api/runs/b-1/signal", { signalName: "go", payload: "x" });
      await until(async () => (await storage.loadWorkflow("b-1"))?.status === "completed");
      const late = await post("/api/runs/b-1/updates", {
        updateName: "setLimit",
        args: { limit: 1 },
      });
      expect(late.status).toBe(409);
      expect(((await late.json()) as ApiError).error).toBe("run_terminal");
    } finally {
      await workflows.stop();
    }
  });
});
