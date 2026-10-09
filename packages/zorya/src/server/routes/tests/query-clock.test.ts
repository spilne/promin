// ---------------------------------------------------------------------------
// POST /api/runs/:id/query — the overall query timeout is a timer on the
// injected clock, cleared once the race settles.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { FakeWallClock } from "@promin/workflow";
import type { WorkerWebSocketServer } from "../../services/worker-ws-server.ts";
import { queryRun } from "../query.ts";

/** Yield to the event loop until `predicate` holds (bounded). */
async function waitFor(predicate: () => boolean): Promise<void> {
  for (let i = 0; i < 1_000 && !predicate(); i++) {
    await new Promise<void>((r) => setImmediate(r));
  }
  expect(predicate()).toBe(true);
}

function queryReq(body: unknown): Request {
  return new Request("http://x/api/runs/wf-1/query", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
}

describe("queryRun — timeout on an injected clock", () => {
  it("answers 504 once the clock reaches timeoutMs when no worker replies", async () => {
    const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
    // A connected worker that never answers.
    const workerWs = {
      connectedWorkers: () => ["w-1"],
      request: () => new Promise<never>(() => {}),
    } as unknown as WorkerWebSocketServer;

    let settled = false;
    const pending = queryRun({ workerWs, clock })(queryReq({ name: "status", timeoutMs: 2_000 }), {
      id: "wf-1",
    }).then((res) => {
      settled = true;
      return res;
    });

    await waitFor(() => clock.pendingCount() === 1);
    clock.advance(1_999);
    await new Promise<void>((r) => setImmediate(r));
    expect(settled).toBe(false);

    clock.advance(1);
    const res = await pending;
    expect(res.status).toBe(504);
    expect(clock.pendingCount()).toBe(0);
  });

  it("clears the timeout timer when a worker answers first", async () => {
    const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
    const workerWs = {
      connectedWorkers: () => ["w-1"],
      request: async () => ({ hosted: true, result: { phase: "ready" } }),
    } as unknown as WorkerWebSocketServer;

    const res = await queryRun({ workerWs, clock })(queryReq({ name: "status" }), { id: "wf-1" });
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ result: { phase: "ready" } });
    expect(clock.pendingCount()).toBe(0);
  });
});
