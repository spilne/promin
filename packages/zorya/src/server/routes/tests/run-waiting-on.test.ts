// ---------------------------------------------------------------------------
// GET /api/runs/:id — `waitingOn` on a suspended run, empty once it ended.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import {
  FakeWallClock,
  InMemoryWorkflowStorage,
  createWorkflowRunner,
  workflow,
} from "@promin/workflow";
import { ZoryaServer } from "../../server.ts";
import { LocalWorkflows } from "../../services/workflows/index.ts";
import type { RunDto } from "../../api-types.ts";

describe("GET /api/runs/:id waitingOn", () => {
  it("lists the signal wait with name, timeout and summary, and is empty once cancelled", async () => {
    const storage = new InMemoryWorkflowStorage();
    const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
    const runner = createWorkflowRunner({ storage, clock });
    const wf = workflow<unknown>({ name: "approval-flow" })
      .journaled("approve", function* (ctx) {
        yield* ctx.signal("manager-approved", { timeout: 2 * 3_600_000 });
        return "ok";
      })
      .build();
    const workflows = new LocalWorkflows({
      storage,
      runner,
      definitions: { "approval-flow": wf },
      sleepScanIntervalMs: 0,
      signalScanIntervalMs: 0,
    });
    const server = new ZoryaServer({ workflows, insecure: true, clock });
    const get = async (id: string) =>
      (await (await server.handle(new Request(`http://x/api/runs/${id}`))).json()) as RunDto;

    await runner.run({ workflow: wf, workflowId: "r1", input: {} }).catch(() => undefined);
    const parked = await get("r1");
    expect(parked.status).toBe("suspended");
    expect(parked.waitingOn).toEqual([
      expect.objectContaining({
        kind: "signal",
        stepName: "approve",
        signalName: "manager-approved",
        until: "2026-01-01T02:00:00.000Z",
      }),
    ]);
    expect(parked.waitingOnSummary).toBe(
      "waiting for signal manager-approved (step approve, times out in 2h)",
    );

    await server.handle(new Request("http://x/api/runs/r1/cancel", { method: "POST" }));
    const done = await get("r1");
    expect(done.status).toBe("failed");
    expect(done.waitingOn).toEqual([]);
    expect(done.waitingOnSummary).toBeUndefined();
  });
});
