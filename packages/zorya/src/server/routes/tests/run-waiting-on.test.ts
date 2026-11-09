// ---------------------------------------------------------------------------
// GET /api/runs/:id — `waitingOn` on a suspended run, empty once it ended.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { fail, succeed, TaggedError } from "@spilne/perfect-core";
import {
  FakeWallClock,
  InMemoryWorkflowStorage,
  createWorkflowRunner,
  workflow,
} from "@promin/workflow";
import { ZoryaServer } from "../../server.ts";
import { LocalWorkflows } from "../../services/workflows/index.ts";
import type { RunDto } from "../../api-types.ts";

class Flaky extends TaggedError("Flaky")<{ readonly message: string }>() {}

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

  it("lists a running step as activity and a failed one as retry-backoff", async () => {
    const storage = new InMemoryWorkflowStorage();
    const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
    const runner = createWorkflowRunner({ storage, clock, executorId: "exec-7" });
    let release!: () => void;
    const held = new Promise<void>((r) => (release = r));
    let calls = 0;
    const wf = workflow<unknown>({ name: "busy-flow" })
      .step(
        "flaky",
        () => {
          calls++;
          return calls === 1 ? fail(new Flaky({ message: "once" })) : succeed(1);
        },
        { retry: { maxRetries: 1, baseDelayMs: 4_000 } },
      )
      .step("slow", async () => {
        await held;
        return 2;
      })
      .build();
    const workflows = new LocalWorkflows({
      storage,
      runner,
      definitions: { "busy-flow": wf },
      sleepScanIntervalMs: 0,
      signalScanIntervalMs: 0,
    });
    const server = new ZoryaServer({ workflows, insecure: true, clock });
    const get = async () =>
      (await (await server.handle(new Request("http://x/api/runs/b1"))).json()) as RunDto;

    const run = runner.run({ workflow: wf, workflowId: "b1", input: {} });
    await new Promise((r) => setTimeout(r, 0));
    const backoff = await get();
    expect(backoff.waitingOn).toEqual([
      expect.objectContaining({
        kind: "retry-backoff",
        stepName: "flaky",
        attempt: 1,
        until: "2026-01-01T00:00:04.000Z",
      }),
    ]);
    expect(backoff.waitingOnSummary).toBe(
      "retrying step flaky after failed attempt 1 (next attempt in 4s)",
    );

    clock.advance(4_000);
    await new Promise((r) => setTimeout(r, 0));
    clock.advance(1_000);
    await new Promise((r) => setTimeout(r, 0));
    const busy = await get();
    expect(busy.waitingOn).toEqual([
      expect.objectContaining({
        kind: "activity",
        stepName: "slow",
        attempt: 1,
        holder: "exec-7",
        since: "2026-01-01T00:00:04.000Z",
      }),
    ]);
    expect(busy.waitingOnSummary).toBe("running step slow (attempt 1, for 1s, on exec-7)");

    release();
    await run;
    expect((await get()).waitingOn).toEqual([]);
  });
});
