// ---------------------------------------------------------------------------
// RunWatcherPool / RunPollWatcher — one shared poller per run, terminal
// statuses (tripwire included) end the stream, a deleted run ends it, and
// storage failures back off on the clock before giving up with an error.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { FakeWallClock, InMemoryWorkflowStorage, type WorkflowState } from "@promin/workflow";
import { RunEventBus } from "../run-event-bus.ts";
import { RunWatcherPool } from "../run-poll-watcher.ts";
import type { RunEvent } from "../api-types.ts";

class CountingStorage extends InMemoryWorkflowStorage {
  loads = 0;
  failing = false;
  gone = false;
  override async loadWorkflow(workflowId: string) {
    this.loads++;
    if (this.failing) throw new Error("connection reset");
    if (this.gone) return null;
    return super.loadWorkflow(workflowId);
  }
}

type Terminal = "completed" | "failed" | "tripwire";

async function finish(storage: InMemoryWorkflowStorage, status: Terminal): Promise<void> {
  const workflowId = "wf-1";
  if (status === "completed") await storage.completeWorkflow({ workflowId, result: 1 });
  else if (status === "failed") await storage.failWorkflow({ workflowId, error: "boom" });
  else await storage.tripwireWorkflow({ workflowId, reason: "guard tripped" });
}

async function setup(): Promise<{
  clock: FakeWallClock;
  storage: CountingStorage;
  pool: RunWatcherPool;
  errors: unknown[];
  state: () => Promise<WorkflowState>;
}> {
  const clock = FakeWallClock.create(0);
  const storage = new CountingStorage({ clock });
  await storage.createWorkflow({ workflowId: "wf-1", workflowName: "order", input: {} });
  const errors: unknown[] = [];
  const pool = new RunWatcherPool({
    storage,
    bus: new RunEventBus(),
    clock,
    intervalMs: 1_000,
    maxConsecutiveFailures: 3,
    onError: ({ error }) => errors.push(error),
  });
  const state = async () => (await storage.loadWorkflow("wf-1"))!;
  return { clock, storage, pool, errors, state };
}

/** Advance the clock by `ms` and let the poll it fires settle. */
async function tick(clock: FakeWallClock, ms: number): Promise<void> {
  clock.advance(ms);
  for (let i = 0; i < 20; i++) await new Promise<void>((r) => setImmediate(r));
}

const types = (events: RunEvent[]) => events.map((e) => e.type);

describe("RunWatcherPool", () => {
  it("shares one poller across viewers and gives each viewer its own snapshot", async () => {
    const { clock, storage, pool, state } = await setup();
    const a: RunEvent[] = [];
    const b: RunEvent[] = [];
    pool.subscribe({ state: await state(), listener: (e) => a.push(e) });
    pool.subscribe({ state: await state(), listener: (e) => b.push(e) });
    expect(pool.activeWatchers()).toBe(1);
    expect(clock.pendingCount()).toBe(1);

    await storage.saveStepResult({
      workflowId: "wf-1",
      stepName: "a",
      result: 1,
      durationMs: 5,
      startedAt: clock.now(),
    });
    storage.loads = 0;
    await tick(clock, 1_000);

    expect(storage.loads).toBe(1);
    // Each event exactly once per viewer (the step write also flips the run to running).
    expect(types(a)).toEqual(["snapshot", "step", "status"]);
    expect(types(b)).toEqual(["snapshot", "step", "status"]);
  });

  it("stops polling when the last viewer leaves", async () => {
    const { clock, pool, state } = await setup();
    const offA = pool.subscribe({ state: await state(), listener: () => {} });
    const offB = pool.subscribe({ state: await state(), listener: () => {} });
    offA();
    expect(clock.pendingCount()).toBe(1);
    offB();
    expect(clock.pendingCount()).toBe(0);
    expect(pool.activeWatchers()).toBe(0);
  });

  for (const status of ["completed", "failed", "tripwire"] as const) {
    it(`ends the stream when the run turns ${status}`, async () => {
      const { clock, storage, pool, state } = await setup();
      const events: RunEvent[] = [];
      pool.subscribe({ state: await state(), listener: (e) => events.push(e) });

      await finish(storage, status);
      await tick(clock, 1_000);

      expect(types(events)).toEqual(["snapshot", "status", "end"]);
      expect(clock.pendingCount()).toBe(0);
      expect(pool.activeWatchers()).toBe(0);
    });
  }

  it("sends snapshot then end straight away for an already-terminal run", async () => {
    const { clock, storage, pool, state } = await setup();
    await finish(storage, "tripwire");
    const events: RunEvent[] = [];
    pool.subscribe({ state: await state(), listener: (e) => events.push(e) });
    expect(types(events)).toEqual(["snapshot", "end"]);
    expect(clock.pendingCount()).toBe(0);
  });

  it("ends the stream when the run is deleted while watched", async () => {
    const { clock, storage, pool, state } = await setup();
    const events: RunEvent[] = [];
    pool.subscribe({ state: await state(), listener: (e) => events.push(e) });
    storage.gone = true;
    await tick(clock, 1_000);
    expect(types(events)).toEqual(["snapshot", "end"]);
    expect(clock.pendingCount()).toBe(0);
  });

  it("backs off on storage errors, recovers, and gives up after the failure budget", async () => {
    const { clock, storage, pool, errors, state } = await setup();
    const events: RunEvent[] = [];
    pool.subscribe({ state: await state(), listener: (e) => events.push(e) });

    storage.failing = true;
    await tick(clock, 1_000); // failure 1 → retry in 2s
    expect(errors).toHaveLength(1);
    await tick(clock, 1_999);
    expect(errors).toHaveLength(1);
    await tick(clock, 1); // failure 2 → retry in 4s
    expect(errors).toHaveLength(2);

    // A success resets the budget.
    storage.failing = false;
    await tick(clock, 4_000);
    expect(errors).toHaveLength(2);
    expect(types(events)).toEqual(["snapshot"]);

    storage.failing = true;
    await tick(clock, 1_000); // 1
    await tick(clock, 2_000); // 2
    await tick(clock, 4_000); // 3 → give up
    expect(errors).toHaveLength(5);
    expect(events.slice(-2)).toEqual([{ type: "error", error: "watch_failed" }, { type: "end" }]);
    expect(clock.pendingCount()).toBe(0);
    expect(pool.activeWatchers()).toBe(0);
  });
});
