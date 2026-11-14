// ---------------------------------------------------------------------------
// DistributedWorkflowRunner `retention`: the leader's sweep loop deletes ended
// runs on the retention cadence; a follower deletes nothing.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { DistributedWorkflowRunner } from "../coordinator.ts";
import { InMemoryStepQueue } from "../in-memory-step-queue.ts";
import type { LeaderElection } from "../leader-election.ts";
import { InMemoryWorkflowStorage } from "../../durable/in-memory-storage.ts";
import { FakeWallClock } from "../../shared/wall-clock.ts";

const HOUR = 3_600_000;

async function waitFor(predicate: () => boolean | Promise<boolean>): Promise<void> {
  for (let i = 0; i < 1_000 && !(await predicate()); i++) {
    await new Promise<void>((r) => setImmediate(r));
  }
  expect(await predicate()).toBe(true);
}

function setup(params: { leads: boolean }) {
  const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
  const storage = new InMemoryWorkflowStorage({ clock });
  const leader: LeaderElection = {
    tryAcquire: async () => params.leads,
    release: async () => {},
  };
  const runner = new DistributedWorkflowRunner({
    storage,
    stepQueue: new InMemoryStepQueue({ clock }),
    leaderElection: leader,
    pollIntervalMs: 1_000,
    clock,
    retention: { completed: HOUR, intervalMs: 10 * 60_000 },
  });
  return { clock, storage, runner };
}

describe("DistributedWorkflowRunner retention", () => {
  it("the leader sweeps ended runs on the retention interval", async () => {
    const { clock, storage, runner } = setup({ leads: true });
    await storage.createWorkflow({ workflowId: "old", workflowName: "w", input: {} });
    await storage.completeWorkflow({ workflowId: "old", result: 1 });

    const loop = runner.startLoop();
    // Time passes beyond the age and the interval; the loop ticks every second.
    for (let i = 0; i < 80; i++) {
      clock.advance(1_000 * 60);
      await new Promise<void>((r) => setImmediate(r));
    }
    await waitFor(async () => (await storage.loadWorkflow("old")) === null);
    await runner.stopLoop();
    await loop;
  });

  it("a follower deletes nothing", async () => {
    const { clock, storage, runner } = setup({ leads: false });
    await storage.createWorkflow({ workflowId: "old", workflowName: "w", input: {} });
    await storage.completeWorkflow({ workflowId: "old", result: 1 });

    const loop = runner.startLoop();
    for (let i = 0; i < 80; i++) {
      clock.advance(1_000 * 60);
      await new Promise<void>((r) => setImmediate(r));
    }
    await runner.stopLoop();
    await loop;
    expect(await storage.loadWorkflow("old")).not.toBeNull();
  });
});
