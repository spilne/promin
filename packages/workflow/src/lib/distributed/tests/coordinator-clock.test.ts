// ---------------------------------------------------------------------------
// DistributedWorkflowRunner sweep loop — the between-tick wait is a timer on
// the injected clock, so FakeWallClock.advance() drives the cadence.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { DistributedWorkflowRunner } from "../coordinator.ts";
import { InMemoryStepQueue } from "../in-memory-step-queue.ts";
import type { LeaderElection } from "../leader-election.ts";
import { InMemoryWorkflowStorage } from "../../durable/in-memory-storage.ts";
import { FakeWallClock } from "../../shared/wall-clock.ts";

/** Yield to the event loop until `predicate` holds (bounded). */
async function waitFor(predicate: () => boolean): Promise<void> {
  for (let i = 0; i < 1_000 && !predicate(); i++) {
    await new Promise<void>((r) => setImmediate(r));
  }
  expect(predicate()).toBe(true);
}

/** Never wins, so each loop iteration is just one tryAcquire + the wait. */
class CountingFollower implements LeaderElection {
  attempts = 0;
  async tryAcquire(): Promise<boolean> {
    this.attempts++;
    return false;
  }
  async release(): Promise<void> {}
}

describe("DistributedWorkflowRunner.startLoop — cadence on the injected clock", () => {
  it("waits pollIntervalMs of clock time between ticks and exits after stopLoop", async () => {
    const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
    const leaderElection = new CountingFollower();
    const runner = new DistributedWorkflowRunner({
      storage: new InMemoryWorkflowStorage({ clock }),
      stepQueue: new InMemoryStepQueue(),
      leaderElection,
      pollIntervalMs: 1_000,
      clock,
    });

    const loop = runner.startLoop();
    await waitFor(() => leaderElection.attempts === 1 && clock.pendingCount() === 1);

    clock.advance(999);
    expect(leaderElection.attempts).toBe(1);

    clock.advance(1);
    await waitFor(() => leaderElection.attempts === 2 && clock.pendingCount() === 1);

    await runner.stopLoop();
    clock.advance(1_000);
    await loop;
    expect(leaderElection.attempts).toBe(2);
    expect(clock.pendingCount()).toBe(0);
  });
});
