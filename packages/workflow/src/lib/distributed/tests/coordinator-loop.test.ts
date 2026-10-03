// ---------------------------------------------------------------------------
// DistributedWorkflowRunner: the sweep loop survives storage / queue errors,
// stopLoop() waits for the loop and releases leadership, and a result waiter
// registered while a run settles is never stranded.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { DistributedWorkflowRunner, type DistributedRunnerErrorEvent } from "../coordinator.ts";
import { InMemoryStepQueue } from "../in-memory-step-queue.ts";
import type { LeaderElection } from "../leader-election.ts";
import type { StepQueue } from "../step-queue.ts";
import { InMemoryWorkflowStorage } from "../../durable/in-memory-storage.ts";
import type { WorkflowState } from "../../durable/workflow-state.ts";
import { FakeWallClock } from "../../shared/wall-clock.ts";

/** Yield to the event loop until `predicate` holds (bounded). */
async function waitFor(predicate: () => boolean): Promise<void> {
  for (let i = 0; i < 1_000 && !predicate(); i++) {
    await new Promise<void>((r) => setImmediate(r));
  }
  expect(predicate()).toBe(true);
}

class TrackingLeader implements LeaderElection {
  acquired = 0;
  released = 0;
  async tryAcquire(): Promise<boolean> {
    this.acquired++;
    return true;
  }
  async release(): Promise<void> {
    this.released++;
  }
}

describe("DistributedWorkflowRunner sweep loop", () => {
  it("a failing sweep is reported and backed off; the loop keeps sweeping", async () => {
    const clock = FakeWallClock.create(0);
    const inner = new InMemoryStepQueue({ clock });
    let sweeps = 0;
    const queue: StepQueue = {
      enqueue: (p) => inner.enqueue(p),
      claim: (p) => inner.claim(p),
      complete: (p) => inner.complete(p),
      fail: (p) => inner.fail(p),
      heartbeat: (p) => inner.heartbeat(p),
      metrics: (p) => inner.metrics(p),
      requeueStuck: async (p) => {
        sweeps++;
        if (sweeps <= 2) throw new Error("queue blip");
        return inner.requeueStuck(p);
      },
    };
    const leader = new TrackingLeader();
    const errors: DistributedRunnerErrorEvent[] = [];
    const runner = new DistributedWorkflowRunner({
      storage: new InMemoryWorkflowStorage({ clock }),
      stepQueue: queue,
      leaderElection: leader,
      pollIntervalMs: 1_000,
      clock,
      onError: (e) => errors.push(e),
    });

    const loop = runner.startLoop();
    await waitFor(() => sweeps === 1 && clock.pendingCount() === 1);
    expect(errors).toEqual([{ source: "sweep", error: expect.any(Error), consecutiveFailures: 1 }]);

    clock.advance(1_000);
    await waitFor(() => sweeps === 2 && clock.pendingCount() === 1);
    // Second failure in a row: the wait doubles.
    clock.advance(1_999);
    expect(sweeps).toBe(2);
    clock.advance(1);
    await waitFor(() => sweeps === 3 && clock.pendingCount() === 1);
    expect(errors).toHaveLength(2);

    clock.advance(1_000);
    await waitFor(() => sweeps === 4 && clock.pendingCount() === 1);

    await runner.stopLoop();
    expect(leader.released).toBe(1);
    expect(clock.pendingCount()).toBe(0);
    await loop;
  });

  it("stopLoop() resolves only after the in-flight sweep and the leader release", async () => {
    const clock = FakeWallClock.create(0);
    let releaseSweep!: () => void;
    const gate = new Promise<void>((r) => (releaseSweep = r));
    let sweepStarted = false;
    const inner = new InMemoryStepQueue({ clock });
    const queue: StepQueue = {
      enqueue: (p) => inner.enqueue(p),
      claim: (p) => inner.claim(p),
      complete: (p) => inner.complete(p),
      fail: (p) => inner.fail(p),
      heartbeat: (p) => inner.heartbeat(p),
      metrics: (p) => inner.metrics(p),
      requeueStuck: async (p) => {
        sweepStarted = true;
        await gate;
        return inner.requeueStuck(p);
      },
    };
    const leader = new TrackingLeader();
    const runner = new DistributedWorkflowRunner({
      storage: new InMemoryWorkflowStorage({ clock }),
      stepQueue: queue,
      leaderElection: leader,
      clock,
    });

    void runner.startLoop();
    await waitFor(() => sweepStarted);
    let stopped = false;
    const stopping = runner.stopLoop().then(() => (stopped = true));
    await new Promise<void>((r) => setImmediate(r));
    expect(stopped).toBe(false);
    expect(leader.released).toBe(0);

    releaseSweep();
    await stopping;
    expect(leader.released).toBe(1);
    expect(clock.pendingCount()).toBe(0);
  });
});

describe("DistributedWorkflowRunner.waitForResult", () => {
  it("a run that settles while the storage check is in flight still resolves the waiter", async () => {
    const storage = new InMemoryWorkflowStorage();
    let releaseLoad!: () => void;
    const loadGate = new Promise<void>((r) => (releaseLoad = r));
    const realLoad = storage.loadWorkflow.bind(storage);
    storage.loadWorkflow = async (id: string): Promise<WorkflowState | null> => {
      // Snapshot first, then stall: the caller sees the pre-settle state.
      const snapshot = await realLoad(id);
      await loadGate;
      return snapshot;
    };
    await storage.createWorkflow({ workflowId: "wf", workflowName: "x", input: {} });
    const runner = new DistributedWorkflowRunner({ storage, stepQueue: new InMemoryStepQueue() });

    let result: unknown = "pending";
    const waiting = runner.waitForResult("wf").then((r) => (result = r));
    await new Promise<void>((r) => setImmediate(r));

    // The run settles in this process while the load is still in flight.
    (runner as unknown as { _resolveWaiters(id: string, r: unknown): void })._resolveWaiters(
      "wf",
      "done",
    );
    releaseLoad();
    await waiting;
    expect(result).toBe("done");
  });
});
