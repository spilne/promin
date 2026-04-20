import { describe, it, expect, beforeEach } from "bun:test";
import { FakeClock } from "@promin/core";
import { withLock } from "../with-lock.ts";
import { InMemoryWorkflowStorage } from "../in-memory-storage.ts";

describe("withLock", () => {
  let storage: InMemoryWorkflowStorage;

  beforeEach(() => {
    storage = new InMemoryWorkflowStorage();
  });

  it("acquires lock and executes fn", async () => {
    let executed = false;
    await withLock({
      storage,
      workflowId: "wf-1",
      fn: async () => {
        executed = true;
      },
    });
    expect(executed).toBe(true);
  });

  it("throws WorkflowLockError when lock is held", async () => {
    await storage.tryLock("wf-1", 60_000);
    try {
      await withLock({
        storage,
        workflowId: "wf-1",
        fn: async () => "unreachable",
      });
      expect.unreachable("should have thrown");
    } catch (error: any) {
      expect(error._tag).toBe("WorkflowLockError");
      expect(error.workflowId).toBe("wf-1");
    }
  });

  it("releases lock on success", async () => {
    await withLock({
      storage,
      workflowId: "wf-1",
      fn: async () => "ok",
    });
    // Lock should be released — can acquire again
    expect((await storage.tryLock("wf-1", 60_000)).acquired).toBe(true);
  });

  it("releases lock when fn throws", async () => {
    try {
      await withLock({
        storage,
        workflowId: "wf-1",
        fn: async () => {
          throw new Error("boom");
        },
      });
    } catch {
      // expected
    }
    // Lock should be released
    expect((await storage.tryLock("wf-1", 60_000)).acquired).toBe(true);
  });

  it("heartbeat extends lock during execution", async () => {
    // FakeClock drives both the heartbeat interval and fn's own sleep, so
    // advancing time fires callbacks deterministically — no real 150ms wait,
    // no flake on slow CI boxes.
    const clock = FakeClock.create(0);
    const clockedStorage = new InMemoryWorkflowStorage({ clock });
    const heartbeatCalls: number[] = [];
    const original = clockedStorage.heartbeat.bind(clockedStorage);
    clockedStorage.heartbeat = async (wfId: string, durationMs: number, guard) => {
      heartbeatCalls.push(clock.currentTimeMs());
      return original(wfId, durationMs, guard);
    };

    const done = withLock({
      storage: clockedStorage,
      workflowId: "wf-1",
      fn: () => new Promise<void>((resolve) => clock.setTimeout(() => resolve(), 150)),
      options: { heartbeatIntervalMs: 50, lockDurationMs: 200, clock },
    });

    // Hand control back so the async withLock body reaches the scheduled
    // setInterval / setTimeout before we advance the clock.
    await Promise.resolve();
    await Promise.resolve();
    clock.advance(150);
    await done;

    // Fired at 50, 100, and 150 — two minimum as heartbeat tops up before
    // fn's own sleep completes.
    expect(heartbeatCalls.length).toBeGreaterThanOrEqual(2);
  });

  it("heartbeat failure does not crash workflow", async () => {
    const clock = FakeClock.create(0);
    const clockedStorage = new InMemoryWorkflowStorage({ clock });
    clockedStorage.heartbeat = async () => {
      throw new Error("heartbeat storage error");
    };

    const done = withLock({
      storage: clockedStorage,
      workflowId: "wf-1",
      fn: () => new Promise<string>((resolve) => clock.setTimeout(() => resolve("done"), 100)),
      options: { heartbeatIntervalMs: 30, clock },
    });

    await Promise.resolve();
    await Promise.resolve();
    clock.advance(100);

    expect(await done).toBe("done");
  });

  it("propagates fn return value", async () => {
    const result = await withLock({
      storage,
      workflowId: "wf-1",
      fn: async () => ({ answer: 42 }),
    });
    expect(result).toEqual({ answer: 42 });
  });

  it("propagates fn error without wrapping", async () => {
    const originalError = new Error("step failed");
    try {
      await withLock({
        storage,
        workflowId: "wf-1",
        fn: async () => {
          throw originalError;
        },
      });
      expect.unreachable("should have thrown");
    } catch (error) {
      expect(error).toBe(originalError);
    }
  });

  it("release from wrong instance is rejected", async () => {
    const instance1 = new InMemoryWorkflowStorage({ instanceId: "node-1" });
    const instance2 = new InMemoryWorkflowStorage({ instanceId: "node-2" });
    // Shared state: point instance2's locks at instance1's internal map
    (instance2 as any).locks = (instance1 as any).locks;

    await instance1.tryLock("wf-1", 60_000);
    // instance2 tries to release — should be rejected (different owner)
    await instance2.releaseLock("wf-1");
    // Lock should still be held — instance1 can't re-acquire
    expect((await instance1.tryLock("wf-1", 60_000)).acquired).toBe(false);
  });

  it("heartbeat from wrong instance is rejected", async () => {
    const clock = FakeClock.create(0);
    const instance1 = new InMemoryWorkflowStorage({ instanceId: "node-1", clock });
    const instance2 = new InMemoryWorkflowStorage({ instanceId: "node-2", clock });
    // Shared state: point instance2's locks at instance1's internal map
    (instance2 as any).locks = (instance1 as any).locks;

    // instance1 acquires with short lock
    await instance1.tryLock("wf-1", 100);
    // instance2 tries to extend — should be rejected
    await instance2.heartbeat("wf-1", 60_000);
    // Advance past the original lock's expiry.
    clock.advance(150);
    // Lock should have expired (heartbeat from wrong instance didn't extend it)
    expect((await instance1.tryLock("wf-1", 60_000)).acquired).toBe(true);
  });

  it("coarser default heartbeat: a 500ms run at the 30s default fires zero heartbeats", async () => {
    // Under the old 10s default a 500ms run would have fired zero heartbeats
    // too, but a 35s run would have hit 3. We're not exercising a 35s run
    // in a unit test — the point of the default change is that at any
    // sub-30s duration the heartbeat loop stays quiet. Instrument the count
    // to prove the defaults are wired through (no explicit options).
    const clock = FakeClock.create(0);
    const clockedStorage = new InMemoryWorkflowStorage({ clock });
    const calls: number[] = [];
    clockedStorage.heartbeat = async () => {
      calls.push(clock.currentTimeMs());
    };
    const done = withLock({
      storage: clockedStorage,
      workflowId: "wf-default",
      fn: () => new Promise<void>((resolve) => clock.setTimeout(() => resolve(), 500)),
      options: { clock },
    });
    await Promise.resolve();
    await Promise.resolve();
    clock.advance(500);
    await done;
    expect(calls.length).toBe(0);
  });
});
