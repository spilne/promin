import { describe, it, expect, beforeEach } from "bun:test";
import { withLock } from "./with-lock.ts";
import { InMemoryWorkflowStorage } from "./in-memory-storage.ts";

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
    expect(await storage.tryLock("wf-1", 60_000)).toBe(true);
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
    expect(await storage.tryLock("wf-1", 60_000)).toBe(true);
  });

  it("heartbeat extends lock during execution", async () => {
    const heartbeatCalls: number[] = [];
    const original = storage.heartbeat.bind(storage);
    storage.heartbeat = async (wfId: string, durationMs: number) => {
      heartbeatCalls.push(Date.now());
      return original(wfId, durationMs);
    };

    await withLock({
      storage,
      workflowId: "wf-1",
      fn: () => new Promise((resolve) => setTimeout(resolve, 150)),
      options: { heartbeatIntervalMs: 50, lockDurationMs: 200 },
    });

    expect(heartbeatCalls.length).toBeGreaterThanOrEqual(2);
  });

  it("heartbeat failure does not crash workflow", async () => {
    storage.heartbeat = async () => {
      throw new Error("heartbeat storage error");
    };

    const result = await withLock({
      storage,
      workflowId: "wf-1",
      fn: () => new Promise((resolve) => setTimeout(() => resolve("done"), 100)),
      options: { heartbeatIntervalMs: 30 },
    });

    expect(result).toBe("done");
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
});
