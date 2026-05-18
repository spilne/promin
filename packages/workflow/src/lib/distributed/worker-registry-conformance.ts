// ---------------------------------------------------------------------------
// Shared conformance suite for WorkerRegistry implementations.
//
// Any backend (in-memory, Postgres, Redis, ...) must pass this suite so the
// semantics that workers and Zorya rely on — idempotent register, silent
// no-op on unknown workers, atomic transition-to-dead — stay consistent
// across backends. In-memory + Postgres tests point at this function; new
// backends land with a four-line test file that imports and invokes it.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import type { WorkerRegistry } from "./worker-registry.ts";

/**
 * Invoke this inside a describe block (or at the top level) to run the
 * full WorkerRegistry conformance suite against a factory.
 *
 * Each test calls `factory()` to get a fresh registry — the factory is
 * responsible for isolation (Postgres tests typically wrap it in
 * `postgresDescribe` which gives a clean schema per test).
 */
export function workerRegistryConformance(params: {
  /** Fresh registry per test. Called inside `it`, so async setup is fine. */
  factory: () => Promise<WorkerRegistry>;
  /**
   * Tolerance for heartbeat-timestamp assertions. In-memory uses Date.now()
   * so 50ms is plenty; Postgres tests may want more if the connection is
   * slow. Default: 50ms.
   */
  heartbeatToleranceMs?: number;
}): void {
  const tolerance = params.heartbeatToleranceMs ?? 50;

  describe("register + list", () => {
    it("new worker lands with its capabilities, concurrency, metadata", async () => {
      const registry = await params.factory();
      await registry.register({
        workerId: "w-1",
        capabilities: ["default"],
        concurrency: 5,
        metadata: { hostname: "node-1" },
      });
      const workers = await registry.list();
      expect(workers).toHaveLength(1);
      expect(workers[0]!.workerId).toBe("w-1");
      expect(workers[0]!.status).toBe("active");
      expect(workers[0]!.capabilities).toEqual(["default"]);
      expect(workers[0]!.concurrency).toBe(5);
      expect(workers[0]!.metadata).toEqual({ hostname: "node-1" });
    });

    it("re-registering the same workerId replaces the prior row", async () => {
      const registry = await params.factory();
      await registry.register({
        workerId: "w-1",
        capabilities: ["old"],
        concurrency: 1,
      });
      await registry.register({
        workerId: "w-1",
        capabilities: ["new", "caps"],
        concurrency: 4,
      });
      const workers = await registry.list();
      expect(workers).toHaveLength(1);
      expect(workers[0]!.capabilities).toEqual(["new", "caps"]);
      expect(workers[0]!.concurrency).toBe(4);
    });
  });

  describe("heartbeat", () => {
    it("advances the lastHeartbeat timestamp", async () => {
      const registry = await params.factory();
      await registry.register({ workerId: "w-1", capabilities: [], concurrency: 1 });
      const before = (await registry.list())[0]!.lastHeartbeat;
      await wait(tolerance + 10);
      await registry.heartbeat("w-1");
      const after = (await registry.list())[0]!.lastHeartbeat;
      expect(after.getTime()).toBeGreaterThan(before.getTime());
    });

    it("silently no-ops on unknown worker", async () => {
      const registry = await params.factory();
      await registry.heartbeat("nobody");
      expect(await registry.list()).toHaveLength(0);
    });
  });

  describe("drain", () => {
    it("transitions the worker to status=draining", async () => {
      const registry = await params.factory();
      await registry.register({ workerId: "w-1", capabilities: [], concurrency: 1 });
      await registry.drain("w-1");
      const draining = await registry.list({ status: "draining" });
      expect(draining).toHaveLength(1);
      expect(draining[0]!.workerId).toBe("w-1");
    });

    it("silently no-ops on unknown worker", async () => {
      const registry = await params.factory();
      await registry.drain("nobody");
      expect(await registry.list()).toHaveLength(0);
    });
  });

  describe("deregister", () => {
    it("retires the worker — keeps the row, sets status=retired + retiredAt", async () => {
      const registry = await params.factory();
      await registry.register({ workerId: "w-1", capabilities: [], concurrency: 1 });
      await registry.deregister("w-1");

      const all = await registry.list();
      expect(all).toHaveLength(1);
      expect(all[0]!.status).toBe("retired");
      expect(all[0]!.retiredAt).toBeInstanceOf(Date);
      // Filterable as retired; no longer counted active.
      expect(await registry.list({ status: "retired" })).toHaveLength(1);
      expect(await registry.list({ status: "active" })).toHaveLength(0);
    });

    it("silently no-ops on unknown worker", async () => {
      const registry = await params.factory();
      await registry.deregister("nobody");
      expect(await registry.list()).toHaveLength(0);
    });
  });

  describe("list", () => {
    it("filters by status", async () => {
      const registry = await params.factory();
      await registry.register({ workerId: "w-1", capabilities: [], concurrency: 1 });
      await registry.register({ workerId: "w-2", capabilities: ["gpu"], concurrency: 2 });
      await registry.drain("w-2");

      const active = await registry.list({ status: "active" });
      expect(active.map((w) => w.workerId).sort()).toEqual(["w-1"]);

      const draining = await registry.list({ status: "draining" });
      expect(draining.map((w) => w.workerId).sort()).toEqual(["w-2"]);
    });

    it("returns all workers regardless of status when no filter", async () => {
      const registry = await params.factory();
      await registry.register({ workerId: "w-1", capabilities: [], concurrency: 1 });
      await registry.register({ workerId: "w-2", capabilities: [], concurrency: 1 });
      await registry.drain("w-2");
      const all = await registry.list();
      expect(all).toHaveLength(2);
    });
  });

  describe("detectDead", () => {
    it("marks stale workers as dead and returns them", async () => {
      const registry = await params.factory();
      await registry.register({ workerId: "w-1", capabilities: [], concurrency: 1 });
      await wait(120);

      const dead = await registry.detectDead(50);
      expect(dead).toHaveLength(1);
      expect(dead[0]!.workerId).toBe("w-1");
      expect(dead[0]!.status).toBe("dead");

      const deadList = await registry.list({ status: "dead" });
      expect(deadList).toHaveLength(1);
    });

    it("leaves fresh workers alone", async () => {
      const registry = await params.factory();
      await registry.register({ workerId: "w-1", capabilities: [], concurrency: 1 });
      const dead = await registry.detectDead(60_000);
      expect(dead).toHaveLength(0);
      const active = await registry.list({ status: "active" });
      expect(active).toHaveLength(1);
    });

    it("is idempotent — running twice does not re-mark already-dead workers", async () => {
      const registry = await params.factory();
      await registry.register({ workerId: "w-1", capabilities: [], concurrency: 1 });
      await wait(120);

      const first = await registry.detectDead(50);
      expect(first).toHaveLength(1);

      const second = await registry.detectDead(50);
      expect(second).toHaveLength(0);
    });

    it("also catches draining workers that stopped heartbeating", async () => {
      const registry = await params.factory();
      await registry.register({ workerId: "w-1", capabilities: [], concurrency: 1 });
      await registry.drain("w-1");
      await wait(120);

      const dead = await registry.detectDead(50);
      expect(dead).toHaveLength(1);
      expect(dead[0]!.workerId).toBe("w-1");
      expect(dead[0]!.status).toBe("dead");
    });

    it("never relabels a retired worker as dead", async () => {
      const registry = await params.factory();
      await registry.register({ workerId: "w-1", capabilities: [], concurrency: 1 });
      await registry.deregister("w-1"); // graceful retire
      await wait(120);

      // A retired worker's heartbeat is stale, but it stopped on purpose.
      const dead = await registry.detectDead(50);
      expect(dead).toHaveLength(0);
      expect(await registry.list({ status: "retired" })).toHaveLength(1);
      expect(await registry.list({ status: "dead" })).toHaveLength(0);
    });
  });

  describe("gc", () => {
    it("reaps a retired worker once retiredAt is older than retainMs", async () => {
      const registry = await params.factory();
      await registry.register({ workerId: "w-1", capabilities: [], concurrency: 1 });
      await registry.deregister("w-1");
      await wait(120);

      const reaped = await registry.gc({ retainMs: 50 });
      expect(reaped).toBe(1);
      expect(await registry.list()).toHaveLength(0);
    });

    it("keeps a retired worker still inside the retention window", async () => {
      const registry = await params.factory();
      await registry.register({ workerId: "w-1", capabilities: [], concurrency: 1 });
      await registry.deregister("w-1");

      const reaped = await registry.gc({ retainMs: 60_000 });
      expect(reaped).toBe(0);
      expect(await registry.list({ status: "retired" })).toHaveLength(1);
    });

    it("reaps a dead worker whose heartbeat is older than retainMs", async () => {
      const registry = await params.factory();
      await registry.register({ workerId: "w-1", capabilities: [], concurrency: 1 });
      await wait(120);
      await registry.detectDead(50); // → dead

      const reaped = await registry.gc({ retainMs: 50 });
      expect(reaped).toBe(1);
      expect(await registry.list()).toHaveLength(0);
    });

    it("keeps a worker with a fresh heartbeat", async () => {
      const registry = await params.factory();
      await registry.register({ workerId: "w-1", capabilities: [], concurrency: 1 });

      const reaped = await registry.gc({ retainMs: 60_000 });
      expect(reaped).toBe(0);
      expect(await registry.list()).toHaveLength(1);
    });
  });
}

async function wait(ms: number): Promise<void> {
  await new Promise((r) => setTimeout(r, ms));
}
