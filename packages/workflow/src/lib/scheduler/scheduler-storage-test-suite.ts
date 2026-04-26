// ---------------------------------------------------------------------------
// Portable SchedulerStorage conformance suite.
//
// Usage:
//   import { schedulerStorageTestSuite } from "@promin/workflow/testing";
//   schedulerStorageTestSuite(() => new MyCustomSchedulerStorage());
//
// One source of truth for the SchedulerStorage interface contract — every
// backend (InMemory, Postgres, Redis, future SQLite/HTTP) must pass the same
// spec. Mirrors `storageTestSuite` and `stepQueueTestSuite`.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import type { SchedulerStorage } from "./scheduler-storage.ts";

/**
 * Run the full SchedulerStorage conformance suite against any implementation.
 * Verifies CRUD, due-tracking, leader election, multi-namespace dispatch,
 * fire-state monotonicity, and back-compat for the global namespace.
 *
 * The factory is called before each test group, so each describe block sees
 * a fresh storage. Backends with shared mutable state (testcontainers, real
 * Redis) should make sure the factory either truncates or scopes to a
 * unique key prefix.
 */
export function schedulerStorageTestSuite(
  factory: () => SchedulerStorage | Promise<SchedulerStorage>,
) {
  describe("SchedulerStorage conformance", () => {
    const getStorage = async () => await factory();

    describe("upsertSchedule + loadSchedule round-trip", () => {
      it("preserves every field on a minimal schedule", async () => {
        const s = await getStorage();
        await s.upsertSchedule({
          id: "min-1",
          intervalMs: 5_000,
        });
        const loaded = await s.loadSchedule("min-1");
        expect(loaded).not.toBeNull();
        expect(loaded!.id).toBe("min-1");
        expect(loaded!.intervalMs).toBe(5_000);
      });

      it("preserves cron / rrule / namespace / metadata / jitter / startAt / endAt", async () => {
        const s = await getStorage();
        const startAt = new Date("2026-01-01T00:00:00Z");
        const endAt = new Date("2027-01-01T00:00:00Z");
        await s.upsertSchedule({
          id: "full-1",
          name: "Daily report",
          namespace: "tenant-a",
          cron: "0 9 * * *",
          timezone: "America/New_York",
          jitterMs: 5_000,
          enabled: true,
          startAt,
          endAt,
          metadata: { workflowName: "report", tags: ["daily"] },
        });
        const loaded = await s.loadSchedule("full-1");
        expect(loaded!.namespace).toBe("tenant-a");
        expect(loaded!.cron).toBe("0 9 * * *");
        expect(loaded!.timezone).toBe("America/New_York");
        expect(loaded!.jitterMs).toBe(5_000);
        expect(loaded!.startAt?.getTime()).toBe(startAt.getTime());
        expect(loaded!.endAt?.getTime()).toBe(endAt.getTime());
        expect(loaded!.metadata).toEqual({ workflowName: "report", tags: ["daily"] });
      });

      it("upsertSchedule replaces fields on second call (real upsert, not insert-only)", async () => {
        const s = await getStorage();
        await s.upsertSchedule({ id: "u-1", intervalMs: 1_000, enabled: true });
        await s.upsertSchedule({ id: "u-1", intervalMs: 2_000, enabled: false });
        const loaded = await s.loadSchedule("u-1");
        expect(loaded!.intervalMs).toBe(2_000);
        expect(loaded!.enabled).toBe(false);
      });

      it("loadSchedule returns null for unknown ids", async () => {
        const s = await getStorage();
        expect(await s.loadSchedule("does-not-exist")).toBeNull();
      });
    });

    describe("setNextRun + findDue", () => {
      it("returns due ids ordered by nextRun ASC", async () => {
        const s = await getStorage();
        await s.upsertSchedule({ id: "early", intervalMs: 1_000 });
        await s.upsertSchedule({ id: "middle", intervalMs: 1_000 });
        await s.upsertSchedule({ id: "late", intervalMs: 1_000 });

        const now = Date.now();
        await s.setNextRun("late", new Date(now - 100));
        await s.setNextRun("early", new Date(now - 300));
        await s.setNextRun("middle", new Date(now - 200));

        const due = await s.findDue({ now: new Date(now), limit: 10 });
        expect(due).toEqual(["early", "middle", "late"]);
      });

      it("respects the limit param after sorting", async () => {
        const s = await getStorage();
        for (const id of ["a", "b", "c", "d"]) {
          await s.upsertSchedule({ id, intervalMs: 1_000 });
          await s.setNextRun(id, new Date(Date.now() - 100));
        }
        const due = await s.findDue({ now: new Date(), limit: 2 });
        expect(due).toHaveLength(2);
      });

      it("filters by namespace — undefined matches global rows only", async () => {
        const s = await getStorage();
        await s.upsertSchedule({ id: "global", intervalMs: 1_000 });
        await s.upsertSchedule({ id: "tenant-a-job", namespace: "tenant-a", intervalMs: 1_000 });
        await s.upsertSchedule({ id: "tenant-b-job", namespace: "tenant-b", intervalMs: 1_000 });
        const past = new Date(Date.now() - 100);
        await s.setNextRun("global", past);
        await s.setNextRun("tenant-a-job", past);
        await s.setNextRun("tenant-b-job", past);

        const globalDue = await s.findDue({ now: new Date(), limit: 10 });
        expect(globalDue).toEqual(["global"]);
        const aDue = await s.findDue({ now: new Date(), limit: 10, namespace: "tenant-a" });
        expect(aDue).toEqual(["tenant-a-job"]);
      });

      it("setNextRun(null) removes the schedule from due tracking", async () => {
        const s = await getStorage();
        await s.upsertSchedule({ id: "rm", intervalMs: 1_000 });
        await s.setNextRun("rm", new Date(Date.now() - 100));
        expect(await s.findDue({ now: new Date(), limit: 10 })).toContain("rm");
        await s.setNextRun("rm", null);
        expect(await s.findDue({ now: new Date(), limit: 10 })).not.toContain("rm");
      });

      it("findDue does NOT return schedules whose nextRun is in the future", async () => {
        const s = await getStorage();
        await s.upsertSchedule({ id: "later", intervalMs: 1_000 });
        await s.setNextRun("later", new Date(Date.now() + 60_000));
        const due = await s.findDue({ now: new Date(), limit: 10 });
        expect(due).not.toContain("later");
      });
    });

    describe("findDueAcross — multi-namespace single call", () => {
      it("returns rows with their namespace, no per-namespace filter", async () => {
        const s = await getStorage();
        const past = new Date(Date.now() - 100);
        await s.upsertSchedule({ id: "g", intervalMs: 1_000 });
        await s.upsertSchedule({ id: "a", namespace: "tenant-a", intervalMs: 1_000 });
        await s.upsertSchedule({ id: "b", namespace: "tenant-b", intervalMs: 1_000 });
        await s.setNextRun("g", past);
        await s.setNextRun("a", past);
        await s.setNextRun("b", past);

        const all = await s.findDueAcross({ now: new Date(), limit: 10 });
        const byId = new Map(all.map((r) => [r.id, r.namespace]));
        expect(byId.get("g")).toBeUndefined();
        expect(byId.get("a")).toBe("tenant-a");
        expect(byId.get("b")).toBe("tenant-b");
      });

      it("namespaces filter restricts the result set (named + global blend)", async () => {
        const s = await getStorage();
        const past = new Date(Date.now() - 100);
        await s.upsertSchedule({ id: "g", intervalMs: 1_000 });
        await s.upsertSchedule({ id: "a", namespace: "tenant-a", intervalMs: 1_000 });
        await s.upsertSchedule({ id: "b", namespace: "tenant-b", intervalMs: 1_000 });
        await s.setNextRun("g", past);
        await s.setNextRun("a", past);
        await s.setNextRun("b", past);

        const filtered = await s.findDueAcross({
          now: new Date(),
          limit: 10,
          namespaces: [undefined, "tenant-a"],
        });
        const ids = filtered.map((r) => r.id).sort();
        expect(ids).toEqual(["a", "g"]);
      });

      it("idle namespaces (no due rows) cost zero — empty system returns []", async () => {
        const s = await getStorage();
        // Many schedules, none due.
        const future = new Date(Date.now() + 60_000);
        for (let i = 0; i < 20; i++) {
          await s.upsertSchedule({
            id: `idle-${i}`,
            namespace: `tenant-${i}`,
            intervalMs: 60_000,
          });
          await s.setNextRun(`idle-${i}`, future);
        }
        const all = await s.findDueAcross({ now: new Date(), limit: 100 });
        expect(all).toEqual([]);
      });
    });

    describe("commitPoll — batch state advance", () => {
      it("advances firedAt + tickIncrement + nextRun for many ids in one call", async () => {
        const s = await getStorage();
        const past = new Date(Date.now() - 100);
        await s.upsertSchedule({ id: "p1", intervalMs: 1_000 });
        await s.upsertSchedule({ id: "p2", intervalMs: 1_000 });
        await s.setNextRun("p1", past);
        await s.setNextRun("p2", past);

        const fireTime = new Date();
        const nextTime = new Date(Date.now() + 1_000);
        await s.commitPoll([
          { id: "p1", firedAt: fireTime, tickIncrement: 1, nextRun: nextTime },
          { id: "p2", firedAt: fireTime, tickIncrement: 2, nextRun: nextTime },
        ]);

        const states = await s.loadScheduleStates(["p1", "p2"]);
        expect(states.get("p1")?.tickCount).toBe(1);
        expect(states.get("p2")?.tickCount).toBe(2);
      });

      it("nextRun: null clears the schedule from due tracking", async () => {
        const s = await getStorage();
        await s.upsertSchedule({ id: "clr", intervalMs: 1_000 });
        await s.setNextRun("clr", new Date(Date.now() - 100));
        await s.commitPoll([{ id: "clr", nextRun: null }]);
        const due = await s.findDue({ now: new Date(), limit: 10 });
        expect(due).not.toContain("clr");
      });
    });

    describe("recordFire — monotonic tickCount", () => {
      it("each call adds count (default 1) — never decrements", async () => {
        const s = await getStorage();
        await s.upsertSchedule({ id: "mono", intervalMs: 1_000 });
        await s.recordFire("mono", new Date());
        await s.recordFire("mono", new Date());
        await s.recordFire("mono", new Date(), 3);
        const state = await s.loadScheduleState("mono");
        expect(state?.tickCount).toBe(5);
      });
    });

    describe("listSchedules + countSchedules", () => {
      it("filters by enabled and namespace independently and together", async () => {
        const s = await getStorage();
        await s.upsertSchedule({ id: "1", intervalMs: 1, namespace: "x", enabled: true });
        await s.upsertSchedule({ id: "2", intervalMs: 1, namespace: "x", enabled: false });
        await s.upsertSchedule({ id: "3", intervalMs: 1, namespace: "y", enabled: true });

        const enabledX = await s.listSchedules({ enabled: true, namespace: "x" });
        expect(enabledX.map((r) => r.id)).toEqual(["1"]);

        const xAll = await s.listSchedules({ namespace: "x" });
        expect(xAll.map((r) => r.id).sort()).toEqual(["1", "2"]);

        expect(await s.countSchedules({ enabled: true })).toBe(2);
        expect(await s.countSchedules({ namespace: "y" })).toBe(1);
      });

      it("limit + offset compose correctly without overlap", async () => {
        const s = await getStorage();
        for (let i = 0; i < 6; i++) {
          await s.upsertSchedule({ id: `pg-${i}`, intervalMs: 1 });
        }
        const page1 = await s.listSchedules({ limit: 3, offset: 0 });
        const page2 = await s.listSchedules({ limit: 3, offset: 3 });
        const page1Ids = new Set(page1.map((r) => r.id));
        const page2Ids = new Set(page2.map((r) => r.id));
        // No overlap.
        for (const id of page2Ids) expect(page1Ids.has(id)).toBe(false);
        expect(page1.length + page2.length).toBeGreaterThanOrEqual(6);
      });
    });

    describe("setEnabled + deleteSchedule", () => {
      it("setEnabled flips the flag without dropping other fields", async () => {
        const s = await getStorage();
        await s.upsertSchedule({ id: "tog", intervalMs: 5_000, name: "Toggleable", enabled: true });
        await s.setEnabled("tog", false);
        const loaded = await s.loadSchedule("tog");
        expect(loaded!.enabled).toBe(false);
        expect(loaded!.name).toBe("Toggleable");
        expect(loaded!.intervalMs).toBe(5_000);
      });

      it("deleteSchedule removes from CRUD and from due tracking in one call", async () => {
        const s = await getStorage();
        await s.upsertSchedule({ id: "rm-it", intervalMs: 1_000 });
        await s.setNextRun("rm-it", new Date(Date.now() - 100));
        await s.deleteSchedule("rm-it");
        expect(await s.loadSchedule("rm-it")).toBeNull();
        expect(await s.findDue({ now: new Date(), limit: 10 })).not.toContain("rm-it");
      });
    });

    describe("tryAcquireLeader — per-namespace lock", () => {
      // Cross-instance contention ("instance A holds, instance B blocked") is
      // intentionally NOT in this portable suite — backends differ on what
      // "instance" means. Postgres uses pg_try_advisory_lock which is
      // session-scoped (reentrant within one connection); two app instances
      // contend only when they hold separate DB connections. Redis +
      // InMemory key the lock by instanceId so same-storage calls contend
      // even from one process. Each backend tests cross-instance contention
      // in its own integration tests with realistic connection topology.

      it("same instance reacquires successfully (TTL refresh, not contention)", async () => {
        const s = await getStorage();
        const a1 = await s.tryAcquireLeader({
          instanceId: "x",
          namespace: "ns",
          ttlMs: 10_000,
        });
        const a2 = await s.tryAcquireLeader({
          instanceId: "x",
          namespace: "ns",
          ttlMs: 10_000,
        });
        expect(a1).toBe(true);
        expect(a2).toBe(true);
      });

      it("different namespaces have independent locks (one Zorya can lead many tenants)", async () => {
        const s = await getStorage();
        const a = await s.tryAcquireLeader({
          instanceId: "single",
          namespace: "tenant-a",
          ttlMs: 10_000,
        });
        const b = await s.tryAcquireLeader({
          instanceId: "single",
          namespace: "tenant-b",
          ttlMs: 10_000,
        });
        expect(a).toBe(true);
        expect(b).toBe(true);
      });
    });
  });
}
