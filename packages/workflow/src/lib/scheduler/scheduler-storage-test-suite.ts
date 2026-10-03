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
import { Stream } from "@spilne/perfect-core";
import { isTickLogStorage, type SchedulerStorage } from "./scheduler-storage.ts";
import { isStaleLeaseError, schedulerLeaderKey } from "./leader-lease.ts";
import { DurableScheduler, type SchedulerErrorEvent } from "./durable-scheduler.ts";
import type { ScheduleTick } from "./types.ts";

/** Poll `check` in real time until it holds. */
async function waitFor(params: { check: () => boolean | Promise<boolean>; what: string }) {
  const deadline = Date.now() + 15_000;
  while (!(await params.check())) {
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${params.what}`);
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
}

/** Drain a scheduler stream in the background; `stop()` ends it and runs its finalizers. */
function drainInBackground(scheduler: DurableScheduler) {
  const seen: ScheduleTick[] = [];
  let fire: () => void = () => {};
  const signal = Stream.fromCallback<void>((emit) => {
    fire = () => emit(undefined);
  });
  const done = scheduler
    .stream()
    .takeUntil(signal)
    .tap((tick) => void seen.push(tick))
    .drain()
    .run();
  return {
    seen,
    stop: async () => {
      fire();
      await done;
    },
  };
}

/** A view of `storage` whose first `commitPoll` blocks until `resume()`. */
function pauseFirstCommit(storage: SchedulerStorage) {
  let resume!: () => void;
  const gate = new Promise<void>((r) => (resume = r));
  let entered = false;
  const view = new Proxy(storage, {
    get(target, prop) {
      if (prop === "commitPoll") {
        return async (params: Parameters<SchedulerStorage["commitPoll"]>[0]) => {
          if (!entered) {
            entered = true;
            await gate;
          }
          return target.commitPoll(params);
        };
      }
      const value = Reflect.get(target, prop, target);
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
  return { view, resume, entered: () => entered };
}

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
      it("upsertSchedule seeds nextRun on insert (enabled) so findDue finds it without an explicit setNextRun call", async () => {
        const s = await getStorage();
        await s.upsertSchedule({ id: "auto-seeded", intervalMs: 1_000 });
        // No explicit setNextRun — the storage must seed nextRun = now on INSERT.
        const due = await s.findDue({ now: new Date(), limit: 10 });
        expect(due).toContain("auto-seeded");
      });

      it("upsertSchedule does NOT seed nextRun when inserting a disabled schedule", async () => {
        const s = await getStorage();
        await s.upsertSchedule({ id: "paused", intervalMs: 1_000, enabled: false });
        // Disabled insert must NOT put the schedule into due-tracking —
        // re-enabling it later will call setNextRun explicitly.
        const due = await s.findDue({ now: new Date(), limit: 10 });
        expect(due).not.toContain("paused");
      });

      it("upsertSchedule (update) does not overwrite an existing nextRun", async () => {
        const s = await getStorage();
        await s.upsertSchedule({ id: "stable", intervalMs: 1_000 });
        const future = new Date(Date.now() + 60_000);
        await s.setNextRun("stable", future);
        // Re-upsert (update path) must not reset nextRun back to now.
        await s.upsertSchedule({ id: "stable", intervalMs: 2_000 });
        const due = await s.findDue({ now: new Date(), limit: 10 });
        expect(due).not.toContain("stable");
      });

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
        await s.commitPoll({
          updates: [
            { id: "p1", firedAt: fireTime, tickIncrement: 1, nextRun: nextTime },
            { id: "p2", firedAt: fireTime, tickIncrement: 2, nextRun: nextTime },
          ],
        });

        const states = await s.loadScheduleStates(["p1", "p2"]);
        expect(states.get("p1")?.tickCount).toBe(1);
        expect(states.get("p2")?.tickCount).toBe(2);
      });

      it("nextRun: null clears the schedule from due tracking", async () => {
        const s = await getStorage();
        await s.upsertSchedule({ id: "clr", intervalMs: 1_000 });
        await s.setNextRun("clr", new Date(Date.now() - 100));
        await s.commitPoll({ updates: [{ id: "clr", nextRun: null }] });
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

      it("metadata filter — top-level key match (containment, not strict equality)", async () => {
        const s = await getStorage();
        await s.upsertSchedule({
          id: "a",
          intervalMs: 1,
          metadata: { kind: "alpha", extra: "ignored" },
        });
        await s.upsertSchedule({ id: "b", intervalMs: 1, metadata: { kind: "beta" } });
        await s.upsertSchedule({ id: "c", intervalMs: 1, metadata: undefined });

        const alphas = await s.listSchedules({ metadata: { kind: "alpha" } });
        expect(alphas.map((r) => r.id)).toEqual(["a"]);
        expect(await s.countSchedules({ metadata: { kind: "alpha" } })).toBe(1);
      });

      it("metadata filter — nested path match (the agent-schedule shape)", async () => {
        // Mirrors what the durable scheduler tool stamps:
        // metadata.target = { type: "agent", agentId, threadId, ... }.
        // The dashboard's "kind = agent" filter / chat per-thread drawer
        // both want this nested-path lookup to match without scanning.
        const s = await getStorage();
        await s.upsertSchedule({
          id: "agent-1",
          intervalMs: 1,
          metadata: {
            target: { type: "agent", agentId: "writer", threadId: "t1", task: "x" },
          },
        });
        await s.upsertSchedule({
          id: "agent-2",
          intervalMs: 1,
          metadata: {
            target: { type: "agent", agentId: "writer", threadId: "t2", task: "y" },
          },
        });
        await s.upsertSchedule({
          id: "wf-1",
          intervalMs: 1,
          metadata: { target: { type: "workflow", name: "send-email" } },
        });

        // Filter by target.type — both agent rows, no workflow row.
        const agents = await s.listSchedules({ metadata: { target: { type: "agent" } } });
        expect(agents.map((r) => r.id).sort()).toEqual(["agent-1", "agent-2"]);

        // Compound filter — type + threadId — narrows to one row.
        const t1 = await s.listSchedules({
          metadata: { target: { type: "agent", threadId: "t1" } },
        });
        expect(t1.map((r) => r.id)).toEqual(["agent-1"]);

        expect(await s.countSchedules({ metadata: { target: { type: "agent" } } })).toBe(2);
      });

      it("metadata filter composes with namespace + enabled", async () => {
        const s = await getStorage();
        await s.upsertSchedule({
          id: "x-on",
          intervalMs: 1,
          namespace: "x",
          enabled: true,
          metadata: { kind: "alpha" },
        });
        await s.upsertSchedule({
          id: "x-off",
          intervalMs: 1,
          namespace: "x",
          enabled: false,
          metadata: { kind: "alpha" },
        });
        await s.upsertSchedule({
          id: "y-on",
          intervalMs: 1,
          namespace: "y",
          enabled: true,
          metadata: { kind: "alpha" },
        });

        const result = await s.listSchedules({
          namespace: "x",
          enabled: true,
          metadata: { kind: "alpha" },
        });
        expect(result.map((r) => r.id)).toEqual(["x-on"]);
      });

      it("metadata filter — extra keys in row's metadata don't break the match", async () => {
        // Containment semantics: the filter only needs to be a SUBSET of
        // the row's metadata. This is the key difference from strict
        // deep-equality and is what makes nested-path filtering useful.
        const s = await getStorage();
        await s.upsertSchedule({
          id: "agent-rich",
          intervalMs: 1,
          metadata: {
            target: { type: "agent", agentId: "writer", threadId: "t1", task: "x" },
            createdByAgent: "writer",
            scheduleId: "agent-rich",
          },
        });
        const matched = await s.listSchedules({
          metadata: { target: { type: "agent" } },
        });
        expect(matched.map((r) => r.id)).toEqual(["agent-rich"]);
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

    describe("disabled schedules stay out of due-tracking", () => {
      const past = () => new Date(Date.now() - 1_000);

      it("findDue and findDueAcross never return a disabled schedule, even with a due nextRun", async () => {
        const s = await getStorage();
        await s.upsertSchedule({ id: "off", intervalMs: 1_000 });
        await s.setEnabled("off", false);
        // A stale nextRun written after the pause (e.g. by a poll that loaded
        // the schedule just before it was paused) must still not surface it.
        await s.setNextRun("off", past());
        expect(await s.findDue({ now: new Date(), limit: 10 })).not.toContain("off");
        const across = await s.findDueAcross({ now: new Date(), limit: 10 });
        expect(across.map((r) => r.id)).not.toContain("off");
      });

      it("paused schedules don't fill the limit and starve active ones", async () => {
        const s = await getStorage();
        for (let i = 0; i < 5; i++) {
          await s.upsertSchedule({ id: `paused-${i}`, intervalMs: 60_000 });
          await s.setNextRun(`paused-${i}`, new Date(Date.now() - 10_000 + i));
          await s.setEnabled(`paused-${i}`, false);
        }
        await s.upsertSchedule({ id: "active", intervalMs: 60_000 });
        await s.setNextRun("active", past());

        expect(await s.findDue({ now: new Date(), limit: 5 })).toEqual(["active"]);
        const across = await s.findDueAcross({ now: new Date(), limit: 5 });
        expect(across.map((r) => r.id)).toEqual(["active"]);
      });

      it("commitPoll does not put a schedule paused since the poll back into due-tracking", async () => {
        const s = await getStorage();
        await s.upsertSchedule({ id: "race", intervalMs: 1_000 });
        await s.setEnabled("race", false);
        await s.commitPoll({ updates: [{ id: "race", nextRun: past() }] });
        expect(await s.findDue({ now: new Date(), limit: 10 })).not.toContain("race");
      });

      it("setEnabled(false) clears nextRun; setEnabled(true) seeds it again at now", async () => {
        const s = await getStorage();
        await s.upsertSchedule({ id: "cycle", intervalMs: 1_000 });
        await s.setNextRun("cycle", new Date(Date.now() + 60_000));
        await s.setEnabled("cycle", false);
        await s.setEnabled("cycle", true);
        // The stale future nextRun was dropped on pause, so resume seeds "now".
        expect(await s.findDue({ now: new Date(Date.now() + 1_000), limit: 10 })).toContain(
          "cycle",
        );
      });

      it("setEnabled(true) on an already-enabled schedule keeps its nextRun", async () => {
        const s = await getStorage();
        await s.upsertSchedule({ id: "keep", intervalMs: 1_000 });
        await s.setNextRun("keep", new Date(Date.now() + 60_000));
        await s.setEnabled("keep", true);
        expect(await s.findDue({ now: new Date(Date.now() + 1_000), limit: 10 })).not.toContain(
          "keep",
        );
      });

      it("upsert with enabled: false drops it from due-tracking; enabling via upsert seeds it", async () => {
        const s = await getStorage();
        await s.upsertSchedule({ id: "up", intervalMs: 1_000 });
        await s.setNextRun("up", past());
        await s.upsertSchedule({ id: "up", intervalMs: 1_000, enabled: false });
        expect(await s.findDue({ now: new Date(), limit: 10 })).not.toContain("up");

        await s.upsertSchedule({ id: "up", intervalMs: 1_000, enabled: true });
        expect(await s.findDue({ now: new Date(Date.now() + 1_000), limit: 10 })).toContain("up");
      });

      it("setEnabled on an unknown id is a no-op", async () => {
        const s = await getStorage();
        await s.setEnabled("ghost", true);
        expect(await s.loadSchedule("ghost")).toBeNull();
        expect(await s.findDue({ now: new Date(Date.now() + 1_000), limit: 10 })).not.toContain(
          "ghost",
        );
      });
    });

    describe("leader leases", () => {
      // Leases are keyed by instanceId, not by connection or process, so one
      // storage object can stand in for several instances. Expiry uses the
      // backend's own clock (the server clock for Postgres and Redis), so
      // the TTL cases wait in real time; the waits are lower bounds only.
      const key = schedulerLeaderKey({ namespace: "ns" });
      const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

      it("a live lease excludes other instances; the holder refreshes it under the same epoch", async () => {
        const s = await getStorage();
        const a1 = await s.tryAcquireLeader({ key, instanceId: "a", ttlMs: 30_000 });
        const b = await s.tryAcquireLeader({ key, instanceId: "b", ttlMs: 30_000 });
        const a2 = await s.tryAcquireLeader({ key, instanceId: "a", ttlMs: 30_000 });

        expect(a1).toEqual({ key, instanceId: "a", epoch: expect.any(Number) });
        expect(b).toBeNull();
        expect(a2).toEqual(a1);
      });

      it("only one of many concurrent acquirers wins", async () => {
        const s = await getStorage();
        const results = await Promise.all(
          Array.from({ length: 10 }, (_, i) =>
            s.tryAcquireLeader({ key, instanceId: `racer-${i}`, ttlMs: 30_000 }),
          ),
        );
        expect(results.filter((r) => r !== null)).toHaveLength(1);
      });

      it("after the TTL another instance takes over with a higher epoch", async () => {
        const s = await getStorage();
        const a = await s.tryAcquireLeader({ key, instanceId: "a", ttlMs: 150 });
        await sleep(400);
        const b = await s.tryAcquireLeader({ key, instanceId: "b", ttlMs: 30_000 });

        expect(b).not.toBeNull();
        expect(b!.epoch).toBeGreaterThan(a!.epoch);
        expect(await s.tryAcquireLeader({ key, instanceId: "a", ttlMs: 30_000 })).toBeNull();
      });

      it("re-taking an expired lease starts a new epoch, even for the same holder", async () => {
        const s = await getStorage();
        const first = await s.tryAcquireLeader({ key, instanceId: "a", ttlMs: 150 });
        await sleep(400);
        const again = await s.tryAcquireLeader({ key, instanceId: "a", ttlMs: 30_000 });
        expect(again!.epoch).toBeGreaterThan(first!.epoch);
      });

      it("releaseLeader hands over immediately; releasing a stale lease is a no-op", async () => {
        const s = await getStorage();
        const a = await s.tryAcquireLeader({ key, instanceId: "a", ttlMs: 60_000 });
        await s.releaseLeader({ lease: a! });
        const b = await s.tryAcquireLeader({ key, instanceId: "b", ttlMs: 60_000 });
        expect(b).not.toBeNull();
        expect(b!.epoch).toBeGreaterThan(a!.epoch);

        await s.releaseLeader({ lease: a! });
        expect(await s.tryAcquireLeader({ key, instanceId: "c", ttlMs: 60_000 })).toBeNull();
      });

      it("namespaces and partitions have independent leases", async () => {
        const s = await getStorage();
        const keys = [
          schedulerLeaderKey({}),
          schedulerLeaderKey({ namespace: "tenant-a" }),
          schedulerLeaderKey({ namespace: "tenant-b" }),
          schedulerLeaderKey({ namespace: "tenant-a", partition: { index: 0, count: 2 } }),
          schedulerLeaderKey({ namespace: "tenant-a", partition: { index: 1, count: 2 } }),
        ];
        expect(new Set(keys).size).toBe(keys.length);
        const leases = await Promise.all(
          keys.map((k, i) => s.tryAcquireLeader({ key: k, instanceId: `i-${i}`, ttlMs: 30_000 })),
        );
        expect(leases.every((l) => l !== null)).toBe(true);
        // One instance can also lead many keys at once.
        const second = await s.tryAcquireLeader({
          key: schedulerLeaderKey({ namespace: "tenant-c" }),
          instanceId: "i-1",
          ttlMs: 30_000,
        });
        expect(second).not.toBeNull();
      });

      it("a stale lease can't commit — nothing is written — while the current lease can", async () => {
        const s = await getStorage();
        await s.upsertSchedule({ id: "fenced", intervalMs: 1_000 });
        const before = await s.findDue({ now: new Date(Date.now() + 60_000), limit: 10 });
        const old = await s.tryAcquireLeader({ key, instanceId: "a", ttlMs: 60_000 });
        await s.releaseLeader({ lease: old! });
        const current = await s.tryAcquireLeader({ key, instanceId: "b", ttlMs: 60_000 });

        const stale = await s
          .commitPoll({
            updates: [
              {
                id: "fenced",
                firedAt: new Date(1_000),
                tickIncrement: 1,
                expectedTickCount: 0,
                nextRun: null,
              },
            ],
            lease: old!,
          })
          .then(
            () => null,
            (error: unknown) => error,
          );
        expect(isStaleLeaseError(stale)).toBe(true);
        expect(await s.loadScheduleState("fenced")).toEqual({ lastFired: null, tickCount: 0 });
        expect(await s.findDue({ now: new Date(Date.now() + 60_000), limit: 10 })).toEqual(before);

        const ok = await s.commitPoll({
          updates: [
            {
              id: "fenced",
              firedAt: new Date(1_000),
              tickIncrement: 1,
              expectedTickCount: 0,
              nextRun: new Date(2_000),
            },
          ],
          lease: current!,
        });
        expect(ok.conflicts).toEqual([]);
        expect(await s.loadScheduleState("fenced")).toEqual({
          lastFired: new Date(1_000),
          tickCount: 1,
        });
      });

      it("a lease from another key can't commit", async () => {
        const s = await getStorage();
        await s.upsertSchedule({ id: "x", intervalMs: 1_000 });
        const other = await s.tryAcquireLeader({
          key: schedulerLeaderKey({ namespace: "other" }),
          instanceId: "a",
          ttlMs: 60_000,
        });
        // Forge a lease on `key` that was never granted.
        const forged = { ...other!, key };
        const result = await s
          .commitPoll({ updates: [{ id: "x", nextRun: null }], lease: forged })
          .then(
            () => null,
            (error: unknown) => error,
          );
        expect(isStaleLeaseError(result)).toBe(true);
      });
    });

    describe("leader failover — two DurableSchedulers", () => {
      it("a leader paused in its commit past the TTL can't commit: every tick number keeps one occurrence", async () => {
        const s = await getStorage();
        const intervalMs = 100;
        // Deep catch-up makes numbering deterministic: tick k is base + k × interval.
        await s.upsertSchedule({ id: "failover", intervalMs, maxCatchUp: 10_000 });
        const base = Date.now() - 1_000;
        await s.recordFire("failover", new Date(base));

        const paused = pauseFirstCommit(s);
        const errorsA: SchedulerErrorEvent[] = [];
        const common = { pollIntervalMs: 50, leaderLockTtlMs: 300, onError: () => {} };
        const a = new DurableScheduler({
          ...common,
          storage: paused.view,
          instanceId: "A",
          onError: (e) => void errorsA.push(e),
        });
        const b = new DurableScheduler({ ...common, storage: s, instanceId: "B" });

        const runA = drainInBackground(a);
        await waitFor({ check: () => paused.entered(), what: "A to deliver and start committing" });
        const runB = drainInBackground(b);
        // B takes over once A's lease lapses, and commits.
        await waitFor({
          check: async () => (await s.loadScheduleState("failover"))!.tickCount > 1,
          what: "B to take over and commit",
        });

        paused.resume();
        await waitFor({ check: () => errorsA.length > 0, what: "A's commit to be rejected" });
        expect(errorsA[0]!.phase).toBe("commit");
        expect(isStaleLeaseError(errorsA[0]!.error)).toBe(true);

        // Let B poll a few more times after A's rejected commit.
        const after = (await s.loadScheduleState("failover"))!.tickCount;
        await waitFor({
          check: async () => (await s.loadScheduleState("failover"))!.tickCount >= after + 2,
          what: "B to keep firing",
        });
        await runA.stop();
        await runB.stop();

        // A's ticks are redelivered by B under the same numbers (at least
        // once). Beyond that: one occurrence per number, numbers without
        // gaps, occurrences moving forward with the number, and a tickCount
        // that A's stale commit didn't inflate.
        expect(runA.seen.length).toBeGreaterThan(0);
        const byNumber = new Map<number, number>();
        for (const t of [...runA.seen, ...runB.seen]) {
          const at = t.scheduledAt.getTime();
          expect(byNumber.get(t.tickNumber) ?? at).toBe(at);
          byNumber.set(t.tickNumber, at);
        }
        const numbers = [...byNumber.keys()].sort((x, y) => x - y);
        expect(numbers).toEqual(numbers.map((_, i) => i + 1));
        for (let i = 1; i < numbers.length; i++) {
          expect(byNumber.get(numbers[i]!)!).toBeGreaterThan(byNumber.get(numbers[i - 1]!)!);
        }
        expect(new Set(runB.seen.map((t) => t.tickNumber)).size).toBe(runB.seen.length);
        expect((await s.loadScheduleState("failover"))!.tickCount).toBe(numbers.length + 1);
      }, 30_000);
    });

    describe("commitPoll — compare-and-set on tickCount", () => {
      it("skips and reports entries whose expectedTickCount no longer matches; applies the rest", async () => {
        const s = await getStorage();
        await s.upsertSchedule({ id: "moved", intervalMs: 1_000 });
        await s.upsertSchedule({ id: "fresh", intervalMs: 1_000 });
        await s.recordFire("moved", new Date(500));

        const result = await s.commitPoll({
          updates: [
            {
              id: "moved",
              firedAt: new Date(1_000),
              tickIncrement: 1,
              expectedTickCount: 0,
              nextRun: null,
            },
            {
              id: "fresh",
              firedAt: new Date(1_000),
              tickIncrement: 2,
              expectedTickCount: 0,
              nextRun: new Date(5_000),
            },
            { id: "gone", firedAt: new Date(1_000), tickIncrement: 1, expectedTickCount: 0 },
          ],
        });

        expect([...result.conflicts].sort()).toEqual(["gone", "moved"]);
        expect(await s.loadScheduleState("moved")).toEqual({
          lastFired: new Date(500),
          tickCount: 1,
        });
        expect(await s.loadScheduleState("fresh")).toEqual({
          lastFired: new Date(1_000),
          tickCount: 2,
        });
        // The skipped entry's `nextRun: null` was not applied either.
        expect(await s.findDue({ now: new Date(Date.now() + 60_000), limit: 10 })).toContain(
          "moved",
        );
      });

      it("an entry without nextRun leaves nextRun as it is", async () => {
        const s = await getStorage();
        await s.upsertSchedule({ id: "keep", intervalMs: 1_000 });
        await s.setNextRun("keep", new Date(10_000));
        await s.commitPoll({
          updates: [{ id: "keep", firedAt: new Date(1_000), tickIncrement: 1 }],
        });
        expect(await s.findDue({ now: new Date(9_999), limit: 10 })).toEqual([]);
        expect(await s.findDue({ now: new Date(10_000), limit: 10 })).toEqual(["keep"]);
        expect((await s.loadScheduleState("keep"))?.tickCount).toBe(1);
      });
    });

    // Optional: tick log capability. Backends declare support by exposing
    // `listTicks` + `countTicks`; the suite skips itself otherwise so a
    // backend without the capability still passes the rest of the spec.
    describe("tick log (when supported)", () => {
      it("commitPoll persists ticks atomically with state advance", async () => {
        const s = await getStorage();
        if (!isTickLogStorage(s)) return;
        await s.upsertSchedule({ id: "log-1", intervalMs: 1_000 });
        const t0: ScheduleTick = {
          scheduleId: "log-1",
          tickNumber: 0,
          scheduledAt: new Date(1_000_000),
          firedAt: new Date(1_000_005),
        };
        const t1: ScheduleTick = {
          scheduleId: "log-1",
          tickNumber: 1,
          scheduledAt: new Date(2_000_000),
          firedAt: new Date(2_000_010),
          metadata: { foo: "bar" },
        };
        await s.commitPoll({
          updates: [
            {
              id: "log-1",
              firedAt: t1.firedAt,
              tickIncrement: 2,
              nextRun: new Date(3_000_000),
              ticks: [t0, t1],
            },
          ],
        });
        const ticks = await s.listTicks({ scheduleId: "log-1" });
        expect(ticks.length).toBe(2);
        // Newest-first by firedAt.
        expect(ticks[0]?.tickNumber).toBe(1);
        expect(ticks[1]?.tickNumber).toBe(0);
        expect(ticks[0]?.metadata).toEqual({ foo: "bar" });
        // tickCount and the count of logged rows match.
        const state = await s.loadScheduleState("log-1");
        const total = await s.countTicks({ scheduleId: "log-1" });
        expect(state?.tickCount).toBe(2);
        expect(total).toBe(2);
      });

      it("listTicks paginates by limit + offset", async () => {
        const s = await getStorage();
        if (!isTickLogStorage(s)) return;
        await s.upsertSchedule({ id: "page", intervalMs: 1_000 });
        const ticks: ScheduleTick[] = Array.from({ length: 25 }, (_, i) => ({
          scheduleId: "page",
          tickNumber: i,
          scheduledAt: new Date(i * 1000),
          firedAt: new Date(i * 1000 + 1),
        }));
        await s.commitPoll({
          updates: [
            { id: "page", firedAt: ticks[24]!.firedAt, tickIncrement: 25, nextRun: null, ticks },
          ],
        });
        const page1 = await s.listTicks({ scheduleId: "page", limit: 10, offset: 0 });
        const page2 = await s.listTicks({ scheduleId: "page", limit: 10, offset: 10 });
        expect(page1[0]?.tickNumber).toBe(24);
        expect(page1[9]?.tickNumber).toBe(15);
        expect(page2[0]?.tickNumber).toBe(14);
        expect(page2[9]?.tickNumber).toBe(5);
        expect(await s.countTicks({ scheduleId: "page" })).toBe(25);
      });

      it("retried commitPoll doesn't double-log (PK on (scheduleId, tickNumber))", async () => {
        const s = await getStorage();
        if (!isTickLogStorage(s)) return;
        await s.upsertSchedule({ id: "retry", intervalMs: 1_000 });
        const t: ScheduleTick = {
          scheduleId: "retry",
          tickNumber: 0,
          scheduledAt: new Date(1_000),
          firedAt: new Date(1_001),
        };
        await s.commitPoll({
          updates: [
            { id: "retry", firedAt: t.firedAt, tickIncrement: 1, nextRun: null, ticks: [t] },
          ],
        });
        // Same tick replayed (e.g., retry after a transient failure). Must
        // be a no-op on the log even if state advances by another count.
        await s.commitPoll({
          updates: [
            { id: "retry", firedAt: t.firedAt, tickIncrement: 0, nextRun: null, ticks: [t] },
          ],
        });
        expect(await s.countTicks({ scheduleId: "retry" })).toBe(1);
      });

      it("deleteSchedule drops the tick log", async () => {
        const s = await getStorage();
        if (!isTickLogStorage(s)) return;
        await s.upsertSchedule({ id: "drop", intervalMs: 1_000 });
        const t: ScheduleTick = {
          scheduleId: "drop",
          tickNumber: 0,
          scheduledAt: new Date(1_000),
          firedAt: new Date(1_001),
        };
        await s.commitPoll({
          updates: [
            { id: "drop", firedAt: t.firedAt, tickIncrement: 1, nextRun: null, ticks: [t] },
          ],
        });
        await s.deleteSchedule("drop");
        expect(await s.countTicks({ scheduleId: "drop" })).toBe(0);
      });
    });
  });
}
