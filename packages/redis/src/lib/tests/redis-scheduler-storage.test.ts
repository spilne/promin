import { describe, expect, it } from "bun:test";
import { FakeWallClock } from "@promin/workflow";
import { schedulerStorageTestSuite } from "@promin/workflow/testing";
import { RedisSchedulerStorage } from "../redis-scheduler-storage.ts";
import { redisDescribe, uniquePrefix } from "./redis-test-utils.ts";

redisDescribe("RedisSchedulerStorage", (redis) => {
  schedulerStorageTestSuite(
    () => new RedisSchedulerStorage({ redis: redis.client(), prefix: uniquePrefix("sched") }),
  );

  describe("upsertSchedule", () => {
    const T0 = Date.parse("2026-01-01T00:00:00.000Z");
    const make = () =>
      new RedisSchedulerStorage({
        redis: redis.client(),
        prefix: uniquePrefix("sched"),
        clock: FakeWallClock.create(T0),
      });

    it("keeps fire state across a replace", async () => {
      const s = make();
      await s.upsertSchedule({ id: "a", intervalMs: 1_000 });
      await s.recordFire("a", new Date(T0), 3);
      await s.upsertSchedule({ id: "a", intervalMs: 2_000 });

      expect(await s.loadScheduleState("a")).toEqual({ lastFired: new Date(T0), tickCount: 3 });
      expect((await s.loadSchedule("a"))?.intervalMs).toBe(2_000);
    });

    it("drops fields removed by a replace", async () => {
      const s = make();
      await s.upsertSchedule({ id: "a", intervalMs: 1_000, name: "old", metadata: { k: 1 } });
      await s.upsertSchedule({ id: "a", intervalMs: 1_000 });
      const cfg = await s.loadSchedule("a");
      expect(cfg?.name).toBeUndefined();
      expect(cfg?.metadata).toBeUndefined();
    });

    it("seeds a new schedule at startAt when that is in the future", async () => {
      const s = make();
      await s.upsertSchedule({ id: "later", intervalMs: 1_000, startAt: new Date(T0 + 60_000) });

      expect(await s.findDue({ now: new Date(T0), limit: 10 })).toEqual([]);
      expect(await s.findDue({ now: new Date(T0 + 60_000), limit: 10 })).toEqual(["later"]);
    });

    it("moves a schedule and its pending next run when the namespace changes", async () => {
      const s = make();
      await s.upsertSchedule({ id: "m", intervalMs: 1_000, namespace: "x" });
      await s.setNextRun("m", new Date(T0 + 5_000));
      await s.upsertSchedule({ id: "m", intervalMs: 1_000, namespace: "y" });

      expect(await s.countSchedules({ namespace: "x" })).toBe(0);
      expect(await s.countSchedules({ namespace: "y" })).toBe(1);
      expect(await s.findDue({ now: new Date(T0 + 5_000), limit: 10, namespace: "x" })).toEqual([]);
      expect(await s.findDue({ now: new Date(T0 + 5_000), limit: 10, namespace: "y" })).toEqual([
        "m",
      ]);
    });
  });

  describe("due-set leftovers", () => {
    it("findDue skips and prunes due-set entries for disabled or deleted schedules", async () => {
      const prefix = uniquePrefix("sched");
      const client = redis.client();
      const s = new RedisSchedulerStorage({
        redis: client,
        prefix,
        clock: FakeWallClock.create(0),
      });
      for (let i = 0; i < 3; i++) {
        await s.upsertSchedule({ id: `off-${i}`, intervalMs: 1_000, enabled: false });
      }
      await s.upsertSchedule({ id: "on", intervalMs: 1_000 });
      // Due-set entries left behind by an older writer, sorting before "on".
      const dueKey = `{${prefix}}:ns:_:due`;
      for (let i = 0; i < 3; i++) await client.zadd(dueKey, -10 + i, `off-${i}`);
      await client.zadd(dueKey, -20, "deleted");

      expect(await s.findDue({ now: new Date(0), limit: 2 })).toEqual(["on"]);
      expect(await client.zrangebyscore(dueKey, "-inf", "+inf")).toEqual(["on"]);
    });
  });
});
