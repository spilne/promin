import { expect, it } from "bun:test";
import { FakeWallClock } from "@promin/workflow";
import { schedulerTestSuite } from "@promin/workflow/testing";
import { RedisDurableScheduler } from "../redis-durable-scheduler.ts";
import { redisDescribe, uniquePrefix } from "./redis-test-utils.ts";

redisDescribe("RedisDurableScheduler conformance", (redis) => {
  schedulerTestSuite("RedisDurableScheduler", () => {
    const scheduler = new RedisDurableScheduler({
      redis: redis.client(),
      prefix: uniquePrefix("sched"),
      pollIntervalMs: 25,
    });
    return {
      scheduler,
    };
  });
});

redisDescribe("RedisDurableScheduler injected clock", (redis) => {
  it("due ticks and their timestamps follow the injected WallClock", async () => {
    // A fixed past instant: read on the real clock instead, the tick's
    // timestamps would land in the present.
    const t0 = Date.parse("2026-01-01T00:00:00Z");
    const clock = FakeWallClock.create(t0);
    const scheduler = new RedisDurableScheduler({
      redis: redis.client(),
      prefix: uniquePrefix("sched-clock"),
      pollIntervalMs: 1_000,
      clock,
    });
    await scheduler.register({ id: "fake-clock-tick", intervalMs: 10_000 });

    // The first poll's ticks are delivered right away, before any wait.
    const [tick] = await scheduler.stream("fake-clock-tick").take(1).toArray().run();
    expect(clock.pendingCount()).toBe(0);

    expect(tick!.scheduledAt.getTime()).toBe(t0);
    expect(tick!.firedAt.getTime()).toBe(t0);
  }, 10_000);
});
