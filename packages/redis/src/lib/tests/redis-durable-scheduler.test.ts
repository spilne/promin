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
      register: (config) => scheduler.registerAsync(config),
      unregister: (id, options) => scheduler.unregisterAsync(id, options),
      pause: (id) => scheduler.pauseAsync(id),
      resume: (id) => scheduler.resumeAsync(id),
      list: async () => scheduler.listAsync(),
    };
  });
});

/** Poll a condition on real time until it holds (or give up after 5s). */
async function waitFor(condition: () => boolean): Promise<void> {
  const deadline = Date.now() + 5_000;
  while (!condition()) {
    if (Date.now() > deadline) throw new Error("waitFor: condition not met within 5s");
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}

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
    await scheduler.registerAsync({ id: "fake-clock-tick", intervalMs: 10_000 });

    const result = scheduler.stream("fake-clock-tick").take(1).toArray().run();
    // The first poll ends by parking on the clock's interval timer.
    await waitFor(() => clock.pendingCount() > 0);
    clock.advance(1_000);
    const [tick] = await result;

    expect(tick!.scheduledAt.getTime()).toBe(t0);
    expect(tick!.firedAt.getTime()).toBe(t0);
  }, 10_000);
});
