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
