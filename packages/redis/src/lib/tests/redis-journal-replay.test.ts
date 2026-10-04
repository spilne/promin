import { journalReplayTestSuite } from "@promin/workflow/testing";
import { RedisWorkflowStorage } from "../redis-workflow-storage.ts";
import { redisDescribe, uniquePrefix } from "./redis-test-utils.ts";

// Journal replay conformance: branch paths written by ctx.parallel
// round-trip through the Redis journal keys, sleep index members and signal
// lookup hash.
redisDescribe("RedisWorkflowStorage journal replay", (redis) => {
  journalReplayTestSuite(
    () => new RedisWorkflowStorage({ redis: redis.client(), prefix: uniquePrefix("jr") }),
    { timingRuns: 10 },
  );
});
