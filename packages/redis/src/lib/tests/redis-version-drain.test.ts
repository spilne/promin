import { versionDrainTestSuite } from "@promin/workflow/testing";
import { RedisWorkflowStorage } from "../redis-workflow-storage.ts";
import { redisDescribe, uniquePrefix } from "./redis-test-utils.ts";

redisDescribe("RedisWorkflowStorage version drain", (redis) => {
  versionDrainTestSuite(
    () => new RedisWorkflowStorage({ redis: redis.client(), prefix: uniquePrefix("drain") }),
  );
});
