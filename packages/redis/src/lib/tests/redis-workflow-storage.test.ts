import { storageTestSuite } from "@promin/workflow/testing";
import { RedisWorkflowStorage } from "../redis-workflow-storage.ts";
import { redisDescribe, uniquePrefix } from "./redis-test-utils.ts";

redisDescribe("RedisWorkflowStorage conformance", (redis) => {
  storageTestSuite(
    () =>
      new RedisWorkflowStorage({
        redis: redis.client(),
        prefix: uniquePrefix("wf"),
      }),
    { hasJournal: true, hasJournaledSuspend: true },
  );
});
