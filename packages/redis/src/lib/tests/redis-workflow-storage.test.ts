import { storageTestSuite } from "@promin/workflow/testing";
import type { WorkflowStorage } from "@promin/workflow";
import { RedisWorkflowStorage } from "../redis-workflow-storage.ts";
import { redisDescribe, uniquePrefix } from "./redis-test-utils.ts";

redisDescribe("RedisWorkflowStorage conformance", (redis) => {
  // Peers share the key prefix (the "database") but hold their own
  // connection and instance id — what a second worker process would see.
  const prefixes = new WeakMap<WorkflowStorage, string>();
  storageTestSuite(
    () => {
      const prefix = uniquePrefix("wf");
      const storage = new RedisWorkflowStorage({ redis: redis.client(), prefix });
      prefixes.set(storage, prefix);
      return storage;
    },
    {
      hasJournal: true,
      hasJournaledSuspend: true,
      hasScannerQueries: true,
      hasCompensationLedger: true,
      createPeer: (storage) =>
        new RedisWorkflowStorage({ redis: redis.client(), prefix: prefixes.get(storage)! }),
    },
  );
});
