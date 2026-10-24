import { zombieWorkerTestSuite } from "@promin/workflow/testing";
import type { WorkflowStorage } from "@promin/workflow";
import { RedisWorkflowStorage } from "../redis-workflow-storage.ts";
import { redisDescribe, uniquePrefix } from "./redis-test-utils.ts";

redisDescribe("RedisWorkflowStorage zombie worker", (redis) => {
  // Two storages on one key prefix, each with its own connection: two
  // workers. The stalled holder's lease runs out when its lock key goes,
  // which is what the key's TTL does at expiry.
  const prefixes = new WeakMap<WorkflowStorage, string>();
  zombieWorkerTestSuite({
    createStorage: () => {
      const prefix = uniquePrefix("wf");
      const storage = new RedisWorkflowStorage({ redis: redis.client(), prefix });
      prefixes.set(storage, prefix);
      return storage;
    },
    createPeer: (storage) =>
      new RedisWorkflowStorage({ redis: redis.client(), prefix: prefixes.get(storage)! }),
    expireLock: async ({ storage, workflowId }) => {
      await redis.client().del(`${prefixes.get(storage)!}:{wf:${workflowId}}:lock`);
    },
  });
});
