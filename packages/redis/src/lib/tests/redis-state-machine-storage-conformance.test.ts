import { stateMachineStorageTestSuite } from "@promin/workflow/testing";
import { RedisStateMachineStorage } from "../redis-state-machine-storage.ts";
import { redisDescribe, uniquePrefix } from "./redis-test-utils.ts";

redisDescribe("RedisStateMachineStorage conformance", (ctx) => {
  // A fresh prefix per test; the peer shares it but holds its own lock tokens.
  let prefix = uniquePrefix("sm-conf");
  stateMachineStorageTestSuite(
    () => {
      prefix = uniquePrefix("sm-conf");
      return new RedisStateMachineStorage({ redis: ctx.client(), prefix });
    },
    { createPeer: () => new RedisStateMachineStorage({ redis: ctx.client(), prefix }) },
  );
});
