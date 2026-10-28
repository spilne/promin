import { expect, it } from "bun:test";
import {
  createWorkflowRunner,
  workflow,
  type Workflow,
  type WorkflowStorage,
} from "@promin/workflow";
import { RedisWorkflowStorage } from "../redis-workflow-storage.ts";
import type { RedisStoreClient } from "../redis-client.ts";
import { redisDescribe, uniquePrefix } from "./redis-test-utils.ts";

// ---------------------------------------------------------------------------
// Redis commands a run sends, with and without `checkpointStep`. Every
// command is awaited before the next step starts, so each is a round trip.
// ---------------------------------------------------------------------------

const STEPS = 100;

function chain(n: number): Workflow<number, number> {
  let b: any = workflow<number>({ name: `chain-${n}` });
  for (let i = 0; i < n; i++) {
    b = b.stepAsync(
      `s${i}`,
      async ({ prev, input }: { prev?: number; input: number }) => (prev ?? input) + 1,
    );
  }
  return b.build();
}

/** Wrap a client, counting every command sent through it. */
function counting(client: RedisStoreClient): { client: RedisStoreClient; count: () => number } {
  let commands = 0;
  const proxy = new Proxy(client, {
    get(target, prop) {
      const value = (target as unknown as Record<string | symbol, unknown>)[prop];
      if (typeof value !== "function") return value;
      return (...args: unknown[]) => {
        commands++;
        return (value as (...a: unknown[]) => unknown).apply(target, args);
      };
    },
  });
  return { client: proxy, count: () => commands };
}

/** `storage` without `checkpointStep`: the runner's separate-writes path. */
function withoutCheckpoint(storage: WorkflowStorage): WorkflowStorage {
  return new Proxy(storage, {
    get(target, prop) {
      if (prop === "checkpointStep") return undefined;
      const value = Reflect.get(target, prop, target);
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
}

redisDescribe("RedisWorkflowStorage round trips", (redis) => {
  async function commandsPerRun(params: { checkpoint: boolean }): Promise<number> {
    const { client, count } = counting(redis.client());
    const storage = new RedisWorkflowStorage({ redis: client, prefix: uniquePrefix("rt") });
    const runner = createWorkflowRunner({
      storage: params.checkpoint ? storage : withoutCheckpoint(storage),
    });
    const before = count();
    expect(await runner.run({ workflow: chain(STEPS), workflowId: "chain", input: 0 })).toBe(STEPS);
    return count() - before;
  }

  it(`a ${STEPS}-step chain sends one command per step with checkpointStep`, async () => {
    const split = await commandsPerRun({ checkpoint: false });
    const checkpointed = await commandsPerRun({ checkpoint: true });
    // 408 and 109 at the time of writing: four commands per step (attempt
    // row, step-row read, step-row write, status read) against one, plus
    // the run's lock, load, completion and index commands.
    expect(split).toBeGreaterThanOrEqual(4 * STEPS);
    expect(checkpointed).toBeLessThanOrEqual(STEPS + 10);
  });
});
