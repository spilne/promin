import { expect, it } from "bun:test";
import { RedisWorkflowStorage } from "../redis-workflow-storage.ts";
import type { RedisStoreClient } from "../redis-client.ts";
import { redisDescribe, uniquePrefix } from "./redis-test-utils.ts";

// ---------------------------------------------------------------------------
// Scanner queries at scale: a scan over many suspended runs reads them in
// bounded pages. Redis answers a page from an id-ordered sorted set, so no
// single reply may carry the whole set of suspended ids, and a page costs a
// bounded number of commands however many runs are suspended.
// ---------------------------------------------------------------------------

const SUSPENDED = 1500;
const PAGE = 25;
/** Items one reply may carry: a page of ids plus the fields of one run. */
const MAX_REPLY_ITEMS = 4 * PAGE;

/** Wrap a client, counting commands and the largest array reply any returned. */
function metered(client: RedisStoreClient): {
  client: RedisStoreClient;
  commands: () => number;
  largestReply: () => number;
  reset: () => void;
} {
  let commands = 0;
  let largest = 0;
  const proxy = new Proxy(client, {
    get(target, prop) {
      const value = (target as unknown as Record<string | symbol, unknown>)[prop];
      if (typeof value !== "function") return value;
      return (...args: unknown[]) => {
        commands++;
        const out = (value as (...a: unknown[]) => unknown).apply(target, args);
        if (out instanceof Promise) {
          return out.then((reply) => {
            if (Array.isArray(reply)) largest = Math.max(largest, reply.length);
            return reply;
          });
        }
        return out;
      };
    },
  });
  return {
    client: proxy,
    commands: () => commands,
    largestReply: () => largest,
    reset: () => {
      commands = 0;
      largest = 0;
    },
  };
}

redisDescribe("Redis scanner queries at scale", (redis) => {
  it("pages 1500 suspended runs with bounded replies and per-page commands", async () => {
    const meter = metered(redis.client());
    const storage = new RedisWorkflowStorage({ redis: meter.client, prefix: uniquePrefix("scan") });
    const past = new Date(Date.now() - 60_000);
    const ids = Array.from({ length: SUSPENDED }, (_, i) => `run-${String(i).padStart(5, "0")}`);
    for (let i = 0; i < ids.length; i += 50) {
      await Promise.all(
        ids.slice(i, i + 50).map(async (workflowId) => {
          await storage.createWorkflow({ workflowId, workflowName: "scale", input: {} });
          await storage.suspendWorkflow({
            workflowId,
            stepName: "nap",
            stepUpdate: { status: "sleeping", stepType: "sleep", wakeAt: past },
          });
        }),
      );
    }

    meter.reset();
    const seen: string[] = [];
    let pages = 0;
    let worstPageCommands = 0;
    let after: string | undefined;
    for (;;) {
      const before = meter.commands();
      const page = await storage.listDueTimers({
        now: new Date(),
        limit: PAGE,
        ...(after !== undefined && { afterWorkflowId: after }),
      });
      worstPageCommands = Math.max(worstPageCommands, meter.commands() - before);
      pages++;
      seen.push(...page.map((r) => r.workflowId));
      if (page.length < PAGE) break;
      after = page.at(-1)!.workflowId;
    }

    expect(seen).toEqual(ids);
    expect(pages).toBe(SUSPENDED / PAGE + 1);
    // Never the whole suspended set in one reply.
    expect(meter.largestReply()).toBeLessThanOrEqual(MAX_REPLY_ITEMS);
    // A page costs a bounded number of commands, independent of the set size.
    expect(worstPageCommands).toBeLessThanOrEqual(6 * PAGE);
  }, 120_000);
});
