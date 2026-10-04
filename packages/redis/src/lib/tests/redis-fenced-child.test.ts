import { describe, expect, it } from "bun:test";
import { RedisWorkflowStorage } from "../redis-workflow-storage.ts";
import type { RedisStoreClient } from "../redis-client.ts";
import { redisDescribe, uniquePrefix } from "./redis-test-utils.ts";

// ---------------------------------------------------------------------------
// A fenced child create commits through the parent's child intents: the
// child's row is provisional until one fenced script on the parent's slot
// records it. These tests stop a create between its steps.
// ---------------------------------------------------------------------------

type Stage = "intent" | "confirm";

/** Wrap a client so `hooks[stage]` runs before the script of that stage. */
function interceptable(client: RedisStoreClient): {
  client: RedisStoreClient;
  hooks: Partial<Record<Stage, () => Promise<void>>>;
} {
  const hooks: Partial<Record<Stage, () => Promise<void>>> = {};
  const stageOf = (keys: readonly string[], args: readonly string[]): Stage | undefined => {
    if (keys.some((k) => k.endsWith(":child-intents"))) return "intent";
    const ops = args.join(" ");
    if (ops.includes('"HDEL"') && ops.includes('"provisional"]')) return "confirm";
    return undefined;
  };
  const proxy = new Proxy(client, {
    get(target, prop) {
      const value = (target as unknown as Record<string | symbol, unknown>)[prop];
      if (typeof value !== "function") return value;
      const fn = (value as (...a: unknown[]) => unknown).bind(target);
      if (prop !== "eval") return fn;
      return async (script: string, numKeys: number, ...rest: unknown[]) => {
        const stage = stageOf(rest.slice(0, numKeys) as string[], rest.slice(numKeys).map(String));
        const hook = stage ? hooks[stage] : undefined;
        if (hook) {
          delete hooks[stage!];
          await hook();
        }
        return fn(script, numKeys, ...rest);
      };
    },
  });
  return { client: proxy, hooks };
}

redisDescribe("RedisWorkflowStorage fenced child create", (redis) => {
  async function setup() {
    const prefix = uniquePrefix("child");
    const raw = redis.client();
    const { client, hooks } = interceptable(redis.client());
    const storage = new RedisWorkflowStorage({ redis: client, prefix });
    const peer = new RedisWorkflowStorage({ redis: redis.client(), prefix });
    await storage.createWorkflow({ workflowId: "p", workflowName: "parent", input: {} });
    const lock = await storage.tryLock({ workflowId: "p", lockDurationMs: 30_000 });
    const child = (input: unknown) => ({
      workflowId: "p-child",
      workflowName: "child",
      input,
      parentWorkflowId: "p",
    });
    return { prefix, raw, storage, peer, hooks, guard: { fenceToken: lock.token! }, child };
  }

  describe("a parent that loses its lock before its intent", () => {
    it("creates no child, and the next holder creates it", async () => {
      const { prefix, raw, storage, peer, hooks, guard, child } = await setup();
      let takeover: string | undefined;
      hooks.intent = async () => {
        await raw.del(`${prefix}:{wf:p}:lock`);
        takeover = (await peer.tryLock({ workflowId: "p", lockDurationMs: 30_000 })).token;
      };

      await expect(storage.createWorkflow({ ...child("zombie"), guard })).rejects.toMatchObject({
        _tag: "FenceTokenMismatchError",
      });

      expect(takeover).toBeDefined();
      expect(await peer.loadWorkflow("p-child")).toBeNull();
      expect(await raw.exists(`${prefix}:{wf:p-child}`)).toBe(0);
      expect(await peer.listWorkflows({ parentId: "p" })).toEqual([]);

      const created = await peer.createWorkflow({
        ...child("holder"),
        guard: { fenceToken: takeover! },
      });
      expect(created).toEqual({ created: true });
      expect((await peer.loadWorkflow("p-child"))!.input).toBe("holder");
      // The stale parent's retry finds the child, and still learns it lost the run.
      await expect(storage.createWorkflow({ ...child("zombie"), guard })).rejects.toMatchObject({
        _tag: "FenceTokenMismatchError",
      });
    });
  });

  describe("a create that stops before the intent", () => {
    it("leaves a provisional row no reader sees, which a retry replaces", async () => {
      const { prefix, raw, storage, peer, hooks, guard, child } = await setup();
      hooks.intent = async () => {
        throw new Error("connection lost");
      };

      await expect(storage.createWorkflow({ ...child("first"), guard })).rejects.toThrow(
        "connection lost",
      );

      const key = `${prefix}:{wf:p-child}`;
      expect(await raw.exists(key)).toBe(1);
      expect(Number(await raw.eval("return redis.call('PTTL', KEYS[1])", 1, key))).toBeGreaterThan(
        0,
      );
      expect(await peer.loadWorkflow("p-child")).toBeNull();
      expect(await peer.loadWorkflowStatus("p-child")).toBeNull();
      expect((await peer.tryLockAndLoad({ workflowId: "p-child", lockDurationMs: 1 })).state).toBe(
        null,
      );
      expect(await peer.countWorkflows({ parentId: "p" })).toBe(0);

      expect(await storage.createWorkflow({ ...child("second"), guard })).toEqual({
        created: true,
      });
      const state = await peer.loadWorkflow("p-child");
      expect(state!.input).toBe("second");
      expect(await raw.hget(key, "provisional")).toBeNull();
      expect(await peer.countWorkflows({ parentId: "p" })).toBe(1);
    });
  });

  describe("a create that stops after the intent", () => {
    it("has committed the child: a reader confirms and indexes it", async () => {
      const { prefix, raw, storage, peer, hooks, guard, child } = await setup();
      hooks.confirm = async () => {
        throw new Error("connection lost");
      };

      await expect(storage.createWorkflow({ ...child("kept"), guard })).rejects.toThrow(
        "connection lost",
      );

      const key = `${prefix}:{wf:p-child}`;
      expect(await raw.hget(key, "provisional")).not.toBeNull();
      expect((await peer.loadWorkflow("p-child"))!.input).toBe("kept");
      expect(await raw.hget(key, "provisional")).toBeNull();
      expect(Number(await raw.eval("return redis.call('PTTL', KEYS[1])", 1, key))).toBe(-1);
      expect((await peer.listWorkflows({ parentId: "p" })).map((w) => w.workflowId)).toEqual([
        "p-child",
      ]);

      const again = await storage.createWorkflow({ ...child("other"), guard });
      expect(again.created).toBe(false);
    });

    it("a cascading cancel of the parent reaches the child", async () => {
      const { storage, peer, hooks, guard, child } = await setup();
      hooks.confirm = async () => {
        throw new Error("connection lost");
      };
      await expect(storage.createWorkflow({ ...child("kept"), guard })).rejects.toThrow(
        "connection lost",
      );

      await peer.cancelWorkflow({ workflowId: "p", cascade: true });

      expect((await peer.loadWorkflowStatus("p-child"))!.status).toBe("failed");
    });
  });
});
