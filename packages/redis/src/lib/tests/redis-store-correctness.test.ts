import { describe, it, expect } from "bun:test";
import { createWorkflowRunner, runJournaledStep, workflow } from "@promin/workflow";
import { RedisWorkflowStorage } from "../redis-workflow-storage.ts";
import { redisDescribe, uniquePrefix } from "./redis-test-utils.ts";

// ---------------------------------------------------------------------------
// Redis-specific regressions for the workflow store: fresh runs and
// continue-as-new re-execute activities (the journal is dropped in the same
// script as the run bump), fresh runs survive retention TTLs, and the
// compare-and-set paths hold under a second connection. The portable
// contract lives in the conformance suite.
// ---------------------------------------------------------------------------

redisDescribe("RedisWorkflowStorage store correctness", (redis) => {
  const create = (prefix = uniquePrefix("rsc")) =>
    new RedisWorkflowStorage({ redis: redis.client(), prefix });

  describe("journal and fresh runs", () => {
    it("startFreshRun makes the next run re-execute its activities", async () => {
      const s = create();
      await s.createWorkflow({ workflowId: "c", workflowName: "w", input: 1 });
      let calls = 0;
      const body = function* (ctx: { activity: (n: string, f: () => Promise<number>) => any }) {
        return (yield* ctx.activity("fetch", async () => ++calls)) as number;
      };
      const run = () =>
        runJournaledStep({
          input: 1,
          prev: undefined,
          workflowId: "c",
          stepName: "s",
          storage: s,
          body: body as never,
        });

      expect(await run()).toBe(1);
      expect(await run()).toBe(1);
      await s.startFreshRun("c");
      expect(await run()).toBe(2);
      expect(calls).toBe(2);
    });

    it("continue-as-new re-runs activities on every chain link", async () => {
      const storage = create();
      const runner = createWorkflowRunner({ storage });
      let activityCalls = 0;
      const wf = workflow<{ count: number }>({ name: "act-counter" })
        .journaled("loop", function* (ctx) {
          const seen = yield* ctx.activity("work", async () => {
            activityCalls++;
            return ctx.input.count;
          });
          if (seen >= 2) return { final: seen };
          ctx.continueAsNew({ count: ctx.input.count + 1 });
        })
        .build();

      const result = await runner.run({ workflow: wf, workflowId: "can-1", input: { count: 0 } });

      expect(result).toEqual({ final: 2 });
      expect(activityCalls).toBe(3);
      const state = await storage.loadWorkflow("can-1");
      expect(state?.status).toBe("completed");
      expect(state?.run).toBe(3);
    });

    it("a fresh run clears the retention TTL set when the previous run finished", async () => {
      const prefix = uniquePrefix("rsc-ttl");
      const client = redis.client();
      const s = new RedisWorkflowStorage({
        redis: client,
        prefix,
        retention: { completedTtlMs: 60_000 },
      });
      await s.createWorkflow({ workflowId: "ttl", workflowName: "w", input: 1 });
      await s.completeWorkflow("ttl", "done");
      expect(
        await (client as unknown as { pttl(k: string): Promise<number> }).pttl(
          `${prefix}:{wf:ttl}`,
        ),
      ).toBeGreaterThan(0);

      await s.startFreshRun("ttl");
      expect(
        await (client as unknown as { pttl(k: string): Promise<number> }).pttl(
          `${prefix}:{wf:ttl}`,
        ),
      ).toBe(-1);
      expect((await s.listWorkflows({ status: "pending" })).map((w) => w.workflowId)).toEqual([
        "ttl",
      ]);
      expect(await s.listWorkflows({ status: "completed" })).toEqual([]);
    });
  });

  describe("compare-and-set across connections", () => {
    it("one signal-token completion wins across two storage instances", async () => {
      const prefix = uniquePrefix("rsc-tok");
      const a = create(prefix);
      const b = create(prefix);
      await a.createWorkflow({ workflowId: "w", workflowName: "w", input: 1 });
      await a.createSignalToken({
        tokenId: "tk",
        workflowId: "w",
        signalName: "x",
        bearer: "b",
        tags: [],
        expiresAt: new Date(Date.now() + 60_000),
      });
      const outcomes = await Promise.all(
        Array.from({ length: 10 }, (_, i) =>
          (i % 2 ? a : b).markSignalTokenCompleted({ tokenId: "tk", value: i, now: new Date() }),
        ),
      );
      expect(outcomes.filter((o) => o.outcome === "delivered")).toHaveLength(1);
    });

    it("concurrent metadata patches from two instances all land", async () => {
      const prefix = uniquePrefix("rsc-meta");
      const a = create(prefix);
      const b = create(prefix);
      await a.createWorkflow({ workflowId: "m", workflowName: "w", input: 1 });
      await Promise.all(
        Array.from({ length: 10 }, (_, i) =>
          (i % 2 ? a : b).setWorkflowMetadata("m", { [`k${i}`]: i }),
        ),
      );
      const metadata = (await a.loadWorkflow("m"))!.metadata!;
      expect(Object.keys(metadata).sort()).toEqual(
        Array.from({ length: 10 }, (_, i) => `k${i}`).sort(),
      );
    });

    it("setWorkflowMetadata on a missing workflow creates nothing", async () => {
      const s = create();
      await s.setWorkflowMetadata("ghost", { a: 1 });
      expect(await s.loadWorkflow("ghost")).toBeNull();
    });
  });
});
