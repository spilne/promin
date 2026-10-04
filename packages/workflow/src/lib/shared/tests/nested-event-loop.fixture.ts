// Run by `nested-event-loop.test.ts` in a child `bun test` process, so a
// regression fails with a timeout there instead of wedging the whole suite:
// once an engine callback is stuck under a nested event loop, the process
// never exits on its own.
//
// Each case settles an engine promise and then immediately blocks on the
// next engine call through `expect(...).resolves`, which spins a nested
// event loop under Bun.

import { describe, expect, it } from "bun:test";
import { runExit } from "@spilne/perfect-core";
import { promiseOrDie, runEffSafe } from "../eff.ts";
import { createEngineScheduler } from "../engine-scheduler.ts";
import { workflow } from "../../durable/workflow-builder.ts";
import { InMemoryWorkflowStorage } from "../../durable/in-memory-storage.ts";
import { createWorkflowRunner } from "../../durable/workflow-runner.ts";

const CASE_TIMEOUT_MS = 2_000;

function trivialWorkflow() {
  return workflow<{ n: number }>({ name: "nested-loop" })
    .step("double", ({ input }) => promiseOrDie(async () => input.n * 2))
    .build();
}

describe("engine promises under a nested event loop", () => {
  it(
    "runEffSafe, then expect(runEffSafe).resolves",
    async () => {
      // A Promise-backed Eff suspends, so the fiber completes from a
      // scheduler task rather than on the caller's stack.
      await runEffSafe(promiseOrDie(async () => 1));
      await expect(runEffSafe(promiseOrDie(async () => 3))).resolves.toEqual({
        data: 3,
        error: null,
      });
    },
    CASE_TIMEOUT_MS,
  );

  it(
    "a scheduler that yields a macrotask on every drain, then expect(...).resolves",
    async () => {
      // Budget 0 routes every drain through the macrotask yield, so callers
      // resume — and block on the nested loop — inside those macrotasks.
      // A far-off pending timer, as any real process has: Bun's loop then
      // blocks in its poll until that timer when a `setImmediate` queued
      // after a nested loop is not picked up.
      const unrelatedTimer = setTimeout(() => {}, 60_000);
      try {
        const scheduler = createEngineScheduler({ microDrainBudget: 0 });
        const step = (n: number) => promiseOrDie(async () => n).map((x) => x + 1);
        for (let i = 0; i < 20; i++) {
          await runExit(step(i), scheduler);
          await expect(runExit(step(i), scheduler)).resolves.toEqual({
            _tag: "Success",
            value: i + 1,
          });
        }
      } finally {
        clearTimeout(unrelatedTimer);
      }
    },
    CASE_TIMEOUT_MS,
  );

  it(
    "runner.run, then expect(runner.run).resolves",
    async () => {
      const runner = createWorkflowRunner({ storage: new InMemoryWorkflowStorage() });
      const wf = trivialWorkflow();
      expect(await runner.run({ workflow: wf, workflowId: "a", input: { n: 1 } })).toBe(2);
      await expect(runner.run({ workflow: wf, workflowId: "b", input: { n: 2 } })).resolves.toBe(4);
    },
    CASE_TIMEOUT_MS,
  );

  it(
    "runner.runSafe, then expect(runner.runSafe).resolves",
    async () => {
      const runner = createWorkflowRunner({ storage: new InMemoryWorkflowStorage() });
      const wf = trivialWorkflow();
      await runner.runSafe({ workflow: wf, workflowId: "a", input: { n: 1 } });
      await expect(
        runner.runSafe({ workflow: wf, workflowId: "b", input: { n: 2 } }),
      ).resolves.toMatchObject({ data: 4, error: null });
    },
    CASE_TIMEOUT_MS,
  );

  it(
    "the engine keeps running after the nested-loop cases",
    async () => {
      const runner = createWorkflowRunner({ storage: new InMemoryWorkflowStorage() });
      expect(
        await runner.run({ workflow: trivialWorkflow(), workflowId: "c", input: { n: 3 } }),
      ).toBe(6);
    },
    CASE_TIMEOUT_MS,
  );
});
