import { describe, it, expect, beforeEach } from "bun:test";
import { Pipeline } from "@promin/core";
import { workflow } from "./durable-pipeline.ts";
import { InMemoryWorkflowStorage } from "./in-memory-storage.ts";
import {
  runJournaledStep,
  JournalNonDeterminismError,
  JournalStorageMissingError,
} from "./journaled-step.ts";
import type { JournaledContext } from "./journaled-step.ts";

// ---------------------------------------------------------------------------
// Tests cover the Phase 1 acceptance gate:
//  - Activity runs once on first execution, result journaled.
//  - Replay of the same (workflowId, stepName) returns the journaled value
//    without re-executing the activity fn.
//  - Storage without ActivityJournalStorage throws at build time.
//  - Activity failure journals Failure; replay rethrows.
//  - Retries are applied before journaling the final exit.
//  - Name mismatch on replay throws JournalNonDeterminismError.
//  - .journaled() composes with other step kinds (.step before/after).
// ---------------------------------------------------------------------------

describe("journaled step", () => {
  let storage: InMemoryWorkflowStorage;

  beforeEach(() => {
    storage = new InMemoryWorkflowStorage();
  });

  describe("happy path", () => {
    it("runs activities once, journals results, returns the body's return value", async () => {
      let createCalls = 0;
      let notifyCalls = 0;

      const result = await workflow<{ user: string }>({ name: "signup", storage })
        .step("load", ({ input }) => Pipeline.succeed(input))
        .journaled("setup", function* (ctx, prev) {
          const created = yield* ctx.activity("create", async () => {
            createCalls++;
            return { id: "u1", name: prev.user };
          });
          const notified = yield* ctx.activity("notify", async () => {
            notifyCalls++;
            return `welcome-${created.id}`;
          });
          return { user: created, greeting: notified };
        })
        .run({ workflowId: "wf-happy", input: { user: "alice" } });

      expect(createCalls).toBe(1);
      expect(notifyCalls).toBe(1);
      expect(result).toEqual({
        user: { id: "u1", name: "alice" },
        greeting: "welcome-u1",
      });

      // Journal has two entries with the right names and values.
      const journal = await storage.loadJournal("wf-happy", "setup");
      expect(journal).toHaveLength(2);
      expect(journal[0]!.activityName).toBe("create");
      expect(journal[0]!.exit).toEqual({
        tag: "Success",
        value: { id: "u1", name: "alice" },
      });
      expect(journal[1]!.activityName).toBe("notify");
    });

    it("replay returns journaled values without re-executing activities", async () => {
      let callCount = 0;
      const body = function* (ctx: JournaledContext<{ msg: string }, { msg: string }>) {
        const hello = yield* ctx.activity("shout", async () => {
          callCount++;
          return ctx.prev.msg.toUpperCase();
        });
        return { shouted: hello };
      };

      // Seed the journal by running once through a workflow.
      await workflow<{ msg: string }>({ name: "echo", storage })
        .step("load", ({ input }) => Pipeline.succeed(input))
        .journaled("body", body as any)
        .run({ workflowId: "wf-echo", input: { msg: "hi" } });

      expect(callCount).toBe(1);

      // Re-run the journaled step directly against the same storage +
      // workflowId. Activity must not fire again.
      const replayed = await runJournaledStep<
        { msg: string },
        { msg: string },
        { shouted: string }
      >({
        input: { msg: "hi" },
        prev: { msg: "hi" },
        workflowId: "wf-echo",
        stepName: "body",
        storage,
        body: body as any,
      });

      expect(replayed).toEqual({ shouted: "HI" });
      expect(callCount).toBe(1); // unchanged — replay didn't invoke fn
    });

    it("crash-between-activities scenario: first activity replays, second runs fresh", async () => {
      let aCalls = 0;
      let bCalls = 0;
      const body = function* (ctx: JournaledContext<unknown, unknown>) {
        const a = yield* ctx.activity("A", async () => {
          aCalls++;
          return "a-result";
        });
        const b = yield* ctx.activity("B", async () => {
          bCalls++;
          return `b-${a}`;
        });
        return { a, b };
      };

      // First run writes both journal entries.
      const first = await runJournaledStep({
        input: {},
        prev: {},
        workflowId: "wf-crash",
        stepName: "step",
        storage,
        body,
      });
      expect(first).toEqual({ a: "a-result", b: "b-a-result" });
      expect(aCalls).toBe(1);
      expect(bCalls).toBe(1);

      // Simulate: second activity's journal entry was lost (crash before append).
      storage.deleteJournalEntry("wf-crash", "step", 1);

      // Replay re-runs ONLY activity B; A comes from journal.
      const replayed = await runJournaledStep({
        input: {},
        prev: {},
        workflowId: "wf-crash",
        stepName: "step",
        storage,
        body,
      });
      expect(replayed).toEqual({ a: "a-result", b: "b-a-result" });
      expect(aCalls).toBe(1); // NOT re-run
      expect(bCalls).toBe(2); // re-ran
    });
  });

  describe("errors and retries", () => {
    it("activity failure: failure is journaled, replay rethrows, fn not re-run", async () => {
      let calls = 0;
      const body = function* (ctx: JournaledContext<unknown, unknown>) {
        const v = yield* ctx.activity("may-fail", async () => {
          calls++;
          throw new Error("boom");
        });
        return v;
      };

      await expect(
        runJournaledStep({
          input: {},
          prev: {},
          workflowId: "wf-fail",
          stepName: "step",
          storage,
          body,
        }),
      ).rejects.toThrow("boom");

      expect(calls).toBe(1);

      const journal = await storage.loadJournal("wf-fail", "step");
      expect(journal).toHaveLength(1);
      expect(journal[0]!.exit).toEqual({ tag: "Failure", error: "boom" });

      // Replay — activity not re-run, error rethrown from journal.
      await expect(
        runJournaledStep({
          input: {},
          prev: {},
          workflowId: "wf-fail",
          stepName: "step",
          storage,
          body,
        }),
      ).rejects.toThrow("boom");
      expect(calls).toBe(1); // unchanged
    });

    it("activity retry: succeeds after N failures, only final success is journaled", async () => {
      let attempts = 0;
      const body = function* (ctx: JournaledContext<unknown, unknown>) {
        const v = yield* ctx.activity(
          "flaky",
          async () => {
            attempts++;
            if (attempts < 3) throw new Error("transient");
            return "finally";
          },
          { retry: { maxRetries: 5, baseDelayMs: 1 } },
        );
        return v;
      };

      const result = await runJournaledStep({
        input: {},
        prev: {},
        workflowId: "wf-retry",
        stepName: "step",
        storage,
        body,
      });

      expect(result).toBe("finally");
      expect(attempts).toBe(3);

      const journal = await storage.loadJournal("wf-retry", "step");
      expect(journal).toHaveLength(1);
      expect(journal[0]!.exit).toEqual({ tag: "Success", value: "finally" });
    });

    it("try/catch inside the body handles activity failures", async () => {
      const body = function* (ctx: JournaledContext<unknown, unknown>) {
        try {
          yield* ctx.activity("will-throw", async () => {
            throw new Error("planned");
          });
          return "not reached";
        } catch (err) {
          return `caught: ${(err as Error).message}`;
        }
      };

      const result = await runJournaledStep({
        input: {},
        prev: {},
        workflowId: "wf-trycatch",
        stepName: "step",
        storage,
        body,
      });

      expect(result).toBe("caught: planned");
    });
  });

  describe("determinism guards", () => {
    it("activity name mismatch on replay throws JournalNonDeterminismError", async () => {
      // First run: activity named "original"
      await runJournaledStep({
        input: {},
        prev: {},
        workflowId: "wf-det",
        stepName: "step",
        storage,
        body: function* (ctx) {
          yield* ctx.activity("original", async () => 1);
          return "ok";
        },
      });

      // "Replay" with a body that renamed the activity — catches drift.
      await expect(
        runJournaledStep({
          input: {},
          prev: {},
          workflowId: "wf-det",
          stepName: "step",
          storage,
          body: function* (ctx) {
            yield* ctx.activity("renamed", async () => 1);
            return "ok";
          },
        }),
      ).rejects.toBeInstanceOf(JournalNonDeterminismError);
    });
  });

  describe("build-time safety", () => {
    it("throws JournalStorageMissingError when storage doesn't support journaling", () => {
      // Bare storage implementing only WorkflowStorage (no journal methods).
      const bareStorage: any = {
        createWorkflow: async () => ({ created: true }),
        saveStepResult: async () => {},
        // ...etc — only need to pass the isActivityJournalStorage check, which
        // it won't (no loadJournal/appendEntry).
      };

      expect(() =>
        workflow({ name: "nope", storage: bareStorage }).journaled("boom", function* (ctx) {
          yield* ctx.activity("x", async () => 1);
          return "never";
        }),
      ).toThrow(JournalStorageMissingError);
    });
  });

  describe("composition", () => {
    it("chains with .step() before and after", async () => {
      const result = await workflow<{ n: number }>({ name: "mixed", storage })
        .step("double", ({ input }) => Pipeline.succeed(input.n * 2))
        .journaled("plus-one-twice", function* (ctx, prev) {
          const a = yield* ctx.activity("a", async () => prev + 1);
          const b = yield* ctx.activity("b", async () => a + 1);
          return b;
        })
        .step("stringify", ({ prev }) => Pipeline.succeed(`result: ${prev}`))
        .run({ workflowId: "wf-mix", input: { n: 5 } });

      // 5 * 2 = 10, +1 = 11, +1 = 12 → "result: 12"
      expect(result).toBe("result: 12");
    });
  });
});
