import { describe, it, expect, beforeEach } from "bun:test";
import { Pipeline } from "@promin/core";
import { workflow } from "../durable-pipeline.ts";
import { InMemoryWorkflowStorage } from "../in-memory-storage.ts";
import { createWorkflowRunner } from "../workflow-runner.ts";
import {
  runJournaledStep,
  JournalNonDeterminismError,
  JournalStorageMissingError,
} from "../journaled-step.ts";
import type { JournaledContext } from "../journaled-step.ts";

// ---------------------------------------------------------------------------
// Tests cover the core .journaled() contract:
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

      const wf = workflow<{ user: string }>({ name: "signup" })
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
        .build();
      const runner = createWorkflowRunner({ storage });
      const result = await runner.run({
        workflow: wf,
        workflowId: "wf-happy",
        input: { user: "alice" },
      });

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
      const seedWf = workflow<{ msg: string }>({ name: "echo" })
        .step("load", ({ input }) => Pipeline.succeed(input))
        .journaled("body", body as any)
        .build();
      const seedRunner = createWorkflowRunner({ storage });
      await seedRunner.run({ workflow: seedWf, workflowId: "wf-echo", input: { msg: "hi" } });

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

  describe("run-time safety", () => {
    it("throws JournalStorageMissingError when the bound storage doesn't support journaling", async () => {
      // Bare storage implementing only WorkflowStorage (no journal methods).
      // Journal support is validated at execute time (not build time)
      // because a pure `Workflow` has no storage to check against — it
      // only gets one through `.bind(storage)`.
      const bareStorage: any = {
        tryLock: async () => ({ acquired: true }),
        renewLock: async () => true,
        releaseLock: async () => {},
        heartbeat: async () => {},
        createWorkflow: async () => ({ created: true }),
        loadWorkflow: async () => ({
          workflowId: "nope-1",
          workflowName: "nope",
          status: "pending",
          input: undefined,
          version: undefined,
          steps: {},
          createdAt: new Date(),
          updatedAt: new Date(),
          metadata: {},
        }),
        saveStepResult: async () => {},
        saveStepFailure: async () => {},
        saveWorkflow: async () => {},
        updateWorkflow: async () => {},
        completeWorkflow: async () => {},
        failWorkflow: async () => {},
        updateStatus: async () => {},
        setHeartbeat: async () => {},
        deliverSignal: async () => {},
        loadSignals: async () => [],
        consumeSignal: async () => null,
        suspendWorkflow: async () => {},
        resumeWorkflow: async () => {},
        // Intentionally NO loadJournal / appendEntry — that's what we're testing.
      };

      const bareWf = workflow({ name: "nope" })
        .journaled("boom", function* (ctx) {
          yield* ctx.activity("x", async () => 1);
          return "never";
        })
        .build();
      const bareRunner = createWorkflowRunner({ storage: bareStorage });
      await expect(
        bareRunner.run({ workflow: bareWf, workflowId: "nope-1", input: undefined }),
      ).rejects.toThrow(/requires a WorkflowStorage that implements ActivityJournalStorage/);
    });
  });

  describe("ctx.patched + ctx.workflowVersion", () => {
    it("ctx.patched returns true for names in the workflow's patches array", async () => {
      const wf = workflow<{ n: number }>({
        name: "p-test",
        version: "2",
        patches: ["new-pricing", "batch"],
      })
        .journaled("body", function* (ctx, prev) {
          const a = ctx.patched("new-pricing");
          const b = ctx.patched("batch");
          const c = ctx.patched("unknown"); // not declared — silently false
          return { a, b, c, n: prev.n };
        })
        .build();
      const runner = createWorkflowRunner({ storage });
      const result = await runner.run({ workflow: wf, workflowId: "p-1", input: { n: 42 } });
      expect(result).toEqual({ a: true, b: true, c: false, n: 42 });
    });

    it("ctx.patched returns false when no patches declared", async () => {
      const wf = workflow<{ n: number }>({ name: "p-none", version: "1" })
        .journaled("body", function* (ctx) {
          return { patched: ctx.patched("new-pricing") };
        })
        .build();
      const runner = createWorkflowRunner({ storage });
      const result = await runner.run({ workflow: wf, workflowId: "p-2", input: { n: 1 } });
      expect(result).toEqual({ patched: false });
    });

    it("ctx.workflowVersion exposes the stored version", async () => {
      const wf = workflow<{ n: number }>({
        name: "v-expose",
        version: "3",
      })
        .journaled("body", function* (ctx) {
          return { version: ctx.workflowVersion };
        })
        .build();
      const runner = createWorkflowRunner({ storage });
      const result = await runner.run({ workflow: wf, workflowId: "v-1", input: { n: 1 } });
      expect(result).toEqual({ version: "3" });
    });

    it("ctx.workflowVersion is undefined when workflow has no version set", async () => {
      const wf = workflow<{ n: number }>({ name: "v-none" })
        .journaled("body", function* (ctx) {
          return { version: ctx.workflowVersion };
        })
        .build();
      const runner = createWorkflowRunner({ storage });
      const result = await runner.run({ workflow: wf, workflowId: "v-2", input: { n: 1 } });
      expect(result).toEqual({ version: undefined });
    });

    it("drain + patched together — v1 takes old path, v2 takes new path", async () => {
      // Same body, different `patches` declaration.
      const body = function* (
        ctx: JournaledContext<{ amount: number }, { amount: number }>,
        prev: { amount: number },
      ) {
        if (ctx.patched("new-pricing")) {
          return { total: prev.amount * 2 };
        } else {
          return { total: prev.amount };
        }
      };

      const v1 = workflow<{ amount: number }>({
        name: "priced",
        version: "1",
        patches: [], // patch off
      })
        .step("load", ({ input }) => Pipeline.succeed(input))
        .journaled("calc", body)
        .build();

      const runner = createWorkflowRunner({ storage });
      await runner.run({ workflow: v1, workflowId: "price-v1", input: { amount: 100 } });
      expect((await storage.getWorkflow("price-v1"))?.result).toEqual({ total: 100 });

      const v2 = workflow<{ amount: number }>({
        name: "priced",
        version: "2",
        onVersionMismatch: "drain",
        previousVersions: [v1],
        patches: ["new-pricing"], // patch on
      })
        .step("load", ({ input }) => Pipeline.succeed(input))
        .journaled("calc", body)
        .build();

      // Resume v1 workflow — drain delegates to v1 which has patches=[] →
      // patched returns false → takes v1 path. Already completed so reads cached.
      const existing = await runner.run({
        workflow: v2,
        workflowId: "price-v1",
        input: { amount: 100 },
      });
      expect(existing).toEqual({ total: 100 });

      // Fresh workflow — uses v2 def directly, patches=["new-pricing"] →
      // patched returns true → takes v2 path.
      const fresh = await runner.run({
        workflow: v2,
        workflowId: "price-v2",
        input: { amount: 100 },
      });
      expect(fresh).toEqual({ total: 200 });
    });
  });

  describe("composition", () => {
    it("chains with .step() before and after", async () => {
      const wf = workflow<{ n: number }>({ name: "mixed" })
        .step("double", ({ input }) => Pipeline.succeed(input.n * 2))
        .journaled("plus-one-twice", function* (ctx, prev) {
          const a = yield* ctx.activity("a", async () => prev + 1);
          const b = yield* ctx.activity("b", async () => a + 1);
          return b;
        })
        .step("stringify", ({ prev }) => Pipeline.succeed(`result: ${prev}`))
        .build();
      const runner = createWorkflowRunner({ storage });
      const result = await runner.run({
        workflow: wf,
        workflowId: "wf-mix",
        input: { n: 5 },
      });

      // 5 * 2 = 10, +1 = 11, +1 = 12 → "result: 12"
      expect(result).toBe("result: 12");
    });
  });
});
