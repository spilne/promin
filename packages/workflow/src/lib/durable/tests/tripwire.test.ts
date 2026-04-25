import { describe, it, expect } from "bun:test";
import { Pipeline } from "@promin/core";
import { workflow } from "../durable-pipeline.ts";
import { InMemoryWorkflowStorage } from "../in-memory-storage.ts";
import { createWorkflowRunner } from "../workflow-runner.ts";
import { WorkflowTripwireError, TripwireStorageMissingError } from "../durable-pipeline-error.ts";

describe("tripwire", () => {
  describe("fire path", () => {
    it("terminates the workflow when the predicate returns true", async () => {
      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });
      const wf = workflow<{ riskScore: number }>({ name: "fraud-stop" })
        .step("load", ({ input }) => Pipeline.succeed(input))
        .tripwire("fraud-check", {
          when: (order) => order.riskScore > 0.9,
          reason: (order) => ({ code: "fraud", score: order.riskScore }),
        })
        .step("charge", ({ prev }) => Pipeline.succeed(`charged:${prev.riskScore}`))
        .build();

      const promise = runner.run({
        workflow: wf,
        workflowId: "wf-fraud-1",
        input: { riskScore: 0.95 },
      });

      await expect(promise).rejects.toBeInstanceOf(WorkflowTripwireError);
    });

    it("persists status = tripwire and the reason payload", async () => {
      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });
      const wf = workflow<{ n: number }>({ name: "trip-state" })
        .step("load", ({ input }) => Pipeline.succeed(input))
        .tripwire("gate", {
          when: (x) => x.n > 100,
          reason: (x) => ({ limit: 100, actual: x.n }),
        })
        .build();

      await runner
        .run({ workflow: wf, workflowId: "wf-state-1", input: { n: 500 } })
        .catch(() => undefined);

      const state = await storage.loadWorkflow("wf-state-1");
      expect(state?.status).toBe("tripwire");
      expect(state?.tripwire).toEqual({ limit: 100, actual: 500 });
      expect(state?.completedAt).toBeDefined();
    });

    it("exposes reason through the thrown WorkflowTripwireError", async () => {
      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });
      const wf = workflow<{ kind: string }>({ name: "trip-error" })
        .tripwire("drop", {
          when: (x) => x.kind === "noop",
          reason: (x) => ({ dropped: x.kind }),
        })
        .build();

      try {
        await runner.run({ workflow: wf, workflowId: "wf-err-1", input: { kind: "noop" } });
        throw new Error("expected tripwire to throw");
      } catch (err) {
        expect(err).toBeInstanceOf(WorkflowTripwireError);
        const tw = err as WorkflowTripwireError;
        expect(tw.stepName).toBe("drop");
        expect(tw.reason).toEqual({ dropped: "noop" });
        expect(tw.workflowId).toBe("wf-err-1");
      }
    });

    it("surfaces tripwire through runSafe as { data: null, error }", async () => {
      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });
      const wf = workflow<{ allow: boolean }>({ name: "trip-safe" })
        .tripwire("gate", {
          when: (x) => !x.allow,
          reason: () => ({ denied: true }),
        })
        .step("proceed", ({ prev }) => Pipeline.succeed(prev))
        .build();

      const result = await runner.runSafe({
        workflow: wf,
        workflowId: "wf-safe-1",
        input: { allow: false },
      });

      expect(result.data).toBeNull();
      expect(result.error).toBeInstanceOf(WorkflowTripwireError);
      expect((result.error as WorkflowTripwireError).reason).toEqual({ denied: true });
    });

    it("does not run steps after the tripwire", async () => {
      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });
      let downstreamRan = false;

      const wf = workflow<{ x: number }>({ name: "trip-stop-dag" })
        .step("load", ({ input }) => Pipeline.succeed(input))
        .tripwire("gate", {
          when: (v) => v.x === 0,
          reason: () => "zero-input",
        })
        .step("after", ({ prev }) => {
          downstreamRan = true;
          return Pipeline.succeed(prev);
        })
        .build();

      await runner
        .run({ workflow: wf, workflowId: "wf-stop-1", input: { x: 0 } })
        .catch(() => undefined);

      expect(downstreamRan).toBe(false);
      const state = await storage.loadWorkflow("wf-stop-1");
      expect(state?.steps["after"]).toBeUndefined();
      expect(state?.steps["gate"]?.status).toBe("completed");
    });

    it("fires the onWorkflowTripwire hook", async () => {
      const storage = new InMemoryWorkflowStorage();
      let hookSeen: { stepName: string; reason: unknown } | null = null;
      const runner = createWorkflowRunner({
        storage,
        hooks: {
          onWorkflowTripwire: (p) => {
            hookSeen = { stepName: p.stepName, reason: p.reason };
          },
        },
      });
      const wf = workflow<{ ok: boolean }>({ name: "trip-hook" })
        .tripwire("gate", {
          when: (v) => !v.ok,
          reason: () => "blocked",
        })
        .build();

      await runner
        .run({ workflow: wf, workflowId: "wf-hook-1", input: { ok: false } })
        .catch(() => undefined);

      expect(hookSeen).toEqual({ stepName: "gate", reason: "blocked" });
    });
  });

  describe("skip path", () => {
    it("passes prev through and continues execution when predicate is false", async () => {
      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });
      const wf = workflow<{ amount: number }>({ name: "trip-pass" })
        .step("load", ({ input }) => Pipeline.succeed(input))
        .tripwire("cap", {
          when: (x) => x.amount > 1_000_000,
          reason: () => "over-cap",
        })
        .step("bill", ({ prev }) => Pipeline.succeed(`billed:${prev.amount}`))
        .build();

      const result = await runner.run({
        workflow: wf,
        workflowId: "wf-pass-1",
        input: { amount: 250 },
      });

      expect(result).toBe("billed:250");
      const state = await storage.loadWorkflow("wf-pass-1");
      expect(state?.status).toBe("completed");
      expect(state?.tripwire).toBeUndefined();
    });

    it("does not invoke reason() when predicate is false", async () => {
      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });
      let reasonCalls = 0;
      const wf = workflow<{ n: number }>({ name: "trip-lazy" })
        .tripwire("gate", {
          when: (v) => v.n < 0,
          reason: (v) => {
            reasonCalls++;
            return { bad: v.n };
          },
        })
        .step("out", ({ prev }) => Pipeline.succeed(prev))
        .build();

      await runner.run({ workflow: wf, workflowId: "wf-lazy-1", input: { n: 5 } });

      expect(reasonCalls).toBe(0);
    });
  });

  describe("status reporting", () => {
    it("getStatus surfaces state = tripwire with the reason", async () => {
      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });
      const wf = workflow<{ denied: boolean }>({ name: "trip-status" })
        .tripwire("gate", {
          when: (v) => v.denied,
          reason: () => ({ err: "nope" }),
        })
        .build();

      await runner
        .run({ workflow: wf, workflowId: "wf-status-1", input: { denied: true } })
        .catch(() => undefined);

      const status = await runner.getStatus("wf-status-1");
      expect(status?.state).toBe("tripwire");
      expect(status?.tripwire).toEqual({ err: "nope" });
    });
  });

  describe("storage capability check", () => {
    it("throws TripwireStorageMissingError when storage lacks tripwireWorkflow", async () => {
      const storage = new InMemoryWorkflowStorage();
      // Shadow the prototype method with an instance-own undefined to
      // simulate a backend without tripwire support. The type guard
      // `isTripwireCapableStorage` checks `typeof === "function"`, so an
      // undefined instance property makes it report not-capable.
      (storage as unknown as { tripwireWorkflow: unknown }).tripwireWorkflow = undefined;

      const runner = createWorkflowRunner({ storage });
      const wf = workflow<{ n: number }>({ name: "trip-miss" })
        .tripwire("gate", {
          when: () => true,
          reason: () => "x",
        })
        .build();

      await expect(
        runner.run({ workflow: wf, workflowId: "wf-miss-1", input: { n: 1 } }),
      ).rejects.toBeInstanceOf(TripwireStorageMissingError);
    });
  });

  describe("tripwire vs failure", () => {
    it("does not trigger compensation (tripwire is not a failure)", async () => {
      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });
      let compensated = false;

      const wf = workflow<{ stop: boolean }>({ name: "trip-no-comp" })
        .step("reserve", ({ input }) => Pipeline.succeed(input), {
          compensate: () => {
            compensated = true;
          },
        })
        .tripwire("gate", {
          when: (v) => v.stop,
          reason: () => "halt",
        })
        .build();

      await runner
        .run({ workflow: wf, workflowId: "wf-nocomp-1", input: { stop: true } })
        .catch(() => undefined);

      expect(compensated).toBe(false);
    });
  });
});
