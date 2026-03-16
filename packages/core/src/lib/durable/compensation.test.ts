import { describe, it, expect } from "bun:test";
import { Data } from "effect";
import { Pipeline } from "../pipeline.ts";
import { workflow } from "./durable-pipeline.ts";
import { InMemoryWorkflowStorage } from "./in-memory-storage.ts";

// ---------------------------------------------------------------------------
// Test error
// ---------------------------------------------------------------------------

class TestError extends Data.TaggedError("TestError")<{
  readonly message: string;
}> {}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function createStorage() {
  return new InMemoryWorkflowStorage();
}

/** Track calls for assertions. */
function tracker() {
  const calls: string[] = [];
  return {
    calls,
    track: (name: string) => {
      calls.push(name);
    },
  };
}

// ---------------------------------------------------------------------------
// Step-level compensation
// ---------------------------------------------------------------------------

describe("Saga rollback — undo completed work when a later step fails", () => {
  it("payment was charged but shipping failed — automatically refund the payment", async () => {
    const t = tracker();
    const storage = createStorage();

    const { error } = await workflow<{ n: number }>({
      name: "compensate-basic",
      storage,
    })
      .step(
        "step-1",
        ({ input }) => {
          t.track("step-1:execute");
          return Pipeline.succeed(input.n * 2);
        },
        {
          compensate: ({ result }) => {
            t.track(`step-1:compensate(${result})`);
            return Pipeline.succeed(undefined as void);
          },
        },
      )
      .step(
        "step-2",
        ({ prev }) => {
          t.track("step-2:execute");
          return Pipeline.succeed(prev + 1);
        },
        {
          compensate: ({ result }) => {
            t.track(`step-2:compensate(${result})`);
            return Pipeline.succeed(undefined as void);
          },
        },
      )
      .step("step-3", () => {
        t.track("step-3:execute");
        return Pipeline.fail(new TestError({ message: "boom" }));
      })
      .runSafe({ workflowId: "comp-1", input: { n: 5 } });

    expect(error).not.toBeNull();
    expect(t.calls).toEqual([
      "step-1:execute",
      "step-2:execute",
      "step-3:execute",
      // Compensations in reverse order
      "step-2:compensate(11)",
      "step-1:compensate(10)",
    ]);
  });

  it("failed step has nothing to undo — only completed steps get rolled back", async () => {
    const t = tracker();
    const storage = createStorage();

    const { error } = await workflow<string>({ name: "no-self-comp", storage })
      .step(
        "ok",
        () => {
          t.track("ok:execute");
          return Pipeline.succeed("done");
        },
        {
          compensate: () => {
            t.track("ok:compensate");
            return Pipeline.succeed(undefined as void);
          },
        },
      )
      .step(
        "fail",
        () => {
          t.track("fail:execute");
          return Pipeline.fail(new TestError({ message: "fail" }));
        },
        {
          compensate: () => {
            t.track("fail:compensate");
            return Pipeline.succeed(undefined as void);
          },
        },
      )
      .runSafe({ workflowId: "comp-no-self", input: "x" });

    expect(error).not.toBeNull();
    // Only "ok" should be compensated, not "fail" (it didn't complete)
    expect(t.calls).toEqual(["ok:execute", "fail:execute", "ok:compensate"]);
  });

  it("read-only steps have no rollback — only steps with undo logic are compensated", async () => {
    const t = tracker();
    const storage = createStorage();

    const { error } = await workflow<string>({ name: "partial-comp", storage })
      .step(
        "step-1",
        () => {
          t.track("step-1:execute");
          return Pipeline.succeed("a");
        },
        {
          compensate: () => {
            t.track("step-1:compensate");
            return Pipeline.succeed(undefined as void);
          },
        },
      )
      .step("step-2", () => {
        t.track("step-2:execute");
        return Pipeline.succeed("b");
      })
      // step-2 has no compensate
      .step("step-3", () => {
        t.track("step-3:execute");
        return Pipeline.fail(new TestError({ message: "fail" }));
      })
      .runSafe({ workflowId: "comp-partial", input: "x" });

    expect(error).not.toBeNull();
    expect(t.calls).toEqual([
      "step-1:execute",
      "step-2:execute",
      "step-3:execute",
      "step-1:compensate",
      // step-2 has no compensate — skipped
    ]);
  });

  it("one refund fails but the other still runs — best-effort rollback", async () => {
    const t = tracker();
    const storage = createStorage();

    const { error } = await workflow<string>({ name: "comp-failure", storage })
      .step(
        "step-1",
        () => {
          t.track("step-1:execute");
          return Pipeline.succeed("a");
        },
        {
          compensate: () => {
            t.track("step-1:compensate");
            return Pipeline.succeed(undefined as void);
          },
        },
      )
      .step(
        "step-2",
        () => {
          t.track("step-2:execute");
          return Pipeline.succeed("b");
        },
        {
          compensate: () => {
            t.track("step-2:compensate-throws");
            throw new Error("compensation failed!");
          },
        },
      )
      .step("step-3", () => {
        t.track("step-3:execute");
        return Pipeline.fail(new TestError({ message: "fail" }));
      })
      .runSafe({ workflowId: "comp-fail", input: "x" });

    expect(error).not.toBeNull();
    // step-2 compensation fails but step-1 still runs
    expect(t.calls).toEqual([
      "step-1:execute",
      "step-2:execute",
      "step-3:execute",
      "step-2:compensate-throws",
      "step-1:compensate",
    ]);
  });

  it("rollback handler knows what was created — can target the exact resource to delete", async () => {
    const storage = createStorage();
    let receivedParams: any = null;

    const { error } = await workflow<{ userId: string }>({ name: "comp-params", storage })
      .step("create", ({ input }) => Pipeline.succeed({ id: "acc_123", user: input.userId }), {
        compensate: (params) => {
          receivedParams = params;
          return Pipeline.succeed(undefined as void);
        },
      })
      .step("fail", () => Pipeline.fail(new TestError({ message: "fail" })))
      .runSafe({ workflowId: "comp-params-1", input: { userId: "u_42" } });

    expect(error).not.toBeNull();
    expect(receivedParams).not.toBeNull();
    expect(receivedParams.result).toEqual({ id: "acc_123", user: "u_42" });
    expect(receivedParams.input).toEqual({ userId: "u_42" });
    expect(receivedParams.workflowId).toBe("comp-params-1");
  });

  it("async rollback handler — call an external API to reverse a charge", async () => {
    const t = tracker();
    const storage = createStorage();

    const { error } = await workflow<string>({ name: "comp-async", storage })
      .step(
        "step-1",
        () => {
          t.track("step-1:execute");
          return Pipeline.succeed("done");
        },
        {
          compensate: async () => {
            t.track("step-1:compensate-async");
          },
        },
      )
      .step("fail", () => Pipeline.fail(new TestError({ message: "fail" })))
      .runSafe({ workflowId: "comp-async-1", input: "x" });

    expect(error).not.toBeNull();
    expect(t.calls).toContain("step-1:compensate-async");
  });

  it("happy path completes — no rollback actions are triggered", async () => {
    const t = tracker();
    const storage = createStorage();

    const { data } = await workflow<number>({ name: "no-comp", storage })
      .step("step-1", ({ input }) => Pipeline.succeed(input + 1), {
        compensate: () => {
          t.track("should-not-run");
          return Pipeline.succeed(undefined as void);
        },
      })
      .runSafe({ workflowId: "no-comp-1", input: 5 });

    expect(data).toBe(6);
    expect(t.calls).toEqual([]);
  });
});

// ---------------------------------------------------------------------------
// StepFailureStrategy interaction
// ---------------------------------------------------------------------------

describe("Failure strategy interaction — skip or fallback avoids unnecessary rollback", () => {
  it("optional enrichment step is skipped on failure — no rollback needed", async () => {
    const t = tracker();
    const storage = createStorage();

    const { data } = await workflow<string>({ name: "skip-no-comp", storage })
      .step(
        "step-1",
        () => {
          t.track("step-1:execute");
          return Pipeline.succeed("a");
        },
        {
          compensate: () => {
            t.track("step-1:compensate");
            return Pipeline.succeed(undefined as void);
          },
        },
      )
      .step(
        "step-2",
        () => {
          t.track("step-2:execute");
          return Pipeline.fail(new TestError({ message: "skip me" }));
        },
        {
          onFailure: "skip",
        },
      )
      .step("step-3", ({ prev }) => {
        t.track("step-3:execute");
        return Pipeline.succeed("ok");
      })
      .runSafe({ workflowId: "skip-1", input: "x" });

    // Workflow succeeds because step-2 was skipped
    expect(data).toBe("ok");
    // No compensation ran
    expect(t.calls).not.toContain("step-1:compensate");
  });

  it("fallback value substituted for failed step — workflow continues without rollback", async () => {
    const t = tracker();
    const storage = createStorage();

    const { data } = await workflow<string>({ name: "fallback-no-comp", storage })
      .step(
        "step-1",
        () => {
          t.track("step-1:execute");
          return Pipeline.succeed("a");
        },
        {
          compensate: () => {
            t.track("step-1:compensate");
            return Pipeline.succeed(undefined as void);
          },
        },
      )
      .step(
        "step-2",
        () => {
          return Pipeline.fail(new TestError({ message: "use fallback" }));
        },
        {
          onFailure: { fallback: () => "fallback-value" },
        },
      )
      .runSafe({ workflowId: "fallback-1", input: "x" });

    expect(data).toBe("fallback-value");
    expect(t.calls).not.toContain("step-1:compensate");
  });

  it("default fail strategy triggers rollback — critical steps must be undone", async () => {
    const t = tracker();
    const storage = createStorage();

    const { error } = await workflow<string>({ name: "fail-comp", storage })
      .step(
        "step-1",
        () => {
          t.track("step-1:execute");
          return Pipeline.succeed("a");
        },
        {
          compensate: () => {
            t.track("step-1:compensate");
            return Pipeline.succeed(undefined as void);
          },
        },
      )
      .step("step-2", () => Pipeline.fail(new TestError({ message: "fail" })))
      .runSafe({ workflowId: "fail-comp-1", input: "x" });

    expect(error).not.toBeNull();
    expect(t.calls).toContain("step-1:compensate");
  });
});

// ---------------------------------------------------------------------------
// Workflow-level retry
// ---------------------------------------------------------------------------

describe("Workflow-level retry — recover from transient failures before giving up", () => {
  it("transient DB timeout on second step — retry succeeds without re-running first step", async () => {
    const t = tracker();
    const storage = createStorage();
    let attempt = 0;

    const { data } = await workflow<number>({
      name: "wf-retry",
      storage,
      retry: { maxRetries: 2, baseDelayMs: 10 },
    })
      .step("step-1", ({ input }) => {
        t.track("step-1:execute");
        return Pipeline.succeed(input * 2);
      })
      .step("step-2", ({ prev }) => {
        attempt++;
        t.track(`step-2:execute(attempt=${attempt})`);
        if (attempt < 2) {
          return Pipeline.fail(new TestError({ message: `fail-${attempt}` }));
        }
        return Pipeline.succeed(prev + 100);
      })
      .runSafe({ workflowId: "wf-retry-1", input: 5 });

    // step-1 runs once (checkpointed), step-2 runs twice (first fails, second succeeds)
    expect(data).toBe(110);
    expect(t.calls).toEqual([
      "step-1:execute",
      "step-2:execute(attempt=1)",
      // Workflow retry — step-1 is checkpointed, only step-2 re-runs
      "step-2:execute(attempt=2)",
    ]);
  });

  it("permanent failure after all retries — rollback only happens once at the end", async () => {
    const t = tracker();
    const storage = createStorage();
    let attempt = 0;

    const { error } = await workflow<string>({
      name: "wf-retry-then-comp",
      storage,
      retry: { maxRetries: 1, baseDelayMs: 10 },
    })
      .step(
        "step-1",
        () => {
          t.track("step-1:execute");
          return Pipeline.succeed("done");
        },
        {
          compensate: () => {
            t.track("step-1:compensate");
            return Pipeline.succeed(undefined as void);
          },
        },
      )
      .step("step-2", () => {
        attempt++;
        t.track(`step-2:execute(attempt=${attempt})`);
        return Pipeline.fail(new TestError({ message: `always-fail-${attempt}` }));
      })
      .runSafe({ workflowId: "wf-retry-comp-1", input: "x" });

    expect(error).not.toBeNull();
    expect(t.calls).toEqual([
      "step-1:execute",
      "step-2:execute(attempt=1)",
      // Workflow retry (step-1 checkpointed, step-2 re-runs)
      "step-2:execute(attempt=2)",
      // All retries exhausted → compensate
      "step-1:compensate",
    ]);
  });

  it("step retries within each workflow attempt — total attempts multiply", async () => {
    const t = tracker();
    const storage = createStorage();
    let totalAttempts = 0;

    const { error } = await workflow<string>({
      name: "combined-retry",
      storage,
      retry: { maxRetries: 1, baseDelayMs: 10 },
    })
      .step(
        "step-1",
        () => {
          t.track("step-1:execute");
          return Pipeline.succeed("ok");
        },
        {
          compensate: () => {
            t.track("step-1:compensate");
            return Pipeline.succeed(undefined as void);
          },
        },
      )
      .step(
        "step-2",
        () => {
          totalAttempts++;
          t.track(`step-2:attempt=${totalAttempts}`);
          return Pipeline.fail(new TestError({ message: `fail-${totalAttempts}` }));
        },
        {
          retry: { maxRetries: 1 },
        },
      )
      .runSafe({ workflowId: "combined-retry-1", input: "x" });

    expect(error).not.toBeNull();
    // Workflow attempt 1: step-2 tries 1,2 (both fail) → workflow fails
    // Workflow attempt 2: step-2 tries 3,4 (both fail) → all exhausted → compensate
    expect(totalAttempts).toBe(4);
    expect(t.calls).toContain("step-1:compensate");
  });

  it("no retry configured — failure triggers immediate rollback", async () => {
    const t = tracker();
    const storage = createStorage();

    const { error } = await workflow<string>({ name: "no-wf-retry", storage })
      .step(
        "step-1",
        () => {
          t.track("step-1:execute");
          return Pipeline.succeed("ok");
        },
        {
          compensate: () => {
            t.track("step-1:compensate");
            return Pipeline.succeed(undefined as void);
          },
        },
      )
      .step("step-2", () => {
        t.track("step-2:execute");
        return Pipeline.fail(new TestError({ message: "fail" }));
      })
      .runSafe({ workflowId: "no-wf-retry-1", input: "x" });

    expect(error).not.toBeNull();
    // No retry, immediate compensation
    expect(t.calls).toEqual(["step-1:execute", "step-2:execute", "step-1:compensate"]);
  });
});

// ---------------------------------------------------------------------------
// Workflow-level onCompensate
// ---------------------------------------------------------------------------

describe("Post-rollback hook — notify ops team after saga compensation", () => {
  it("report lists which steps were rolled back and which rollbacks failed", async () => {
    const storage = createStorage();
    let report: any = null;

    const { error } = await workflow<string>({
      name: "oncomp-report",
      storage,
      compensate: {
        onComplete: (params) => {
          report = params;
          return Pipeline.succeed(undefined as void);
        },
      },
    })
      .step("step-1", () => Pipeline.succeed("a"), {
        compensate: () => Pipeline.succeed(undefined as void),
      })
      .step("step-2", () => Pipeline.succeed("b"), {
        compensate: () => {
          throw new Error("comp-failed");
        },
      })
      .step("fail", () => Pipeline.fail(new TestError({ message: "boom" })))
      .runSafe({ workflowId: "oncomp-1", input: "x" });

    expect(error).not.toBeNull();
    expect(report).not.toBeNull();
    expect(report.input).toBe("x");
    expect(report.compensatedSteps).toContain("step-1");
    expect(report.failedCompensations).toHaveLength(1);
    expect(report.failedCompensations[0].stepName).toBe("step-2");
  });

  it("notification hook crashes — original business error is still surfaced", async () => {
    const storage = createStorage();

    const { error } = await workflow<string>({
      name: "oncomp-fail",
      storage,
      compensate: {
        onComplete: () => {
          throw new Error("onComplete blew up");
        },
      },
    })
      .step("step-1", () => Pipeline.succeed("a"), {
        compensate: () => Pipeline.succeed(undefined as void),
      })
      .step("fail", () => Pipeline.fail(new TestError({ message: "original error" })))
      .runSafe({ workflowId: "oncomp-fail-1", input: "x" });

    // Original error should be preserved, not masked by onComplete failure
    expect(error).not.toBeNull();
    expect((error as any).message).toBe("original error");
  });

  it("async notification hook — post rollback summary to Slack", async () => {
    const storage = createStorage();
    const t = tracker();

    await workflow<string>({
      name: "oncomp-async",
      storage,
      compensate: {
        onComplete: async () => {
          t.track("onComplete-async");
        },
      },
    })
      .step("step-1", () => Pipeline.succeed("a"))
      .step("fail", () => Pipeline.fail(new TestError({ message: "fail" })))
      .runSafe({ workflowId: "oncomp-async-1", input: "x" });

    expect(t.calls).toContain("onComplete-async");
  });
});

// ---------------------------------------------------------------------------
// DAG compensation order
// ---------------------------------------------------------------------------

describe("DAG rollback order — undo dependent steps before their prerequisites", () => {
  it("parallel branches rolled back before the shared root step", async () => {
    const t = tracker();
    const storage = createStorage();

    await workflow<string>({ name: "dag-comp", storage })
      .step(
        "a",
        () => {
          t.track("a:execute");
          return Pipeline.succeed("A");
        },
        {
          compensate: () => {
            t.track("a:compensate");
            return Pipeline.succeed(undefined as void);
          },
        },
      )
      .step(
        "b",
        { dependsOn: ["a"] },
        () => {
          t.track("b:execute");
          return Pipeline.succeed("B");
        },
        {
          compensate: () => {
            t.track("b:compensate");
            return Pipeline.succeed(undefined as void);
          },
        },
      )
      .step(
        "c",
        { dependsOn: ["a"] },
        () => {
          t.track("c:execute");
          return Pipeline.succeed("C");
        },
        {
          compensate: () => {
            t.track("c:compensate");
            return Pipeline.succeed(undefined as void);
          },
        },
      )
      .step("d", { dependsOn: ["b", "c"] }, () => {
        t.track("d:execute");
        return Pipeline.fail(new TestError({ message: "fail" }));
      })
      .runSafe({ workflowId: "dag-comp-1", input: "x" });

    // c, b, a — reverse of definition order for completed steps
    const compCalls = t.calls.filter((c) => c.includes("compensate"));
    expect(compCalls).toEqual(["c:compensate", "b:compensate", "a:compensate"]);
  });
});

// ---------------------------------------------------------------------------
// Full cascade: step retry → workflow retry → compensation
// ---------------------------------------------------------------------------

describe("Full recovery cascade — step retry, workflow retry, then rollback", () => {
  it("exhausts step retries, then workflow retries, then compensates — full escalation path", async () => {
    const t = tracker();
    const storage = createStorage();
    let step2Calls = 0;

    const { error } = await workflow<string>({
      name: "full-cascade",
      storage,
      retry: { maxRetries: 1, baseDelayMs: 10 },
      compensate: {
        onComplete: ({ compensatedSteps }) => {
          t.track(`onCompensate:[${compensatedSteps.join(",")}]`);
          return Pipeline.succeed(undefined as void);
        },
      },
    })
      .step(
        "step-1",
        () => {
          t.track("step-1:execute");
          return Pipeline.succeed("done");
        },
        {
          compensate: () => {
            t.track("step-1:compensate");
            return Pipeline.succeed(undefined as void);
          },
        },
      )
      .step(
        "step-2",
        () => {
          step2Calls++;
          t.track(`step-2:call=${step2Calls}`);
          return Pipeline.fail(new TestError({ message: `fail-${step2Calls}` }));
        },
        {
          retry: { maxRetries: 1 },
        },
      )
      .runSafe({ workflowId: "cascade-1", input: "x" });

    expect(error).not.toBeNull();

    // step-1 executes once, checkpointed
    // step-2: call 1 (fail) → step retry → call 2 (fail) → workflow fails
    // workflow retry: step-1 checkpointed, step-2: call 3 (fail) → step retry → call 4 (fail)
    // all exhausted → compensation
    expect(t.calls).toEqual([
      "step-1:execute",
      "step-2:call=1",
      // step retry
      "step-2:call=2",
      // workflow retry (step-1 checkpointed)
      "step-2:call=3",
      // step retry
      "step-2:call=4",
      // compensation
      "step-1:compensate",
      "onCompensate:[step-1]",
    ]);
  });

  it("transient network glitch clears up on retry — no rollback needed", async () => {
    const storage = createStorage();
    let step2Calls = 0;

    const { data } = await workflow<number>({
      name: "transient-recovery",
      storage,
      retry: { maxRetries: 2, baseDelayMs: 10 },
    })
      .step("step-1", ({ input }) => Pipeline.succeed(input + 1), {
        compensate: () => {
          throw new Error("should not compensate");
        },
      })
      .step("step-2", ({ prev }) => {
        step2Calls++;
        if (step2Calls < 3) {
          return Pipeline.fail(new TestError({ message: "transient" }));
        }
        return Pipeline.succeed(prev * 10);
      })
      .runSafe({ workflowId: "transient-1", input: 5 });

    // step-2 fails twice (workflow retries), succeeds on third
    expect(data).toBe(60); // (5+1)*10
    expect(step2Calls).toBe(3);
  });
});

// ---------------------------------------------------------------------------
// Edge cases
// ---------------------------------------------------------------------------

describe("Edge cases — boundary conditions for compensation logic", () => {
  it("single step fails with no prior work — nothing to roll back", async () => {
    const storage = createStorage();
    const t = tracker();

    // Single step that fails — nothing to compensate
    const { error } = await workflow<string>({
      name: "single-fail",
      storage,
      compensate: {
        onComplete: () => {
          t.track("onCompensate");
          return Pipeline.succeed(undefined as void);
        },
      },
    })
      .step("only-step", () => Pipeline.fail(new TestError({ message: "fail" })))
      .runSafe({ workflowId: "single-fail-1", input: "x" });

    expect(error).not.toBeNull();
    // onCompensate still called (with empty lists)
    expect(t.calls).toContain("onCompensate");
  });

  it("very first step fails — its own compensate is not invoked since it never completed", async () => {
    const t = tracker();
    const storage = createStorage();

    await workflow<string>({ name: "first-fail", storage })
      .step("step-1", () => Pipeline.fail(new TestError({ message: "fail" })), {
        compensate: () => {
          t.track("should-not-run");
          return Pipeline.succeed(undefined as void);
        },
      })
      .runSafe({ workflowId: "first-fail-1", input: "x" });

    // step-1 failed, so its compensate should NOT run (it never completed)
    expect(t.calls).toEqual([]);
  });

  it("pre-built workflow definition retains all retry and rollback settings", async () => {
    const storage = createStorage();
    const t = tracker();
    let step2Calls = 0;

    const definition = workflow<string>({
      name: "build-preserves",
      storage,
      retry: { maxRetries: 1, baseDelayMs: 10 },
      compensate: {
        onComplete: () => {
          t.track("onCompensate");
          return Pipeline.succeed(undefined as void);
        },
      },
    })
      .step("step-1", () => Pipeline.succeed("ok"), {
        compensate: () => {
          t.track("step-1:compensate");
          return Pipeline.succeed(undefined as void);
        },
      })
      .step("step-2", () => {
        step2Calls++;
        return Pipeline.fail(new TestError({ message: `fail-${step2Calls}` }));
      })
      .build();

    const { error } = await definition.runSafe({ workflowId: "build-1", input: "x" });

    expect(error).not.toBeNull();
    // Workflow retry happened (step2Calls > 1)
    expect(step2Calls).toBe(2);
    // Compensation ran
    expect(t.calls).toContain("step-1:compensate");
    expect(t.calls).toContain("onCompensate");
  });
});

// ---------------------------------------------------------------------------
// Workflow retry `when` predicate
// ---------------------------------------------------------------------------

describe("Selective retry — only retry transient errors, fail fast on permanent ones", () => {
  it("business validation error is permanent — skip retries and compensate immediately", async () => {
    const t = tracker();
    const storage = createStorage();
    let step2Calls = 0;

    const { error } = await workflow<string>({
      name: "when-skip",
      storage,
      retry: {
        maxRetries: 3,
        baseDelayMs: 10,
        when: (err: any) => err._tag !== "TestError", // TestError is NOT retryable
      },
    })
      .step(
        "step-1",
        () => {
          t.track("step-1:execute");
          return Pipeline.succeed("ok");
        },
        {
          compensate: () => {
            t.track("step-1:compensate");
            return Pipeline.succeed(undefined as void);
          },
        },
      )
      .step("step-2", () => {
        step2Calls++;
        t.track(`step-2:call=${step2Calls}`);
        return Pipeline.fail(new TestError({ message: "non-retryable" }));
      })
      .runSafe({ workflowId: "when-skip-1", input: "x" });

    expect(error).not.toBeNull();
    // Despite maxRetries: 3, TestError is not retryable → no workflow retry → immediate compensation
    expect(step2Calls).toBe(1);
    expect(t.calls).toContain("step-1:compensate");
  });

  it("network errors retry, but auth errors stop immediately — mixed error types", async () => {
    const storage = createStorage();
    let step2Calls = 0;

    class RetryableError extends Data.TaggedError("RetryableError")<{
      readonly message: string;
    }> {}

    const { error } = await workflow<string>({
      name: "when-mixed",
      storage,
      retry: {
        maxRetries: 3,
        baseDelayMs: 10,
        when: (err: any) => err._tag === "RetryableError",
      },
    })
      .step("step-1", () => Pipeline.succeed("ok"))
      .step("step-2", (): Pipeline<never, TestError | RetryableError> => {
        step2Calls++;
        if (step2Calls < 3) {
          return Pipeline.fail(new RetryableError({ message: "transient" }));
        }
        return Pipeline.fail(new TestError({ message: "permanent" }));
      })
      .runSafe({ workflowId: "when-mixed-1", input: "x" });

    expect(error).not.toBeNull();
    // Attempts 1,2: RetryableError → workflow retries
    // Attempt 3: TestError → not retryable → stops, no more retries
    expect(step2Calls).toBe(3);
    expect((error as any)._tag).toBe("TestError");
  });
});

// ---------------------------------------------------------------------------
// Attempt counter in step context
// ---------------------------------------------------------------------------

describe("Attempt tracking — steps know which try they are on for backoff decisions", () => {
  it("step-level retry increments the attempt counter each time", async () => {
    const storage = createStorage();
    const attempts: number[] = [];

    const { data } = await workflow<string>({ name: "attempt-step", storage })
      .step(
        "flaky",
        (ctx) => {
          attempts.push(ctx.attempt);
          if (attempts.length < 3) {
            return Pipeline.fail(new TestError({ message: "transient" }));
          }
          return Pipeline.succeed("ok");
        },
        {
          retry: { maxRetries: 5 },
        },
      )
      .runSafe({ workflowId: "attempt-step-1", input: "x" });

    expect(data).toBe("ok");
    expect(attempts).toEqual([1, 2, 3]);
  });

  it("attempt counter keeps incrementing across workflow retries — no reset", async () => {
    const storage = createStorage();
    const attempts: number[] = [];
    let totalCalls = 0;

    const { data } = await workflow<string>({
      name: "attempt-wf",
      storage,
      retry: { maxRetries: 2, baseDelayMs: 10 },
    })
      .step("step-1", () => Pipeline.succeed("ok"))
      .step("step-2", (ctx) => {
        totalCalls++;
        attempts.push(ctx.attempt);
        if (totalCalls < 3) {
          return Pipeline.fail(new TestError({ message: "transient" }));
        }
        return Pipeline.succeed("done");
      })
      .runSafe({ workflowId: "attempt-wf-1", input: "x" });

    expect(data).toBe("done");
    // Attempt 1 (workflow attempt 1), attempt 2 (workflow attempt 2), attempt 3 (workflow attempt 3)
    expect(attempts).toEqual([1, 2, 3]);
  });

  it("combined step and workflow retries — monotonically increasing attempt numbers", async () => {
    const storage = createStorage();
    const attempts: number[] = [];

    const { error } = await workflow<string>({
      name: "attempt-combined",
      storage,
      retry: { maxRetries: 1, baseDelayMs: 10 },
    })
      .step("step-1", () => Pipeline.succeed("ok"))
      .step(
        "step-2",
        (ctx) => {
          attempts.push(ctx.attempt);
          return Pipeline.fail(new TestError({ message: `fail-${ctx.attempt}` }));
        },
        {
          retry: { maxRetries: 1 },
        },
      )
      .runSafe({ workflowId: "attempt-combined-1", input: "x" });

    expect(error).not.toBeNull();
    // Attempts are monotonically increasing across step + workflow retries
    expect(attempts[0]).toBe(1);
    for (let i = 1; i < attempts.length; i++) {
      expect(attempts[i]).toBeGreaterThan(attempts[i - 1]!);
    }
    // At least 3 total attempts (step retry + workflow retry + step retry)
    expect(attempts.length).toBeGreaterThanOrEqual(3);
  });
});

// ---------------------------------------------------------------------------
// Compensation trigger modes
// ---------------------------------------------------------------------------

describe("Immediate rollback — undo right away without retrying the workflow", () => {
  it("financial transaction needs instant reversal — skip workflow retries", async () => {
    const t = tracker();
    const storage = createStorage();
    let step2Calls = 0;

    const { error } = await workflow<string>({
      name: "immediate-comp",
      storage,
      retry: { maxRetries: 3, baseDelayMs: 10 }, // would retry 3x normally
      compensate: { trigger: "immediate" },
    })
      .step(
        "step-1",
        () => {
          t.track("step-1:execute");
          return Pipeline.succeed("ok");
        },
        {
          compensate: () => {
            t.track("step-1:compensate");
            return Pipeline.succeed(undefined as void);
          },
        },
      )
      .step("step-2", () => {
        step2Calls++;
        t.track(`step-2:call=${step2Calls}`);
        return Pipeline.fail(new TestError({ message: "fail" }));
      })
      .runSafe({ workflowId: "immediate-1", input: "x" });

    expect(error).not.toBeNull();
    // Despite retry: { maxRetries: 3 }, immediate trigger skips workflow retries
    expect(step2Calls).toBe(1);
    expect(t.calls).toEqual(["step-1:execute", "step-2:call=1", "step-1:compensate"]);
  });

  it("step-level retries still exhaust before immediate rollback kicks in", async () => {
    const t = tracker();
    const storage = createStorage();
    let step2Calls = 0;

    const { error } = await workflow<string>({
      name: "immediate-step-retry",
      storage,
      retry: { maxRetries: 2 },
      compensate: { trigger: "immediate" },
    })
      .step(
        "step-1",
        () => {
          t.track("step-1:execute");
          return Pipeline.succeed("ok");
        },
        {
          compensate: () => {
            t.track("step-1:compensate");
            return Pipeline.succeed(undefined as void);
          },
        },
      )
      .step(
        "step-2",
        () => {
          step2Calls++;
          t.track(`step-2:call=${step2Calls}`);
          return Pipeline.fail(new TestError({ message: "fail" }));
        },
        {
          retry: { maxRetries: 2 },
        },
      )
      .runSafe({ workflowId: "immediate-step-retry-1", input: "x" });

    expect(error).not.toBeNull();
    // Step retries: 3 calls (1 + 2 retries), then immediate compensation (no workflow retry)
    expect(step2Calls).toBe(3);
    expect(t.calls).toContain("step-1:compensate");
  });
});

describe("After-retries rollback (default) — exhaust all retries before compensating", () => {
  it("workflow retries twice before giving up and rolling back", async () => {
    const t = tracker();
    const storage = createStorage();
    let step2Calls = 0;

    const { error } = await workflow<string>({
      name: "after-retries-comp",
      storage,
      retry: { maxRetries: 1, baseDelayMs: 10 },
      compensate: { trigger: "after-retries" },
    })
      .step(
        "step-1",
        () => {
          t.track("step-1:execute");
          return Pipeline.succeed("ok");
        },
        {
          compensate: () => {
            t.track("step-1:compensate");
            return Pipeline.succeed(undefined as void);
          },
        },
      )
      .step("step-2", () => {
        step2Calls++;
        t.track(`step-2:call=${step2Calls}`);
        return Pipeline.fail(new TestError({ message: "fail" }));
      })
      .runSafe({ workflowId: "after-retries-1", input: "x" });

    expect(error).not.toBeNull();
    // 2 workflow attempts (1 + 1 retry), then compensation
    expect(step2Calls).toBe(2);
    expect(t.calls).toContain("step-1:compensate");
  });
});

// ---------------------------------------------------------------------------
// Compensation retry
// ---------------------------------------------------------------------------

describe("Compensation retry — retry the rollback itself if the undo API is flaky", () => {
  it("refund API fails twice then succeeds — rollback retries until it works", async () => {
    const t = tracker();
    const storage = createStorage();
    let compAttempts = 0;

    const { error } = await workflow<string>({
      name: "comp-retry",
      storage,
      compensate: {
        retry: { maxRetries: 2, baseDelayMs: 10 },
      },
    })
      .step("step-1", () => Pipeline.succeed("ok"), {
        compensate: () => {
          compAttempts++;
          t.track(`step-1:compensate-attempt=${compAttempts}`);
          if (compAttempts < 3) {
            throw new Error(`comp-fail-${compAttempts}`);
          }
          return Pipeline.succeed(undefined as void);
        },
      })
      .step("fail", () => Pipeline.fail(new TestError({ message: "fail" })))
      .runSafe({ workflowId: "comp-retry-1", input: "x" });

    expect(error).not.toBeNull();
    // Compensation retried 3 times (1 + 2 retries), succeeded on 3rd
    expect(compAttempts).toBe(3);
    expect(t.calls).toEqual([
      "step-1:compensate-attempt=1",
      "step-1:compensate-attempt=2",
      "step-1:compensate-attempt=3",
    ]);
  });

  it("rollback retries exhausted — failure recorded for manual intervention", async () => {
    const storage = createStorage();
    let report: any = null;

    const { error } = await workflow<string>({
      name: "comp-retry-exhaust",
      storage,
      compensate: {
        retry: { maxRetries: 1, baseDelayMs: 10 },
        onComplete: (params) => {
          report = params;
          return Pipeline.succeed(undefined as void);
        },
      },
    })
      .step("step-1", () => Pipeline.succeed("ok"), {
        compensate: () => {
          throw new Error("always fails");
        },
      })
      .step("fail", () => Pipeline.fail(new TestError({ message: "fail" })))
      .runSafe({ workflowId: "comp-retry-exhaust-1", input: "x" });

    expect(error).not.toBeNull();
    expect(report).not.toBeNull();
    expect(report.compensatedSteps).toEqual([]);
    expect(report.failedCompensations).toHaveLength(1);
    expect(report.failedCompensations[0].stepName).toBe("step-1");
  });

  it("no retry by default", async () => {
    const storage = createStorage();
    let compAttempts = 0;
    let report: any = null;

    const { error } = await workflow<string>({
      name: "comp-no-retry",
      storage,
      compensate: {
        onComplete: (params) => {
          report = params;
          return Pipeline.succeed(undefined as void);
        },
      },
    })
      .step("step-1", () => Pipeline.succeed("ok"), {
        compensate: () => {
          compAttempts++;
          throw new Error("fail");
        },
      })
      .step("fail", () => Pipeline.fail(new TestError({ message: "fail" })))
      .runSafe({ workflowId: "comp-no-retry-1", input: "x" });

    expect(error).not.toBeNull();
    // Only 1 attempt — no retry
    expect(compAttempts).toBe(1);
    expect(report.failedCompensations).toHaveLength(1);
  });
});
