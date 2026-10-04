// ---------------------------------------------------------------------------
// `.match()` selector lookup and the no-match failure:
//   * selector keys resolve against `cases`' own properties only, so keys
//     like "constructor" / "toString" (often user data) fall to `default`
//     or MatchError instead of hitting Object.prototype;
//   * no case + no default is a typed MatchError failure (not a defect), so
//     workflow-level retry predicates and runSafe callers see it.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { succeed } from "@spilne/perfect-core";
import { MatchError } from "../steps/match-step.ts";
import { workflow } from "../workflow-builder.ts";
import { InMemoryWorkflowStorage } from "../in-memory-storage.ts";
import { createWorkflowRunner, InProcessStepExecutor } from "../workflow-runner.ts";

function setup() {
  const storage = new InMemoryWorkflowStorage();
  const runner = createWorkflowRunner({ storage });
  return { storage, runner };
}

describe(".match() — selector uses own keys only", () => {
  for (const key of ["constructor", "toString", "hasOwnProperty", "__proto__", "valueOf"]) {
    it(`selector key "${key}" falls back to default`, async () => {
      const { storage, runner } = setup();
      const wf = workflow<string>({ name: "match-proto-default" })
        .match("m", {
          on: (s) => s,
          cases: { express: () => succeed("E") },
          default: ({ prev }) => succeed(`default:${prev}`),
        })
        .build();

      const r = await runner.runSafe({ workflow: wf, workflowId: `mpd-${key}`, input: key });

      expect(r.error).toBeNull();
      expect(r.data).toBe(`default:${key}`);
      const state = await storage.loadWorkflow(`mpd-${key}`);
      expect(state?.steps["m"]?.metadata).toEqual({ matchCase: "default", matchMode: "selector" });
    });
  }

  it('selector key "toString" without a default fails with MatchError', async () => {
    const { runner } = setup();
    const wf = workflow<string>({ name: "match-proto-nodefault" })
      .match("m", { on: (s) => s, cases: { express: () => succeed("E") } })
      .build();

    const r = await runner.runSafe({ workflow: wf, workflowId: "mpn-1", input: "toString" });

    expect(r.error).toBeInstanceOf(MatchError);
    expect((r.error as MatchError).selectorKey).toBe("toString");
    expect((r.error as MatchError).stepName).toBe("m");
  });

  it("an own case named like a prototype member still matches", async () => {
    const { runner } = setup();
    const wf = workflow<string>({ name: "match-own-proto-name" })
      .match("m", {
        on: (s) => s,
        cases: { toString: () => succeed("own toString case") },
        default: () => succeed("default"),
      })
      .build();

    const r = await runner.runSafe({ workflow: wf, workflowId: "mop-1", input: "toString" });
    expect(r.data).toBe("own toString case");
  });
});

describe(".match() — no match is a typed failure", () => {
  it("records the step as failed and surfaces MatchError through runSafe", async () => {
    const { storage, runner } = setup();
    const wf = workflow<string>({ name: "match-typed" })
      .match("m", { cases: [{ label: "a", when: (s) => s === "a", then: () => succeed(1) }] })
      .build();

    const r = await runner.runSafe({ workflow: wf, workflowId: "mt-1", input: "zzz" });

    expect(r.error).toBeInstanceOf(MatchError);
    expect((r.error as MatchError).mode).toBe("predicate");
    const state = await storage.loadWorkflow("mt-1");
    expect(state?.status).toBe("failed");
    expect(state?.steps["m"]?.status).toBe("failed");
  });

  it("a distributed step executor reports it as a step failure, not a rejection", async () => {
    const storage = new InMemoryWorkflowStorage();
    const wf = workflow<string>({ name: "match-executor" })
      .match("m", { on: (s) => s, cases: { a: () => succeed(1) } })
      .build();
    const executor = new InProcessStepExecutor({ workflow: wf, storage });

    const result = await executor.executeStep({
      workflowId: "me-1",
      stepName: "m",
      input: "zzz",
      prevResults: {},
      attempt: 1,
    });

    expect(result.ok).toBe(false);
    expect(result.ok ? undefined : result.error).toMatch(/no case for selector key "zzz"/);
  });

  it("a workflow retry predicate on MatchError re-runs the step", async () => {
    const { runner } = setup();
    let calls = 0;
    const wf = workflow<string>({
      name: "match-retry",
      retry: { maxRetries: 1, baseDelayMs: 1, when: (e) => e._tag === "MatchError" },
    })
      .match("m", {
        // First attempt selects a missing key; the retry selects "a".
        on: () => (calls++ === 0 ? "missing" : "a"),
        cases: { a: () => succeed("A") },
      })
      .build();

    const r = await runner.runSafe({ workflow: wf, workflowId: "mr-1", input: "x" });

    expect(r.error).toBeNull();
    expect(r.data).toBe("A");
    expect(calls).toBe(2);
  });
});
