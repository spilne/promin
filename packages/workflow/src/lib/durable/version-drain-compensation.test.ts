// ---------------------------------------------------------------------------
// Versioning drain + saga compensation — proves the right version's code
// handles rollback.
//
// Scenario that needs locking down (promin-998c): with
// `onVersionMismatch: "drain"` + `previousVersions: [v1]`, a workflow
// stored as v1 that fails during the drain-resolved resume must run v1's
// compensation handlers — NOT v2's. v2's handlers can reference step
// results that don't exist in v1's DAG, so firing them against a v1
// workflow is an accident waiting to happen. The drain precheck in
// durable-pipeline.ts already delegates the whole run to the previous
// definition, which pulls in its compensate closures — these tests
// assert that invariant so future refactors don't regress it.
//
// Covers:
//   1. Inline `previousVersions: [v1]` + drain: v1's compensation fires.
//   2. WorkflowVersionRegistry as the resolver: same scenario via the
//      registry's run() method (which routes by stored version).
//   3. Sanity check: a fresh v2 workflow uses v2's compensation, so the
//      drain assertions above aren't false positives from "neither
//      version compensates in these tests."
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { Data } from "effect";
import { Pipeline } from "@promin/core";
import { workflow } from "./durable-pipeline.ts";
import { InMemoryWorkflowStorage } from "./in-memory-storage.ts";
import { WorkflowVersionRegistry } from "./workflow-version-registry.ts";

class BoomError extends Data.TaggedError("BoomError")<{
  readonly stepName: string;
  readonly message: string;
}> {}

describe("versioning drain — compensation uses the stored version's code", () => {
  it("drain-resolved v1 fires v1's compensation, not v2's", async () => {
    const storage = new InMemoryWorkflowStorage();
    const v1Log: string[] = [];
    const v2Log: string[] = [];

    const v1 = workflow<{ n: number }>({ name: "pay", version: "1" })
      .step("charge", ({ input }) => Pipeline.succeed(input.n * 10), {
        compensate: () => {
          v1Log.push("v1:refund");
          return Pipeline.succeed(undefined as void);
        },
      })
      .step("ship", () =>
        Pipeline.fail(new BoomError({ stepName: "ship", message: "v1 ship failed" })),
      )
      .build()
      .bind(storage);

    const v2 = workflow<{ n: number }>({
      name: "pay",
      version: "2",
      onVersionMismatch: "drain",
      previousVersions: [v1],
    })
      .step("charge", ({ input }) => Pipeline.succeed(input.n * 100), {
        compensate: () => {
          v2Log.push("v2:refund");
          return Pipeline.succeed(undefined as void);
        },
      })
      .step("ship", () =>
        Pipeline.fail(new BoomError({ stepName: "ship", message: "v2 ship failed" })),
      )
      .bind(storage);

    // Seed a pending workflow with stored version=1 — simulates an
    // in-flight v1 run that the operator is about to resume after a
    // deploy to v2.
    await storage.createWorkflow({
      workflowId: "wf-drain-comp",
      workflowName: "pay",
      input: { n: 5 },
      version: "1",
    });

    // Drive via v2 — drain precheck sees stored version=1, delegates the
    // whole run to v1's definition. v1 runs "charge" (success), "ship"
    // (fails), then compensation unwinds in reverse.
    const { error } = await v2.runSafe({ workflowId: "wf-drain-comp", input: { n: 5 } });
    expect(error).not.toBeNull();

    // v1's compensation ran. v2's did NOT.
    expect(v1Log).toEqual(["v1:refund"]);
    expect(v2Log).toEqual([]);
  });

  it("WorkflowVersionRegistry.run() routes compensation to the stored version", async () => {
    const storage = new InMemoryWorkflowStorage();
    const v1Log: string[] = [];
    const v2Log: string[] = [];
    const registry = new WorkflowVersionRegistry({ storage });

    const v1 = workflow<{ n: number }>({ name: "pay", version: "1" })
      .step("charge", ({ input }) => Pipeline.succeed(input.n * 10), {
        compensate: () => {
          v1Log.push("v1:refund");
          return Pipeline.succeed(undefined as void);
        },
      })
      .step("ship", () =>
        Pipeline.fail(new BoomError({ stepName: "ship", message: "v1 ship failed" })),
      )
      .build()
      .bind(storage);

    const v2 = workflow<{ n: number }>({ name: "pay", version: "2" })
      .step("charge", ({ input }) => Pipeline.succeed(input.n * 100), {
        compensate: () => {
          v2Log.push("v2:refund");
          return Pipeline.succeed(undefined as void);
        },
      })
      .step("ship", () =>
        Pipeline.fail(new BoomError({ stepName: "ship", message: "v2 ship failed" })),
      )
      .build()
      .bind(storage);

    registry.register(v1);
    registry.register(v2);

    // Seed a pending v1 workflow. Registry.run() reads its stored version
    // and dispatches to v1.run(), so v1's compensation closures fire on
    // failure.
    await storage.createWorkflow({
      workflowId: "wf-registry-comp",
      workflowName: "pay",
      input: { n: 5 },
      version: "1",
    });

    await expect(
      registry.run({ workflowId: "wf-registry-comp", input: { n: 5 }, name: "pay" }),
    ).rejects.toThrow();

    expect(v1Log).toEqual(["v1:refund"]);
    expect(v2Log).toEqual([]);
  });

  it("fresh v2 workflow uses v2's compensation (sanity check for the drain tests)", async () => {
    // Without this case, the drain assertions above could pass even if
    // NEITHER version's compensation fired (e.g. a regression that
    // silently skipped compensation in both paths). Running a fresh v2
    // and asserting v2 logs rules that out.
    const storage = new InMemoryWorkflowStorage();
    const v2Log: string[] = [];

    const v2 = workflow<{ n: number }>({ name: "pay-fresh", version: "2" })
      .step("charge", ({ input }) => Pipeline.succeed(input.n * 100), {
        compensate: () => {
          v2Log.push("v2:refund");
          return Pipeline.succeed(undefined as void);
        },
      })
      .step("ship", () =>
        Pipeline.fail(new BoomError({ stepName: "ship", message: "fresh v2 fail" })),
      )
      .build()
      .bind(storage);

    const { error } = await v2.runSafe({ workflowId: "wf-fresh-v2", input: { n: 5 } });
    expect(error).not.toBeNull();
    expect(v2Log).toEqual(["v2:refund"]);
  });
});
