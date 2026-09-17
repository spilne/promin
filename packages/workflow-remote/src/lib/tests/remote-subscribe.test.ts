// ---------------------------------------------------------------------------
// Polling-subscribe fallback over RemoteWorkflowStorage.
//
// The wire (wire.ts) intentionally has no `subscribeToWorkflow` method —
// streaming over a single JSON POST round-trip doesn't fit. The runner
// detects that and falls back to `_pollSubscribe`, which diffs successive
// `loadWorkflow` snapshots into `WorkflowRunEvent`s. This test pins that
// behavior end-to-end so any future change (native HTTP push, SSE) lands
// without silently regressing the fallback.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { Pipeline } from "@promin/core";
import {
  InMemoryWorkflowStorage,
  createWorkflowRunner,
  isSubscribableStorage,
  workflow,
  type WorkflowRunEvent,
} from "@promin/workflow";
import { RemoteWorkflowStorage } from "../remote-workflow-storage.ts";
import { createWorkflowStorageHandler } from "../storage-http-handler.ts";

describe("RemoteWorkflowStorage — polling subscribe fallback", () => {
  it("delivers step + workflow terminal events via polling", async () => {
    const backing = new InMemoryWorkflowStorage();
    const handler = createWorkflowStorageHandler(backing);
    const remote = new RemoteWorkflowStorage({
      url: "http://test.local/storage",
      fetch: handler,
    });

    // Sanity: the wire deliberately omits subscribeToWorkflow, so the
    // runner must fall through to the polling path.
    expect(isSubscribableStorage(remote)).toBe(false);

    const runner = createWorkflowRunner({ storage: remote });

    const wf = workflow<number>({ name: "remote-sub" })
      .step("a", ({ input }) => Pipeline.succeed(input + 1))
      .step("b", ({ prev }) => Pipeline.succeed(prev * 2))
      .build();

    // Subscribe before run() so the poller is already ticking by the time
    // step rows materialize.
    const events: WorkflowRunEvent[] = [];
    const collect = (async () => {
      for await (const ev of runner.subscribe("remote-sub-1", { pollIntervalMs: 20 })) {
        events.push(ev);
        if (ev.type === "workflow-completed" || ev.type === "workflow-failed") break;
      }
    })();

    await runner.run({ workflow: wf, workflowId: "remote-sub-1", input: 5 });
    await collect;

    const types = events.map((e) => e.type);
    // Polling diff doesn't synthesize step-started (no per-tick snapshot of
    // "running" → "running" delta), so we only assert what the fallback
    // does emit: step-completed transitions + the workflow terminal.
    expect(types).toContain("step-completed");
    expect(types[types.length - 1]).toBe("workflow-completed");

    const stepCompletes = events.filter(
      (e): e is Extract<WorkflowRunEvent, { type: "step-completed" }> =>
        e.type === "step-completed",
    );
    expect(stepCompletes.map((e) => e.stepName).sort()).toEqual(["a", "b"]);
  });

  it("delivers workflow-failed when the run fails", async () => {
    const backing = new InMemoryWorkflowStorage();
    const handler = createWorkflowStorageHandler(backing);
    const remote = new RemoteWorkflowStorage({
      url: "http://test.local/storage",
      fetch: handler,
    });

    const runner = createWorkflowRunner({ storage: remote });

    const wf = workflow<number>({ name: "remote-sub-fail" })
      .step("boom", () => Pipeline.fail(new Error("kaboom") as never))
      .build();

    const events: WorkflowRunEvent[] = [];
    const collect = (async () => {
      for await (const ev of runner.subscribe("remote-sub-fail-1", { pollIntervalMs: 20 })) {
        events.push(ev);
        if (ev.type === "workflow-failed" || ev.type === "workflow-completed") break;
      }
    })();

    await runner.runSafe({ workflow: wf, workflowId: "remote-sub-fail-1", input: 0 });
    await collect;

    const types = events.map((e) => e.type);
    expect(types).toContain("step-failed");
    expect(types[types.length - 1]).toBe("workflow-failed");
  });
});
