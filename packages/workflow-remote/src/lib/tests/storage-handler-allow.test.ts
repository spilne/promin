// ---------------------------------------------------------------------------
// `allow` scoping on the storage RPC handler, and `RUNNER_STORAGE_METHODS`
// being enough for a runner: the journal replay conformance suite and a
// stepped workflow run through a handler limited to it.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { InMemoryWorkflowStorage, createWorkflowRunner, workflow } from "@promin/workflow";
import { journalReplayTestSuite } from "@promin/workflow/testing";
import { RemoteWorkflowStorage } from "../remote-workflow-storage.ts";
import { RUNNER_STORAGE_METHODS, createWorkflowStorageHandler } from "../storage-http-handler.ts";

function runnerScoped(backing = new InMemoryWorkflowStorage()): RemoteWorkflowStorage {
  return new RemoteWorkflowStorage({
    url: "http://test.local/storage",
    fetch: createWorkflowStorageHandler(backing, {
      allow: ({ method }) => RUNNER_STORAGE_METHODS.has(method),
    }),
  });
}

journalReplayTestSuite(() => runnerScoped());

describe("storage handler allow", () => {
  it("answers 403 for a method outside the scope without touching storage", async () => {
    const backing = new InMemoryWorkflowStorage();
    let purged = false;
    backing.purgeCompleted = async () => {
      purged = true;
      return 0;
    };
    const handler = createWorkflowStorageHandler(backing, {
      allow: ({ method }) => RUNNER_STORAGE_METHODS.has(method),
    });
    const res = await handler(
      new Request("http://x/storage", {
        method: "POST",
        body: JSON.stringify({ method: "purgeCompleted", params: { olderThan: 0 } }),
      }),
    );
    expect(res.status).toBe(403);
    expect(purged).toBe(false);
  });

  it("hands the request to allow", async () => {
    const seen: string[] = [];
    const handler = createWorkflowStorageHandler(new InMemoryWorkflowStorage(), {
      allow: ({ method, req }) => {
        seen.push(`${method}:${req.headers.get("x-role")}`);
        return true;
      },
    });
    const res = await handler(
      new Request("http://x/storage", {
        method: "POST",
        headers: { "x-role": "worker" },
        body: JSON.stringify({ method: "loadWorkflow", params: { workflowId: "nope" } }),
      }),
    );
    expect(res.status).toBe(200);
    expect(seen).toEqual(["loadWorkflow:worker"]);
  });

  it("answers 404, not a prototype lookup, for inherited names", async () => {
    const handler = createWorkflowStorageHandler(new InMemoryWorkflowStorage());
    const res = await handler(
      new Request("http://x/storage", {
        method: "POST",
        body: JSON.stringify({ method: "toString", params: {} }),
      }),
    );
    expect(res.status).toBe(404);
  });

  it("keeps signal-token methods out of the runner scope", () => {
    for (const m of [
      "createSignalToken",
      "findSignalTokenById",
      "completeSignalToken",
      "listSignalTokensForWorkflow",
      "purgeCompleted",
      "cancelWorkflow",
      "resetSteps",
    ] as const) {
      expect(RUNNER_STORAGE_METHODS.has(m)).toBe(false);
    }
  });

  it("runs a stepped workflow end to end within the runner scope", async () => {
    const backing = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage: runnerScoped(backing) });
    const wf = workflow<{ n: number }>({ name: "scoped" })
      .stepAsync("double", async ({ input }) => input.n * 2)
      .stepAsync("inc", async ({ input }) => input.n + 1)
      .build();
    await runner.run({ workflow: wf, workflowId: "scoped-1", input: { n: 4 } });
    expect((await backing.loadWorkflow("scoped-1"))?.status).toBe("completed");
  });
});
