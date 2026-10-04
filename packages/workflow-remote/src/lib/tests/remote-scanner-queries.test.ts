// ---------------------------------------------------------------------------
// Scanner queries over the wire: a sleep / signal scanner on a
// RemoteWorkflowStorage runs the backend's `listDueTimers` /
// `listSignalWakeups` instead of paging `listWorkflows`, and a backend
// without them surfaces a clear error.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import {
  FakeWallClock,
  InMemoryWorkflowStorage,
  createSignalScanner,
  createSleepScanner,
  type Workflow,
  type WorkflowRunner,
  type WorkflowStorage,
} from "@promin/workflow";
import { RemoteWorkflowStorage } from "../remote-workflow-storage.ts";
import { createWorkflowStorageHandler } from "../storage-http-handler.ts";

async function waitFor(predicate: () => boolean): Promise<void> {
  for (let i = 0; i < 2_000 && !predicate(); i++) {
    await new Promise<void>((r) => setImmediate(r));
  }
  expect(predicate()).toBe(true);
}

/** A remote storage over `backing` that records every RPC method name. */
function recordingRemote(backing: WorkflowStorage): {
  remote: RemoteWorkflowStorage;
  methods: string[];
} {
  const handler = createWorkflowStorageHandler(backing);
  const methods: string[] = [];
  const remote = new RemoteWorkflowStorage({
    url: "http://test.local/storage",
    fetch: async (req) => {
      const body = (await req.clone().json()) as { method: string };
      methods.push(body.method);
      return handler(req);
    },
  });
  return { remote, methods };
}

/** A runner that only records which runs a scanner resumed. */
function recordingRunner(resumed: string[]): WorkflowRunner {
  return {
    run: async (params: { workflowId: string }) => {
      resumed.push(params.workflowId);
    },
  } as unknown as WorkflowRunner;
}

const resolveWorkflow = () => ({ name: "scan-wf" }) as unknown as Workflow<unknown, unknown>;

describe("RemoteWorkflowStorage scanner queries", () => {
  it("the sleep scanner finds due timers through listDueTimers, not listWorkflows", async () => {
    const clock = FakeWallClock.create(10_000);
    const backing = new InMemoryWorkflowStorage({ clock });
    await backing.createWorkflow({ workflowId: "s1", workflowName: "scan-wf", input: 1 });
    await backing.suspendWorkflow({
      workflowId: "s1",
      stepName: "nap",
      stepUpdate: {
        status: "sleeping",
        stepType: "sleep",
        wakeAt: new Date(5_000),
      },
    });
    const { remote, methods } = recordingRemote(backing);
    const resumed: string[] = [];
    const scanner = createSleepScanner({
      storage: remote,
      runner: recordingRunner(resumed),
      resolveWorkflow,
      clock,
    });

    void scanner.start();
    await waitFor(() => resumed.length === 1);
    await scanner.stop();

    expect(resumed).toEqual(["s1"]);
    expect(methods).toContain("listDueTimers");
    expect(methods).not.toContain("listWorkflows");
  });

  it("the signal scanner finds delivered signals through listSignalWakeups", async () => {
    const clock = FakeWallClock.create(10_000);
    const backing = new InMemoryWorkflowStorage({ clock });
    await backing.createWorkflow({ workflowId: "w1", workflowName: "scan-wf", input: 1 });
    await backing.suspendWorkflow({
      workflowId: "w1",
      stepName: "wait",
      stepUpdate: {
        status: "waiting_for_signal",
        stepType: "signal",
        signalName: "go",
      },
    });
    await backing.deliverSignal({ workflowId: "w1", signalName: "go", payload: { ok: true } });
    const { remote, methods } = recordingRemote(backing);
    const resumed: string[] = [];
    const scanner = createSignalScanner({
      storage: remote,
      runner: recordingRunner(resumed),
      resolveWorkflow,
      clock,
    });

    void scanner.start();
    await waitFor(() => resumed.length === 1);
    await scanner.stop();

    expect(resumed).toEqual(["w1"]);
    expect(methods).toContain("listSignalWakeups");
    expect(methods).not.toContain("listWorkflows");
    expect(methods).not.toContain("loadSignals");
  });

  it("a backend without the scanner queries surfaces a clear error", async () => {
    const backing = new InMemoryWorkflowStorage();
    const bare = new Proxy(backing, {
      get: (target, prop) =>
        prop === "listDueTimers" || prop === "listSignalWakeups" || prop === "listOrphanedRuns"
          ? undefined
          : Reflect.get(target, prop, target),
    });
    const { remote } = recordingRemote(bare);

    await expect(remote.listDueTimers({ now: new Date(), limit: 1 })).rejects.toThrow(
      "storage does not implement listDueTimers",
    );
    await expect(remote.listSignalWakeups({ limit: 1 })).rejects.toThrow(
      "storage does not implement listSignalWakeups",
    );
    await expect(
      remote.listOrphanedRuns({ now: new Date(), updatedBefore: new Date(), limit: 1 }),
    ).rejects.toThrow("storage does not implement listOrphanedRuns");
  });
});
