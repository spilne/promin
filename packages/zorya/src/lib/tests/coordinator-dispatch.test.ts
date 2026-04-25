// ---------------------------------------------------------------------------
// End-to-end test for coordinator-driven step dispatch (promin-2c29).
//
// Wires:
//   - ZoryaServer with `coordination: { enabled: true }`
//   - InMemoryStepQueue + InMemoryWorkerRegistry shared between server +
//     workers via an in-process fetch handler (no TCP, no real network).
//   - Two ZoryaWorker instances in `mode: "step"` advertising the same
//     workflow.
//
// Exercises the full path: trigger via /api/runs/trigger/:name → coordinator
// submits → enqueues steps → step-mode workers claim and execute → storage
// reflects per-step + workflow completion.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { Pipeline } from "@promin/core";
import {
  InMemoryStepQueue,
  InMemoryWorkerRegistry,
  InMemoryWorkflowStorage,
  workflow,
} from "@promin/workflow";
import { ZoryaClient, ZoryaWorker } from "@promin/zorya-client";
import { ZoryaServer } from "../../server/server.ts";

const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

async function pollUntil(
  predicate: () => boolean | Promise<boolean>,
  timeoutMs: number,
  intervalMs = 25,
): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) return true;
    await sleep(intervalMs);
  }
  return false;
}

describe("Coordinator-driven step dispatch (promin-2c29)", () => {
  it("triggers → coordinator dispatches steps → step-mode workers run them → workflow completes", async () => {
    const storage = new InMemoryWorkflowStorage();
    const stepQueue = new InMemoryStepQueue();
    const workerRegistry = new InMemoryWorkerRegistry();

    const server = new ZoryaServer({
      storage,
      workerProtocol: { stepQueue, workerRegistry },
      coordination: {
        enabled: true,
        pollIntervalMs: 50,
        stepPollIntervalMs: 25,
      },
    });
    expect(server.coordinator).toBeDefined();
    server.startCoordinator();

    // Loopback fetch — point ZoryaClient at the in-process server handler.
    const fetch = (req: Request) => server.handle(req);
    const client = new ZoryaClient({ url: "http://test.local", fetch });

    // Workflow with three independent steps so the coordinator schedules
    // them in parallel and each can land on a different worker.
    const wf = workflow<{ id: number }>({ name: "fan-out" })
      .step("a", ({ input }) => Pipeline.succeed(`a:${input.id}`))
      .step("b", ({ input }) => Pipeline.succeed(`b:${input.id}`))
      .step("c", ({ input }) => Pipeline.succeed(`c:${input.id}`))
      .build();

    const w1 = new ZoryaWorker({
      client,
      workflows: [wf],
      mode: "step",
      stepPolling: { intervalMs: 25, heartbeatMs: 1_000 },
      heartbeatIntervalMs: 1_000,
    });
    const w2 = new ZoryaWorker({
      client,
      workflows: [wf],
      mode: "step",
      stepPolling: { intervalMs: 25, heartbeatMs: 1_000 },
      heartbeatIntervalMs: 1_000,
    });
    await w1.start();
    await w2.start();

    // Trigger via the public HTTP endpoint to exercise the
    // CoordinatedTriggerService path.
    const triggered = await client.triggerWorkflow("fan-out", {
      input: { id: 7 },
      workflowId: "fan-out-1",
    });
    expect(triggered.workflowId).toBe("fan-out-1");

    // Wait for the workflow to complete via the coordinator + worker fleet.
    const ok = await pollUntil(
      async () => (await storage.loadWorkflow("fan-out-1"))?.status === "completed",
      5_000,
    );
    expect(ok).toBe(true);

    const state = await storage.loadWorkflow("fan-out-1");
    expect(state?.status).toBe("completed");
    expect(state?.steps.a?.status).toBe("completed");
    expect(state?.steps.b?.status).toBe("completed");
    expect(state?.steps.c?.status).toBe("completed");

    await w1.stop();
    await w2.stop();
    server.stop();
  });

  it("five-step fan-out completes under two step-mode workers", async () => {
    const storage = new InMemoryWorkflowStorage();
    const stepQueue = new InMemoryStepQueue();
    const workerRegistry = new InMemoryWorkerRegistry();

    const server = new ZoryaServer({
      storage,
      workerProtocol: { stepQueue, workerRegistry },
      coordination: { enabled: true, pollIntervalMs: 25, stepPollIntervalMs: 25 },
    });
    server.startCoordinator();

    const fetch = (req: Request) => server.handle(req);
    const client = new ZoryaClient({ url: "http://test.local", fetch });

    // Five independent steps. Each task gets claimed exactly once thanks
    // to the queue's atomic claim semantics, so two workers running in
    // parallel can drain them without double execution.
    const wf = workflow<number>({ name: "split" })
      .step("s1", () => Pipeline.succeed(1))
      .step("s2", () => Pipeline.succeed(2))
      .step("s3", () => Pipeline.succeed(3))
      .step("s4", () => Pipeline.succeed(4))
      .step("s5", () => Pipeline.succeed(5))
      .build();

    const mkWorker = (id: string) =>
      new ZoryaWorker({
        client,
        workflows: [wf],
        mode: "step",
        workerId: id,
        stepPolling: { intervalMs: 25, heartbeatMs: 1_000 },
        heartbeatIntervalMs: 1_000,
      });

    const w1 = mkWorker("w1");
    const w2 = mkWorker("w2");
    await w1.start();
    await w2.start();

    await client.triggerWorkflow("split", { input: 0, workflowId: "split-1" });

    const ok = await pollUntil(
      async () => (await storage.loadWorkflow("split-1"))?.status === "completed",
      5_000,
    );
    expect(ok).toBe(true);

    const state = await storage.loadWorkflow("split-1");
    for (const name of ["s1", "s2", "s3", "s4", "s5"]) {
      expect(state?.steps[name]?.status).toBe("completed");
    }

    await w1.stop();
    await w2.stop();
    server.stop();
  });

  it("rejects coordination.enabled without a stepQueue", () => {
    expect(
      () =>
        new ZoryaServer({
          storage: new InMemoryWorkflowStorage(),
          coordination: { enabled: true },
        }),
    ).toThrow(/stepQueue/);
  });
});
