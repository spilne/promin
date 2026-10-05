import { succeed, fail } from "@spilne/perfect-core";
import { describe, it, expect } from "bun:test";
import { workflow, InMemoryWorkflowStorage } from "../../../index.ts";
import { createWorkflowRunner } from "../../durable/workflow-runner.ts";
import { MapStepRegistry } from "../step-registry.ts";
import { InMemoryStepQueue } from "../in-memory-step-queue.ts";
import { createWorker } from "../worker.ts";
import { StepQueueExecutor } from "../step-queue-executor.ts";
import { RoutingStepExecutor } from "../../durable/runner/routing-step-executor.ts";

/** A runner that sends `remoteSteps` to workers over `stepQueue`. */
function routedRunner(params: {
  storage: InMemoryWorkflowStorage;
  stepQueue: InMemoryStepQueue;
  remoteSteps: readonly string[];
  pollIntervalMs: number;
}) {
  const { storage, stepQueue, remoteSteps, pollIntervalMs } = params;
  return createWorkflowRunner({
    storage,
    stepExecutor: new RoutingStepExecutor({
      remote: new StepQueueExecutor({ stepQueue, storage, pollIntervalMs }),
      remoteSteps,
      storage,
    }),
  });
}

// ---------------------------------------------------------------------------
// Hybrid dispatch — engine runs locally, dispatches specific steps to workers
// ---------------------------------------------------------------------------

describe("Hybrid dispatch — run simple steps locally, offload heavy steps to workers", () => {
  it("video download runs locally, transcription offloaded to GPU worker", async () => {
    const storage = new InMemoryWorkflowStorage();
    const stepQueue = new InMemoryStepQueue();
    const log: string[] = [];

    // GPU worker — only handles "transcribe"
    const gpuRegistry = new MapStepRegistry();
    gpuRegistry.register({
      stepName: "transcribe",
      handler: (ctx) => {
        log.push("transcribe:remote");
        return succeed(`transcribed: ${ctx.prev}`);
      },
    });

    const gpuWorker = createWorker({
      stepQueue,
      registry: gpuRegistry,
      capabilities: ["gpu"],
      pollIntervalMs: 50,
    });
    void gpuWorker.start();

    // "transcribe" goes to the GPU worker, the rest runs locally
    const wf = workflow<{ videoId: string }>({ name: "hybrid" })
      .step("download", ({ input }) => {
        log.push("download:local");
        return succeed(`video-${input.videoId}`);
      })
      .step(
        "transcribe",
        { dependsOn: ["download"] },
        ({ deps }) => succeed(`transcribed: ${deps.download}`),
        { needs: ["gpu"] },
      )
      .step("format", { dependsOn: ["transcribe"] }, ({ deps }) => {
        log.push("format:local");
        return succeed(`formatted: ${deps.transcribe}`);
      })
      .build();
    const runner = routedRunner({
      storage,
      stepQueue,
      remoteSteps: ["transcribe"],
      pollIntervalMs: 100,
    });
    await runner.run({ workflow: wf, workflowId: "hybrid-1", input: { videoId: "abc" } });

    await gpuWorker.stop();

    // download ran locally, transcribe ran on GPU worker, format ran locally
    expect(log).toContain("download:local");
    expect(log).toContain("transcribe:remote");
    expect(log).toContain("format:local");
  });

  it("local steps still get retries and checkpointing — full engine features", async () => {
    const storage = new InMemoryWorkflowStorage();
    const stepQueue = new InMemoryStepQueue();
    let attempts = 0;

    // No worker needed for this test — only local steps
    const wf = workflow<number>({ name: "local-retry" })
      .step(
        "flaky",
        ({ input }) => {
          attempts++;
          if (attempts < 3) return fail(new Error("transient") as never);
          return succeed(input * 2);
        },
        {
          retry: { maxRetries: 5 },
        },
      )
      .build();
    // Nothing dispatched.
    const runner = routedRunner({ storage, stepQueue, remoteSteps: [], pollIntervalMs: 100 });
    const result = await runner.run({ workflow: wf, workflowId: "local-1", input: 5 });

    expect(result).toBe(10);
    expect(attempts).toBe(3);
  });

  it("no step executor — everything runs locally as a normal workflow", async () => {
    const storage = new InMemoryWorkflowStorage();

    const wf = workflow<number>({ name: "no-dispatch" })
      .step("double", ({ input }) => succeed(input * 2))
      .step("add", ({ prev }) => succeed(prev + 100))
      .build();
    const runner = createWorkflowRunner({ storage });
    const result = await runner.run({ workflow: wf, workflowId: "nd-1", input: 5 });

    expect(result).toBe(110);
  });

  it("remote worker step fails — error propagates back to the calling workflow", async () => {
    const storage = new InMemoryWorkflowStorage();
    const stepQueue = new InMemoryStepQueue();

    // Worker that fails
    const registry = new MapStepRegistry();
    registry.register({
      stepName: "bad-step",
      handler: () => {
        throw new Error("remote failure");
      },
    });

    const worker = createWorker({
      stepQueue,
      registry,
      capabilities: ["remote"],
      pollIntervalMs: 50,
    });
    void worker.start();

    const wf = workflow<string>({ name: "dispatch-fail" })
      .step("local-ok", () => succeed("ok"))
      .step("bad-step", { dependsOn: ["local-ok"] }, () => succeed("should not run locally"), {
        needs: ["remote"],
      })
      .build();
    const runner = routedRunner({
      storage,
      stepQueue,
      remoteSteps: ["bad-step"],
      pollIntervalMs: 100,
    });
    const { error } = await runner.runSafe({
      workflow: wf,
      workflowId: "fail-1",
      input: "x",
    });

    await worker.stop();

    expect(error).not.toBeNull();
  });
});
