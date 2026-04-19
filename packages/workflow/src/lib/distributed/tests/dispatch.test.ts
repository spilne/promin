import { describe, it, expect } from "bun:test";
import { Pipeline } from "@promin/core";
import { workflow, InMemoryWorkflowStorage } from "../../durable/index.ts";
import { createWorkflowRunner } from "../../durable/workflow-runner.ts";
import { MapStepRegistry } from "../step-registry.ts";
import { InMemoryStepQueue } from "../in-memory-step-queue.ts";
import { createWorker } from "../worker.ts";

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
    gpuRegistry.register("transcribe", (ctx) => {
      log.push("transcribe:remote");
      return Pipeline.succeed(`transcribed: ${ctx.prev}`);
    });

    const gpuWorker = createWorker({
      storage,
      stepQueue,
      registry: gpuRegistry,
      capabilities: ["gpu"],
      pollIntervalMs: 50,
    });
    void gpuWorker.start();

    // Run workflow with dispatch — "transcribe" goes to GPU worker, rest runs locally
    const wf = workflow<{ videoId: string }>({
      name: "hybrid",
      dispatch: {
        stepQueue,
        remoteSteps: ["transcribe"],
        pollIntervalMs: 100,
      },
    })
      .step("download", ({ input }) => {
        log.push("download:local");
        return Pipeline.succeed(`video-${input.videoId}`);
      })
      .step(
        "transcribe",
        { dependsOn: ["download"] },
        ({ deps }) => Pipeline.succeed(`transcribed: ${deps.download}`),
        { needs: ["gpu"] },
      )
      .step("format", { dependsOn: ["transcribe"] }, ({ deps }) => {
        log.push("format:local");
        return Pipeline.succeed(`formatted: ${deps.transcribe}`);
      })
      .build();
    const runner = createWorkflowRunner({ storage });
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
    const wf = workflow<number>({
      name: "local-retry",
      dispatch: {
        stepQueue,
        remoteSteps: [], // nothing dispatched
      },
    })
      .step(
        "flaky",
        ({ input }) => {
          attempts++;
          if (attempts < 3) return Pipeline.fail(new Error("transient") as never);
          return Pipeline.succeed(input * 2);
        },
        {
          retry: { maxRetries: 5 },
        },
      )
      .build();
    const runner = createWorkflowRunner({ storage });
    const result = await runner.run({ workflow: wf, workflowId: "local-1", input: 5 });

    expect(result).toBe(10);
    expect(attempts).toBe(3);
  });

  it("no dispatch config — everything runs locally as a normal workflow", async () => {
    const storage = new InMemoryWorkflowStorage();

    const wf = workflow<number>({ name: "no-dispatch" })
      .step("double", ({ input }) => Pipeline.succeed(input * 2))
      .step("add", ({ prev }) => Pipeline.succeed(prev + 100))
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
    registry.register("bad-step", () => {
      throw new Error("remote failure");
    });

    const worker = createWorker({
      storage,
      stepQueue,
      registry,
      capabilities: ["remote"],
      pollIntervalMs: 50,
    });
    void worker.start();

    const wf = workflow<string>({
      name: "dispatch-fail",
      dispatch: {
        stepQueue,
        remoteSteps: ["bad-step"],
        pollIntervalMs: 100,
      },
    })
      .step("local-ok", () => Pipeline.succeed("ok"))
      .step(
        "bad-step",
        { dependsOn: ["local-ok"] },
        () => Pipeline.succeed("should not run locally"),
        { needs: ["remote"] },
      )
      .build();
    const runner = createWorkflowRunner({ storage });
    const { error } = await runner.runSafe({
      workflow: wf,
      workflowId: "fail-1",
      input: "x",
    });

    await worker.stop();

    expect(error).not.toBeNull();
  });
});
