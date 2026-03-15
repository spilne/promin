import { describe, it, expect } from "bun:test";
import {
  Pipeline,
  MapStepRegistry,
  InMemoryWorkflowStorage,
  InMemoryStepQueue,
  createWorker,
} from "@promin/core";
import { LocalProcessRuntime } from "./local-process-runtime.ts";
import { containerStep } from "./container-step.ts";

// ---------------------------------------------------------------------------
// LocalProcessRuntime
// ---------------------------------------------------------------------------

describe("LocalProcessRuntime", () => {
  it("runs a command and captures stdout", async () => {
    const runtime = new LocalProcessRuntime();

    const result = await runtime.run({
      spec: {
        image: "", // not used for local
        command: ["echo", "hello world"],
      },
      input: "{}",
      stepName: "test",
      workflowId: "wf-1",
    });

    expect(result.exitCode).toBe(0);
    expect(result.stdout.trim()).toBe("hello world");
  });

  it("captures stderr and non-zero exit code", async () => {
    const runtime = new LocalProcessRuntime();

    const result = await runtime.run({
      spec: {
        image: "",
        command: ["sh", "-c", "echo 'error msg' >&2 && exit 1"],
      },
      input: "{}",
      stepName: "fail-test",
      workflowId: "wf-2",
    });

    expect(result.exitCode).toBe(1);
    expect(result.stderr.trim()).toBe("error msg");
  });

  it("passes input via env PIPELINE_INPUT_PATH", async () => {
    const runtime = new LocalProcessRuntime();

    const result = await runtime.run({
      spec: {
        image: "",
        command: ["sh", "-c", "cat $PIPELINE_INPUT_PATH"],
      },
      input: JSON.stringify({ greeting: "hello" }),
      stepName: "input-test",
      workflowId: "wf-3",
    });

    expect(result.exitCode).toBe(0);
    expect(JSON.parse(result.stdout.trim())).toEqual({ greeting: "hello" });
  });

  it("reads output from PIPELINE_OUTPUT_PATH", async () => {
    const runtime = new LocalProcessRuntime();

    const result = await runtime.run({
      spec: {
        image: "",
        command: ["sh", "-c", `echo '{"result": 42}' > $PIPELINE_OUTPUT_PATH`],
      },
      input: "{}",
      stepName: "output-test",
      workflowId: "wf-4",
    });

    expect(result.exitCode).toBe(0);
    expect(result.output).toEqual({ result: 42 });
  });

  it("passes custom env variables", async () => {
    const runtime = new LocalProcessRuntime();

    const result = await runtime.run({
      spec: {
        image: "",
        command: ["sh", "-c", "echo $MY_VAR"],
        env: { MY_VAR: "custom-value" },
      },
      input: "{}",
      stepName: "env-test",
      workflowId: "wf-5",
    });

    expect(result.stdout.trim()).toBe("custom-value");
  });

  it("respects timeout", async () => {
    const runtime = new LocalProcessRuntime();

    const result = await runtime.run({
      spec: {
        image: "",
        command: ["sleep", "10"],
        timeoutMs: 100,
      },
      input: "{}",
      stepName: "timeout-test",
      workflowId: "wf-6",
    });

    expect(result.exitCode).toBe(-1);
    expect(result.stderr).toContain("Timed out");
    expect(result.durationMs).toBeLessThan(500);
  });

  it("reports duration", async () => {
    const runtime = new LocalProcessRuntime();

    const result = await runtime.run({
      spec: {
        image: "",
        command: ["sh", "-c", "sleep 0.05"],
      },
      input: "{}",
      stepName: "duration-test",
      workflowId: "wf-7",
    });

    expect(result.durationMs).toBeGreaterThan(30);
  });

  it("cleans up temp files", async () => {
    const runtime = new LocalProcessRuntime();
    const { existsSync } = require("node:fs");

    // Run and capture the temp path from env
    const result = await runtime.run({
      spec: {
        image: "",
        command: ["sh", "-c", "echo $PIPELINE_INPUT_PATH"],
      },
      input: "{}",
      stepName: "cleanup-test",
      workflowId: "wf-8",
    });

    const inputPath = result.stdout.trim();
    // Temp dir should be cleaned up
    expect(existsSync(inputPath)).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// containerStep — integration with worker
// ---------------------------------------------------------------------------

describe("containerStep", () => {
  it("creates a step handler from container spec", async () => {
    const runtime = new LocalProcessRuntime();
    const [handler] = containerStep({
      spec: {
        image: "",
        command: ["sh", "-c", 'echo \'{"msg":"from-container"}\' > $PIPELINE_OUTPUT_PATH'],
      },
      runtime,
    });

    const result = await (
      handler({
        input: "hello",
        prev: "world",
        deps: {},
        workflowId: "wf-cs-1",
        stepName: "test",
        attempt: 1,
      }) as Pipeline<unknown, any>
    ).runPromise();

    expect(result).toEqual({ msg: "from-container" });
  });

  it("integrates with worker via StepRegistry", async () => {
    const storage = new InMemoryWorkflowStorage();
    const queue = new InMemoryStepQueue();
    const registry = new MapStepRegistry();
    const runtime = new LocalProcessRuntime();

    const [handler, options] = containerStep({
      spec: {
        image: "",
        command: [
          "sh",
          "-c",
          // Echo a JSON output to the output file
          `echo '{"result":"container-ok"}' > $PIPELINE_OUTPUT_PATH`,
        ],
      },
      runtime,
    });

    registry.register("container-step", handler, options);

    await storage.createWorkflow({ workflowId: "cs-1", workflowName: "test", input: {} });
    await queue.enqueue({
      workflowId: "cs-1",
      stepName: "container-step",
      queue: "default",
      input: {},
      prevResults: {},
    });

    const worker = createWorker({
      storage,
      stepQueue: queue,
      registry,
      queues: ["default"],
      pollIntervalMs: 50,
    });

    void worker.start();
    await new Promise((r) => setTimeout(r, 500));
    await worker.stop();

    const state = await storage.loadWorkflow("cs-1");
    expect(state?.steps["container-step"]?.status).toBe("completed");
    expect(state?.steps["container-step"]?.result).toEqual({ result: "container-ok" });
  });
});
