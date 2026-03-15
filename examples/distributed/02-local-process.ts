/**
 * LocalProcessRuntime — run container steps as local subprocesses
 *
 * No Docker needed. Great for dev/test. The "container" is just a
 * local command that reads PIPELINE_INPUT_PATH and writes PIPELINE_OUTPUT_PATH.
 */

import { MapStepRegistry, InMemoryWorkflowStorage, InMemoryStepQueue, createWorker } from "@promin/core";
import { containerStep, LocalProcessRuntime } from "@promin/container";

const storage = new InMemoryWorkflowStorage();
const queue = new InMemoryStepQueue();
const runtime = new LocalProcessRuntime();

// ---------------------------------------------------------------------------
// Register steps that run as local processes
// ---------------------------------------------------------------------------

const registry = new MapStepRegistry();

// Shell script step — reads input, processes, writes output
const [wordCountHandler] = containerStep({
  spec: {
    image: "", // not used for local
    command: [
      "sh", "-c",
      // Count words in the input text, write result as JSON
      `text=$(cat $PIPELINE_INPUT_PATH | grep -o '"prev":"[^"]*"' | cut -d'"' -f4) && \
       count=$(echo "$text" | wc -w | tr -d ' ') && \
       echo "{\"wordCount\": $count}" > $PIPELINE_OUTPUT_PATH`,
    ],
    timeoutMs: 5_000,
  },
  runtime,
});
registry.register("word-count", wordCountHandler);

// Node.js script step
const [transformHandler] = containerStep({
  spec: {
    image: "",
    command: [
      "bun", "-e",
      `const fs = require('fs');
       const ctx = JSON.parse(fs.readFileSync(process.env.PIPELINE_INPUT_PATH, 'utf-8'));
       const result = { upper: String(ctx.prev).toUpperCase(), length: String(ctx.prev).length };
       fs.writeFileSync(process.env.PIPELINE_OUTPUT_PATH, JSON.stringify(result));`,
    ],
    timeoutMs: 5_000,
  },
  runtime,
});
registry.register("transform", transformHandler);

// Simple echo step — output via stdout (no output file)
const [echoHandler] = containerStep({
  spec: {
    image: "",
    command: ["sh", "-c", `echo '{"echo": true}'`],
  },
  runtime,
});
registry.register("echo", echoHandler);

// ---------------------------------------------------------------------------
// Run
// ---------------------------------------------------------------------------

async function main() {
  await storage.createWorkflow({ workflowId: "local-1", workflowName: "test", input: {} });

  await queue.enqueue({
    workflowId: "local-1",
    stepName: "echo",
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

  const state = await storage.loadWorkflow("local-1");
  console.log("Result:", state?.steps["echo"]?.result);
  // { echo: true }
}

export { registry, main };
