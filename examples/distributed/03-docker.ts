/**
 * DockerRuntime — run steps as Docker containers
 *
 * Each step runs in an isolated Docker container with:
 * - Volume-mounted I/O directory
 * - Resource limits (memory, CPU)
 * - Custom environment variables
 * - Timeout with automatic kill
 *
 * Requires: Docker Engine running locally.
 */

import { Pipeline, MapStepRegistry, workflow, InMemoryWorkflowStorage, InMemoryStepQueue } from "@promin/core";
import { containerStep, DockerRuntime } from "@promin/container";

const storage = new InMemoryWorkflowStorage();
const stepQueue = new InMemoryStepQueue();

const runtime = new DockerRuntime({
  network: "host",             // or a custom Docker network
  // extraArgs: ["--gpus", "all"],  // for GPU access
});

// ---------------------------------------------------------------------------
// Python data processing step
// ---------------------------------------------------------------------------

const registry = new MapStepRegistry();

// Python step — runs in a Python container
const [pythonHandler, pythonOptions] = containerStep({
  spec: {
    image: "python:3.12-slim",
    command: [
      "python", "-c",
      `
import json, os
ctx = json.load(open(os.environ['PIPELINE_INPUT_PATH']))
data = ctx.get('prev', [])
result = {'count': len(data), 'sum': sum(data) if data else 0}
json.dump(result, open(os.environ['PIPELINE_OUTPUT_PATH'], 'w'))
      `,
    ],
    memoryLimit: "256m",
    cpuLimit: "0.5",
    timeoutMs: 30_000,
  },
  runtime,
});
registry.register("python-aggregate", pythonHandler, pythonOptions);

// Node step — runs in a Node container
const [nodeHandler, nodeOptions] = containerStep({
  spec: {
    image: "node:20-alpine",
    command: [
      "node", "-e",
      `
const fs = require('fs');
const ctx = JSON.parse(fs.readFileSync(process.env.PIPELINE_INPUT_PATH, 'utf-8'));
const result = { transformed: String(ctx.prev).toUpperCase(), timestamp: Date.now() };
fs.writeFileSync(process.env.PIPELINE_OUTPUT_PATH, JSON.stringify(result));
      `,
    ],
    memoryLimit: "128m",
    timeoutMs: 10_000,
  },
  runtime,
});
registry.register("node-transform", nodeHandler, nodeOptions);

// ML training step — heavy resources
const [mlHandler, mlOptions] = containerStep({
  spec: {
    image: "pytorch/pytorch:2.0.0-cuda11.7-cudnn8-runtime",
    command: ["python", "/app/train.py"],
    memoryLimit: "8g",
    cpuLimit: "4",
    gpu: true,
    timeoutMs: 3600_000, // 1 hour
    env: {
      WANDB_API_KEY: process.env.WANDB_API_KEY ?? "",
      MODEL_NAME: "gpt2-finetune",
    },
  },
  runtime,
  options: {
    retry: { maxRetries: 1 },
    onFailure: { fallback: () => ({ status: "failed", error: "training failed" }) },
  },
});
registry.register("train-model", mlHandler, mlOptions);

// ---------------------------------------------------------------------------
// Hybrid workflow — local + Docker steps
// ---------------------------------------------------------------------------

const mlPipeline = workflow<{ dataset: string; epochs: number }>({
  name: "ml-pipeline",
  storage,
  dispatch: {
    stepQueue,
    routing: { "train-model": "gpu" },
  },
})
  // Local: validate input
  .step("validate", ({ input }) =>
    Pipeline.succeed({ dataset: input.dataset, epochs: input.epochs, valid: true }),
  )
  // Docker: train model on GPU
  .step("train-model", { dependsOn: ["validate"] }, ({ deps }) =>
    Pipeline.succeed({ model: "trained", dataset: deps.validate.dataset }),
  )
  // Local: save results
  .step("save", { dependsOn: ["train-model"] }, ({ deps }) =>
    Pipeline.succeed({ saved: true, model: deps["train-model"] }),
  )
  .build();

export { registry, mlPipeline };
