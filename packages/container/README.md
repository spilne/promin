# @promin/container

Run workflow steps as isolated containers. Input is serialized to JSON, mounted in the container. Container executes, writes output. Runtime reads it back.

Three runtimes, same interface:

| Runtime               | Use for                 | Needs               |
| --------------------- | ----------------------- | ------------------- |
| `LocalProcessRuntime` | Dev/test                | Nothing (Bun.spawn) |
| `DockerRuntime`       | Staging, single machine | Docker Engine       |
| `K8sRuntime`          | Production, multi-node  | Kubernetes cluster  |

## Quick Start

```typescript
import { containerStep, LocalProcessRuntime } from "@promin/container";
import { MapStepRegistry, createWorker } from "@promin/workflow";

const runtime = new LocalProcessRuntime();

const registry = new MapStepRegistry();
registry.register(
  "train-model",
  ...containerStep({
    spec: {
      image: "my-ml-image:latest",
      command: ["python", "train.py"],
      memoryLimit: "4g",
      timeoutMs: 300_000,
    },
    runtime,
  }),
);

const worker = createWorker({
  storage,
  stepQueue,
  registry,
  queues: ["gpu"],
});
worker.start();
```

## I/O Protocol

Containers receive input and produce output via a simple file protocol:

```
Environment variables:
  PIPELINE_INPUT_PATH    → /pipeline/input.json  (or /pipeline/input/input.json on K8s)
  PIPELINE_OUTPUT_PATH   → /pipeline/output.json (or /pipeline/output/output.json on K8s)
  PIPELINE_STEP_NAME     → step name
  PIPELINE_WORKFLOW_ID   → workflow ID

Container reads input:
  input.json = { "input": ..., "prev": ..., "deps": {...}, "workflowId": "...", "stepName": "...", "attempt": 1 }

Container writes output:
  output.json = { "result": "any JSON value" }

If no output file: stdout is parsed as JSON (or returned as string).
```

Any language can implement a container step:

```python
# train.py
import json, os

with open(os.environ["PIPELINE_INPUT_PATH"]) as f:
    ctx = json.load(f)

model = train(ctx["prev"]["data"])

with open(os.environ["PIPELINE_OUTPUT_PATH"], "w") as f:
    json.dump({"accuracy": model.score, "path": "s3://models/latest"}, f)
```

## Runtimes

### LocalProcessRuntime

Runs commands as local subprocesses. No Docker needed. For dev and testing.

```typescript
import { LocalProcessRuntime } from "@promin/container";

const runtime = new LocalProcessRuntime();

const result = await runtime.run({
  spec: {
    image: "", // not used
    command: ["python", "train.py"],
    env: { MODEL_TYPE: "xgboost" },
    timeoutMs: 60_000,
  },
  input: JSON.stringify({ data: [1, 2, 3] }),
  stepName: "train",
  workflowId: "wf-1",
});

console.log(result.exitCode); // 0
console.log(result.output); // parsed from PIPELINE_OUTPUT_PATH
console.log(result.durationMs);
```

### DockerRuntime

Runs containers via `docker run` CLI. Volume-mounts a temp directory for I/O.

```typescript
import { DockerRuntime } from "@promin/container";

const runtime = new DockerRuntime({
  network: "workflows", // Docker network
  extraArgs: ["--gpus", "all"], // pass-through args
});

registry.register(
  "transcribe",
  ...containerStep({
    spec: {
      image: "openai/whisper:latest",
      command: ["python", "-m", "whisper", "--input", "/pipeline/input.json"],
      memoryLimit: "8g",
      cpuLimit: "4",
      timeoutMs: 600_000,
    },
    runtime,
  }),
);
```

### K8sRuntime

Creates Kubernetes Jobs. Input mounted via ConfigMap, output read from pod logs.

```typescript
import { K8sRuntime } from "@promin/container";

const runtime = new K8sRuntime({
  namespace: "ml-workflows",
  nodeSelector: { "nvidia.com/gpu": "true" },
  serviceAccount: "workflow-runner",
  imagePullSecrets: ["registry-creds"],
  ttlAfterFinished: 3600,
});

registry.register(
  "train-model",
  ...containerStep({
    spec: {
      image: "my-registry.com/ml-trainer:v2",
      command: ["python", "train.py"],
      memoryLimit: "16Gi",
      cpuLimit: "8",
      gpu: true,
      timeoutMs: 3600_000,
    },
    runtime,
    options: {
      retry: { maxRetries: 2 },
      onFailure: { fallback: () => ({ status: "failed", model: null }) },
    },
  }),
);
```

## Mixed Workflow — In-Process + Container Steps

Same workflow, some steps local, some containerized:

```typescript
import { workflow, createWorker } from "@promin/workflow";

const processVideo = workflow<{ videoId: string }>({
  name: "process-video",
  storage,
  dispatch: {
    stepQueue,
    routing: { transcribe: "gpu", "train-model": "gpu" },
  },
})
  .step("download", ({ input }) => downloadVideo(input.videoId)) // local
  .step("transcribe", { dependsOn: ["download"] }, fn) // → GPU worker (container)
  .step("summarize", { dependsOn: ["transcribe"] }, fn) // local
  .build();

// GPU worker runs container steps
const gpuWorker = createWorker({
  storage,
  stepQueue,
  registry: gpuRegistry, // has containerStep("transcribe") registered
  queues: ["gpu"],
});
```
