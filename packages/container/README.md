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
import { InMemoryWorkflowStorage } from "@promin/workflow";
import { createWorker, InMemoryStepQueue, MapStepRegistry } from "@promin/workflow/distributed";

const runtime = new LocalProcessRuntime();

const [handler, options] = containerStep({
  spec: {
    image: "my-ml-image:latest",
    command: ["python", "train.py"],
    memoryLimit: "4g",
    timeoutMs: 300_000,
  },
  runtime,
});
const registry = new MapStepRegistry();
registry.register({ stepName: "train-model", handler, ...options });

const worker = createWorker({
  storage: new InMemoryWorkflowStorage(), // the shared workflow storage in production
  stepQueue: new InMemoryStepQueue(), // the shared step queue
  registry,
  capabilities: ["gpu"], // claims steps declared with { needs: ["gpu"] }
});
void worker.start();
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
import { containerStep, DockerRuntime } from "@promin/container";
import { MapStepRegistry } from "@promin/workflow/distributed";

const runtime = new DockerRuntime({
  network: "workflows", // Docker network
  extraArgs: ["--gpus", "all"], // pass-through args
});

const [handler, options] = containerStep({
  spec: {
    image: "openai/whisper:latest",
    command: ["python", "-m", "whisper", "--input", "/pipeline/input.json"],
    memoryLimit: "8g",
    cpuLimit: "4",
    timeoutMs: 600_000,
  },
  runtime,
});
const registry = new MapStepRegistry();
registry.register({ stepName: "transcribe", handler, ...options });
```

### K8sRuntime

Creates Kubernetes Jobs. Input mounted via ConfigMap, output read from pod logs.

```typescript
import { containerStep, K8sRuntime } from "@promin/container";
import { MapStepRegistry } from "@promin/workflow/distributed";

const runtime = new K8sRuntime({
  namespace: "ml-workflows",
  nodeSelector: { "nvidia.com/gpu": "true" },
  serviceAccount: "workflow-runner",
  imagePullSecrets: ["registry-creds"],
  ttlAfterFinished: 3600,
});

const [handler, options] = containerStep({
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
});
const registry = new MapStepRegistry();
registry.register({ stepName: "train-model", handler, ...options });
```

## Mixed Workflow — In-Process + Container Steps

Same workflow, some steps local, some containerized:

```typescript
import { containerStep, DockerRuntime } from "@promin/container";
import {
  createWorkflowRunner,
  InMemoryWorkflowStorage,
  RoutingStepExecutor,
  workflow,
} from "@promin/workflow";
import {
  createWorker,
  InMemoryStepQueue,
  MapStepRegistry,
  StepQueueExecutor,
} from "@promin/workflow/distributed";
import { succeed } from "@spilne/perfect-core";

const storage = new InMemoryWorkflowStorage();
const stepQueue = new InMemoryStepQueue();

const processVideo = workflow<{ videoId: string }>({ name: "process-video" })
  .step("download", ({ input }) => succeed({ path: `/tmp/${input.videoId}.mp4` })) // local
  .step("transcribe", { dependsOn: ["download"] }, () => succeed({ text: "" }), { needs: ["gpu"] }) // → GPU worker
  .step("summarize", { dependsOn: ["transcribe"] }, ({ deps }) => succeed(deps.transcribe.text)) // local
  .build();

// "transcribe" goes through the step queue; every other step runs in-process.
const runner = createWorkflowRunner({
  storage,
  stepExecutor: new RoutingStepExecutor({
    remote: new StepQueueExecutor({ stepQueue, storage }),
    remoteSteps: ["transcribe"],
    storage,
  }),
});

// GPU worker runs the container step.
const [handler, options] = containerStep({
  spec: { image: "openai/whisper:latest", command: ["python", "-m", "whisper"] },
  runtime: new DockerRuntime(),
});
const gpuRegistry = new MapStepRegistry();
gpuRegistry.register({ stepName: "transcribe", handler, ...options });
const gpuWorker = createWorker({
  storage,
  stepQueue,
  registry: gpuRegistry,
  capabilities: ["gpu"],
});
void gpuWorker.start();

await runner.run({ workflow: processVideo, workflowId: "video-1", input: { videoId: "abc" } });
```
