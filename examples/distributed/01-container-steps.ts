/**
 * Container steps — run workflow steps in isolated containers
 *
 * Business flow:
 * 1. A video is submitted for processing
 * 2. The video is validated locally (lightweight check, no special hardware needed)
 * 3. Audio transcription and video encoding are dispatched to GPU worker machines
 * 4. Each heavy step runs inside its own container (Whisper for transcription, FFmpeg for encoding)
 * 5. A coordinator routes steps to the right worker pool based on hardware requirements
 * 6. Results from all parallel steps are collected and saved together
 *
 * Steps can be written in any language; the container contract is input JSON in, output JSON out.
 */

import {
  workflow,
  Pipeline,
  InMemoryWorkflowStorage,
  MapStepRegistry,
  InMemoryStepQueue,
  createWorker,
  createCoordinator,
} from "@promin/core";
import {
  containerStep,
  LocalProcessRuntime,
  DockerRuntime,
  K8sRuntime,
} from "@promin/container";

const storage = new InMemoryWorkflowStorage();
const stepQueue = new InMemoryStepQueue();

// ---------------------------------------------------------------------------
// Pick runtime based on environment
// ---------------------------------------------------------------------------

function createRuntime() {
  const env = process.env.RUNTIME ?? "local";

  switch (env) {
    case "docker":
      return new DockerRuntime({ network: "workflows" });
    case "k8s":
      return new K8sRuntime({
        namespace: "ml-workflows",
        nodeSelector: { "nvidia.com/gpu": "true" },
        serviceAccount: "workflow-runner",
      });
    default:
      return new LocalProcessRuntime();
  }
}

const runtime = createRuntime();

// ---------------------------------------------------------------------------
// Register container-backed steps
// ---------------------------------------------------------------------------

const gpuRegistry = new MapStepRegistry();

// Python ML training step
const [trainHandler, trainOptions] = containerStep({
  spec: {
    image: "my-ml-image:latest",
    command: ["python", "train.py"],
    memoryLimit: "8g",
    gpu: true,
    timeoutMs: 3600_000,
    env: { WANDB_PROJECT: "my-project" },
  },
  runtime,
  options: {
    retry: { maxRetries: 2 },
    onFailure: { fallback: () => ({ status: "failed", model: null }) },
  },
});
gpuRegistry.register("train-model", trainHandler, trainOptions);

// Go video encoding step
const [encodeHandler, encodeOptions] = containerStep({
  spec: {
    image: "video-encoder:latest",
    command: ["./encode", "--format", "h265"],
    memoryLimit: "4g",
    cpuLimit: "4",
    timeoutMs: 600_000,
  },
  runtime,
});
gpuRegistry.register("encode-video", encodeHandler, encodeOptions);

// Whisper transcription step
const [transcribeHandler, transcribeOptions] = containerStep({
  spec: {
    image: "openai/whisper:latest",
    command: ["python", "-m", "whisper"],
    memoryLimit: "4g",
    gpu: true,
    timeoutMs: 300_000,
  },
  runtime,
});
gpuRegistry.register("transcribe", transcribeHandler, transcribeOptions);

// ---------------------------------------------------------------------------
// Workflow — mix of local and container steps
// ---------------------------------------------------------------------------

const processVideo = workflow<{ videoId: string; s3Path: string }>({
  name: "process-video",
  storage,
  dispatch: {
    stepQueue,
    routing: { "transcribe": "gpu", "encode-video": "gpu" },
  },
})
  .step("validate", ({ input }) =>
    Pipeline.succeed({ videoId: input.videoId, inputPath: input.s3Path, valid: true }),
  )
  .step("transcribe", { dependsOn: ["validate"] }, ({ deps }) =>
    Pipeline.succeed({ transcript: `transcribed: ${deps.validate.videoId}` }),
  )
  .step("encode-video", { dependsOn: ["validate"] }, ({ deps }) =>
    Pipeline.succeed({ encoded: `encoded: ${deps.validate.videoId}` }),
  )
  .step("save", { dependsOn: ["transcribe", "encode-video"] }, ({ deps }) =>
    Pipeline.succeed({ transcript: deps.transcribe, video: deps["encode-video"], saved: true }),
  )
  .build();

// ---------------------------------------------------------------------------
// Run it
// ---------------------------------------------------------------------------

async function main() {
  // Start GPU worker (on a GPU machine)
  const gpuWorker = createWorker({
    storage,
    stepQueue,
    registry: gpuRegistry,
    queues: ["gpu"],
    concurrency: 2,
  });
  void gpuWorker.start();

  // Submit via coordinator
  const coordinator = createCoordinator({
    storage,
    stepQueue,
    routing: { "transcribe": "gpu", "encode-video": "gpu" },
  });

  await coordinator.submit({
    workflow: processVideo,
    workflowId: "video-abc",
    input: { videoId: "abc", s3Path: "s3://videos/abc.mp4" },
  });
}

export { processVideo, gpuRegistry, main };
