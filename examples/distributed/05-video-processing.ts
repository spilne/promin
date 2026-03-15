/**
 * Video processing pipeline — realistic media company scenario
 *
 * When a creator uploads a video:
 * 1. Download from S3 (CPU worker)
 * 2. In parallel: transcribe audio (GPU) + generate thumbnails (CPU) + encode H.265 (GPU)
 * 3. Store metadata + update search index (local)
 * 4. Notify creator (local)
 *
 * Different steps need different hardware. The coordinator routes
 * them to the right worker pool.
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
import { containerStep, DockerRuntime } from "@promin/container";

const storage = new InMemoryWorkflowStorage();
const stepQueue = new InMemoryStepQueue();

const runtime = new DockerRuntime({
  network: "media-pipeline",
});

// ---------------------------------------------------------------------------
// Step registry — container steps for media processing
// ---------------------------------------------------------------------------

const mediaRegistry = new MapStepRegistry();

// FFmpeg thumbnail generation (CPU)
const [thumbHandler] = containerStep({
  spec: {
    image: "jrottenberg/ffmpeg:4.4-alpine",
    command: [
      "sh", "-c",
      "ffmpeg -i /pipeline/input/video.mp4 -vf 'select=eq(n,0)+eq(n,100)+eq(n,300)' -vsync vfr /pipeline/output/thumb_%d.jpg 2>/dev/null && echo '{\"thumbnails\": 3}' > /pipeline/output/output.json",
    ],
    memoryLimit: "1g",
    cpuLimit: "2",
    timeoutMs: 120_000,
  },
  runtime,
});
mediaRegistry.register("generate-thumbnails", thumbHandler);

// Whisper transcription (GPU)
const [whisperHandler, whisperOptions] = containerStep({
  spec: {
    image: "openai/whisper:large-v3",
    command: ["python", "-m", "whisper", "--model", "large-v3", "--output_format", "json"],
    memoryLimit: "8g",
    gpu: true,
    timeoutMs: 600_000, // 10 min
  },
  runtime,
  options: {
    retry: { maxRetries: 2 },
    onFailure: { fallback: () => ({ transcript: null, error: "transcription failed" }) },
  },
});
mediaRegistry.register("transcribe-audio", whisperHandler, whisperOptions);

// H.265 encoding (GPU-accelerated via NVENC)
const [encodeHandler, encodeOptions] = containerStep({
  spec: {
    image: "nvcr.io/nvidia/video-codec:2.0",
    command: [
      "ffmpeg", "-hwaccel", "cuda",
      "-i", "/pipeline/input/video.mp4",
      "-c:v", "hevc_nvenc", "-preset", "p7",
      "-c:a", "aac", "-b:a", "192k",
      "/pipeline/output/encoded.mp4",
    ],
    memoryLimit: "4g",
    gpu: true,
    timeoutMs: 1800_000, // 30 min for long videos
  },
  runtime,
  options: {
    retry: { maxRetries: 1 },
  },
});
mediaRegistry.register("encode-h265", encodeHandler, encodeOptions);

// ---------------------------------------------------------------------------
// Video processing workflow — DAG with parallel branches
// ---------------------------------------------------------------------------

const processUpload = workflow<{
  videoId: string;
  creatorId: string;
  s3Path: string;
  title: string;
}>({
  name: "process-upload",
  storage,
  type: "media",
  metadata: { team: "media-pipeline" },
  dispatch: {
    stepQueue,
    routing: {
      "generate-thumbnails": "cpu",
      "transcribe-audio": "gpu",
      "encode-h265": "gpu",
    },
  },
  compensate: {
    trigger: "after-retries",
    onComplete: ({ error, compensatedSteps }) =>
      Pipeline.fromPromise(async () => {
        console.log(`Upload processing failed. Cleaned up: ${compensatedSteps.join(", ")}`);
        // await notifyCreator(input.creatorId, "processing-failed");
      }),
  },
})
  // Local: validate upload
  .step("validate", ({ input }) =>
    Pipeline.succeed({
      videoId: input.videoId,
      creatorId: input.creatorId,
      s3Path: input.s3Path,
      title: input.title,
      valid: true,
    }),
  )

  // Container (CPU): generate thumbnails — runs in parallel with transcribe + encode
  .step("generate-thumbnails", { dependsOn: ["validate"] }, ({ deps }) =>
    Pipeline.succeed({
      thumbnails: [`/thumbs/${deps.validate.videoId}/1.jpg`, `/thumbs/${deps.validate.videoId}/2.jpg`],
    }),
  )

  // Container (GPU): transcribe audio
  .step("transcribe-audio", { dependsOn: ["validate"] }, ({ deps }) =>
    Pipeline.succeed({
      transcript: `Transcription of video ${deps.validate.videoId}`,
      language: "en",
      segments: 42,
    }),
  )

  // Container (GPU): encode to H.265
  .step("encode-h265", { dependsOn: ["validate"] }, ({ deps }) =>
    Pipeline.succeed({
      encodedPath: `s3://encoded/${deps.validate.videoId}/video.mp4`,
      codec: "h265",
      sizeMb: 150,
    }),
  )

  // Local: store metadata (depends on ALL parallel steps)
  .step("store-metadata", { dependsOn: ["generate-thumbnails", "transcribe-audio", "encode-h265"] }, ({ deps }) =>
    Pipeline.fromPromise(async () => {
      const metadata = {
        thumbnails: deps["generate-thumbnails"].thumbnails,
        transcript: deps["transcribe-audio"].transcript,
        encodedPath: deps["encode-h265"].encodedPath,
        indexedAt: new Date().toISOString(),
      };
      // await db.videos.update(input.videoId, metadata);
      // await searchIndex.upsert(input.videoId, { title: input.title, transcript: metadata.transcript });
      return metadata;
    }),
  )

  // Local: notify creator
  .step("notify-creator", { dependsOn: ["store-metadata"] }, ({ deps, input }) =>
    Pipeline.fromPromise(async () => {
      // await pushNotification.send(input.creatorId, "Your video is ready!");
      // await email.send(input.email, "video-processed", { title: input.title });
      return { notified: true, creatorId: input.creatorId };
    }),
  )
  .build();

// ---------------------------------------------------------------------------
// Infrastructure setup
// ---------------------------------------------------------------------------

async function startVideoProcessing() {
  // CPU workers: thumbnails, lighter tasks
  const cpuWorker = createWorker({
    storage,
    stepQueue,
    registry: mediaRegistry,
    queues: ["cpu"],
    concurrency: 10,
    pollIntervalMs: 1000,
  });

  // GPU workers: transcription + encoding (expensive, fewer)
  const gpuWorker = createWorker({
    storage,
    stepQueue,
    registry: mediaRegistry,
    queues: ["gpu"],
    concurrency: 4, // 4 GPUs available
    pollIntervalMs: 1000,
  });

  const coordinator = createCoordinator({
    storage,
    stepQueue,
    routing: {
      "generate-thumbnails": "cpu",
      "transcribe-audio": "gpu",
      "encode-h265": "gpu",
    },
    pollIntervalMs: 2000,
  });

  void cpuWorker.start();
  void gpuWorker.start();
  void coordinator.start();

  // Process an upload
  await coordinator.submit({
    workflow: processUpload,
    workflowId: `upload-${Date.now()}`,
    input: {
      videoId: "vid_abc123",
      creatorId: "creator_42",
      s3Path: "s3://uploads/vid_abc123.mp4",
      title: "How to Build a Pipeline Platform",
    },
  });
}

export { processUpload, mediaRegistry, startVideoProcessing };
