/**
 * Multi-queue worker setup for the video pipeline.
 * Coordinator routes steps to specialized queues.
 * Each worker process handles its own queue.
 */

import {
  createCoordinator,
  createWorker,
  MapStepRegistry,
  InMemoryWorkflowStorage,
  InMemoryStepQueue,
} from "@promin/workflow";
import { Pipeline } from "@promin/core";

const storage = new InMemoryWorkflowStorage();
const stepQueue = new InMemoryStepQueue();

// --- Coordinator process ---

const coordinator = createCoordinator({
  storage,
  stepQueue,
  routing: {
    transcribe: "gpu",
    summarize: "ai",
    // everything else → "default"
  },
});

// --- Default worker (download, general tasks) ---

const defaultRegistry = new MapStepRegistry();
defaultRegistry.register("download", (ctx) =>
  Pipeline.fn(async () => {
    const videoId = (ctx.input as any).videoId;
    return { path: `/tmp/${videoId}.mp4` };
  }),
);

const defaultWorker = createWorker({
  storage,
  stepQueue,
  registry: defaultRegistry,
  queues: ["default"],
  concurrency: 5,
});

// --- GPU worker (transcription) ---

const gpuRegistry = new MapStepRegistry();
gpuRegistry.register("transcribe", (ctx) =>
  Pipeline.fn(async () => {
    const path = (ctx.prev as any).path;
    return { text: `Transcription of ${path}` };
  }),
);

const gpuWorker = createWorker({
  storage,
  stepQueue,
  registry: gpuRegistry,
  queues: ["gpu"],
  concurrency: 2,
});

// --- AI worker (summarization) ---

const aiRegistry = new MapStepRegistry();
aiRegistry.register("summarize", (ctx) =>
  Pipeline.fn(async () => {
    const text = (ctx.prev as any).text;
    return { summary: `Summary: ${text.slice(0, 50)}` };
  }),
);

const aiWorker = createWorker({
  storage,
  stepQueue,
  registry: aiRegistry,
  queues: ["ai"],
  concurrency: 10,
});

// Start all processes
await coordinator.start();
await defaultWorker.start();
await gpuWorker.start();
await aiWorker.start();

// Submit work
declare const processVideo: any;
await coordinator.submit({
  workflow: processVideo,
  workflowId: "video-abc",
  input: { videoId: "abc" },
});
