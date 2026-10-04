/**
 * Multi-queue worker setup for the video pipeline.
 * Coordinator routes steps to specialized queues.
 * Each worker process handles its own queue.
 */

import { InMemoryWorkflowStorage } from "@promin/workflow";
import { createDistributedWorkflowRunner } from "@promin/workflow/distributed";
import { createWorker, MapStepRegistry, InMemoryStepQueue } from "@promin/workflow/distributed";

const storage = new InMemoryWorkflowStorage();
const stepQueue = new InMemoryStepQueue();

// --- Coordinator process ---

// Routing is now declared on each step via `needs` (see the workflow
// definition in 01-video-pipeline.ts). Workers declare capabilities;
// the coordinator just dispatches.
const coordinator = createDistributedWorkflowRunner({ storage, stepQueue });

// --- Default worker (download, general tasks) ---

const defaultRegistry = new MapStepRegistry();
defaultRegistry.register({
  stepName: "download",
  handler: async (ctx) => {
    const videoId = (ctx.input as any).videoId;
    return { path: `/tmp/${videoId}.mp4` };
  },
});

const defaultWorker = createWorker({
  storage,
  stepQueue,
  registry: defaultRegistry,
  capabilities: ["default"],
  concurrency: 5,
});

// --- GPU worker (transcription) ---

const gpuRegistry = new MapStepRegistry();
gpuRegistry.register({
  stepName: "transcribe",
  handler: async (ctx) => {
    const path = (ctx.prev as any).path;
    return { text: `Transcription of ${path}` };
  },
});

const gpuWorker = createWorker({
  storage,
  stepQueue,
  registry: gpuRegistry,
  capabilities: ["gpu"],
  concurrency: 2,
});

// --- AI worker (summarization) ---

const aiRegistry = new MapStepRegistry();
aiRegistry.register({
  stepName: "summarize",
  handler: async (ctx) => {
    const text = (ctx.prev as any).text;
    return { summary: `Summary: ${text.slice(0, 50)}` };
  },
});

const aiWorker = createWorker({
  storage,
  stepQueue,
  registry: aiRegistry,
  capabilities: ["ai"],
  concurrency: 10,
});

// Start all processes
void coordinator.startLoop();
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
