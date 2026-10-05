/**
 * Specialized workers for the video pipeline.
 * Steps declare the capabilities they need; workers declare what they offer
 * and the step names they host. The queue matches them inside each claim.
 */

import { InMemoryWorkflowStorage, workflow } from "@promin/workflow";
import {
  createDistributedWorkflowRunner,
  createWorker,
  InMemoryStepQueue,
  MapStepRegistry,
} from "@promin/workflow/distributed";
import { succeed } from "@spilne/perfect-core";

// Shared by every process (Postgres / Redis / SQLite stores in production).
const storage = new InMemoryWorkflowStorage();
const stepQueue = new InMemoryStepQueue();

// The definition. Under the distributed runner each step becomes a queue
// task named after the step; the registered worker handlers run, not these
// bodies (they run when the same workflow is used in-process).
const processVideo = workflow<{ videoId: string }>({ name: "process-video" })
  .step("download", ({ input }) => succeed({ path: `/tmp/${input.videoId}.mp4` }))
  .step("transcribe", ({ prev }) => succeed({ text: `Transcription of ${prev.path}` }), {
    needs: ["gpu"],
  })
  .step("summarize", ({ prev }) => succeed({ summary: prev.text.slice(0, 50) }), {
    needs: ["ai"],
  })
  .build();

// --- Coordinator process ---
const coordinator = createDistributedWorkflowRunner({ storage, stepQueue });

// --- Default worker (download) ---
const defaultRegistry = new MapStepRegistry();
defaultRegistry.register({
  stepName: "download",
  handler: async (ctx) => ({ path: `/tmp/${(ctx.input as { videoId: string }).videoId}.mp4` }),
});
const defaultWorker = createWorker({
  storage,
  stepQueue,
  registry: defaultRegistry,
  concurrency: 5,
});

// --- GPU worker (transcription) ---
const gpuRegistry = new MapStepRegistry();
gpuRegistry.register({
  stepName: "transcribe",
  // `ctx.deps` holds every result the run has so far, by step name.
  handler: async (ctx) => {
    const { path } = ctx.deps["download"] as { path: string };
    return { text: `Transcription of ${path}` };
  },
  retry: { maxRetries: 2 },
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
    const { text } = ctx.deps["transcribe"] as { text: string };
    return { summary: text.slice(0, 50) };
  },
});
const aiWorker = createWorker({
  storage,
  stepQueue,
  registry: aiRegistry,
  capabilities: ["ai"],
  concurrency: 10,
});

// Start the loops. `start()` resolves only once a worker stops, so don't await it.
void coordinator.startLoop();
for (const worker of [defaultWorker, gpuWorker, aiWorker]) void worker.start();

const result = await coordinator.run({
  workflow: processVideo,
  workflowId: "video-abc",
  input: { videoId: "abc" },
});
console.log(result); // { summary: "Transcription of /tmp/abc.mp4" }

// Graceful shutdown: unfinished tasks are released to other workers after 10 s.
await Promise.all([defaultWorker, gpuWorker, aiWorker].map((w) => w.stop({ timeoutMs: 10_000 })));
await coordinator.stopLoop();
