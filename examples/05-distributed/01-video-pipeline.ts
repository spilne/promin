/**
 * Video processing across specialized machines.
 * Download on default workers, transcribe on GPU, summarize on AI workers.
 * Same workflow runs in-process (dev) or distributed (prod).
 */

import { workflow, InMemoryWorkflowStorage } from "@promin/core";

const storage = new InMemoryWorkflowStorage();

// Define the workflow — same code for both modes
const processVideo = workflow<{ videoId: string }>({
  name: "process-video",
  storage,
})
  .stepAsync("download", async ({ input }) => {
    const path = await downloadVideo(input.videoId);
    return { path };
  })
  .stepAsync("transcribe", { dependsOn: ["download"] }, async ({ deps }) => {
    const text = await transcribe(deps.download.path);
    return { text };
  })
  .stepAsync("summarize", { dependsOn: ["transcribe"] }, async ({ deps }) => {
    const summary = await summarize(deps.transcribe.text);
    return { summary };
  })
  .build();

// Dev: run everything in-process
const result = await processVideo.run({
  workflowId: "video-abc",
  input: { videoId: "abc" },
});

console.log(result); // { summary: "..." }

// Prod: distributed across machines (see 02-multi-queue-workers.ts)

// Stubs
async function downloadVideo(_id: string) {
  return "/tmp/video.mp4";
}
async function transcribe(_path: string) {
  return "Hello world, this is a test video.";
}
async function summarize(_text: string) {
  return "A test video greeting.";
}
