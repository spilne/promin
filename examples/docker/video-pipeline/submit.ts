// ---------------------------------------------------------------------------
// CLI: submit a video-pipeline run and wait for it to finish.
//
// Usage:
//   bun run submit.ts [videoId]
//   docker compose run --rm submit abc-123
//
// Prints progress as steps complete, then the final result.
// ---------------------------------------------------------------------------

import { createCoordinator } from "@promin/workflow";
import { buildStack } from "./shared.ts";
import { buildVideoWorkflow, type VideoInput } from "./workflow.ts";

const videoId = process.argv[2] ?? `video-${Date.now()}`;
const sourceUrl = `s3://demo-bucket/${videoId}.mov`;

const { storage, stepQueue, close } = await buildStack();

// The persistent coordinator.ts process runs the scheduler. This submit
// script just needs to:
//   1. Register the DAG + enqueue the first step via coordinator.submit()
//   2. Poll storage until the workflow resolves
// We don't start a local scheduling loop here — that would collide with
// the cluster coordinator (both would try to enqueue steps).
const submitter = createCoordinator({ storage, stepQueue });
const wf = buildVideoWorkflow(storage);
const workflowId = `${videoId}-${Math.random().toString(36).slice(2, 8)}`;

console.log(`[submit] enqueuing workflow ${workflowId} for video ${videoId}`);
await submitter.submit<VideoInput>({
  workflow: wf,
  workflowId,
  input: { videoId, sourceUrl },
});

console.log(`[submit] waiting for completion...`);
const timeoutMs = 60_000;
const pollMs = 500;
const start = Date.now();
while (true) {
  if (Date.now() - start > timeoutMs) {
    console.error(`[submit] timed out after ${timeoutMs}ms`);
    process.exit(1);
  }
  const state = await storage.loadWorkflow(workflowId);
  if (state?.status === "completed") {
    console.log(`[submit] done — status=completed, result=`, state.result);
    break;
  }
  if (state?.status === "failed") {
    console.error(`[submit] failed:`, state.error);
    process.exit(1);
  }
  await new Promise((r) => setTimeout(r, pollMs));
}

await close();
process.exit(0);
