// ---------------------------------------------------------------------------
// Coordinator entry point — also the workflow submitter for this demo.
//
// Runs the distributed scheduling loop AND periodically submits fresh demo
// workflows on the same instance. Keeping submission inside the running
// coordinator is what makes the example reliable: a single `enqueued` set
// guards the initial step, so there's no race with a throwaway external
// submitter double-enqueueing `decode` (see git history: an earlier
// version used a separate submit.ts container and hit exactly that bug).
//
// Routing:
//   transcode → "gpu"  (worker-transcode)
//   thumbnail → "cpu"  (worker-thumbnail)
//   decode / metadata / notify → default queue (worker-default).
//
// Submit shape: this demo uses the registry-keyed form —
// `coordinator.submit({ name, workflowId, input })`. The workflow
// definition lives only in the coordinator process; a separate submitter
// (HTTP handler, CLI, cron) would post the name over the wire and never
// need to import `buildVideoWorkflow`.
// ---------------------------------------------------------------------------

import { createCoordinator, WorkflowVersionRegistry } from "@promin/workflow";
import { buildStack } from "./shared.ts";
import { buildVideoWorkflow, type VideoInput } from "./workflow.ts";

const SUBMIT_INTERVAL_MS = Number(process.env["SUBMIT_INTERVAL_MS"] ?? 5_000);

const { storage, stepQueue, close } = await buildStack();

// Registry holds pure Workflow definitions + a single storage. The
// coordinator uses its own `storage` config for state I/O; the registry's
// storage is only consulted if someone calls `registry.run()` directly
// (we don't — the coordinator handles dispatch). Pass it anyway so the
// registry is self-sufficient in case a future entry point needs it.
const registry = new WorkflowVersionRegistry({ storage });
registry.register(buildVideoWorkflow());

// Routing lives on the step itself via `needs` (see workflow.ts).
// Workers declare capabilities that match. The coordinator just
// orchestrates the DAG — no routing table.
const coordinator = createCoordinator({
  storage,
  stepQueue,
  registry,
  pollIntervalMs: 500,
});

console.log("[coordinator] starting");
// coordinator.start() runs its own poll loop and never resolves until
// .stop(). Don't await it — fire and continue so the submit timer can run.
coordinator.start().catch((err) => {
  console.error("[coordinator] loop crashed", err);
  process.exit(1);
});

let counter = 0;
const submitDemo = async (): Promise<void> => {
  counter += 1;
  const videoId = `demo-video-${counter}`;
  const workflowId = `${videoId}-${Math.random().toString(36).slice(2, 8)}`;
  try {
    await coordinator.submit<VideoInput>({
      name: "video-pipeline",
      workflowId,
      input: { videoId, sourceUrl: `s3://demo-bucket/${videoId}.mov` },
    });
    console.log(`[coordinator] submitted ${workflowId} (#${counter})`);
  } catch (err) {
    console.error(`[coordinator] submit ${workflowId} failed`, err);
  }
};

// Kick off one immediately so `docker compose up` produces output fast,
// then schedule the repeating submissions.
await submitDemo();
const submitTimer = setInterval(() => {
  submitDemo().catch(() => undefined);
}, SUBMIT_INTERVAL_MS);

console.log(`[coordinator] running — new workflow every ${SUBMIT_INTERVAL_MS}ms (Ctrl+C to stop)`);

const shutdown = async (): Promise<void> => {
  console.log("[coordinator] shutting down");
  clearInterval(submitTimer);
  await coordinator.stop();
  await close();
  process.exit(0);
};
process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
