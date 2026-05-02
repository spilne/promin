// ---------------------------------------------------------------------------
// Coordinator entry point — drives the DAG and submits demo datasets.
//
// Singleflight demo: each dataset is submitted twice in rapid succession
// using the same deterministic workflowId. The second submit finds the
// workflow already in flight and is skipped. You'll see:
//
//   [coordinator] submitted dataset-pipeline-1 (#1)
//   [coordinator] dataset-pipeline-1 already in flight — duplicate skipped
//
// This is the safe idempotency story: callers can fire-and-forget without
// worrying about duplicate execution — the coordinator's createWorkflow
// returns { created: false } on conflict and re-evaluates the ready set
// rather than re-running the workflow from scratch.
// ---------------------------------------------------------------------------

import { createCoordinator, WorkflowVersionRegistry } from "@promin/workflow";
import { buildStack } from "./shared.ts";
import { buildDataPipelineWorkflow, type DatasetInput } from "./workflow.ts";

const SUBMIT_INTERVAL_MS = Number(process.env["SUBMIT_INTERVAL_MS"] ?? 8_000);

const { storage, stepQueue, close } = await buildStack();

const registry = new WorkflowVersionRegistry();
registry.register(buildDataPipelineWorkflow());

const coordinator = createCoordinator({
  storage,
  stepQueue,
  registry,
  pollIntervalMs: 500,
});

console.log("[coordinator] starting");
coordinator.startLoop().catch((err) => {
  console.error("[coordinator] loop crashed", err);
  process.exit(1);
});

let counter = 0;
const submitDemo = async (): Promise<void> => {
  counter += 1;
  const datasetId = `demo-dataset-${counter}`;
  // Deterministic workflowId — same dataset always maps to the same ID.
  // A retry or duplicate trigger with the same datasetId is a no-op.
  const workflowId = `dataset-pipeline-${counter}`;
  const input: DatasetInput = {
    datasetId,
    recordCount: 30,
    source: `s3://raw-data/${datasetId}.parquet`,
  };

  try {
    await coordinator.submit<DatasetInput>({ name: "data-pipeline", workflowId, input });
    console.log(`[coordinator] submitted ${workflowId} (#${counter})`);
  } catch (err) {
    console.error(`[coordinator] submit ${workflowId} failed`, err);
    return;
  }

  // Immediately try the same workflowId again — simulates a double-trigger
  // (e.g. S3 event + manual retry). The second attempt should be a no-op.
  const existing = await coordinator.status(workflowId);
  if (existing && existing.status !== "completed" && existing.status !== "failed") {
    console.log(`[coordinator] ${workflowId} already in flight — duplicate skipped`);
  }
};

await submitDemo();
const submitTimer = setInterval(() => {
  submitDemo().catch(() => undefined);
}, SUBMIT_INTERVAL_MS);

console.log(`[coordinator] running — new dataset every ${SUBMIT_INTERVAL_MS}ms (Ctrl+C to stop)`);

const shutdown = async (): Promise<void> => {
  console.log("[coordinator] shutting down");
  clearInterval(submitTimer);
  await coordinator.stopLoop();
  await close();
  process.exit(0);
};
process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
