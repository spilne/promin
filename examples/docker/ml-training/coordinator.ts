// ---------------------------------------------------------------------------
// Coordinator entry point — registers both workflow versions and alternates
// submitting v1 and v2 experiments to demonstrate version coexistence.
//
// You will see output like:
//   [coordinator] submitted exp-1-v1 (version=1)
//   [coordinator] submitted exp-2-v2 (version=2)
//   [coordinator] submitted exp-3-v1 (version=1)
//   ...
//
// Both versions run concurrently. The GPU worker handles train-model for
// either version; the CPU worker runs the extra `optimize` step only when
// a v2 experiment is dispatched.
// ---------------------------------------------------------------------------

import { createCoordinator, WorkflowVersionRegistry } from "@promin/workflow";
import { buildStack } from "./shared.ts";
import { buildModelWorkflow, type ModelInput } from "./workflow.ts";

const SUBMIT_INTERVAL_MS = Number(process.env["SUBMIT_INTERVAL_MS"] ?? 10_000);

const { storage, stepQueue, close } = await buildStack();

const registry = new WorkflowVersionRegistry();
registry.register(buildModelWorkflow("1"));
registry.register(buildModelWorkflow("2"));

const coordinator = createCoordinator({
  storage,
  stepQueue,
  registry,
  pollIntervalMs: 500,
});

console.log("[coordinator] starting (versions: 1, 2 registered)");
coordinator.start().catch((err) => {
  console.error("[coordinator] loop crashed", err);
  process.exit(1);
});

let counter = 0;
const submitDemo = async (): Promise<void> => {
  counter += 1;
  // Alternate between v1 and v2 to show both versions running side by side.
  const version = counter % 2 === 1 ? "1" : "2";
  const experimentId = `exp-${counter}-v${version}`;
  const workflowId = `ml-training-${counter}`;
  const input: ModelInput = {
    experimentId,
    datasetPath: `s3://ml-datasets/experiment-${counter}.parquet`,
    epochs: 3,
  };

  try {
    await coordinator.submit<ModelInput>({ name: "ml-training", version, workflowId, input });
    console.log(`[coordinator] submitted ${workflowId} (version=${version})`);
  } catch (err) {
    console.error(`[coordinator] submit ${workflowId} failed`, err);
  }
};

await submitDemo();
const submitTimer = setInterval(() => {
  submitDemo().catch(() => undefined);
}, SUBMIT_INTERVAL_MS);

console.log(
  `[coordinator] running — new experiment every ${SUBMIT_INTERVAL_MS}ms (Ctrl+C to stop)`,
);

const shutdown = async (): Promise<void> => {
  console.log("[coordinator] shutting down");
  clearInterval(submitTimer);
  await coordinator.stop();
  await close();
  process.exit(0);
};
process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
