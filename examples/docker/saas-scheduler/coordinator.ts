// ---------------------------------------------------------------------------
// Coordinator entry point — alternates premium and standard tenant jobs.
//
// You will see output like:
//   [coordinator] submitted tenant-A job-1 (tier=premium → premium-job workflow)
//   [coordinator] submitted tenant-B job-2 (tier=standard → standard-job workflow)
//   [coordinator] submitted tenant-A job-3 (tier=premium → premium-job workflow)
//   ...
//
// Premium jobs are routed to worker-premium (dedicated, fast).
// Standard jobs are routed to worker-default (shared pool).
// The execute step duration difference (~500ms vs ~1.5s) shows the SLA gap.
// ---------------------------------------------------------------------------

import { createCoordinator, WorkflowVersionRegistry } from "@promin/workflow";
import { buildStack } from "./shared.ts";
import { buildPremiumJobWorkflow, buildStandardJobWorkflow, type TenantJob } from "./workflow.ts";

const SUBMIT_INTERVAL_MS = Number(process.env["SUBMIT_INTERVAL_MS"] ?? 5_000);

const { storage, stepQueue, close } = await buildStack();

const registry = new WorkflowVersionRegistry();
registry.register(buildPremiumJobWorkflow());
registry.register(buildStandardJobWorkflow());

const coordinator = createCoordinator({
  storage,
  stepQueue,
  registry,
  pollIntervalMs: 500,
});

console.log("[coordinator] starting (premium-job + standard-job registered)");
coordinator.startLoop().catch((err) => {
  console.error("[coordinator] loop crashed", err);
  process.exit(1);
});

let counter = 0;
const submitDemo = async (): Promise<void> => {
  counter += 1;
  // Alternate: odd = premium (tenant-A), even = standard (tenant-B)
  const isPremium = counter % 2 === 1;
  const tenantId = isPremium ? "tenant-A" : "tenant-B";
  const tier = isPremium ? "premium" : "standard";
  const jobId = `job-${counter}`;
  const workflowName = isPremium ? "premium-job" : "standard-job";
  const workflowId = `${tenantId}-${jobId}`;

  const input: TenantJob = {
    tenantId,
    jobId,
    tier,
    payload: { taskType: "report-generation", params: { month: "2026-04" } },
  };

  try {
    await coordinator.submit<TenantJob>({ name: workflowName, workflowId, input });
    console.log(
      `[coordinator] submitted ${tenantId} ${jobId} (tier=${tier} → ${workflowName} workflow)`,
    );
  } catch (err) {
    console.error(`[coordinator] submit ${workflowId} failed`, err);
  }
};

await submitDemo();
const submitTimer = setInterval(() => {
  submitDemo().catch(() => undefined);
}, SUBMIT_INTERVAL_MS);

console.log(`[coordinator] running — new job every ${SUBMIT_INTERVAL_MS}ms (Ctrl+C to stop)`);

const shutdown = async (): Promise<void> => {
  console.log("[coordinator] shutting down");
  clearInterval(submitTimer);
  await coordinator.stopLoop();
  await close();
  process.exit(0);
};
process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
