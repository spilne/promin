// ---------------------------------------------------------------------------
// Demo server — seeds an in-memory storage with sample runs and serves the
// dashboard. Run with:
//   bun run packages/zorya/examples/demo.ts
// ---------------------------------------------------------------------------

import { InMemoryWorkflowStorage, InMemorySchedulerStorage } from "@promin/workflow";
import { ZoryaServer } from "../src/index.ts";
import path from "node:path";

const storage = new InMemoryWorkflowStorage();
const schedulerStorage = new InMemorySchedulerStorage();

async function seed() {
  // Completed order workflow with a couple of finished steps.
  await storage.createWorkflow({
    workflowId: "wf-demo-1",
    workflowName: "order",
    workflowType: "order",
    input: { orderId: 42, customer: "acme" },
  });
  const start = new Date(Date.now() - 2000);
  await storage.saveStepResult({
    workflowId: "wf-demo-1",
    stepName: "validate",
    result: { ok: true },
    durationMs: 120,
    startedAt: start,
  });
  await storage.saveStepResult({
    workflowId: "wf-demo-1",
    stepName: "charge",
    result: { charged: 100 },
    durationMs: 240,
    startedAt: new Date(start.getTime() + 120),
  });
  await storage.saveStepResult({
    workflowId: "wf-demo-1",
    stepName: "notify",
    result: { sent: true },
    durationMs: 80,
    startedAt: new Date(start.getTime() + 400),
  });
  await storage.completeWorkflow("wf-demo-1", { ok: true, orderId: 42 });

  // Running workflow.
  await storage.createWorkflow({
    workflowId: "wf-demo-2",
    workflowName: "payment",
    workflowType: "payment",
    input: { amount: 500 },
  });
  await storage.saveStepResult({
    workflowId: "wf-demo-2",
    stepName: "authorize",
    result: { authId: "auth-123" },
    durationMs: 300,
    startedAt: new Date(Date.now() - 400),
  });

  // Pending (queued) workflow.
  await storage.createWorkflow({
    workflowId: "wf-demo-3",
    workflowName: "order",
    workflowType: "order",
    input: { orderId: 43 },
  });

  // Failed workflow.
  await storage.createWorkflow({
    workflowId: "wf-demo-4",
    workflowName: "payment",
    workflowType: "payment",
    input: { amount: 9999 },
  });
  await storage.saveStepFailure({
    workflowId: "wf-demo-4",
    stepName: "authorize",
    error: "Card declined",
    durationMs: 500,
    startedAt: new Date(Date.now() - 3000),
  });

  // Schedules — all linked to the seeded workflow names (order, payment) so
  // clicking through from the Schedules page lands on real runs.
  await schedulerStorage.upsertSchedule({
    id: "daily-orders-batch",
    name: "Daily orders batch",
    cron: "0 9 * * *",
    timezone: "America/Edmonton",
    enabled: true,
    metadata: { workflowName: "order", input: { source: "daily-batch" } },
  });
  await schedulerStorage.upsertSchedule({
    id: "hourly-order-sync",
    name: "Hourly order sync",
    cron: "0 * * * *",
    timezone: "UTC",
    enabled: true,
    overlapPolicy: "skip",
    metadata: { workflowName: "order", input: { source: "hourly-sync" } },
  });
  await schedulerStorage.upsertSchedule({
    id: "payment-retry-every-15m",
    name: "Payment retry sweep",
    intervalMs: 15 * 60 * 1000,
    enabled: true,
    jitterMs: 30_000,
    metadata: { workflowName: "payment", input: { mode: "retry" } },
  });
  await schedulerStorage.upsertSchedule({
    id: "weekly-payment-audit",
    name: "Weekly payment audit (paused)",
    cron: "0 2 * * 1",
    timezone: "UTC",
    enabled: false,
    metadata: { workflowName: "payment", input: { mode: "audit" } },
  });
  // Record some fake fire history so the UI has something to show.
  await schedulerStorage.recordFire("hourly-order-sync", new Date(Date.now() - 45 * 60 * 1000), 12);
  await schedulerStorage.recordFire(
    "payment-retry-every-15m",
    new Date(Date.now() - 3 * 60 * 1000),
    287,
  );
  await schedulerStorage.recordFire(
    "daily-orders-batch",
    new Date(Date.now() - 18 * 60 * 60 * 1000),
    42,
  );
}

await seed();

const uiDir = path.join(import.meta.dir, "..", "dist", "public");

const server = new ZoryaServer({
  storage,
  scheduler: schedulerStorage,
  uiDir,
  // Example workers provider so the Workers tab has data.
  workers: {
    listWorkers: async () => [
      {
        workerId: "worker-1",
        status: "online",
        queue: "default",
        activeTasks: 2,
        completedToday: 142,
        lastHeartbeatAt: new Date().toISOString(),
      },
      {
        workerId: "worker-2",
        status: "online",
        queue: "default",
        activeTasks: 0,
        completedToday: 98,
        lastHeartbeatAt: new Date().toISOString(),
      },
      {
        workerId: "worker-3",
        status: "offline",
        queue: "gpu",
        activeTasks: 0,
        completedToday: 23,
        lastHeartbeatAt: new Date(Date.now() - 5 * 60 * 1000).toISOString(),
      },
    ],
  },
});

const port = Number(process.env.PORT ?? 4100);
const { port: actualPort, hostname } = server.listen({ port });
const host = hostname === "0.0.0.0" ? "localhost" : hostname;
console.log(`Zorya demo server on http://${host}:${actualPort}`);
