import type { SchedulerStorage } from "@promin/workflow";

export interface SeedSchedulesDeps {
  readonly schedulerStorage: SchedulerStorage;
}

export async function seedSchedules({ schedulerStorage }: SeedSchedulesDeps): Promise<void> {
  await schedulerStorage.upsertSchedule({
    id: "orders-every-15s",
    name: "Orders every 15s",
    intervalMs: 15_000,
    enabled: true,
    namespace: "tenant-a",
    metadata: { workflowName: "order" },
  });
  await schedulerStorage.upsertSchedule({
    id: "payments-every-30s",
    name: "Payments every 30s",
    intervalMs: 30_000,
    enabled: true,
    jitterMs: 2_000,
    namespace: "tenant-a",
    metadata: { workflowName: "payment" },
  });
  await schedulerStorage.upsertSchedule({
    id: "video-transcodes-every-45s",
    name: "Video transcodes every 45s",
    intervalMs: 45_000,
    enabled: true,
    namespace: "tenant-b",
    metadata: { workflowName: "video-transcode" },
  });
  await schedulerStorage.upsertSchedule({
    id: "onboarding-every-60s",
    name: "Onboarding every 60s",
    intervalMs: 60_000,
    enabled: true,
    namespace: "tenant-b",
    metadata: { workflowName: "onboarding" },
  });
  await schedulerStorage.upsertSchedule({
    id: "etl-hourly",
    name: "ETL hourly",
    cron: "0 * * * *",
    timezone: "UTC",
    enabled: true,
    metadata: { workflowName: "etl" },
  });
  await schedulerStorage.upsertSchedule({
    id: "fulfillment-every-40s",
    name: "Order fulfillment saga every 40s",
    intervalMs: 40_000,
    enabled: true,
    namespace: "tenant-a",
    metadata: { workflowName: "order-fulfillment" },
  });
  await schedulerStorage.upsertSchedule({
    id: "batch-every-50s",
    name: "Batch process every 50s",
    intervalMs: 50_000,
    enabled: true,
    metadata: { workflowName: "batch-process" },
  });
  await schedulerStorage.upsertSchedule({
    id: "approvals-every-75s",
    name: "Approval flow every 75s",
    intervalMs: 75_000,
    enabled: true,
    metadata: { workflowName: "approval-flow" },
  });
  await schedulerStorage.upsertSchedule({
    id: "manual-approval-hourly",
    name: "Manual approval (hourly, awaits operator)",
    cron: "0 * * * *",
    timezone: "UTC",
    enabled: true,
    metadata: { workflowName: "manual-approval" },
  });
  await schedulerStorage.upsertSchedule({
    id: "research-every-90s",
    name: "Research (journaled multi-activity) every 90s",
    intervalMs: 90_000,
    enabled: true,
    namespace: "tenant-b",
    metadata: { workflowName: "research" },
  });
  await schedulerStorage.upsertSchedule({
    id: "weekly-payment-audit",
    name: "Weekly payment audit (paused)",
    cron: "0 2 * * 1",
    timezone: "UTC",
    enabled: false,
    metadata: { workflowName: "payment", input: { mode: "audit" } },
  });

  const all = await schedulerStorage.listSchedules({ limit: 500 });
  const now = new Date();
  for (const s of all) {
    if (s.enabled === false) continue;
    const state = await schedulerStorage.loadScheduleState(s.id);
    if (state?.lastFired) continue;
    await schedulerStorage.setNextRun(s.id, now);
  }
}
