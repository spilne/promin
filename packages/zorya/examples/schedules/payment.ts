import type { DurableScheduleConfig } from "@promin/workflow";

// Demonstrates that a single module can export multiple schedules — the
// scanner picks them all up (array exports get flattened too, so either
// shape works).

export const paymentsEvery30s: DurableScheduleConfig = {
  id: "payments-every-30s",
  name: "Payments every 30s",
  intervalMs: 30_000,
  enabled: true,
  jitterMs: 2_000,
  namespace: "tenant-a",
  metadata: {
    workflowName: "payment",
    namespace: "tenant-a",
    input: { amount: 1500, currency: "USD" },
  },
};

export const weeklyPaymentAudit: DurableScheduleConfig = {
  id: "weekly-payment-audit",
  name: "Weekly payment audit (paused)",
  cron: "0 2 * * 1",
  timezone: "UTC",
  enabled: false,
  metadata: { workflowName: "payment", input: { mode: "audit" } },
};
