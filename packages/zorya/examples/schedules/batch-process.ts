import type { DurableScheduleConfig } from "@promin/workflow";

export const batchEvery50s: DurableScheduleConfig = {
  id: "batch-every-50s",
  name: "Batch process every 50s",
  intervalMs: 50_000,
  enabled: true,
  metadata: {
    workflowName: "batch-process",
    input: { batchId: "batch-demo", itemCount: 8 },
  },
};
