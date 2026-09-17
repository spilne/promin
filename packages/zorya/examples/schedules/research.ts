import type { DurableScheduleConfig } from "@promin/workflow";

export const researchEvery90s: DurableScheduleConfig = {
  id: "research-every-90s",
  name: "Research (journaled multi-activity) every 90s",
  intervalMs: 90_000,
  enabled: true,
  namespace: "tenant-b",
  metadata: {
    workflowName: "research",
    namespace: "tenant-b",
    input: { topic: "durable-execution", sourceCount: 4 },
  },
};
