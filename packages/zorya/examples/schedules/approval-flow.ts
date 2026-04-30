import type { DurableScheduleConfig } from "@promin/workflow";

export const approvalsEvery75s: DurableScheduleConfig = {
  id: "approvals-every-75s",
  name: "Approval flow every 75s",
  intervalMs: 75_000,
  enabled: true,
  metadata: {
    workflowName: "approval-flow",
    input: { requestId: 9001, requester: "demo-user" },
  },
};
