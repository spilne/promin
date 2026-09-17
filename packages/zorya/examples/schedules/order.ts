import type { DurableScheduleConfig } from "@promin/workflow";

export const ordersEvery15s: DurableScheduleConfig = {
  id: "orders-every-15s",
  name: "Orders every 15s",
  intervalMs: 15_000,
  enabled: true,
  namespace: "tenant-a",
  metadata: {
    workflowName: "order",
    namespace: "tenant-a",
    input: { orderId: 4242, customer: "cust-42" },
  },
};
