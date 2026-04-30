import type { DurableScheduleConfig } from "@promin/workflow";

export const fulfillmentEvery40s: DurableScheduleConfig = {
  id: "fulfillment-every-40s",
  name: "Order fulfillment saga every 40s",
  intervalMs: 40_000,
  enabled: true,
  namespace: "tenant-a",
  metadata: {
    workflowName: "order-fulfillment",
    namespace: "tenant-a",
    input: { orderId: 7301, items: ["sku-a", "sku-b"] },
  },
};
