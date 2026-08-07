export function inputFor(name: string): unknown {
  switch (name) {
    case "order":
      return {
        orderId: Math.floor(Math.random() * 10_000),
        customer: `cust-${Math.floor(Math.random() * 100)}`,
      };
    case "payment":
      return { amount: Math.floor(Math.random() * 5_000) + 100, currency: "USD" };
    case "video-transcode":
      return {
        videoId: `vid-${Math.floor(Math.random() * 1_000_000)}`,
        url: "https://example.com/video.mp4",
      };
    case "onboarding":
      return { email: `user-${Math.floor(Math.random() * 10_000)}@example.com` };
    case "etl":
      return { source: "events-prod", batch: Math.floor(Math.random() * 100) };
    case "order-fulfillment":
      return {
        orderId: Math.floor(Math.random() * 10_000),
        items: ["sku-a", "sku-b"],
      };
    case "batch-process":
      return {
        batchId: `batch-${Math.floor(Math.random() * 10_000)}`,
        itemCount: 6 + Math.floor(Math.random() * 8),
      };
    case "approval-flow":
      return {
        requestId: Math.floor(Math.random() * 10_000),
        requester: `user-${Math.floor(Math.random() * 100)}`,
      };
    case "manual-approval":
      return {
        requestId: Math.floor(Math.random() * 10_000),
        requester: "scheduler",
        description: "Hourly manual-approval demo — open /approvals and resolve",
      };
    case "research":
      return {
        topic: ["durable-execution", "distributed-systems", "saga-patterns", "event-sourcing"][
          Math.floor(Math.random() * 4)
        ],
        sourceCount: 3 + Math.floor(Math.random() * 3),
      };
    case "versioned-greeter":
      return { name: ["world", "Promin", "Zorya", "Claude"][Math.floor(Math.random() * 4)] };
    default:
      return {};
  }
}
