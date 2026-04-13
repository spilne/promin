/**
 * Durable state machine — event-driven with typed per-state context.
 * Unlike workflows (DAG, forward-only), state machines support cycles
 * and external events driving transitions.
 */

import { stateMachine, InMemoryStateMachineStorage } from "@promin/workflow";

// ---------------------------------------------------------------------------
// 1. Define states — each state has its own typed context
// ---------------------------------------------------------------------------

type OrderStates = {
  draft: {
    context: { items: string[]; customerId: string };
    transitions: { submit: "submitted"; cancel: "cancelled" };
  };
  submitted: {
    context: { items: string[]; customerId: string; submittedAt: Date };
    transitions: { approve: "approved"; reject: "rejected" };
  };
  approved: {
    context: { items: string[]; customerId: string; approvedBy: string };
    transitions: { ship: "shipped" };
  };
  rejected: {
    context: { items: string[]; customerId: string; reason: string };
    transitions: { resubmit: "draft" }; // cycle back to draft!
  };
  shipped: {
    context: { items: string[]; trackingId: string };
    transitions: { deliver: "delivered" };
  };
  delivered: {
    context: { items: string[]; trackingId: string; deliveredAt: Date };
    transitions: {};
  };
  cancelled: {
    context: { reason: string };
    transitions: {};
  };
};

// ---------------------------------------------------------------------------
// 2. Build the machine
// ---------------------------------------------------------------------------

const storage = new InMemoryStateMachineStorage();

const orderMachine = stateMachine<OrderStates>({
  name: "order",
  storage,
  limits: { maxTransitions: 100 }, // safety: prevent infinite loops
})
  .state("draft")
  .state("submitted", {
    onEnter: async (ctx) => console.log(`Order submitted with ${(ctx as any).items.length} items`),
  })
  .state("approved")
  .state("rejected")
  .state("shipped")
  .state("delivered", { terminal: true })
  .state("cancelled", { terminal: true })

  // Simple transition
  .on("submit", {
    from: "draft",
    to: "submitted",
    action: (ctx: { items: string[]; customerId: string }) => ({
      items: ctx.items,
      customerId: ctx.customerId,
      submittedAt: new Date(),
    }),
  })

  // Conditional transition — action decides target
  .on("approve", {
    from: "submitted",
    to: "approved",
    guard: (ctx: any) => ctx.items.length > 0, // can't approve empty order
    action: (ctx: any) => ({
      items: ctx.items,
      customerId: ctx.customerId,
      approvedBy: "admin",
    }),
  })

  .on("reject", {
    from: "submitted",
    to: "rejected",
    action: (ctx: any) => ({
      items: ctx.items,
      customerId: ctx.customerId,
      reason: "Out of stock",
    }),
  })

  // Cycle: rejected → draft (resubmit)
  .on("resubmit", {
    from: "rejected",
    to: "draft",
    action: (ctx: any) => ({
      items: ctx.items,
      customerId: ctx.customerId,
    }),
  })

  .on("ship", {
    from: "approved",
    to: "shipped",
    action: async (ctx: any) => ({
      items: ctx.items,
      trackingId: `TRACK-${Date.now()}`,
    }),
    retry: { maxRetries: 3, baseDelayMs: 1000 }, // retry shipping API
    onError: "cancelled", // if shipping fails after retries → cancel
  })

  .on("deliver", {
    from: "shipped",
    to: "delivered",
    action: (ctx: any) => ({
      items: ctx.items,
      trackingId: ctx.trackingId,
      deliveredAt: new Date(),
    }),
  })

  // Multi-source: cancel from draft or submitted
  .on("cancel", {
    from: ["draft", "submitted"],
    to: "cancelled",
    action: () => ({ reason: "Customer cancelled" }),
  })

  .initial("draft")
  .build();

// ---------------------------------------------------------------------------
// 3. Run it
// ---------------------------------------------------------------------------

// Happy path
await orderMachine.start({
  id: "order-1",
  context: { items: ["Widget", "Gadget"], customerId: "cust-42" },
  metadata: { channel: "web" },
});

await orderMachine.send({ id: "order-1", event: "submit" });
await orderMachine.send({ id: "order-1", event: "approve" });
await orderMachine.send({ id: "order-1", event: "ship" });
await orderMachine.send({ id: "order-1", event: "deliver" });

const finalState = await orderMachine.getState("order-1");
console.log("Final state:", finalState);
// { current: "delivered", context: { items: [...], trackingId: "...", deliveredAt: ... } }

// Rejection + resubmit (cycle)
await orderMachine.start({
  id: "order-2",
  context: { items: ["Thingamajig"], customerId: "cust-99" },
});
await orderMachine.send({ id: "order-2", event: "submit" });
await orderMachine.send({ id: "order-2", event: "reject" });
await orderMachine.send({ id: "order-2", event: "resubmit" }); // back to draft!
console.log("After resubmit:", await orderMachine.getState("order-2"));
// { current: "draft", context: { items: ["Thingamajig"], customerId: "cust-99" } }

// Event history (audit trail)
const history = await orderMachine.getHistory("order-1");
console.log(`Order-1 had ${history.length} transitions`);
