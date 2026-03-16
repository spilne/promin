/**
 * Synchronous workflow from API — wait for result
 *
 * Business flow:
 * 1. Customer submits a checkout request with their cart and payment method
 * 2. System validates the cart contents and calculates the total
 * 3. Inventory is reserved for each item in the order
 * 4. Payment is charged (with automatic retry on transient failures)
 * 5. Order is confirmed and a confirmation record is created
 * 6. If any step fails, previous steps are automatically rolled back (inventory released, payment refunded)
 *
 * Retrying with the same order ID resumes the existing workflow rather than starting a new one.
 */

import { workflow, Pipeline, InMemoryWorkflowStorage } from "@promin/core";

const storage = new InMemoryWorkflowStorage();

// ---------------------------------------------------------------------------
// Checkout workflow
// ---------------------------------------------------------------------------

interface CheckoutInput {
  orderId: string;
  userId: string;
  items: { productId: string; quantity: number; price: number }[];
  paymentMethod: string;
}

const checkout = workflow<CheckoutInput>({
  name: "checkout",
  storage,
  type: "order",
  compensate: {
    trigger: "immediate",
    onComplete: ({ error, compensatedSteps }) =>
      Pipeline.fromPromise(async () => {
        console.log(`Checkout failed: ${error}. Rolled back: ${compensatedSteps.join(", ")}`);
        // await alerting.notify("checkout-failure", { error, compensatedSteps });
      }),
  },
})
  .stepAsync("validate-cart", async ({ input }) => {
    // Verify items are still available and prices haven't changed
    const total = input.items.reduce((sum, i) => sum + i.price * i.quantity, 0);
    if (total <= 0) throw new Error("Empty cart");
    return { items: input.items, total, currency: "USD" };
  })

  .stepAsync("reserve-inventory", async ({ prev, input }) => {
    // Reserve stock — returns reservation IDs
    const reservations = prev.items.map((item) => ({
      productId: item.productId,
      reservationId: `res_${item.productId}_${Date.now()}`,
      quantity: item.quantity,
    }));
    return { reservations, total: prev.total };
  }, {
    compensate: async ({ result }) => {
      // Release reserved inventory on failure
      for (const res of (result as any).reservations) {
        console.log(`Releasing reservation ${res.reservationId}`);
        // await inventoryService.release(res.reservationId);
      }
    },
  })

  .stepAsync("charge-payment", async ({ prev, input }) => {
    // Charge the customer
    return {
      paymentId: `pay_${Date.now()}`,
      amount: prev.total,
      method: input.paymentMethod,
      status: "charged",
    };
  }, {
    retry: { maxRetries: 2, baseDelayMs: 1000 },
    compensate: async ({ result }) => {
      console.log(`Refunding payment ${(result as any).paymentId}`);
      // await paymentService.refund((result as any).paymentId);
    },
  })

  .stepAsync("confirm-order", async ({ prev, input }) => {
    // Create order record and send confirmation
    return {
      orderId: input.orderId,
      paymentId: prev.paymentId,
      amount: prev.amount,
      status: "confirmed",
      confirmedAt: new Date().toISOString(),
    };
  })
  .build();

// ---------------------------------------------------------------------------
// API route handler
// ---------------------------------------------------------------------------

async function handleCheckout(request: {
  user: { id: string; email: string };
  body: { items: { productId: string; quantity: number; price: number }[]; paymentMethod: string };
}) {
  const orderId = `ord_${request.user.id}_${Date.now()}`;

  const { data, error } = await checkout.runSafe({
    workflowId: `checkout-${orderId}`,
    input: {
      orderId,
      userId: request.user.id,
      items: request.body.items,
      paymentMethod: request.body.paymentMethod,
    },
  });

  if (error) {
    // Compensation already ran — inventory released, payment refunded
    return { status: 500, body: { error: "Checkout failed. No charges applied." } };
  }

  return { status: 200, body: { order: data } };
}

// Retry-safe: same orderId = resume existing workflow
async function handleRetry(request: { user: { id: string }; orderId: string }) {
  const state = await storage.loadWorkflow(`checkout-${request.orderId}`);
  if (!state) return { status: 404, body: { error: "Order not found" } };
  if (state.status === "completed") return { status: 200, body: { order: state.result } };
  if (state.status === "failed") return { status: 500, body: { error: state.error } };
  return { status: 202, body: { status: "processing" } };
}

export { checkout, handleCheckout, handleRetry };
