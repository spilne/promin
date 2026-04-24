// ---------------------------------------------------------------------------
// order workflow — validates, charges, notifies, cleans up. Each step has a
// 2s-20s random delay so the timeline is visibly animated and some runs
// stay running long enough to be inspected in the dashboard.
// ---------------------------------------------------------------------------

import { workflow } from "@promin/workflow";

export interface OrderInput {
  orderId: number;
  customer?: string;
  source?: string;
}

async function sleep(ms: number): Promise<void> {
  await new Promise((r) => setTimeout(r, ms));
}

/** Random delay in ms, uniformly distributed in [minMs, maxMs). */
function delay(minMs: number, maxMs: number): number {
  return minMs + Math.floor(Math.random() * (maxMs - minMs));
}

export const orderWorkflow = workflow<OrderInput>({ name: "order" })
  .stepAsync("validate", async ({ input }) => {
    await sleep(delay(2_000, 8_000));
    if (!input.orderId || input.orderId < 0) {
      throw new Error(`Invalid orderId: ${input.orderId}`);
    }
    return { orderId: input.orderId, customer: input.customer ?? "anon", validated: true };
  })
  .stepAsync("charge", async ({ prev }) => {
    await sleep(delay(5_000, 20_000));
    // Occasionally fail so the dashboard shows failures too.
    if (Math.random() < 0.08) {
      throw new Error("Card declined");
    }
    return { orderId: prev.orderId, charged: true, amountCents: 999 };
  })
  .stepAsync("notify-email", async ({ prev }) => {
    await sleep(delay(3_000, 10_000));
    return { channel: "email", to: `${prev.orderId}@example.com`, delivered: true };
  })
  .stepAsync("cleanup", async () => {
    await sleep(delay(2_000, 6_000));
    return { cleaned: true };
  })
  .build();
