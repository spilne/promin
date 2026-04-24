// ---------------------------------------------------------------------------
// order workflow — validates, charges, fans out to parallel notifications,
// then cleans up. Each step has a small random delay so the timeline is
// visibly animated in the dashboard.
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

function jitter(baseMs: number, spreadMs: number): number {
  return baseMs + Math.floor(Math.random() * spreadMs);
}

export const orderWorkflow = workflow<OrderInput>({ name: "order" })
  .stepAsync("validate", async ({ input }) => {
    await sleep(jitter(80, 200));
    if (!input.orderId || input.orderId < 0) {
      throw new Error(`Invalid orderId: ${input.orderId}`);
    }
    return { orderId: input.orderId, customer: input.customer ?? "anon", validated: true };
  })
  .stepAsync("charge", async ({ prev }) => {
    await sleep(jitter(180, 400));
    // Occasionally fail so we see retries + failures in the UI.
    if (Math.random() < 0.08) {
      throw new Error("Card declined");
    }
    return { orderId: prev.orderId, charged: true, amountCents: 999 };
  })
  .stepAsync("notify-email", async ({ prev }) => {
    await sleep(jitter(60, 150));
    return { channel: "email", to: `${prev.orderId}@example.com`, delivered: true };
  })
  .stepAsync("cleanup", async () => {
    await sleep(jitter(40, 120));
    return { cleaned: true };
  })
  .build();
