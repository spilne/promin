// ---------------------------------------------------------------------------
// payment workflow — authorize, capture, ledger entry. Higher failure rate
// than `order` so the dashboard shows failures too.
// ---------------------------------------------------------------------------

import { workflow } from "@promin/workflow";

export interface PaymentInput {
  amount: number;
  currency?: string;
  mode?: string;
}

async function sleep(ms: number): Promise<void> {
  await new Promise((r) => setTimeout(r, ms));
}

function delay(minMs: number, maxMs: number): number {
  return minMs + Math.floor(Math.random() * (maxMs - minMs));
}

export const paymentWorkflow = workflow<PaymentInput>({ name: "payment", type: "billing" })
  .stepAsync("authorize", async ({ input }) => {
    await sleep(delay(5_000, 20_000));
    if (Math.random() < 0.15) {
      throw new Error(`Auth declined for amount ${input.amount}`);
    }
    return {
      authId: `auth-${Math.floor(Math.random() * 1_000_000)}`,
      amount: input.amount,
      currency: input.currency ?? "USD",
    };
  })
  .stepAsync("capture", async ({ prev }) => {
    await sleep(delay(4_000, 15_000));
    return { captureId: `cap-${Math.floor(Math.random() * 1_000_000)}`, authId: prev.authId };
  })
  .stepAsync("ledger-entry", async ({ prev }) => {
    await sleep(delay(2_000, 8_000));
    return { ledgerId: `lg-${Math.floor(Math.random() * 1_000_000)}`, captureId: prev.captureId };
  })
  .build();
