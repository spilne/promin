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

function jitter(baseMs: number, spreadMs: number): number {
  return baseMs + Math.floor(Math.random() * spreadMs);
}

export const paymentWorkflow = workflow<PaymentInput>({ name: "payment" })
  .stepAsync("authorize", async ({ input }) => {
    await sleep(jitter(120, 300));
    // ~15% authorization failure rate.
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
    await sleep(jitter(100, 250));
    return { captureId: `cap-${Math.floor(Math.random() * 1_000_000)}`, authId: prev.authId };
  })
  .stepAsync("ledger-entry", async ({ prev }) => {
    await sleep(jitter(60, 180));
    return { ledgerId: `lg-${Math.floor(Math.random() * 1_000_000)}`, captureId: prev.captureId };
  })
  .build();
