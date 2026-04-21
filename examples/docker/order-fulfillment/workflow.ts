// ---------------------------------------------------------------------------
// Order fulfillment workflow — saga compensation + signal-wait demo.
//
// Shape:
//   reserve-inventory (needs: inventory)
//       │                          compensate: release held stock
//       ▼
//   charge-payment    (needs: payment)   ← flaky: retries + eventually succeeds
//       │                          compensate: refund
//       ▼
//   generate-label    (needs: shipping)
//       │
//       ▼
//   notify-warehouse  (default)
//       │
//       ▼
//   wait-for-shipped  (signal)      ← suspends until warehouse sends "shipped"
//       │
//       ▼
//   mark-delivered    (default)
//
// Saga contract: if `charge-payment` fails after all retries, the runner
// cascades compensation last-completed-first — `reserve-inventory`'s
// compensate fires and releases held stock. Nothing further runs.
//
// The bodies are stubs (sleep + log) but the DAG + compensate hooks are
// real — a coordinator picking this workflow up actually runs them in the
// order above, actually suspends at the signal, actually fires
// compensation on the cascaded failure path.
// ---------------------------------------------------------------------------

import { Pipeline } from "@promin/core";
import { workflow, type StepHandler } from "@promin/workflow";

export interface OrderInput {
  readonly orderId: string;
  readonly userId: string;
  readonly items: ReadonlyArray<{ sku: string; qty: number }>;
  readonly amountCents: number;
}

export interface InventoryResult {
  readonly reservationId: string;
}

export interface PaymentResult {
  readonly transactionId: string;
  readonly amountCents: number;
}

export interface LabelResult {
  readonly trackingNumber: string;
  readonly carrier: string;
}

export interface ShippedSignal {
  readonly trackingNumber: string;
  readonly shippedAt: string;
}

export interface DeliveredResult {
  readonly orderId: string;
  readonly deliveredAt: string;
}

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

/** Build the order fulfillment workflow as a pure `Workflow` definition. */
export function buildOrderWorkflow() {
  return (
    workflow<OrderInput>({
      name: "order-fulfillment",
      version: "1",
      // Saga: if the workflow fails anywhere below, run compensation
      // bottom-up for every completed step that has a `compensate` hook.
      compensate: { trigger: "after-retries" },
    })
      .stepAsync(
        "reserve-inventory",
        async (): Promise<InventoryResult> => {
          throw new Error("placeholder — worker runs the real handler");
        },
        {
          needs: ["inventory"],
          compensate: async ({ result }) => {
            // Release held stock on failure cascade. In a real impl this
            // would call the inventory service's release endpoint.
            const { reservationId } = result as InventoryResult;
            console.log(`[compensate] releasing reservation ${reservationId}`);
            await sleep(30);
          },
        },
      )
      .stepAsync(
        "charge-payment",
        { dependsOn: ["reserve-inventory"] },
        async (): Promise<PaymentResult> => {
          throw new Error("placeholder");
        },
        {
          needs: ["payment"],
          // Retry transient payment-gateway failures before giving up.
          retry: { maxRetries: 3, baseDelayMs: 500 },
          compensate: async ({ result }) => {
            const { transactionId } = result as PaymentResult;
            console.log(`[compensate] refunding ${transactionId}`);
            await sleep(30);
          },
        },
      )
      .stepAsync(
        "generate-label",
        { dependsOn: ["charge-payment"] },
        async (): Promise<LabelResult> => {
          throw new Error("placeholder");
        },
        { needs: ["shipping"] },
      )
      .stepAsync("notify-warehouse", { dependsOn: ["generate-label"] }, async (): Promise<void> => {
        throw new Error("placeholder");
      })
      // Suspend until the warehouse posts a "shipped" signal. Resume
      // happens when `storage.deliverSignal(workflowId, "shipped", ...)`
      // fires from whatever delivered the real event (webhook, Kafka
      // consumer, UI action).
      .waitForSignal<ShippedSignal>("wait-for-shipped", {
        signalName: "shipped",
        timeoutMs: 7 * 24 * 60 * 60_000, // 7 days
      })
      .stepAsync("mark-delivered", async (): Promise<DeliveredResult> => {
        throw new Error("placeholder");
      })
      .build()
  );
}

// ---------------------------------------------------------------------------
// Step handlers — each worker picks the subset it cares about.
// ---------------------------------------------------------------------------

export const reserveInventoryHandler: StepHandler = (ctx) =>
  Pipeline.fromPromise(async () => {
    const { orderId, items } = ctx.input as OrderInput;
    console.log(`[inventory] ${ctx.workflowId} — reserving ${items.length} line(s) for ${orderId}`);
    await sleep(200);
    return {
      reservationId: `res-${orderId}-${Math.random().toString(36).slice(2, 8)}`,
    } satisfies InventoryResult;
  });

/**
 * Payment handler with simulated flakiness — fails the first attempt,
 * succeeds on retry. Exercises the retry-with-backoff path configured on
 * the step. Remove the random-fail for deterministic demo output.
 */
export const chargePaymentHandler: StepHandler = (ctx) =>
  Pipeline.fromPromise(async () => {
    const { orderId, amountCents } = ctx.input as OrderInput;
    console.log(`[payment] ${ctx.workflowId} — charging ${amountCents}¢ (attempt ${ctx.attempt})`);
    await sleep(300);
    // First attempt fails for demo purposes — retry-with-backoff kicks in.
    if (ctx.attempt === 1) {
      throw new Error("transient: payment gateway 503");
    }
    return {
      transactionId: `tx-${orderId}-${Math.random().toString(36).slice(2, 8)}`,
      amountCents,
    } satisfies PaymentResult;
  });

export const generateLabelHandler: StepHandler = (ctx) =>
  Pipeline.fromPromise(async () => {
    const { orderId } = ctx.input as OrderInput;
    console.log(`[shipping] ${ctx.workflowId} — generating label for ${orderId}`);
    await sleep(150);
    return {
      trackingNumber: `1Z${orderId.replace(/[^A-Z0-9]/gi, "").toUpperCase()}`,
      carrier: "DEMO",
    } satisfies LabelResult;
  });

export const notifyWarehouseHandler: StepHandler = (ctx) =>
  Pipeline.fromPromise(async () => {
    const { orderId } = ctx.input as OrderInput;
    console.log(`[notify] ${ctx.workflowId} — warehouse notified for ${orderId}`);
    await sleep(80);
  });

export const markDeliveredHandler: StepHandler = (ctx) =>
  Pipeline.fromPromise(async () => {
    const { orderId } = ctx.input as OrderInput;
    const shipped = (ctx.deps as { "wait-for-shipped"?: ShippedSignal })["wait-for-shipped"];
    console.log(
      `[delivered] ${ctx.workflowId} — order ${orderId} shipped via ${shipped?.trackingNumber ?? "(no tracking)"}`,
    );
    await sleep(50);
    return {
      orderId,
      deliveredAt: new Date().toISOString(),
    } satisfies DeliveredResult;
  });
