// ---------------------------------------------------------------------------
// order-fulfillment workflow — saga with compensation.
//
// validate → reserve-stock → charge → ship
// If ship fails, the saga compensates in reverse:
//   ship-rollback ← charge-refund ← release-stock
// Dashboard: the Step tab shows compensationStatus + compensatedAt when
// the workflow ends in the compensating/failed path.
// ---------------------------------------------------------------------------

import { workflow } from "@promin/workflow";
import { Pipeline } from "@promin/core";

export interface OrderFulfillmentInput {
  orderId: number;
  items?: string[];
}

function delay(minMs: number, maxMs: number): number {
  return minMs + Math.floor(Math.random() * (maxMs - minMs));
}

function pSleep(ms: number): Pipeline<void, never> {
  return Pipeline.fromPromise(() => new Promise<void>((r) => setTimeout(r, ms)));
}

function pSuccess<T>(v: T, minMs: number, maxMs: number): Pipeline<T, never> {
  return pSleep(delay(minMs, maxMs)).map(() => v);
}

/**
 * Simulates a side effect that can fail. Returns a Pipeline<T> that either
 * resolves after a random delay or fails with `msg`. The fail rate is
 * passed in so individual callsites can tune it.
 */
function pMaybeFail<T>(
  ok: T,
  opts: { minMs: number; maxMs: number; failRate: number; msg: string },
): Pipeline<T, Error> {
  return pSleep(delay(opts.minMs, opts.maxMs)).flatMap(() =>
    Math.random() < opts.failRate
      ? (Pipeline.fail(new Error(opts.msg)) as Pipeline<T, Error>)
      : Pipeline.succeed(ok),
  );
}

export const orderFulfillmentWorkflow = workflow<OrderFulfillmentInput>({
  name: "order-fulfillment",
  type: "saga",
})
  .step("validate", ({ input }) => pSuccess({ orderId: input.orderId }, 1_500, 5_000), {
    // read-only step → no compensate; nothing to roll back
  })
  .step(
    "reserve-stock",
    ({ prev }) => pSuccess({ orderId: prev.orderId, reserved: true }, 3_000, 8_000),
    {
      compensate: () => pSleep(delay(1_000, 3_000)).map(() => undefined),
    },
  )
  .step(
    "charge",
    ({ prev }) =>
      pMaybeFail(
        { orderId: prev.orderId, chargeId: `ch-${Math.floor(Math.random() * 1e6)}` },
        { minMs: 2_000, maxMs: 8_000, failRate: 0.08, msg: "Card declined" },
      ),
    {
      compensate: () => pSleep(delay(1_500, 4_000)).map(() => undefined),
    },
  )
  .step(
    "ship",
    ({ prev }) =>
      pMaybeFail(
        { orderId: prev.orderId, trackingId: `trk-${Math.floor(Math.random() * 1e6)}` },
        {
          minMs: 4_000,
          maxMs: 15_000,
          // Higher fail rate so the compensation cascade actually fires
          // reasonably often in the demo.
          failRate: 0.35,
          msg: "Carrier rejected shipment",
        },
      ),
    {
      compensate: () => pSleep(delay(2_000, 6_000)).map(() => undefined),
    },
  )
  .build();
