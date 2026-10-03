// ---------------------------------------------------------------------------
// order-fulfillment workflow — saga with compensation.
//
// validate → reserve-stock → charge → ship
// If ship fails, the saga compensates in reverse:
//   ship-rollback ← charge-refund ← release-stock
// Dashboard: the Step tab shows compensationStatus + compensatedAt when
// the workflow ends in the compensating/failed path.
// ---------------------------------------------------------------------------

import { TaggedError, fail, sleep, succeed, type Eff, type Throws } from "@spilne/perfect-core";
import { workflow } from "@promin/workflow";

export interface OrderFulfillmentInput {
  orderId: number;
  items?: string[];
}

/** A simulated downstream rejection (declined card, refused shipment). */
export class FulfillmentError extends TaggedError("FulfillmentError")<{
  readonly message: string;
}>() {}

function delay(minMs: number, maxMs: number): number {
  return minMs + Math.floor(Math.random() * (maxMs - minMs));
}

function pSleep(ms: number): Eff<void, never> {
  return sleep(ms);
}

function pSuccess<T>(v: T, minMs: number, maxMs: number): Eff<T, never> {
  return pSleep(delay(minMs, maxMs)).map(() => v);
}

/**
 * Simulates a side effect that can fail. Returns an Eff that either
 * resolves after a random delay or fails with `msg`. The fail rate is
 * passed in so individual callsites can tune it.
 */
function pMaybeFail<T>(
  ok: T,
  opts: { minMs: number; maxMs: number; failRate: number; msg: string },
): Eff<T, Throws<FulfillmentError>> {
  return pSleep(delay(opts.minMs, opts.maxMs)).flatMap(
    (): Eff<T, Throws<FulfillmentError>> =>
      Math.random() < opts.failRate
        ? fail(new FulfillmentError({ message: opts.msg }))
        : succeed(ok),
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
