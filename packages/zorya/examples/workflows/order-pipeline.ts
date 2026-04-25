// ---------------------------------------------------------------------------
// order-pipeline workflow — demonstrates nested workflows via .subworkflow().
//
// A single parent run spawns three independently-durable children. Each
// child shows up in the dashboard with its own row, its own step graph,
// and a `parentWorkflowId` pointing back at this run. The parent's
// run-detail view picks them up under the Children tab.
//
//   order-pipeline (parent)
//   ├── reserve-inventory (child)
//   ├── charge-card        (child)
//   └── ship-label         (child)
//
// The intent is to show:
//   - sub-runs persist independently (you can re-run a single child)
//   - the parent's step bar reflects the full child duration
//   - parent ↔ child navigation works (Children tab on parent, "Parent"
//     link on each child's Overview tab)
// ---------------------------------------------------------------------------

import { workflow } from "@promin/workflow";

export interface OrderPipelineInput {
  orderId: number;
  customer?: string;
  amountCents?: number;
}

const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));
const delay = (minMs: number, maxMs: number) => minMs + Math.floor(Math.random() * (maxMs - minMs));

// ---------------------------------------------------------------------------
// Children — each is a self-contained workflow that the parent calls into.
// They're exported so the worker can advertise + run them on the same fleet.
// ---------------------------------------------------------------------------

interface ReserveInventoryInput {
  orderId: number;
  sku?: string;
}

export const reserveInventoryWorkflow = workflow<ReserveInventoryInput>({
  name: "reserve-inventory",
  type: "commerce-child",
})
  .stepAsync("lookup-sku", async ({ input }) => {
    await sleep(delay(800, 2_000));
    return { orderId: input.orderId, sku: input.sku ?? `SKU-${input.orderId % 1000}` };
  })
  .stepAsync("hold", async ({ prev }) => {
    await sleep(delay(1_000, 3_000));
    return { orderId: prev.orderId, sku: prev.sku, reservationId: `r-${Date.now()}` };
  })
  .build();

interface ChargeCardInput {
  orderId: number;
  customer: string;
  amountCents: number;
}

export const chargeCardWorkflow = workflow<ChargeCardInput>({
  name: "charge-card",
  type: "commerce-child",
})
  .stepAsync("authorize", async ({ input }) => {
    await sleep(delay(1_500, 3_500));
    if (Math.random() < 0.05) throw new Error("Card declined");
    return { orderId: input.orderId, authCode: `auth-${Date.now()}` };
  })
  .stepAsync("capture", async ({ prev, input }) => {
    await sleep(delay(800, 2_000));
    return {
      orderId: prev.orderId,
      authCode: prev.authCode,
      capturedCents: input.amountCents,
    };
  })
  .build();

interface ShipLabelInput {
  orderId: number;
  customer: string;
}

export const shipLabelWorkflow = workflow<ShipLabelInput>({
  name: "ship-label",
  type: "commerce-child",
})
  .stepAsync("rate", async ({ input }) => {
    await sleep(delay(500, 1_500));
    return { orderId: input.orderId, carrier: "UPS", costCents: 599 };
  })
  .stepAsync("print", async ({ prev }) => {
    await sleep(delay(700, 1_800));
    return { orderId: prev.orderId, tracking: `1Z${Math.floor(Math.random() * 1e9)}` };
  })
  .build();

// ---------------------------------------------------------------------------
// Parent — wires the three children together. `.subworkflow()` writes the
// parentWorkflowId back-reference and runs the child as a step; the
// `prev` of the next step is the child's typed return value.
// ---------------------------------------------------------------------------

export const orderPipelineWorkflow = workflow<OrderPipelineInput>({
  name: "order-pipeline",
  type: "commerce",
})
  .step("validate", ({ input }) => ({
    orderId: input.orderId,
    customer: input.customer ?? "anon",
    amountCents: input.amountCents ?? 1999,
  }))
  .subworkflow("reserve", reserveInventoryWorkflow, {
    workflowId: (prev) => `order-${prev.orderId}.reserve`,
    input: (prev) => ({ orderId: prev.orderId }),
  })
  .subworkflow("charge", chargeCardWorkflow, {
    workflowId: (prev) => `order-${prev.orderId}.charge`,
    // Subworkflow `input` receives only the immediately-prior step's result,
    // so the customer + amount fields the parent's `validate` collected don't
    // reach this far on their own. Children that need parent-state would
    // either re-thread it through their predecessor's output (the cleanest
    // pattern), or accept defaults — for the demo we accept defaults.
    input: (prev) => ({ orderId: prev.orderId, customer: "anon", amountCents: 1999 }),
  })
  .subworkflow("ship", shipLabelWorkflow, {
    workflowId: (prev) => `order-${prev.orderId}.ship`,
    input: (prev) => ({ orderId: prev.orderId, customer: "anon" }),
  })
  .step("finalize", ({ prev }) => ({
    orderId: prev.orderId,
    tracking: prev.tracking,
    completedAt: new Date().toISOString(),
  }))
  .build();
