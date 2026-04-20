/**
 * Order processing workflow — validate, charge, ship.
 * If it crashes after charging, it resumes from shipping (not re-charges).
 */

import { workflow, createWorkflowRunner } from "@promin/workflow";
import { PostgresWorkflowStorage, migrate } from "@promin/postgres";

declare const db: any;

await migrate(db);
const storage = await PostgresWorkflowStorage.create({ db });
const runner = createWorkflowRunner({ storage });

const processOrder = workflow<{ orderId: string; amount: number }>({
  name: "process-order",
})
  .stepAsync("validate", async ({ input }) => {
    if (input.amount <= 0) throw new Error("Invalid amount");
    return { valid: true, orderId: input.orderId };
  })
  .stepAsync("charge", async ({ prev }) => {
    const tx = await chargeCard(prev.orderId, 99.99);
    return { txId: tx.id };
  })
  .stepAsync("ship", async ({ prev }) => {
    const tracking = await createShipment(prev.txId);
    return { trackingNumber: tracking.number };
  })
  .build();

// Run — crashes resume from last completed step
const result = await runner.run({
  workflow: processOrder,
  workflowId: "order-123",
  input: { orderId: "ord_42", amount: 99.99 },
});

console.log(result); // { trackingNumber: "1Z..." }

// Stubs
async function chargeCard(_id: string, _amount: number) {
  return { id: "tx_1" };
}
async function createShipment(_txId: string) {
  return { number: "1Z999" };
}
