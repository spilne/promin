/**
 * Bank transfer with saga compensation.
 * Debit sender → credit recipient.
 * If credit fails, the debit is automatically reversed.
 */

import { workflow, InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";

const storage = new InMemoryWorkflowStorage();
const runner = createWorkflowRunner({ storage });

const transfer = workflow<{ from: string; to: string; amount: number }>({
  name: "bank-transfer",
  retry: { maxRetries: 2 },
  compensate: { trigger: "after-retries" },
})
  .stepAsync(
    "debit",
    async ({ input }) => {
      const tx = await bankApi.debit(input.from, input.amount);
      return { txId: tx.id };
    },
    {
      compensate: async ({ result }) => {
        await bankApi.refund(result.txId);
      },
    },
  )
  .stepAsync(
    "credit",
    async ({ input }) => {
      const tx = await bankApi.credit(input.to, input.amount);
      return { txId: tx.id };
    },
    {
      compensate: async ({ result }) => {
        await bankApi.refund(result.txId);
      },
    },
  )
  .stepAsync("notify", async ({ input }) => {
    await email.send(input.from, `Transferred $${input.amount} to ${input.to}`);
  })
  .build();

// If credit fails after retries → debit is automatically reversed
await runner.runSafe({
  workflow: transfer,
  workflowId: "txn-001",
  input: { from: "alice", to: "bob", amount: 100 },
});

// Stubs
const bankApi = {
  debit: async (_account: string, _amount: number) => ({ id: "tx_1" }),
  credit: async (_account: string, _amount: number) => ({ id: "tx_2" }),
  refund: async (_txId: string) => {},
};
const email = { send: async (_to: string, _msg: string) => {} };
