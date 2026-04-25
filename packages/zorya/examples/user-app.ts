// ---------------------------------------------------------------------------
// user-app.ts — what a real application looks like when it uses Zorya.
//
// Run alongside the demo:
//   # terminal 1: bun run packages/zorya/examples/demo.ts
//   # terminal 2: bun --conditions=@promin/source run packages/zorya/examples/user-app.ts
//
// What this shows (the two patterns you actually want):
//
// PATTERN A — Trigger an EXISTING workflow that runs somewhere else.
//   You don't host the workflow code; you just want Zorya to run it and
//   give you the result back. Comment out PATTERN B and run this against
//   a workflow that already exists in the demo (e.g. "order").
//
// PATTERN B — Define a workflow IN your app, host the worker IN your
//   process, and call it like a regular async function. The workflow
//   code runs locally; storage + journaling go through Zorya so you get
//   durability + the dashboard for free. This is the Temporal-app shape.
//
// In both, the app does:
//   1. await client.start(...) → WorkflowHandle
//   2. await handle.result()    → resolved Output
//   3. branch business logic on the result
// ---------------------------------------------------------------------------

import { workflow } from "@promin/workflow";
import { ZoryaClient, ZoryaWorker } from "@promin/zorya-client";

// ---------------------------------------------------------------------------
// PATTERN B — define + host + run a workflow in this same process.
//
// `verifyOrder` is a regular workflow. The two `.stepAsync` calls run
// sequentially with their results journaled to Zorya (so a crash mid-
// run resumes without losing work). The whole thing is just a normal
// JS function tree — no SDK ceremony, just `.build()`.

const verifyOrder = workflow<{ orderId: number; amountCents: number }>({
  name: "verify-order",
  type: "app",
})
  .stepAsync("validate", async ({ input }) => {
    await new Promise((r) => setTimeout(r, 200));
    return {
      valid: input.orderId > 0 && input.amountCents > 0,
      orderId: input.orderId,
    };
  })
  .stepAsync("score-risk", async ({ prev, input }) => {
    await new Promise((r) => setTimeout(r, 300));
    // Toy risk model — orders > $1000 are high risk; even orderIds
    // are flagged as suspicious; risky+invalid are auto-rejected.
    const risky = input.amountCents > 100_000 || input.orderId % 2 === 0;
    return { ...prev, risky, riskScore: risky ? 85 : 12 };
  })
  .build();

// ---------------------------------------------------------------------------
// Boot — point at the running Zorya, host the workflow, advertise it.

const url = process.env["ZORYA_URL"] ?? "http://localhost:4100";
const client = new ZoryaClient({ url, apiKey: process.env["ZORYA_API_KEY"] });
const worker = new ZoryaWorker({ client, workflows: [verifyOrder] });
await worker.start();

console.log(`[app] connected to ${url} — worker ${worker.workerId}`);

// ---------------------------------------------------------------------------
// PATTERN B — call the workflow from regular app code, branch on its
// result. Imagine this is the body of an HTTP handler or queue consumer.

async function processOrder(orderId: number, amountCents: number): Promise<void> {
  console.log(`[app] processing order ${orderId} ($${(amountCents / 100).toFixed(2)})…`);

  // .start() returns a WorkflowHandle<Output>; .result() awaits the
  // run's terminal status and resolves to the Output of the LAST step.
  const handle = await client.start(verifyOrder, {
    input: { orderId, amountCents },
  });
  const result = await handle.result();

  // Application code branches on the durable workflow's output.
  if (!result.valid) {
    console.log(`[app]  → order ${orderId}: REJECTED (invalid)`);
    return;
  }
  if (result.risky) {
    console.log(`[app]  → order ${orderId}: HOLD for review (risk ${result.riskScore})`);
    return;
  }
  console.log(`[app]  → order ${orderId}: APPROVED (risk ${result.riskScore})`);
}

// ---------------------------------------------------------------------------
// PATTERN A — trigger an EXISTING workflow that's hosted elsewhere
// (like "order", which the demo registers in-process). Same shape:
// `startByName` + `.result()`. Returns `unknown` since we don't have
// the workflow's static type at this site.

async function callExistingDemoWorkflow(): Promise<void> {
  console.log(`[app] triggering "order" (hosted by the demo, not this process)…`);
  const handle = await client.startByName("order", {
    input: { orderId: 999, customer: "user-app" },
  });
  const result = await handle.result().catch((e) => ({ error: String(e) }));
  console.log(`[app]  → order completed:`, result);
}

// ---------------------------------------------------------------------------
// Drive some realistic traffic: a few in-house verifications, then one
// remote-workflow call. All of these show up in the Zorya dashboard
// under Runs, with their full step DAG + per-step status.

const orders: Array<[number, number]> = [
  [1, 4_999], // valid, low-risk → APPROVED
  [2, 200_000], // even orderId + big amount → HOLD
  [3, 7_500], // valid, low-risk → APPROVED
  [-1, 100], // invalid → REJECTED
  [4, 12_000], // even orderId → HOLD
];

for (const [id, cents] of orders) {
  await processOrder(id, cents);
}

await callExistingDemoWorkflow();

console.log(`\n[app] done driving traffic. Worker is still up — Ctrl+C to disconnect.`);

const shutdown = async (sig: string) => {
  console.log(`[app] ${sig} — disconnecting…`);
  await worker.stop().catch(() => {});
  process.exit(0);
};
process.on("SIGINT", () => void shutdown("SIGINT"));
process.on("SIGTERM", () => void shutdown("SIGTERM"));
