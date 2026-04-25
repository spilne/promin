// ---------------------------------------------------------------------------
// user-app.ts — what a real application looks like when it uses Zorya.
//
// Run alongside the demo:
//   # terminal 1: bun run packages/zorya/examples/demo.ts
//   # terminal 2: bun --conditions=@promin/source run packages/zorya/examples/user-app.ts
//
// Two patterns shown, both useful, both backed by Zorya's storage + dashboard:
//
// PATTERN A — Trigger an EXISTING workflow that lives in another process
//   (e.g. the demo's "order" workflow). Use `client.startByName(...)` and
//   `handle.result()` to fire-and-await. Your app doesn't host the
//   workflow code; Zorya routes the run to whichever worker advertises it.
//
// PATTERN B — Run YOUR workflow as a regular async function from app code.
//   `ZoryaRunner` executes the workflow IN your process, persisting every
//   state transition + activity-journal entry to Zorya over the wire.
//   No queue, no advertisement, no risk of accidentally claiming someone
//   else's runs — you only run what you call. Same dashboard visibility.
//
// (The third pattern, `ZoryaWorker`, is the right choice when the workflow
//  uses `ctx.sleep` and must resume after this process exits, or when you
//  want the dashboard's Trigger button to dispatch into your process.
//  See examples/split/worker.ts.)
// ---------------------------------------------------------------------------

import { workflow } from "@promin/workflow";
import { ZoryaClient, ZoryaRunner } from "@promin/zorya-client";

// ---------------------------------------------------------------------------
// PATTERN B — define a workflow inline. Just JS; .build() snapshots the DAG.

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
// Boot — point at the running Zorya. ZoryaRunner is the local-runner-with-
// remote-storage variant: workflow runs HERE, Zorya stores everything.

const url = process.env["ZORYA_URL"] ?? "http://localhost:4100";
const client = new ZoryaClient({ url, apiKey: process.env["ZORYA_API_KEY"] });
const runner = new ZoryaRunner({ client });

console.log(`[app] connected to ${url}`);

// ---------------------------------------------------------------------------
// Pattern B in action — call the workflow like a normal async function.

async function processOrder(orderId: number, amountCents: number): Promise<void> {
  console.log(`[app] processing order ${orderId} ($${(amountCents / 100).toFixed(2)})…`);

  // The whole workflow runs in this process. Storage RPCs hit Zorya so
  // the dashboard shows the run with full step DAG. Output is typed
  // off `verifyOrder`'s last step.
  const result = await runner.run(verifyOrder, { orderId, amountCents });

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
// Pattern A — trigger an EXISTING workflow that's hosted by the demo
// process (the "order" workflow scanned out of examples/workflows/).
// `startByName` enqueues a start; the workflow runs THERE, not here.

async function callExistingDemoWorkflow(): Promise<void> {
  console.log(`[app] triggering "order" (hosted by the demo)…`);
  const handle = await client.startByName("order", {
    input: { orderId: 999, customer: "user-app" },
  });
  // .result() awaits the run's terminal status. The demo's "order"
  // workflow has random 2-20s sleeps, so this can take a while.
  const result = await handle.result().catch((e) => ({ error: String(e) }));
  console.log(`[app]  → order completed:`, result);
}

// ---------------------------------------------------------------------------
// Drive realistic traffic: a few in-process verifications, then one
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

console.log(`\n[app] done. Exiting — runner has no background loops to stop.`);
