// ---------------------------------------------------------------------------
// worker.ts — user-owned worker that connects to a remote Zorya server.
// Runs workflows locally; storage writes go over the wire.
//
// Run (in another terminal, after server.ts is up):
//   bun run packages/zorya/examples/split/worker.ts
// ---------------------------------------------------------------------------

import { ZoryaClient, ZoryaWorker } from "@promin/zorya-client";
import { orderWorkflow } from "../workflows/order.ts";
import { paymentWorkflow } from "../workflows/payment.ts";
import { onboardingWorkflow } from "../workflows/onboarding.ts";
import { etlWorkflow } from "../workflows/etl.ts";
import { videoTranscodeWorkflow } from "../workflows/video-transcode.ts";
import { orderFulfillmentWorkflow } from "../workflows/order-fulfillment.ts";
import { batchProcessWorkflow } from "../workflows/batch-process.ts";
import { approvalFlowWorkflow } from "../workflows/approval-flow.ts";

const url = process.env.ZORYA_URL ?? "http://localhost:4100";
const apiKey = process.env.ZORYA_API_KEY;

const client = new ZoryaClient({ url, apiKey });

const workflows = [
  orderWorkflow,
  paymentWorkflow,
  onboardingWorkflow,
  etlWorkflow,
  videoTranscodeWorkflow,
  orderFulfillmentWorkflow,
  batchProcessWorkflow,
  approvalFlowWorkflow,
] as const;

function sampleInputFor(name: string): unknown {
  switch (name) {
    case "order":
      return { orderId: 1, customer: "cust-0" };
    case "payment":
      return { amount: 100, currency: "USD" };
    case "video-transcode":
      return { videoId: "vid-1" };
    case "onboarding":
      return { email: "user@example.com" };
    case "etl":
      return { source: "events-prod" };
    case "order-fulfillment":
      return { orderId: 1 };
    case "batch-process":
      return { itemCount: 6 };
    case "approval-flow":
      return { requestId: 1 };
    default:
      return {};
  }
}

const worker = new ZoryaWorker({
  client,
  workflows: workflows as unknown as ReadonlyArray<
    import("@promin/workflow").Workflow<unknown, unknown>
  >,
  sampleInput: sampleInputFor,
});

await worker.start();
console.log(`Worker ${worker.workerId} connected to ${url}`);
console.log(`  Advertised ${workflows.length} workflows`);
console.log(`  Kick off a run:`);
console.log(`    await worker.run({ workflow: 'order', input: { orderId: 42 } })`);

// Keep the process alive + optionally run one demo workflow on startup
// so the dashboard has activity without needing manual triggers.
if (process.env.DEMO_RUN !== "false") {
  setInterval(() => {
    const pick = workflows[Math.floor(Math.random() * workflows.length)]!;
    void worker.run({ workflow: pick, input: sampleInputFor(pick.name) }).catch(() => {
      // Per-run failures are recorded in storage — stay up and keep ticking.
    });
  }, 5_000);
  console.log(`  - Firing one random workflow every 5s (set DEMO_RUN=false to disable)`);
}

// Graceful shutdown so the worker unregisters itself.
const stop = async () => {
  console.log("Stopping worker…");
  await worker.stop();
  process.exit(0);
};
process.on("SIGINT", stop);
process.on("SIGTERM", stop);
