// ---------------------------------------------------------------------------
// worker.ts — user-owned worker that connects to a remote Zorya server.
// Runs workflows locally; storage writes go over the wire.
//
// Run (in another terminal, after server.ts is up):
//   bun run packages/zorya/examples/split/worker.ts
// ---------------------------------------------------------------------------

import { workflow } from "@promin/workflow";
import { ZoryaClient, ZoryaWorker } from "@promin/zorya-client";
import { orderWorkflow } from "../workflows/order.ts";
import { paymentWorkflow } from "../workflows/payment.ts";
import { onboardingWorkflow } from "../workflows/onboarding.ts";
import { etlWorkflow } from "../workflows/etl.ts";
import { videoTranscodeWorkflow } from "../workflows/video-transcode.ts";
import { orderFulfillmentWorkflow } from "../workflows/order-fulfillment.ts";
import { batchProcessWorkflow } from "../workflows/batch-process.ts";
import { approvalFlowWorkflow } from "../workflows/approval-flow.ts";

// ---------------------------------------------------------------------------
// Self-contained workflows defined inline in the worker process. They show
// up on the server via `advertise()` so you can trigger them from the
// dashboard Workflows page (or POST /api/runs/trigger/:name).
// ---------------------------------------------------------------------------

const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

interface HelloInput {
  name?: string;
}

const helloWorkflow = workflow<HelloInput>({ name: "hello-world", type: "demo" })
  .stepAsync("greet", async ({ input }) => {
    await sleep(500);
    return { message: `Hello, ${input.name ?? "world"}!` };
  })
  .build();

interface FanOutInput {
  items?: string[];
}

const fanOutWorkflow = workflow<FanOutInput>({ name: "fan-out-demo", type: "demo" })
  .stepAsync("collect", async ({ input }) => {
    await sleep(200);
    return input.items ?? ["alpha", "bravo", "charlie"];
  })
  .mapOverAsync("process-item", { array: "collect", concurrency: 2 }, async (item: string) => {
    await sleep(500 + Math.floor(Math.random() * 1500));
    return { item, upper: item.toUpperCase() };
  })
  .stepAsync("summarise", async ({ prev }) => {
    return { processed: prev.length, words: prev.map((r) => r.upper) };
  })
  .build();

interface FlakyInput {
  failUntilAttempt?: number;
}

const flakyWorkflow = workflow<FlakyInput>({ name: "flaky-retry-demo", type: "demo" })
  .stepAsync(
    "maybe-fail",
    async ({ input, attempt }) => {
      await sleep(300);
      const threshold = input.failUntilAttempt ?? 2;
      if (attempt < threshold) {
        throw new Error(`Attempt ${attempt} — not yet (needs ${threshold})`);
      }
      return { attempt, ok: true };
    },
    { retry: { maxRetries: 3, baseDelayMs: 500 } },
  )
  .build();

// ---------------------------------------------------------------------------
// Journaled-step demo. Inside the generator body, each `yield* ctx.activity`
// is recorded in the activity journal — on resume after a crash, completed
// activities replay from the journal instead of re-running. ctx.sleep is
// journaled too, so a sleeping run survives a worker restart. Works
// end-to-end over the remote wire (RemoteWorkflowStorage proxies the
// ActivityJournalStorage / JournaledSuspendStorage methods).
// ---------------------------------------------------------------------------

interface ResearchInput {
  topic?: string;
}

const researchWorkflow = workflow<ResearchInput>({ name: "journaled-research", type: "demo" })
  .journaled("research", function* (ctx, input) {
    const topic = input.topic ?? "workflows";

    const sources = yield* ctx.activity("fetch-sources", async () => {
      await sleep(400);
      return [
        { id: 1, url: `https://example.com/${topic}/intro` },
        { id: 2, url: `https://example.com/${topic}/deep-dive` },
        { id: 3, url: `https://example.com/${topic}/comparisons` },
      ];
    });

    const summaries: Array<{ id: number; words: number }> = [];
    for (const src of sources) {
      const summary = yield* ctx.activity(`analyze-${src.id}`, async () => {
        await sleep(300 + Math.floor(Math.random() * 700));
        return { id: src.id, words: 50 + Math.floor(Math.random() * 200) };
      });
      summaries.push(summary);
    }

    // ctx.sleep is journaled too — survives worker restarts.
    yield* ctx.sleep(500);

    const report = yield* ctx.activity("compose-report", async () => {
      await sleep(400);
      return {
        topic,
        totalWords: summaries.reduce((s, x) => s + x.words, 0),
        sourceCount: summaries.length,
      };
    });

    return report;
  })
  .build();

const url = process.env.ZORYA_URL ?? "http://localhost:4100";
const apiKey = process.env.ZORYA_API_KEY;

const client = new ZoryaClient({ url, apiKey });

const workflows = [
  helloWorkflow,
  fanOutWorkflow,
  flakyWorkflow,
  researchWorkflow,
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
    case "hello-world":
      return { name: "dashboard" };
    case "fan-out-demo":
      return { items: ["alpha", "bravo", "charlie", "delta"] };
    case "flaky-retry-demo":
      return { failUntilAttempt: 2 };
    case "journaled-research":
      return { topic: "durable-execution" };
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
console.log(`  Advertised ${workflows.length} workflows:`);
for (const wf of workflows) {
  console.log(`    - ${wf.name}`);
}
console.log(`  Trigger manually from the dashboard → Workflows page, or:`);
console.log(
  `    curl -X POST ${url}/api/runs/trigger/hello-world \\\n      -H 'content-type: application/json' \\\n      -d '{"input":{"name":"curl"}}'`,
);

// Auto-fire loop is opt-in (DEMO_RUN=true) so the worker stays idle until
// manually triggered — handy when inspecting a single run in the dashboard.
if (process.env.DEMO_RUN === "true") {
  setInterval(() => {
    const pick = workflows[Math.floor(Math.random() * workflows.length)]!;
    void worker.run({ workflow: pick, input: sampleInputFor(pick.name) }).catch(() => {
      // Per-run failures are recorded in storage — stay up and keep ticking.
    });
  }, 5_000);
  console.log(`  - Auto-firing one random workflow every 5s (DEMO_RUN=true)`);
} else {
  console.log(`  - Idle (set DEMO_RUN=true to auto-fire one random workflow every 5s)`);
}

// Graceful shutdown so the worker unregisters itself.
const stop = async () => {
  console.log("Stopping worker…");
  await worker.stop();
  process.exit(0);
};
process.on("SIGINT", stop);
process.on("SIGTERM", stop);
