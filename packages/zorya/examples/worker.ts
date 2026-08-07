// ---------------------------------------------------------------------------
// worker.ts — remote worker that joins the demo server.
//
// Run after the demo is up:
//   bun run packages/zorya/examples/demo.ts
//   # in another terminal:
//   bun run packages/zorya/examples/worker.ts
//
// What this demonstrates:
//   1. A worker process advertising workflows the demo's in-process map
//      doesn't know about (`hello-world`, `fan-out-demo`).
//   2. The demo's `LocalWorkflows + QueuedWorkflows-fallback` chain
//      handles trigger requests for those names: not in definitions →
//      falls through → enqueued to workflowStarts → this worker claims
//      and runs them.
//   3. Worker writes storage (run state, step results, journal entries)
//      back to the demo's SQLite via the /rpc/storage proxy.
//   4. Same dashboard, single source of truth, two processes.
//
// Triggering: open the dashboard's Workflows page — `hello-world` and
// `fan-out-demo` show up because this worker advertised them. Click
// trigger; the run lands here. Or via curl:
//
//   curl -X POST http://localhost:4100/api/runs/trigger/hello-world \
//     -H 'content-type: application/json' \
//     -d '{"input":{"name":"curl"}}'
//
// To run additional workers (capability routing / load distribution),
// just start more processes.
// ---------------------------------------------------------------------------

import { workflow } from "@promin/workflow";
import { createZoryaWorkerBuilder, ZoryaClient } from "@promin/zorya-client";
import { createDemoLogger } from "./demo/logger.ts";

const url = process.env["ZORYA_URL"] ?? "http://localhost:4100";
const logger = createDemoLogger("worker");

const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

// hello-world — minimal two-step workflow that proves the round-trip.
// Versioned + drained on mismatch so concurrent workers running v1 can
// finish their existing runs while new triggers go to v2.
interface HelloInput {
  name?: string;
}
const helloV1 = workflow<HelloInput>({ name: "hello-world", type: "demo", version: "1" })
  .stepAsync("greet", async ({ input }) => {
    await sleep(300);
    return { message: `Hello, ${input.name ?? "world"}!` };
  })
  .build();
const hello = workflow<HelloInput>({
  name: "hello-world",
  type: "demo",
  version: "2",
  onVersionMismatch: "drain",
  previousVersions: [helloV1],
})
  .stepAsync("greet", async ({ input }) => {
    await sleep(300);
    return { message: `Hello, ${input.name ?? "world"}!` };
  })
  .stepAsync("farewell", async ({ prev }) => {
    await sleep(200);
    return {
      ...prev,
      farewell: `Bye, ${prev.message.replace("Hello, ", "").replace("!", "")}.`,
    };
  })
  .build();

// fan-out-demo — three independent steps in parallel; renders a wider
// timeline so you can see step-level concurrency on the run detail page.
interface FanOutInput {
  items?: string[];
}
const fanOut = workflow<FanOutInput>({ name: "fan-out-demo", type: "demo" })
  .stepAsync("a", async () => {
    await sleep(500);
    return { result: "a-done" };
  })
  .stepAsync("b", async () => {
    await sleep(700);
    return { result: "b-done" };
  })
  .stepAsync("c", async () => {
    await sleep(400);
    return { result: "c-done" };
  })
  .build();

const workflows = [hello, fanOut];

function sampleInputFor(name: string): unknown {
  switch (name) {
    case "hello-world":
      return { name: "world" };
    case "fan-out-demo":
      return { items: ["x", "y", "z"] };
    default:
      return {};
  }
}

const client = new ZoryaClient({ url });
const worker = createZoryaWorkerBuilder()
  .client(client)
  .workflows(...workflows)
  .sampleInput(sampleInputFor)
  .build();

await worker.start();

logger.log(`Worker ${worker.workerId} connected to ${url}`);
logger.log(`  Advertised: ${workflows.map((w) => `${w.name}@${w.version ?? "—"}`).join(", ")}`);
logger.log(`  Trigger from the dashboard or via curl:`);
logger.log(
  `    curl -X POST ${url}/api/runs/trigger/hello-world \\\n      -H 'content-type: application/json' -d '{"input":{"name":"curl"}}'`,
);

// Graceful shutdown — unregisters the worker so the demo's Workers page
// doesn't show stale entries.
const stop = async () => {
  logger.log("Stopping worker…");
  await worker.stop();
  process.exit(0);
};
process.on("SIGINT", stop);
process.on("SIGTERM", stop);
