// ---------------------------------------------------------------------------
// worker.ts — remote worker that joins the demo server.
//
// Run after the demo is up:
//   bun run demo:server
//   # in another terminal:
//   bun run demo:worker
//
// What this demonstrates:
//   1. A worker process advertising workflows the demo's in-process map
//      doesn't know about (`hello-world`, `fan-out-demo`).
//   2. The demo's `LocalWorkflows + QueuedWorkflows-fallback` chain
//      handles trigger requests for those names: not in definitions →
//      falls through → enqueued to workflowStarts → this worker claims
//      the start (only of a version it hosts) and drives the whole run.
//   3. Worker writes storage (run state, step results, journal entries)
//      back to the demo's SQLite via the /rpc/storage proxy, fenced by
//      the run lock.
//   4. Sleeps, signals and orphaned runs come back through the same
//      queue: Zorya wakes and recovers them, a hosting worker drives them.
//   5. Start this before the server, or restart the server: the worker
//      retries and re-advertises on its own.
//
// Triggering: open the dashboard's Workflows page — `hello-world` and
// `fan-out-demo` show up because this worker advertised them. Click
// trigger; the run lands here. Or via curl:
//
//   curl -X POST http://localhost:4100/api/workflows/hello-world/runs \
//     -H 'content-type: application/json' \
//     -d '{"input":{"name":"curl"}}'
//
// To run additional workers (capability routing / load distribution),
// just start more processes.
// ---------------------------------------------------------------------------

import { workflow } from "@promin/workflow";
import { ZoryaClient, ZoryaWorker } from "@promin/zorya-client";

const url = process.env["ZORYA_URL"] ?? "http://localhost:4100";

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

const client = new ZoryaClient({ url, apiKey: process.env["ZORYA_WORKER_API_KEY"] });
const worker = new ZoryaWorker({
  client,
  workflows: workflows as unknown as ReadonlyArray<
    import("@promin/workflow").Workflow<unknown, unknown>
  >,
  sampleInput: sampleInputFor,
});

worker.start();
await worker.ready();

console.log(`Worker ${worker.workerId} connected to ${url}`);
console.log(`  Advertised: ${workflows.map((w) => `${w.name}@${w.version ?? "—"}`).join(", ")}`);
console.log(`  Trigger from the dashboard or via curl:`);
console.log(
  `    curl -X POST ${url}/api/workflows/hello-world/runs \\\n      -H 'content-type: application/json' -d '{"input":{"name":"curl"}}'`,
);

// Graceful shutdown — waits up to 10s for runs in flight, gives the rest
// back to the queue, and unregisters the worker.
const stop = async () => {
  console.log("Stopping worker…");
  await worker.stop({ timeoutMs: 10_000 });
  process.exit(0);
};
process.on("SIGINT", stop);
process.on("SIGTERM", stop);
