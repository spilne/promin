// ---------------------------------------------------------------------------
// worker.ts — step-mode worker against a coordinator-driven Zorya server.
//
// Each worker advertises its workflow definitions and then claims
// INDIVIDUAL STEPS from the server's step queue (`mode: "step"`). The
// coordinator on the server side decides which step is ready and pushes
// it; this worker just runs the body and writes the result back.
//
// Run two of these in separate terminals to see steps distributed across
// workers:
//
//   bun run packages/zorya/examples/split-step/worker.ts
//   bun run packages/zorya/examples/split-step/worker.ts   # second terminal
//
// Then trigger from the dashboard or via the API:
//   curl -X POST http://localhost:4101/api/runs/trigger/fan-out \
//        -H 'content-type: application/json' \
//        -d '{"input": {"id": 42}}'
// ---------------------------------------------------------------------------

import { Pipeline } from "@promin/core";
import { workflow } from "@promin/workflow";
import { ZoryaClient, ZoryaWorker } from "@promin/zorya-client";

const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

// Three independent steps fanning out from the input. With two step-mode
// workers running, each step should land on a different worker.
const fanOutWorkflow = workflow<{ id: number }>({ name: "fan-out" })
  .stepAsync("fetch-user", async ({ input }) => {
    console.log(`[${process.pid}] fetch-user(${input.id})`);
    await sleep(150);
    return { user: `user-${input.id}` };
  })
  .stepAsync("fetch-orders", async ({ input }) => {
    console.log(`[${process.pid}] fetch-orders(${input.id})`);
    await sleep(150);
    return { count: input.id };
  })
  .stepAsync("fetch-prefs", async ({ input }) => {
    console.log(`[${process.pid}] fetch-prefs(${input.id})`);
    await sleep(150);
    return { theme: input.id % 2 === 0 ? "light" : "dark" };
  })
  .step("merge", { dependsOn: ["fetch-user", "fetch-orders", "fetch-prefs"] }, ({ deps }) => {
    console.log(`[${process.pid}] merge`);
    return Pipeline.succeed({
      user: (deps["fetch-user"] as { user: string }).user,
      orderCount: (deps["fetch-orders"] as { count: number }).count,
      theme: (deps["fetch-prefs"] as { theme: string }).theme,
    });
  })
  .build();

const url = process.env.ZORYA_URL ?? "http://localhost:4101";
const client = new ZoryaClient({ url });

const worker = new ZoryaWorker({
  client,
  workflows: [fanOutWorkflow],
  mode: "step",
  // Slightly aggressive polling for the demo; production: 250ms+ is fine.
  stepPolling: { intervalMs: 100, limit: 5 },
  labels: { example: "split-step" },
});

await worker.start();
console.log(`Step-mode worker ${worker.workerId} connected to ${url}`);
console.log(`  PID: ${process.pid}`);
console.log(`  Advertising: fan-out`);
console.log(`  Mode: step (claim individual step tasks)`);

const shutdown = async () => {
  console.log("Shutting down…");
  await worker.stop();
  process.exit(0);
};
process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
