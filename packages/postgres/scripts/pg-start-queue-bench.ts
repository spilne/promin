// ---------------------------------------------------------------------------
// PgWorkflowStartQueue micro-benchmark
//
// Spins up a fresh postgres testcontainer (image: postgres:17-alpine) and
// measures six workloads. Run from the repo root:
//
//   cd packages/postgres
//   bun --conditions=@promin/source run scripts/pg-start-queue-bench.ts
//
// Reference run (Apple Silicon, postgres in local docker, postgres-js
// client; expect ±20% drift across runs):
//
//   1. enqueue × 1000 sequential       : ~1500 ops/sec
//   2. enqueue × 1000 parallel(50)     : ~5000 ops/sec
//   3. drain 1000 single worker (10/call): ~1000 ops/sec
//   4. drain 1000 via 10-worker bursts  : ~4000 ops/sec, 10 bursts
//   5. drain 1000 via 10×version-filtered: ~3300 ops/sec, ~17 bursts
//   6. e2e enqueue→claim→complete × 500: ~240 ops/sec
//
// What to watch for in regressions:
//   - workload 4 stops draining in exactly 10 bursts → the narrow-window
//     SKIP LOCKED optimization regressed (W=K path is broken)
//   - workload 5 burst count climbing significantly above ~17 → the
//     wide-window path is over-locking more than expected
//   - any workload's ops/sec dropping by >25% after a postgres-js or
//     drizzle bump is worth investigating before merging the bump
// ---------------------------------------------------------------------------

import { PgWorkflowStartQueue } from "../src/index.ts";
import { PostgresTestContainer } from "../src/lib/test-utils.ts";

const pg = new PostgresTestContainer();
await pg.start();
const q = new PgWorkflowStartQueue({ db: pg.db });
await q.ensureTable();

function fmt(ms: number, ops: number): string {
  const opsPerSec = ops / (ms / 1000);
  return `${ms.toFixed(0)} ms total · ${(ms / ops).toFixed(2)} ms/op · ${opsPerSec.toFixed(0)} ops/sec`;
}

async function reset(): Promise<void> {
  await pg.sql.unsafe(`TRUNCATE TABLE wf_workflow_starts`);
}

// 1. Sequential enqueue ----------------------------------------------------
{
  await reset();
  const N = 1000;
  const t0 = performance.now();
  for (let i = 0; i < N; i++) {
    await q.enqueue({
      workflowId: `wf-${i}`,
      workflowName: "bench",
      input: { i },
    });
  }
  const dt = performance.now() - t0;
  console.log(`1. enqueue × ${N} sequential       : ${fmt(dt, N)}`);
}

// 2. Parallel enqueue (50 concurrent) -------------------------------------
{
  await reset();
  const N = 1000;
  const concurrency = 50;
  const t0 = performance.now();
  for (let batch = 0; batch < N; batch += concurrency) {
    const tasks = Array.from({ length: Math.min(concurrency, N - batch) }, (_, j) =>
      q.enqueue({
        workflowId: `wf-${batch + j}`,
        workflowName: "bench",
        input: { i: batch + j },
      }),
    );
    await Promise.all(tasks);
  }
  const dt = performance.now() - t0;
  console.log(`2. enqueue × ${N} parallel(50)     : ${fmt(dt, N)}`);
}

// 3. Single-worker claim drain (10 at a time) -----------------------------
{
  // Set up 1000 enqueued.
  await reset();
  for (let i = 0; i < 1000; i++) {
    await q.enqueue({ workflowId: `wf-${i}`, workflowName: "bench", input: { i } });
  }
  let drained = 0;
  const t0 = performance.now();
  while (drained < 1000) {
    const claimed = await q.claim({
      workflowSpecs: [{ name: "bench", versions: [] }],
      workerId: "single",
      limit: 10,
    });
    if (claimed.length === 0) break;
    drained += claimed.length;
    for (const c of claimed) await q.complete(c.id);
  }
  const dt = performance.now() - t0;
  console.log(`3. drain 1000 single worker (10/call): ${fmt(dt, drained)} (drained ${drained})`);
}

// 4. 10-worker concurrent burst, no version filter ------------------------
{
  await reset();
  const N = 1000;
  for (let i = 0; i < N; i++) {
    await q.enqueue({ workflowId: `wf-${i}`, workflowName: "bench", input: { i } });
  }
  const concurrentWorkers = 10;
  let totalClaimed = 0;
  let bursts = 0;
  const t0 = performance.now();
  while (totalClaimed < N) {
    bursts++;
    const calls = Array.from({ length: concurrentWorkers }, (_, n) =>
      q.claim({
        workflowSpecs: [{ name: "bench", versions: [] }],
        workerId: `worker-${n}`,
        limit: 10,
      }),
    );
    const results = await Promise.all(calls);
    const claimed = results.flat();
    totalClaimed += claimed.length;
    if (claimed.length === 0) break;
    await Promise.all(claimed.map((c) => q.complete(c.id)));
  }
  const dt = performance.now() - t0;
  console.log(`4. drain ${N} via 10-worker bursts  : ${fmt(dt, totalClaimed)} · ${bursts} bursts`);
}

// 5. 10-worker concurrent burst, version-filtered -------------------------
{
  await reset();
  const N = 1000;
  for (let i = 0; i < N; i++) {
    await q.enqueue({
      workflowId: `wf-${i}`,
      workflowName: "bench",
      input: { i },
      version: i % 2 === 0 ? "1" : "2",
    });
  }
  let totalClaimed = 0;
  let bursts = 0;
  const t0 = performance.now();
  while (totalClaimed < N) {
    bursts++;
    const calls = Array.from({ length: 10 }, (_, n) => {
      const v = n < 5 ? "1" : "2";
      return q.claim({
        workflowSpecs: [{ name: "bench", versions: [v] }],
        workerId: `worker-${n}-v${v}`,
        limit: 10,
      });
    });
    const results = await Promise.all(calls);
    const claimed = results.flat();
    totalClaimed += claimed.length;
    if (claimed.length === 0) break;
    await Promise.all(claimed.map((c) => q.complete(c.id)));
  }
  const dt = performance.now() - t0;
  console.log(`5. drain ${N} via 10×version-filtered: ${fmt(dt, totalClaimed)} · ${bursts} bursts`);
}

// 6. End-to-end (enqueue → claim → complete, single trip per row) ---------
{
  await reset();
  const N = 500;
  const t0 = performance.now();
  for (let i = 0; i < N; i++) {
    await q.enqueue({ workflowId: `wf-${i}`, workflowName: "bench", input: { i } });
    const [c] = await q.claim({
      workflowSpecs: [{ name: "bench", versions: [] }],
      workerId: "e2e",
      limit: 1,
    });
    if (c) await q.complete(c.id);
  }
  const dt = performance.now() - t0;
  console.log(`6. e2e enqueue→claim→complete × ${N}: ${fmt(dt, N)}`);
}

await pg.stop();
console.log("\ndone.");
