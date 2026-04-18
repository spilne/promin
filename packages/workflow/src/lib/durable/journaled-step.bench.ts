// ---------------------------------------------------------------------------
// Journaled-step micro-benchmarks.
//
// Measures the per-activity overhead of a `.journaled()` step body against
// an in-memory storage. The numbers are a floor — real deployments pay
// extra for Postgres/Redis round-trips — but regressions in the engine
// itself (extra allocations, extra Map lookups, extra scope bookkeeping)
// show up here first.
//
// Scope:
//   * fresh-run cost per activity (1 / 10 / 100 activities in a linear body)
//   * replay cost once the journal is full (how fast does loadJournal +
//     replay traversal go?)
//   * ctx.parallel fan-out overhead (1 / 10 / 100 concurrent branches)
//   * payloadHash cost (SHA-256 per 3-arg activity, off vs on)
//
// Run: bun packages/workflow/src/lib/durable/journaled-step.bench.ts
// ---------------------------------------------------------------------------

import { bench, group, run } from "mitata";
import { runJournaledStep } from "./journaled-step.ts";
import { InMemoryWorkflowStorage } from "./in-memory-storage.ts";

// Unique workflowId per fresh-run iteration so each bench call starts
// with an empty journal. Storage construction is ~free on InMemory.
let counter = 0;
const nextId = (): string => `bench-${++counter}`;

// ---------------------------------------------------------------------------
// Bodies
// ---------------------------------------------------------------------------

// Single 2-arg activity.
function* oneActivity(ctx: any): any {
  return yield* ctx.activity("a", async () => 1);
}

// N linear 2-arg activities.
const linearBody = (n: number) =>
  function* (ctx: any): any {
    let last = 0;
    for (let i = 0; i < n; i++) {
      last = yield* ctx.activity(`a${i}`, async () => i);
    }
    return last;
  };

// N 3-arg activities. Same body for both "no hash" and "hash on" runs —
// the difference is the runner-level `payloadHash` flag at the call site,
// so the fn's shape stays constant and the SHA-256 cost is isolated.
const linear3Arg = (n: number) =>
  function* (ctx: any): any {
    let last = 0;
    for (let i = 0; i < n; i++) {
      last = yield* ctx.activity(`a${i}`, { i }, async (x: { i: number }) => x.i);
    }
    return last;
  };

// N parallel branches (fan-out).
const parallelBody = (n: number) =>
  function* (ctx: any): any {
    const branches = [] as any[];
    for (let i = 0; i < n; i++) {
      branches.push(ctx.activity(`b${i}`, async () => i));
    }
    return yield* ctx.parallel(branches);
  };

// ---------------------------------------------------------------------------
// Fresh-run benches
// ---------------------------------------------------------------------------

group("fresh run — linear body", () => {
  bench("1 activity", async () => {
    await runJournaledStep({
      input: undefined,
      prev: undefined,
      workflowId: nextId(),
      stepName: "s",
      storage: new InMemoryWorkflowStorage(),
      body: oneActivity,
    });
  });
  bench("10 activities", async () => {
    await runJournaledStep({
      input: undefined,
      prev: undefined,
      workflowId: nextId(),
      stepName: "s",
      storage: new InMemoryWorkflowStorage(),
      body: linearBody(10),
    });
  });
  bench("100 activities", async () => {
    await runJournaledStep({
      input: undefined,
      prev: undefined,
      workflowId: nextId(),
      stepName: "s",
      storage: new InMemoryWorkflowStorage(),
      body: linearBody(100),
    });
  });
});

// ---------------------------------------------------------------------------
// Replay benches — pre-populate once, then measure the cost of replay.
// Each iteration re-runs the same body; the activities hit the journal and
// don't actually execute their fn.
// ---------------------------------------------------------------------------

async function seedReplay(
  storage: InMemoryWorkflowStorage,
  workflowId: string,
  body: (ctx: any) => Generator<any, any, any>,
): Promise<void> {
  await runJournaledStep({
    input: undefined,
    prev: undefined,
    workflowId,
    stepName: "s",
    storage,
    body,
  });
}

// Pre-build replay fixtures. Mitata can't do async setup inside group(), so
// we seed at module load and reuse the same storage across iterations.
const replay1 = {
  storage: new InMemoryWorkflowStorage(),
  id: nextId(),
  body: oneActivity,
};
const replay10 = {
  storage: new InMemoryWorkflowStorage(),
  id: nextId(),
  body: linearBody(10),
};
const replay100 = {
  storage: new InMemoryWorkflowStorage(),
  id: nextId(),
  body: linearBody(100),
};
await seedReplay(replay1.storage, replay1.id, replay1.body);
await seedReplay(replay10.storage, replay10.id, replay10.body);
await seedReplay(replay100.storage, replay100.id, replay100.body);

group("replay — journal pre-populated", () => {
  bench("1 activity", async () => {
    await runJournaledStep({
      input: undefined,
      prev: undefined,
      workflowId: replay1.id,
      stepName: "s",
      storage: replay1.storage,
      body: replay1.body,
    });
  });
  bench("10 activities", async () => {
    await runJournaledStep({
      input: undefined,
      prev: undefined,
      workflowId: replay10.id,
      stepName: "s",
      storage: replay10.storage,
      body: replay10.body,
    });
  });
  bench("100 activities", async () => {
    await runJournaledStep({
      input: undefined,
      prev: undefined,
      workflowId: replay100.id,
      stepName: "s",
      storage: replay100.storage,
      body: replay100.body,
    });
  });
});

// ---------------------------------------------------------------------------
// ctx.parallel fan-out
// ---------------------------------------------------------------------------

group("fresh run — ctx.parallel fan-out", () => {
  bench("1 branch", async () => {
    await runJournaledStep({
      input: undefined,
      prev: undefined,
      workflowId: nextId(),
      stepName: "s",
      storage: new InMemoryWorkflowStorage(),
      body: parallelBody(1),
    });
  });
  bench("10 branches", async () => {
    await runJournaledStep({
      input: undefined,
      prev: undefined,
      workflowId: nextId(),
      stepName: "s",
      storage: new InMemoryWorkflowStorage(),
      body: parallelBody(10),
    });
  });
  bench("100 branches", async () => {
    await runJournaledStep({
      input: undefined,
      prev: undefined,
      workflowId: nextId(),
      stepName: "s",
      storage: new InMemoryWorkflowStorage(),
      body: parallelBody(100),
    });
  });
});

// ---------------------------------------------------------------------------
// payloadHash overhead — 3-arg form with and without hashing, fixed at 10
// activities so the SHA-256 signal isn't diluted by activity count.
// ---------------------------------------------------------------------------

group("fresh run — payloadHash cost (10 activities)", () => {
  bench("3-arg activity, no hash", async () => {
    await runJournaledStep({
      input: undefined,
      prev: undefined,
      workflowId: nextId(),
      stepName: "s",
      storage: new InMemoryWorkflowStorage(),
      body: linear3Arg(10),
    });
  });
  bench("3-arg activity, payloadHash on", async () => {
    await runJournaledStep({
      input: undefined,
      prev: undefined,
      workflowId: nextId(),
      stepName: "s",
      storage: new InMemoryWorkflowStorage(),
      payloadHash: true,
      body: linear3Arg(10),
    });
  });
});

await run();
