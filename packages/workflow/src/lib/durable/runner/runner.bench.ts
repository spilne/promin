// ---------------------------------------------------------------------------
// Runner benchmarks — scheduling cost and storage round-trips of the DAG
// executor, on `InMemoryWorkflowStorage`.
//
// Scenarios:
//   * sequential chains of 100 / 1k / 2k / 4k inline steps (scheduling cost
//     per step; any super-linear growth shows up between 1k and 4k)
//   * a 1k chain through `InProcessStepExecutor` (the executor path)
//   * fan-out of 100 / 1k parallel branches
//   * a latency proxy: every storage call waits 2 ms first, for a 100-step
//     chain and a 100-way fan-out (round-trips, not CPU)
//   * storage calls made by a 100-step chain
//
// Each scenario reports the median of a few runs after one warm-up run.
//
// Run: bun packages/workflow/src/lib/durable/runner/runner.bench.ts
// ---------------------------------------------------------------------------

import { succeed } from "@spilne/perfect-core";
import { workflow, type Workflow } from "../durable-pipeline.ts";
import { InMemoryWorkflowStorage } from "../in-memory-storage.ts";
import { createWorkflowRunner, InProcessStepExecutor } from "../workflow-runner.ts";
import type { WorkflowStorage } from "../workflow-storage.ts";

/** Simulated latency of one storage call in the latency-proxy scenarios. */
const STORAGE_LATENCY_MS = 2;

let counter = 0;
const nextId = (): string => `bench-${++counter}`;

function chain(n: number): Workflow<number, number> {
  let b: any = workflow<number>({ name: `chain-${n}` });
  for (let i = 0; i < n; i++) {
    b = b.step(`s${i}`, ({ prev, input }: { prev?: number; input: number }) =>
      succeed((prev ?? input) + 1),
    );
  }
  return b.build();
}

function fanOut(n: number): Workflow<number, unknown> {
  const branches: Record<string, (p: { prev: number }) => unknown> = {};
  for (let i = 0; i < n; i++) branches[`b${i}`] = ({ prev }) => succeed(prev + i);
  return (workflow<number>({ name: `fan-${n}` }) as any)
    .step("root", ({ input }: { input: number }) => succeed(input))
    .parallelSteps("fork", branches)
    .build();
}

/** Wraps every storage method to wait `latencyMs` before the real call. */
function withLatency(storage: WorkflowStorage, latencyMs: number): WorkflowStorage {
  return new Proxy(storage, {
    get(target, key, receiver) {
      const value = Reflect.get(target, key, receiver);
      if (typeof value !== "function" || typeof key !== "string") return value;
      return async (...args: unknown[]) => {
        await new Promise((r) => setTimeout(r, latencyMs));
        return value.apply(target, args);
      };
    },
  });
}

/** Counts calls per storage method. */
function withCallCounts(storage: WorkflowStorage): {
  storage: WorkflowStorage;
  counts: Record<string, number>;
} {
  const counts: Record<string, number> = {};
  const wrapped = new Proxy(storage, {
    get(target, key, receiver) {
      const value = Reflect.get(target, key, receiver);
      if (typeof value !== "function" || typeof key !== "string") return value;
      return (...args: unknown[]) => {
        counts[key] = (counts[key] ?? 0) + 1;
        return value.apply(target, args);
      };
    },
  });
  return { storage: wrapped, counts };
}

async function time(label: string, run: () => Promise<unknown>, reps = 5): Promise<void> {
  await run();
  const samples: number[] = [];
  for (let i = 0; i < reps; i++) {
    const start = performance.now();
    await run();
    samples.push(performance.now() - start);
  }
  samples.sort((a, b) => a - b);
  const median = samples[Math.floor(samples.length / 2)]!;
  console.log(
    `${label.padEnd(36)} median ${median.toFixed(1).padStart(7)} ms   min ${samples[0]!.toFixed(1).padStart(7)} ms`,
  );
}

const runInline = (wf: Workflow<number, unknown>) => () =>
  createWorkflowRunner({ storage: new InMemoryWorkflowStorage() }).run({
    workflow: wf,
    workflowId: nextId(),
    input: 0,
  });

for (const n of [100, 1_000, 2_000, 4_000]) {
  await time(`inline chain ${n}`, runInline(chain(n)));
}

{
  const wf = chain(1_000);
  await time("executor chain 1000", () => {
    const storage = new InMemoryWorkflowStorage();
    const stepExecutor = new InProcessStepExecutor({
      workflow: wf as Workflow<unknown, unknown>,
      storage,
    });
    return createWorkflowRunner({ storage, stepExecutor }).run({
      workflow: wf,
      workflowId: nextId(),
      input: 0,
    });
  });
}

for (const n of [100, 1_000]) {
  await time(`inline fan-out ${n}`, runInline(fanOut(n)));
}

for (const [label, wf] of [
  [`chain 100 @${STORAGE_LATENCY_MS}ms/call`, chain(100)],
  [`fan-out 100 @${STORAGE_LATENCY_MS}ms/call`, fanOut(100)],
] as const) {
  await time(
    label,
    () =>
      createWorkflowRunner({
        storage: withLatency(new InMemoryWorkflowStorage(), STORAGE_LATENCY_MS),
      }).run({ workflow: wf as Workflow<number, unknown>, workflowId: nextId(), input: 0 }),
    3,
  );
}

{
  const { storage, counts } = withCallCounts(new InMemoryWorkflowStorage());
  await createWorkflowRunner({ storage }).run({
    workflow: chain(100),
    workflowId: nextId(),
    input: 0,
  });
  console.log("storage calls, chain 100:", JSON.stringify(counts));
}
