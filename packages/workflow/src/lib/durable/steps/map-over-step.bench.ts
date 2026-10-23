// ---------------------------------------------------------------------------
// `.mapOver()` benchmarks — fan-out cost and resume cost against the
// in-memory storage.
//
// Scenarios:
//   * a fresh run over 1k / 10k elements
//   * a resumed run over 10k elements of which 9,999 already have a saved
//     task row (the run crashed on the last element). Element calls are
//     counted, so the output shows how many elements the resume ran again.
//
// Each scenario reports the median of a few runs after one warm-up run.
//
// Run: bun packages/workflow/src/lib/durable/steps/map-over-step.bench.ts
// ---------------------------------------------------------------------------

import { succeed } from "@spilne/perfect-core";
import { LosslessJsonCodec } from "@spilne/perfect-core/connect";
import { InMemoryWorkflowStorage } from "../in-memory-storage.ts";
import { workflow } from "../workflow-builder.ts";
import { createWorkflowRunner } from "../workflow-runner.ts";

let counter = 0;
const nextId = (): string => `bench-${++counter}`;
let elementCalls = 0;

const mapWorkflow = workflow<number>({ name: "map-bench" })
  .step("source", ({ input }) => succeed(Array.from({ length: input }, (_, i) => i)))
  .mapOver("double", { array: "source" }, (n) => {
    elementCalls++;
    return succeed(n * 2);
  })
  .build();

async function time(label: string, run: () => Promise<unknown>, reps = 5): Promise<number> {
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
    `${label.padEnd(40)} median ${median.toFixed(1).padStart(8)} ms   min ${samples[0]!.toFixed(1).padStart(8)} ms`,
  );
  return median;
}

for (const n of [1_000, 10_000]) {
  await time(`fresh mapOver ${n}`, () =>
    createWorkflowRunner({ storage: new InMemoryWorkflowStorage() }).run({
      workflow: mapWorkflow,
      workflowId: nextId(),
      input: n,
    }),
  );
}

/**
 * A run of `mapWorkflow` over `n` elements that stopped inside the map
 * step: the source step completed and every element but the last has a
 * saved task row. Seeding is not timed.
 */
async function seedCrashedRun(n: number): Promise<{
  storage: InMemoryWorkflowStorage;
  workflowId: string;
}> {
  const storage = new InMemoryWorkflowStorage();
  const workflowId = nextId();
  await storage.createWorkflow({ workflowId, workflowName: mapWorkflow.name, input: n });
  const source = Array.from({ length: n }, (_, i) => i);
  await storage.saveStepResult({
    workflowId,
    stepName: "source",
    result: LosslessJsonCodec.encode(source),
    durationMs: 0,
    startedAt: new Date(),
  });
  for (let i = 0; i < n - 1; i++) {
    await storage.saveTaskResult({
      workflowId,
      stepName: "double",
      taskIndex: i,
      result: LosslessJsonCodec.encode(i * 2),
    });
  }
  return { storage, workflowId };
}

{
  const n = 10_000;
  const seeded: Array<{ storage: InMemoryWorkflowStorage; workflowId: string }> = [];
  for (let i = 0; i < 6; i++) seeded.push(await seedCrashedRun(n));
  elementCalls = 0;
  await time(`resume mapOver ${n} (${n - 1} saved)`, () => {
    const { storage, workflowId } = seeded.pop()!;
    return createWorkflowRunner({ storage }).run({ workflow: mapWorkflow, workflowId, input: n });
  });
  console.log(`  element calls per resume: ${elementCalls / 6}`);
}
