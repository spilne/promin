// ---------------------------------------------------------------------------
// Builder benchmarks — the cost of constructing workflow definitions.
//
// Scenarios:
//   * linear chains of 100 / 1k / 4k / 10k `.step()` calls, then `.build()`
//     (per-append cost; super-linear growth shows up between 1k and 10k)
//   * a 4k chain built and run in memory (build share of build + run)
//   * a 1k `.parallelSteps()` fan-out (branch-name validation)
//   * branching: 1k builders forked from one 1k-step prefix
//
// Each scenario reports the median of a few runs after one warm-up run.
//
// Run: bun packages/workflow/src/lib/durable/workflow-builder.bench.ts
// ---------------------------------------------------------------------------

import { succeed } from "@spilne/perfect-core";
import { workflow } from "./workflow-builder.ts";
import type { Workflow } from "./workflow-types.ts";
import { InMemoryWorkflowStorage } from "./in-memory-storage.ts";
import { createWorkflowRunner } from "./workflow-runner.ts";

let counter = 0;
const nextId = (): string => `bench-${++counter}`;

function buildChain(n: number): Workflow<number, number> {
  let b: any = workflow<number>({ name: `chain-${n}` });
  for (let i = 0; i < n; i++) {
    b = b.step(`s${i}`, ({ prev }: { prev: number }) => succeed(prev + 1));
  }
  return b.build();
}

async function time(label: string, run: () => unknown, reps = 5): Promise<number> {
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
  return median;
}

for (const n of [100, 1_000, 4_000, 10_000]) {
  await time(`build chain ${n}`, () => buildChain(n));
}

{
  const build = await time("build chain 4000 (again)", () => buildChain(4_000));
  const total = await time("build + run chain 4000", () =>
    createWorkflowRunner({ storage: new InMemoryWorkflowStorage() }).run({
      workflow: buildChain(4_000),
      workflowId: nextId(),
      input: 0,
    }),
  );
  console.log(`build share of build + run at 4000: ${((build / total) * 100).toFixed(0)}%`);
}

{
  const branches: Record<string, (p: { prev: number }) => unknown> = {};
  for (let i = 0; i < 1_000; i++) branches[`b${i}`] = ({ prev }) => succeed(prev + i);
  await time("build parallelSteps 1000", () =>
    (workflow<number>({ name: "fan" }) as any)
      .step("root", ({ input }: { input: number }) => succeed(input))
      .parallelSteps("fork", branches)
      .build(),
  );
}

{
  let prefix: any = workflow<number>({ name: "prefix" });
  for (let i = 0; i < 1_000; i++) {
    prefix = prefix.step(`s${i}`, ({ prev }: { prev: number }) => succeed(prev + 1));
  }
  await time("fork 1000 branches off a 1000 prefix", () => {
    for (let i = 0; i < 1_000; i++) {
      prefix.step(`tail`, ({ prev }: { prev: number }) => succeed(prev));
    }
  });
  await time("  ... and build each", () => {
    for (let i = 0; i < 1_000; i++) {
      prefix.step(`tail`, ({ prev }: { prev: number }) => succeed(prev)).build();
    }
  });
}
