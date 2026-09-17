// ---------------------------------------------------------------------------
// batch-process workflow — demonstrates .mapOver() fan-out.
//
// fetch-items → mapOver (process each) → aggregate
// Dashboard: the Step tab for "process" shows the per-task table with
// individual status + attempt counters. stepType = "map".
// ---------------------------------------------------------------------------

import { workflow } from "@promin/workflow";
import { Pipeline } from "@promin/core";

export interface BatchProcessInput {
  batchId?: string;
  itemCount?: number;
}

function delay(minMs: number, maxMs: number): number {
  return minMs + Math.floor(Math.random() * (maxMs - minMs));
}

function pSleep(ms: number): Pipeline<void, never> {
  return Pipeline.fromPromise(() => new Promise<void>((r) => setTimeout(r, ms)));
}

export const batchProcessWorkflow = workflow<BatchProcessInput>({
  name: "batch-process",
  type: "data",
})
  .step("fetch-items", ({ input }) => {
    const n = input.itemCount ?? 8 + Math.floor(Math.random() * 8);
    return pSleep(delay(1_500, 4_000)).map(() =>
      Array.from({ length: n }, (_, i) => ({ id: i, value: Math.floor(Math.random() * 1000) })),
    );
  })
  .mapOver(
    "process",
    { array: "fetch-items", concurrency: 3 },
    (item: { id: number; value: number }) =>
      pSleep(delay(800, 3_000)).map(() => ({ id: item.id, doubled: item.value * 2 })),
  )
  .step("aggregate", ({ prev }) =>
    pSleep(delay(1_000, 3_000)).map(() => {
      const items = prev as ReadonlyArray<{ id: number; doubled: number }>;
      return {
        count: items.length,
        sum: items.reduce((s, i) => s + i.doubled, 0),
      };
    }),
  )
  .build();
