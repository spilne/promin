// ---------------------------------------------------------------------------
// etl workflow — classic extract / multi-transform / load.
//
// DAG:
//    extract ─┬─▶ clean ─────┐
//             ├─▶ deduplicate ┤
//             └─▶ enrich ─────┴─▶ validate ─▶ load ─▶ archive
// ---------------------------------------------------------------------------

import { workflow } from "@promin/workflow";

export interface EtlInput {
  source: string;
  batch?: number;
}

async function sleep(ms: number): Promise<void> {
  await new Promise((r) => setTimeout(r, ms));
}

function delay(minMs: number, maxMs: number): number {
  return minMs + Math.floor(Math.random() * (maxMs - minMs));
}

export const etlWorkflow = workflow<EtlInput>({ name: "etl", type: "data" })
  .stepAsync("extract", async ({ input }) => {
    await sleep(delay(3_000, 10_000));
    const rows = 1000 + Math.floor(Math.random() * 9000);
    return { source: input.source, rows };
  })
  .stepAsync("clean", { dependsOn: ["extract"] }, async ({ deps }) => {
    await sleep(delay(2_000, 7_000));
    return {
      rows: Math.floor(deps.extract.rows * 0.95),
      dropped: Math.floor(deps.extract.rows * 0.05),
    };
  })
  .stepAsync("deduplicate", { dependsOn: ["extract"] }, async ({ deps }) => {
    await sleep(delay(2_500, 8_000));
    return {
      rows: Math.floor(deps.extract.rows * 0.97),
      duplicates: Math.floor(deps.extract.rows * 0.03),
    };
  })
  .stepAsync("enrich", { dependsOn: ["extract"] }, async ({ deps }) => {
    await sleep(delay(4_000, 12_000));
    if (Math.random() < 0.1) throw new Error("Enrichment API rate-limited");
    return { rows: deps.extract.rows, columnsAdded: 4 };
  })
  .stepAsync("validate", { dependsOn: ["clean", "deduplicate", "enrich"] }, async ({ deps }) => {
    await sleep(delay(2_000, 6_000));
    return { rows: deps.clean.rows, valid: deps.clean.rows, invalid: 0 };
  })
  .stepAsync("load", { dependsOn: ["validate"] }, async ({ deps }) => {
    await sleep(delay(3_000, 10_000));
    return { loaded: deps.validate.rows, target: "warehouse" };
  })
  .stepAsync("archive", { dependsOn: ["load"] }, async ({ deps }) => {
    await sleep(delay(1_500, 5_000));
    return { archivedRows: deps.load.loaded, archivedAt: new Date().toISOString() };
  })
  .build();
