/**
 * ETL pipeline with parallel steps.
 * Extract → (enrich + validate in parallel) → load.
 * Enrich and validate have no dependency on each other, so they run concurrently.
 */

import { flow } from "@promin/workflow";

interface RawData {
  records: { id: string; value: number }[];
}

const etl = flow<{ source: string }>("etl-pipeline")
  .stepAsync("extract", async ({ input }) => {
    const data = await fetchFromSource(input.source);
    return data;
  })
  .stepAsync("enrich", { dependsOn: ["extract"] }, async ({ deps }) => {
    return deps.extract.records.map((r) => ({ ...r, enrichedAt: Date.now() }));
  })
  .stepAsync("validate", { dependsOn: ["extract"] }, async ({ deps }) => {
    const invalid = deps.extract.records.filter((r) => r.value < 0);
    return { total: deps.extract.records.length, invalid: invalid.length };
  })
  .stepAsync("load", { dependsOn: ["enrich", "validate"] }, async ({ deps }) => {
    if (deps.validate.invalid > 0) {
      throw new Error(`${deps.validate.invalid} invalid records`);
    }
    await insertRows(deps.enrich);
    return { loaded: deps.enrich.length };
  });

const result = await etl.execute({ source: "s3://data/2026-04-06.parquet" });
console.log(result); // { loaded: 1000 }

// Stubs
async function fetchFromSource(_src: string): Promise<RawData> {
  return { records: [{ id: "1", value: 42 }] };
}
async function insertRows(_rows: unknown[]) {}
