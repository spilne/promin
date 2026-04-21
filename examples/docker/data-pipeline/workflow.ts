// ---------------------------------------------------------------------------
// Data pipeline workflow — parallel fan-out/fan-in + singleflight dedup.
//
// Shape:
//   ingest-dataset    (needs: ingest)
//           │
//      ┌────┼────┐
//      ▼    ▼    ▼
//   transform-shard-a  transform-shard-b  transform-shard-c
//                      (needs: transform, run in parallel)
//      └────┬────┘
//            ▼
//   reduce-results    (needs: loader)
//           │
//           ▼
//   publish-report    (default)
//
// Singleflight: the workflow is built with `idempotency.onInFlight = "join"`.
// Submitting the same workflowId while a run is in flight returns the
// existing run — no duplicate execution. The coordinator demonstrates this
// by attempting two rapid submits of the same datasetId.
// ---------------------------------------------------------------------------

import { Pipeline } from "@promin/core";
import { workflow, type StepHandler } from "@promin/workflow";

export interface DatasetInput {
  readonly datasetId: string;
  readonly recordCount: number;
  readonly source: string;
}

export interface IngestResult {
  readonly records: ReadonlyArray<{ id: string; value: number }>;
  readonly ingestedAt: string;
}

export interface ShardResult {
  readonly shard: string;
  readonly processed: number;
  readonly sum: number;
}

export interface ReduceResult {
  readonly totalProcessed: number;
  readonly totalSum: number;
  readonly shards: ReadonlyArray<string>;
}

export interface ReportResult {
  readonly datasetId: string;
  readonly reportUrl: string;
  readonly publishedAt: string;
}

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

export function buildDataPipelineWorkflow() {
  return (
    workflow<DatasetInput>({
      name: "data-pipeline",
      version: "1",
    })
      .stepAsync(
        "ingest-dataset",
        async (): Promise<IngestResult> => {
          throw new Error("placeholder — worker runs the real handler");
        },
        { needs: ["ingest"] },
      )
      .stepAsync(
        "transform-shard-a",
        { dependsOn: ["ingest-dataset"] },
        async (): Promise<ShardResult> => {
          throw new Error("placeholder");
        },
        { needs: ["transform"] },
      )
      .stepAsync(
        "transform-shard-b",
        { dependsOn: ["ingest-dataset"] },
        async (): Promise<ShardResult> => {
          throw new Error("placeholder");
        },
        { needs: ["transform"] },
      )
      .stepAsync(
        "transform-shard-c",
        { dependsOn: ["ingest-dataset"] },
        async (): Promise<ShardResult> => {
          throw new Error("placeholder");
        },
        { needs: ["transform"] },
      )
      .stepAsync(
        "reduce-results",
        { dependsOn: ["transform-shard-a", "transform-shard-b", "transform-shard-c"] },
        async (): Promise<ReduceResult> => {
          throw new Error("placeholder");
        },
        { needs: ["loader"] },
      )
      .stepAsync(
        "publish-report",
        { dependsOn: ["reduce-results"] },
        async (): Promise<ReportResult> => {
          throw new Error("placeholder");
        },
      )
      // Duplicate submissions within 5 min join the running execution rather
      // than spawning a second one. Useful when an upstream trigger fires twice
      // (e.g. S3 event + manual re-trigger) — only one pipeline runs.
      .build({ idempotency: { ttl: 5 * 60_000, onInFlight: "join" } })
  );
}

// ---------------------------------------------------------------------------
// Step handlers — each worker registers the subset it owns.
// ---------------------------------------------------------------------------

export const ingestDatasetHandler: StepHandler = (ctx) =>
  Pipeline.fromPromise(async () => {
    const { datasetId, recordCount, source } = ctx.input as DatasetInput;
    console.log(`[ingest] ${ctx.workflowId} — reading ${recordCount} records from ${source}`);
    await sleep(250);
    const records = Array.from({ length: recordCount }, (_, i) => ({
      id: `${datasetId}-${i}`,
      value: Math.floor(Math.random() * 100),
    }));
    return { records, ingestedAt: new Date().toISOString() } satisfies IngestResult;
  });

function makeShardHandler(shardLabel: string): StepHandler {
  return (ctx) =>
    Pipeline.fromPromise(async () => {
      const ingest = (ctx.deps as { "ingest-dataset"?: IngestResult })["ingest-dataset"];
      const records = ingest?.records ?? [];
      // Each shard processes every 3rd record starting from its offset.
      const shardIndex = shardLabel === "a" ? 0 : shardLabel === "b" ? 1 : 2;
      const mine = records.filter((_, i) => i % 3 === shardIndex);
      console.log(
        `[transform-${shardLabel}] ${ctx.workflowId} — processing ${mine.length}/${records.length} records`,
      );
      await sleep(300);
      const sum = mine.reduce((acc, r) => acc + r.value, 0);
      return { shard: shardLabel, processed: mine.length, sum } satisfies ShardResult;
    });
}

export const transformShardAHandler = makeShardHandler("a");
export const transformShardBHandler = makeShardHandler("b");
export const transformShardCHandler = makeShardHandler("c");

export const reduceResultsHandler: StepHandler = (ctx) =>
  Pipeline.fromPromise(async () => {
    const deps = ctx.deps as {
      "transform-shard-a"?: ShardResult;
      "transform-shard-b"?: ShardResult;
      "transform-shard-c"?: ShardResult;
    };
    const shards = [deps["transform-shard-a"], deps["transform-shard-b"], deps["transform-shard-c"]]
      .filter(Boolean)
      .map((s) => s!);
    const totalProcessed = shards.reduce((acc, s) => acc + s.processed, 0);
    const totalSum = shards.reduce((acc, s) => acc + s.sum, 0);
    console.log(
      `[reduce] ${ctx.workflowId} — merging ${shards.length} shards, ${totalProcessed} rows, sum=${totalSum}`,
    );
    await sleep(100);
    return {
      totalProcessed,
      totalSum,
      shards: shards.map((s) => s.shard),
    } satisfies ReduceResult;
  });

export const publishReportHandler: StepHandler = (ctx) =>
  Pipeline.fromPromise(async () => {
    const { datasetId } = ctx.input as DatasetInput;
    const reduce = (ctx.deps as { "reduce-results"?: ReduceResult })["reduce-results"];
    const reportUrl = `s3://reports/${datasetId}/summary.json`;
    console.log(
      `[publish] ${ctx.workflowId} — report for ${datasetId}: ${reduce?.totalProcessed} rows → ${reportUrl}`,
    );
    await sleep(80);
    return {
      datasetId,
      reportUrl,
      publishedAt: new Date().toISOString(),
    } satisfies ReportResult;
  });
