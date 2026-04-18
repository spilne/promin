// ---------------------------------------------------------------------------
// Chunked executor — execute streamable plans chunk by chunk
// ---------------------------------------------------------------------------

import type { LogicalPlan, SourcePlan, GroupByPlan } from "./logical-plan.ts";
import { classifyPlan } from "./plan-classifier.ts";
import { ArrayExecutor } from "./array-executor.ts";
import { createAccumulator, accumulate, finalize, type GroupAccumulator } from "./streaming-agg.ts";

/**
 * Execute a plan in chunks, yielding arrays of rows.
 *
 * For streamable plans (filter, map, select, etc.), processes the source data
 * in fixed-size chunks with constant memory — each chunk is independently
 * executed through the chain of stateless operations.
 *
 * For materializing plans (sort, groupBy, join, etc.), falls back to full
 * execution and yields the entire result as a single chunk.
 */
export async function* executeChunked<T>(params: {
  plan: LogicalPlan;
  chunkSize: number;
}): AsyncGenerator<T[]> {
  const { plan, chunkSize } = params;
  const streamability = classifyPlan(plan);

  if (streamability === "materializing") {
    // Can't stream — execute fully and yield as one chunk
    const executor = new ArrayExecutor();
    const all = await executor.execute<T>(plan);
    yield all;
    return;
  }

  if (streamability === "aggregating") {
    // GroupBy with streamable input — per-chunk accumulation
    const groupByPlan = plan as GroupByPlan;
    const groupCols = groupByPlan.columns;
    const aggs = groupByPlan.aggs;
    const aggEntries = Object.entries(aggs);

    // Extract source and streamable ops from the GroupBy's input
    const { source, ops } = extractSourceAndOps(groupByPlan.input);

    const executor = new ArrayExecutor();
    const singleGroupCol = groupCols.length === 1;

    // Global accumulators across all chunks
    const globalGroups = new Map<
      string,
      { keyValues: Record<string, unknown>; accs: GroupAccumulator[] }
    >();

    const consumeChunk = (chunk: unknown[]) => {
      const chunkPlan = rebuildPlan({ _tag: "Source", data: chunk }, ops);
      const rows = executor.executeSync(chunkPlan);

      for (const row of rows) {
        const r = row as Record<string, unknown>;
        const key = singleGroupCol
          ? String(r[groupCols[0]!])
          : groupCols.map((c) => String(r[c])).join("\0");

        let group = globalGroups.get(key);
        if (!group) {
          const keyValues: Record<string, unknown> = {};
          for (const c of groupCols) keyValues[c] = r[c];
          group = {
            keyValues,
            accs: aggEntries.map(([, fn]) => createAccumulator(fn)),
          };
          globalGroups.set(key, group);
        }

        for (let j = 0; j < aggEntries.length; j++) {
          const [col, aggFn] = aggEntries[j]!;
          if (typeof aggFn === "object" && aggFn._tag === "expr") {
            if (aggFn.filter && !aggFn.filter.fn(r)) continue;
            accumulate(group.accs[j]!, aggFn, aggFn.expr.fn(r));
          } else {
            accumulate(group.accs[j]!, aggFn, r[col]);
          }
        }
      }
    };

    for await (const chunk of sourceChunks(source, chunkSize)) {
      consumeChunk(chunk);
    }

    // Finalize all groups
    const result: unknown[] = [];
    for (const { keyValues, accs } of globalGroups.values()) {
      const row: Record<string, unknown> = { ...keyValues };
      for (let j = 0; j < aggEntries.length; j++) {
        const [col, aggFn] = aggEntries[j]!;
        row[col] = finalize(accs[j]!, aggFn);
      }
      result.push(row);
    }

    yield result as T[];
    return;
  }

  // Extract the source plan and the chain of streamable ops
  const { source, ops } = extractSourceAndOps(plan);
  const executor = new ArrayExecutor();

  for await (const chunk of sourceChunks(source, chunkSize)) {
    const chunkPlan = rebuildPlan({ _tag: "Source", data: chunk }, ops);
    const result = executor.executeSync<T>(chunkPlan);
    if (result.length > 0) yield result as T[];
  }
}

/**
 * Yield fixed-size chunks from a SourcePlan. Prefers `stream()` when it's
 * available and the plan doesn't already carry materialised data — that
 * lets disk-backed sources (streaming CSV, etc.) flow through the chunked
 * executor without loading the whole file into memory. Falls back to `load()`,
 * then to `source.data`.
 */
async function* sourceChunks(source: SourcePlan, chunkSize: number): AsyncGenerator<unknown[]> {
  if (source.stream && source.data.length === 0) {
    let buf: unknown[] = [];
    for await (const row of source.stream()) {
      buf.push(row);
      if (buf.length >= chunkSize) {
        yield buf;
        buf = [];
      }
    }
    if (buf.length > 0) yield buf;
    return;
  }

  const data = source.load && source.data.length === 0 ? await source.load() : source.data;
  for (let i = 0; i < data.length; i += chunkSize) {
    const chunk = data.slice(i, i + chunkSize);
    if (chunk.length === 0) break;
    yield chunk;
  }
}

// ---------------------------------------------------------------------------
// Internals
// ---------------------------------------------------------------------------

interface PlanOp {
  build: (input: LogicalPlan) => LogicalPlan;
}

/**
 * Walk the plan tree from the root down to the Source, collecting each
 * intermediate operation. Returns the source and the ordered list of ops
 * to replay on each chunk.
 */
function extractSourceAndOps(plan: LogicalPlan): { source: SourcePlan; ops: PlanOp[] } {
  const ops: PlanOp[] = [];
  let current = plan;

  while (current._tag !== "Source") {
    const node = current as any;
    ops.unshift({
      build: (input: LogicalPlan) => ({ ...node, input }),
    });
    current = node.input;
  }

  return { source: current as SourcePlan, ops };
}

function rebuildPlan(source: LogicalPlan, ops: PlanOp[]): LogicalPlan {
  let plan = source;
  for (const op of ops) {
    plan = op.build(plan);
  }
  return plan;
}
