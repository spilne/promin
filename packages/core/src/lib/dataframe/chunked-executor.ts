// ---------------------------------------------------------------------------
// Chunked executor — execute streamable plans chunk by chunk
// ---------------------------------------------------------------------------

import type { LogicalPlan, SourcePlan } from "./logical-plan.ts";
import { classifyPlan } from "./plan-classifier.ts";
import { ArrayExecutor } from "./array-executor.ts";

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

  // Extract the source plan and the chain of streamable ops
  const { source, ops } = extractSourceAndOps(plan);

  // Resolve source data — call load() if present, otherwise use inline data
  const data = source.load && source.data.length === 0 ? await source.load() : source.data;

  const executor = new ArrayExecutor();

  for (let i = 0; i < data.length; i += chunkSize) {
    const chunk = data.slice(i, i + chunkSize);
    if (chunk.length === 0) break;

    // Rebuild the plan with the chunk as source, apply streamable ops
    const chunkPlan = rebuildPlan({ _tag: "Source", data: chunk }, ops);
    const result = executor.executeSync<T>(chunkPlan);
    if (result.length > 0) yield result;
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
