// ---------------------------------------------------------------------------
// Operator fusion — shared utilities for fusing adjacent pure operators
// (map, filter, filterMap, tap) into a single function call per element.
//
// Used by: OptimizedStreamPipeline, RawStream, TopologyRunner
// ---------------------------------------------------------------------------

import { Stream, Chunk } from "effect";

/**
 * Sentinel value returned by fused functions to signal a filtered-out element.
 * Unique symbol — cannot collide with any user value.
 */
export const SKIP: unique symbol = Symbol("SKIP");

/** A single pure operator that can be fused. */
export type FusibleOp =
  | { readonly tag: "map"; readonly fn: (value: any) => any }
  | { readonly tag: "filter"; readonly fn: (value: any) => boolean }
  | { readonly tag: "filterMap"; readonly fn: (value: any) => any | undefined }
  | { readonly tag: "tap"; readonly fn: (value: any) => void };

/**
 * Compile an array of fusible ops into a single function.
 * Returns SKIP sentinel for filtered-out elements.
 */
export function compileFused(ops: FusibleOp[]): (value: any) => any {
  if (ops.length === 0) return (v: any) => v;

  if (ops.length === 1) {
    const op = ops[0];
    switch (op.tag) {
      case "map":
        return op.fn;
      case "filter":
        return (v: any) => (op.fn(v) ? v : SKIP);
      case "filterMap":
        return (v: any) => {
          const r = op.fn(v);
          return r === undefined ? SKIP : r;
        };
      case "tap":
        return (v: any) => {
          op.fn(v);
          return v;
        };
    }
  }

  return (value: any) => {
    let v: any = value;
    for (let i = 0; i < ops.length; i++) {
      const op = ops[i];
      switch (op.tag) {
        case "map":
          v = op.fn(v);
          break;
        case "filter":
          if (!op.fn(v)) return SKIP;
          break;
        case "filterMap": {
          const r = op.fn(v);
          if (r === undefined) return SKIP;
          v = r;
          break;
        }
        case "tap":
          op.fn(v);
          break;
      }
    }
    return v;
  };
}

/** Check if an ops array contains any filter-type operations. */
export function hasFilterOps(ops: FusibleOp[]): boolean {
  return ops.some((op) => op.tag === "filter" || op.tag === "filterMap");
}

/**
 * Apply a fused function to an Effect Stream via mapChunks.
 * Processes entire chunks in a tight for-loop — one Effect runtime
 * step per chunk (~4096 elements), not per element.
 */
export function fuseOpsToStream<E>(
  stream: Stream.Stream<any, E>,
  ops: FusibleOp[],
): Stream.Stream<any, E> {
  if (ops.length === 0) return stream;

  const fused = compileFused(ops);
  const hasFilter = hasFilterOps(ops);

  return Stream.mapChunks(stream, (chunk) => {
    if (hasFilter) {
      const src = Chunk.toArray(chunk);
      const result: any[] = [];
      for (let i = 0; i < src.length; i++) {
        const v = fused(src[i]);
        if (v !== SKIP) result.push(v);
      }
      return Chunk.unsafeFromArray(result);
    }
    return Chunk.map(chunk, fused);
  });
}
