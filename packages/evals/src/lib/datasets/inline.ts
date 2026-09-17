// ---------------------------------------------------------------------------
// inlineDataset — cases supplied directly in code. The simplest EvalDataset.
// ---------------------------------------------------------------------------

import type { EvalCase, EvalDataset } from "../types.ts";

/** Wrap an in-memory array of cases as an `EvalDataset`. */
export function inlineDataset(items: ReadonlyArray<EvalCase>, id = "inline"): EvalDataset {
  return {
    id,
    async *cases() {
      for (const evalCase of items) yield evalCase;
    },
  };
}
