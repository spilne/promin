// ---------------------------------------------------------------------------
// inlineDataset — cases supplied directly in code. The simplest EvalDataset.
// ---------------------------------------------------------------------------

import type { EvalCase, EvalDataset } from "../types.ts";

/** Wrap an in-memory array of cases as an `EvalDataset`. */
export function inlineDataset(params: {
  readonly cases: ReadonlyArray<EvalCase>;
  /** Dataset id. Default `"inline"`. */
  readonly id?: string;
}): EvalDataset {
  const { cases } = params;
  return {
    id: params.id ?? "inline",
    async *cases() {
      for (const evalCase of cases) yield evalCase;
    },
  };
}
