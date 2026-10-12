// ---------------------------------------------------------------------------
// storedDataset — materialize a dataset from an EvalDatasetStore.
// ---------------------------------------------------------------------------

import type { EvalDataset } from "../types.ts";
import type { EvalDatasetStore } from "./types.ts";

/** Wrap a stored dataset as an `EvalDataset` for `runEval` / `runMatrix`. */
export function storedDataset(params: {
  readonly store: EvalDatasetStore;
  readonly datasetId: string;
}): EvalDataset {
  const { store, datasetId } = params;
  return {
    id: datasetId,
    async *cases() {
      const cases = await store.get(datasetId);
      if (cases === null) {
        throw new Error(`storedDataset: dataset "${datasetId}" not found in the store`);
      }
      for (const evalCase of cases) yield evalCase;
    },
  };
}
