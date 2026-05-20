// ---------------------------------------------------------------------------
// InMemoryEvalDatasetStore — the default, process-local EvalDatasetStore.
// ---------------------------------------------------------------------------

import type { EvalCase } from "../types.ts";
import type { EvalDatasetStore } from "./types.ts";

export class InMemoryEvalDatasetStore implements EvalDatasetStore {
  private readonly datasets = new Map<string, EvalCase[]>();

  async save(datasetId: string, cases: ReadonlyArray<EvalCase>): Promise<void> {
    this.datasets.set(datasetId, [...cases]);
  }

  async get(datasetId: string): Promise<EvalCase[] | null> {
    const cases = this.datasets.get(datasetId);
    return cases !== undefined ? [...cases] : null;
  }

  async list(): Promise<string[]> {
    return [...this.datasets.keys()].sort();
  }

  async delete(datasetId: string): Promise<void> {
    this.datasets.delete(datasetId);
  }
}
