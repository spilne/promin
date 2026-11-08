// ---------------------------------------------------------------------------
// InMemoryEvalDatasetStore — the default, process-local EvalDatasetStore.
// ---------------------------------------------------------------------------

import type { EvalCase } from "../types.ts";
import type { EvalDatasetStore } from "./types.ts";

export class InMemoryEvalDatasetStore implements EvalDatasetStore {
  private readonly datasets = new Map<string, EvalCase[]>();

  async save(params: {
    readonly datasetId: string;
    readonly cases: ReadonlyArray<EvalCase>;
  }): Promise<void> {
    this.datasets.set(params.datasetId, [...params.cases]);
  }

  async get(datasetId: string): Promise<EvalCase[] | null> {
    const cases = this.datasets.get(datasetId);
    return cases !== undefined ? [...cases] : null;
  }

  async appendCase(datasetId: string, evalCase: EvalCase): Promise<boolean> {
    const cases = this.datasets.get(datasetId) ?? [];
    if (cases.some((item) => item.id === evalCase.id)) return false;
    this.datasets.set(datasetId, [...cases, evalCase]);
    return true;
  }

  async replaceCase(datasetId: string, evalCase: EvalCase): Promise<boolean> {
    const cases = this.datasets.get(datasetId);
    if (!cases?.some((item) => item.id === evalCase.id)) return false;
    this.datasets.set(
      datasetId,
      cases.map((item) => (item.id === evalCase.id ? evalCase : item)),
    );
    return true;
  }

  async deleteCase(datasetId: string, caseId: string): Promise<boolean> {
    const cases = this.datasets.get(datasetId);
    if (!cases?.some((item) => item.id === caseId)) return false;
    this.datasets.set(
      datasetId,
      cases.filter((item) => item.id !== caseId),
    );
    return true;
  }

  async list(): Promise<string[]> {
    return [...this.datasets.keys()].sort();
  }

  async delete(datasetId: string): Promise<void> {
    this.datasets.delete(datasetId);
  }
}
