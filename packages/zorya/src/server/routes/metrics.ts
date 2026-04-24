// ---------------------------------------------------------------------------
// Metrics route — /api/metrics
//
// Computes counts per status and (if available) percentile durations across
// the currently loaded workflows. Backends with dedicated metrics stores
// (e.g. PgWorkflowMetrics) can override via ZoryaServerConfig.metrics.
// ---------------------------------------------------------------------------

import type { WorkflowStorage, WorkflowStatus } from "@promin/workflow";
import { json } from "../router.ts";
import type { MetricsDto } from "../api-types.ts";

export interface MetricsProvider {
  getMetrics(): Promise<MetricsDto>;
}

const ALL_STATUSES: WorkflowStatus[] = [
  "pending",
  "running",
  "suspended",
  "completed",
  "failed",
  "compensating",
];

export class StorageMetricsProvider implements MetricsProvider {
  private readonly storage: WorkflowStorage;
  /** Page size when scanning for metrics. Default 500. */
  private readonly pageSize: number;

  constructor(storage: WorkflowStorage, opts?: { pageSize?: number }) {
    this.storage = storage;
    this.pageSize = opts?.pageSize ?? 500;
  }

  async getMetrics(): Promise<MetricsDto> {
    const byStatus: Record<WorkflowStatus, number> = {
      pending: 0,
      running: 0,
      suspended: 0,
      completed: 0,
      failed: 0,
      compensating: 0,
    };
    const durations: number[] = [];
    let total = 0;

    let offset = 0;
    while (true) {
      const page = await this.storage.listWorkflows({ limit: this.pageSize, offset });
      if (page.length === 0) break;
      for (const w of page) {
        total += 1;
        byStatus[w.status] += 1;
        if (w.completedAt) {
          durations.push(w.completedAt.getTime() - w.createdAt.getTime());
        }
      }
      if (page.length < this.pageSize) break;
      offset += page.length;
      // Hard cap so we never scan unbounded storages synchronously.
      if (offset >= 5000) break;
    }

    // Ensure every status is represented.
    for (const s of ALL_STATUSES) if (!(s in byStatus)) byStatus[s] = 0;

    return {
      total,
      byStatus,
      avgDurationMs: avg(durations),
      p95DurationMs: percentile(durations, 0.95),
      p99DurationMs: percentile(durations, 0.99),
    };
  }
}

export function getMetrics(provider: MetricsProvider) {
  return async (): Promise<Response> => {
    const m = await provider.getMetrics();
    return json(200, m);
  };
}

function avg(xs: number[]): number | undefined {
  if (xs.length === 0) return undefined;
  let sum = 0;
  for (const x of xs) sum += x;
  return Math.round(sum / xs.length);
}

function percentile(xs: number[], p: number): number | undefined {
  if (xs.length === 0) return undefined;
  const sorted = [...xs].sort((a, b) => a - b);
  const idx = Math.min(sorted.length - 1, Math.floor(p * sorted.length));
  return sorted[idx];
}
