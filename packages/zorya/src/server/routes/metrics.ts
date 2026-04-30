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
  "tripwire",
];

export class StorageMetricsProvider implements MetricsProvider {
  private readonly storage: WorkflowStorage;

  constructor(storage: WorkflowStorage, _opts?: { pageSize?: number }) {
    this.storage = storage;
  }

  async getMetrics(): Promise<MetricsDto> {
    const byStatus: Record<WorkflowStatus, number> = {
      pending: 0,
      running: 0,
      suspended: 0,
      completed: 0,
      failed: 0,
      compensating: 0,
      tripwire: 0,
    };

    if (this.storage.countWorkflows) {
      // Fast path: one COUNT(*) query per status, each hitting the status index.
      const counts = await Promise.all(
        ALL_STATUSES.map((s) => this.storage.countWorkflows!({ status: s })),
      );
      for (let i = 0; i < ALL_STATUSES.length; i++) {
        byStatus[ALL_STATUSES[i]] = counts[i];
      }
    } else {
      // Fallback: lean scan without blob columns.
      const lister = (this.storage.listWorkflowSummaries ?? this.storage.listWorkflows).bind(
        this.storage,
      );
      let offset = 0;
      while (true) {
        const page = await lister({ limit: 500, offset });
        if (page.length === 0) break;
        for (const w of page) byStatus[w.status] = (byStatus[w.status] ?? 0) + 1;
        if (page.length < 500) break;
        offset += page.length;
        if (offset >= 5000) break;
      }
    }

    const total = ALL_STATUSES.reduce((s, k) => s + byStatus[k], 0);

    // Duration percentiles: lean scan of completed rows only, no blob columns.
    const lister = (this.storage.listWorkflowSummaries ?? this.storage.listWorkflows).bind(
      this.storage,
    );
    const completedPage = await lister({ status: "completed", limit: 2000 });
    const durations = completedPage
      .filter((w) => w.completedAt != null)
      .map((w) => w.completedAt!.getTime() - w.createdAt.getTime());

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
