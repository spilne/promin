// ---------------------------------------------------------------------------
// Grid — Airflow-style "N recent runs as columns, steps as rows" view.
// Also powers per-workflow sparklines on the runs list.
//
// GET /api/workflows/:name/grid?limit=25
//   { runs: [{ workflowId, status, createdAt, completedAt?, steps: {name: status}[] }] }
//
// GET /api/workflows/sparklines?limit=14
//   { [workflowName]: [{ status, createdAt }] }
// ---------------------------------------------------------------------------

import type { WorkflowStorage, WorkflowStatus, StepStatus } from "@promin/workflow";
import { json, jsonError } from "../router.ts";
import type { ExtendedStepStatus } from "../api-types.ts";

export interface GridRunDto {
  workflowId: string;
  status: WorkflowStatus;
  createdAt: string;
  startedAt?: string;
  completedAt?: string;
  /** Step name → effective status (including synthesised upstream_failed). */
  steps: Record<string, ExtendedStepStatus>;
}

export interface GridResponse {
  runs: GridRunDto[];
  /** Union of step names across the returned runs, in stable encounter order. */
  stepNames: string[];
}

export interface SparklinesResponse {
  [workflowName: string]: Array<{ status: WorkflowStatus; createdAt: string }>;
}

function computeUpstreamFailed(
  steps: Record<string, { status: StepStatus; dependsOn: string[] }>,
): Record<string, ExtendedStepStatus> {
  const out: Record<string, ExtendedStepStatus> = {};
  for (const [name, s] of Object.entries(steps)) {
    if (s.status !== "pending") {
      out[name] = s.status;
      continue;
    }
    const hasFailedDep = s.dependsOn.some((dep) => {
      const up = steps[dep];
      return (
        up &&
        (up.status === "failed" ||
          up.status === "compensation_failed" ||
          out[dep] === "upstream_failed")
      );
    });
    out[name] = hasFailedDep ? "upstream_failed" : "pending";
  }
  return out;
}

export function getWorkflowGrid(storage: WorkflowStorage) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const name = params.name;
    if (!name) return jsonError(400, "missing_name");
    const url = new URL(req.url);
    const limit = Math.max(
      1,
      Math.min(100, Number.parseInt(url.searchParams.get("limit") ?? "25", 10) || 25),
    );
    const rows = await storage.listWorkflows({ name, limit });

    const seenStepNames: string[] = [];
    const seen = new Set<string>();
    const runs: GridRunDto[] = [];
    for (const w of rows) {
      const steps = computeUpstreamFailed(
        Object.fromEntries(
          Object.entries(w.steps).map(([k, s]) => [
            k,
            { status: s.status, dependsOn: s.dependsOn },
          ]),
        ),
      );
      for (const n of Object.keys(steps)) {
        if (!seen.has(n)) {
          seen.add(n);
          seenStepNames.push(n);
        }
      }
      runs.push({
        workflowId: w.workflowId,
        status: w.status,
        createdAt: w.createdAt.toISOString(),
        startedAt: w.startedAt?.toISOString(),
        completedAt: w.completedAt?.toISOString(),
        steps,
      });
    }
    const response: GridResponse = { runs, stepNames: seenStepNames };
    return json(200, response);
  };
}

export interface HistoryRunDto {
  workflowId: string;
  status: WorkflowStatus;
  version?: string;
  createdAt: string;
  startedAt?: string;
  completedAt?: string;
  /** completedAt - startedAt in ms, when terminal. Undefined for in-flight or failed-before-start. */
  totalMs?: number;
  /** stepName → { durationMs, status, attempt }. Only executed steps. */
  steps: Record<string, { durationMs?: number; status: StepStatus; attempt: number }>;
}

export interface HistoryResponse {
  runs: HistoryRunDto[];
  /** Union of step names seen across the returned runs, in DAG order. */
  stepNames: string[];
}

/**
 * GET /api/workflows/:name/history?limit=50
 *
 * Per-run totals + per-step durations for the most recent runs of a
 * workflow, oldest-first. Feeds the workflow-detail bar chart so operators
 * can eyeball duration trends and spot degradation in individual steps.
 */
export function getWorkflowHistory(storage: WorkflowStorage) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const name = params.name;
    if (!name) return jsonError(400, "missing_name");
    const url = new URL(req.url);
    const limit = Math.max(
      1,
      Math.min(200, Number.parseInt(url.searchParams.get("limit") ?? "50", 10) || 50),
    );
    // Storage returns newest-first; the chart wants oldest on the left.
    const rows = (await storage.listWorkflows({ name, limit })).slice().reverse();

    const seenStepNames: string[] = [];
    const seen = new Set<string>();
    const runs: HistoryRunDto[] = rows.map((w) => {
      const steps: HistoryRunDto["steps"] = {};
      for (const [stepName, s] of Object.entries(w.steps)) {
        if (!seen.has(stepName)) {
          seen.add(stepName);
          seenStepNames.push(stepName);
        }
        steps[stepName] = {
          durationMs: s.durationMs,
          status: s.status,
          attempt: s.attempt,
        };
      }
      const totalMs =
        w.startedAt && w.completedAt ? w.completedAt.getTime() - w.startedAt.getTime() : undefined;
      return {
        workflowId: w.workflowId,
        status: w.status,
        version: w.version,
        createdAt: w.createdAt.toISOString(),
        startedAt: w.startedAt?.toISOString(),
        completedAt: w.completedAt?.toISOString(),
        totalMs,
        steps,
      };
    });

    const response: HistoryResponse = { runs, stepNames: seenStepNames };
    return json(200, response);
  };
}

export function getSparklines(storage: WorkflowStorage) {
  return async (req: Request): Promise<Response> => {
    const url = new URL(req.url);
    const limit = Math.max(
      1,
      Math.min(50, Number.parseInt(url.searchParams.get("limit") ?? "14", 10) || 14),
    );
    // One scan, grouped client-side. Uses the lean summary path when the
    // storage supports it — sparklines only need name/status/createdAt.
    const rows = storage.listWorkflowSummaries
      ? await storage.listWorkflowSummaries({ limit: 500 })
      : await storage.listWorkflows({ limit: 500 });
    const bucket: Record<string, Array<{ status: WorkflowStatus; createdAt: string }>> = {};
    for (const w of rows) {
      const list = (bucket[w.workflowName] = bucket[w.workflowName] ?? []);
      if (list.length < limit) {
        list.push({ status: w.status, createdAt: w.createdAt.toISOString() });
      }
    }
    return json(200, bucket as SparklinesResponse);
  };
}
