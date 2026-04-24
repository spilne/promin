import type {
  RunDto,
  RunListResponse,
  RunListQuery,
  MetricsDto,
  WorkersResponse,
  SignalRequest,
} from "../../server/api-types.ts";
import type { SchedulesResponse, ScheduleDto } from "../../server/routes/schedules.ts";
import type {
  SignalHistoryResponse,
  AttemptsResponse,
  RunHistoryResponse,
  ChildrenResponse,
} from "../../server/routes/run-extras.ts";
import type { GridResponse, SparklinesResponse } from "../../server/routes/grid.ts";

const BASE = ""; // served from same origin

function authHeader(): HeadersInit {
  const k = localStorage.getItem("zorya_api_key");
  return k ? { authorization: `Bearer ${k}` } : {};
}

async function req<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    ...init,
    headers: { ...(init?.headers ?? {}), ...authHeader() },
  });
  if (!res.ok) throw new ApiError(res.status, await res.text());
  return (await res.json()) as T;
}

export class ApiError extends Error {
  constructor(
    public status: number,
    body: string,
  ) {
    super(`HTTP ${status}: ${body}`);
  }
}

export const api = {
  listRuns(q: RunListQuery = {}): Promise<RunListResponse> {
    const params = new URLSearchParams();
    for (const [k, v] of Object.entries(q)) {
      if (v !== undefined && v !== null && v !== "") params.set(k, String(v));
    }
    const qs = params.toString();
    return req<RunListResponse>(`/api/runs${qs ? `?${qs}` : ""}`);
  },
  getRun(id: string): Promise<RunDto> {
    return req<RunDto>(`/api/runs/${encodeURIComponent(id)}`);
  },
  cancelRun(id: string): Promise<{ ok: boolean }> {
    return req(`/api/runs/${encodeURIComponent(id)}/cancel`, { method: "POST" });
  },
  signalRun(id: string, body: SignalRequest): Promise<{ ok: boolean }> {
    return req(`/api/runs/${encodeURIComponent(id)}/signal`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
  },
  getMetrics(): Promise<MetricsDto> {
    return req<MetricsDto>(`/api/metrics`);
  },
  listWorkers(): Promise<WorkersResponse> {
    return req<WorkersResponse>(`/api/workers`);
  },
  eventsUrl(id: string): string {
    return `${BASE}/api/runs/${encodeURIComponent(id)}/events`;
  },
  getRunSignals(id: string): Promise<SignalHistoryResponse> {
    return req(`/api/runs/${encodeURIComponent(id)}/signals`);
  },
  getRunAttempts(id: string, stepName?: string): Promise<AttemptsResponse> {
    const qs = stepName ? `?stepName=${encodeURIComponent(stepName)}` : "";
    return req(`/api/runs/${encodeURIComponent(id)}/attempts${qs}`);
  },
  getRunHistory(id: string): Promise<RunHistoryResponse> {
    return req(`/api/runs/${encodeURIComponent(id)}/history`);
  },
  getRunChildren(id: string): Promise<ChildrenResponse> {
    return req(`/api/runs/${encodeURIComponent(id)}/children`);
  },
  markRunSuccess(id: string): Promise<{ ok: boolean }> {
    return req(`/api/runs/${encodeURIComponent(id)}/mark-success`, { method: "POST" });
  },
  markRunFailed(id: string, reason?: string): Promise<{ ok: boolean }> {
    return req(`/api/runs/${encodeURIComponent(id)}/mark-failed`, {
      method: "POST",
      headers: reason ? { "content-type": "application/json" } : undefined,
      body: reason ? JSON.stringify({ reason }) : undefined,
    });
  },
  rerunRun(id: string): Promise<{ ok: boolean }> {
    return req(`/api/runs/${encodeURIComponent(id)}/rerun`, { method: "POST" });
  },
  getWorkflowGrid(name: string, limit = 25): Promise<GridResponse> {
    return req(`/api/workflows/${encodeURIComponent(name)}/grid?limit=${limit}`);
  },
  getSparklines(limit = 14): Promise<SparklinesResponse> {
    return req(`/api/workflows/sparklines?limit=${limit}`);
  },
  listWorkflowNames(): Promise<{ names: string[]; types?: string[]; namespaces?: string[] }> {
    return req(`/api/workflows`);
  },
  listSchedules(): Promise<SchedulesResponse & { configured?: boolean }> {
    return req(`/api/schedules`);
  },
  createSchedule(body: {
    id: string;
    name?: string;
    cron?: string;
    intervalMs?: number;
    rrule?: string;
    timezone?: string;
    enabled?: boolean;
    workflowName?: string;
    input?: unknown;
  }): Promise<ScheduleDto> {
    return req(`/api/schedules`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
  },
  patchSchedule(id: string, body: { enabled?: boolean }): Promise<ScheduleDto> {
    return req(`/api/schedules/${encodeURIComponent(id)}`, {
      method: "PATCH",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
  },
  deleteSchedule(id: string): Promise<{ ok: boolean }> {
    return req(`/api/schedules/${encodeURIComponent(id)}`, { method: "DELETE" });
  },
};
