import type {
  RunDto,
  RunListResponse,
  RunListQuery,
  MetricsDto,
  WorkersResponse,
  SignalRequest,
} from "../../server/api-types.ts";
import type {
  SchedulesResponse,
  ScheduleDto,
  ScheduleHistoryResponse,
  ScheduleUpcomingResponse,
} from "../../server/routes/schedules.ts";
import type {
  SignalHistoryResponse,
  AttemptsResponse,
  RunHistoryResponse,
  ChildrenResponse,
  StepJournalResponse,
} from "../../server/routes/run-extras.ts";
import type {
  GridResponse,
  HistoryResponse,
  SparklinesResponse,
} from "../../server/routes/grid.ts";
import type { WorkflowDefDto, WorkflowDefsResponse } from "../../server/routes/workflow-defs.ts";
import type {
  AgentsListResponse,
  AgentThreadsResponse,
  RegisteredAgent,
  ThreadInvokeResponse,
  ThreadMessagesResponse,
} from "../../server/routes/agents.ts";

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
    const { orderBy, orderDir, metadata, ...rest } = q;
    const params = new URLSearchParams();
    for (const [k, v] of Object.entries(rest)) {
      if (v !== undefined && v !== null && v !== "") params.set(k, String(v));
    }
    // Server reads `?sort=col:dir` (single param) so the URL stays compact
    // and cycle/clear semantics are atomic. Encode both halves only when
    // a column is explicitly chosen — server defaults to createdAt:desc.
    if (orderBy) params.set("sort", `${orderBy}:${orderDir ?? "desc"}`);
    // Metadata travels as a single JSON-encoded param so nested values can
    // round-trip (URLSearchParams would flatten an object via String() to
    // "[object Object]").
    if (metadata && Object.keys(metadata).length > 0) {
      params.set("metadata", JSON.stringify(metadata));
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
  getHealth(): Promise<{ ok: boolean }> {
    return req<{ ok: boolean }>(`/api/health`);
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
  getRunStepJournal(id: string, stepName: string): Promise<StepJournalResponse> {
    return req(`/api/runs/${encodeURIComponent(id)}/journal/${encodeURIComponent(stepName)}`);
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
  getWorkflowHistory(name: string, limit = 50): Promise<HistoryResponse> {
    return req(`/api/workflows/${encodeURIComponent(name)}/history?limit=${limit}`);
  },
  getSparklines(limit = 14): Promise<SparklinesResponse> {
    return req(`/api/workflows/sparklines?limit=${limit}`);
  },
  listWorkflowDefs(): Promise<WorkflowDefsResponse> {
    return req(`/api/workflows/definitions`);
  },
  getWorkflowDef(name: string): Promise<WorkflowDefDto> {
    return req(`/api/workflows/${encodeURIComponent(name)}/definition`);
  },
  triggerWorkflow(
    name: string,
    body: { input?: unknown; workflowId?: string; namespace?: string; version?: string },
  ): Promise<{ workflowId: string }> {
    return req(`/api/runs/trigger/${encodeURIComponent(name)}`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
  },
  listWorkflowNames(
    params: { namespace?: string } = {},
  ): Promise<{ names: string[]; types?: string[]; namespaces?: string[] }> {
    const qp = new URLSearchParams();
    if (params.namespace) qp.set("namespace", params.namespace);
    const qs = qp.toString();
    return req(`/api/workflows${qs ? `?${qs}` : ""}`);
  },
  listSchedules(
    params: { namespace?: string } = {},
  ): Promise<SchedulesResponse & { configured?: boolean }> {
    const qp = new URLSearchParams();
    if (params.namespace) qp.set("namespace", params.namespace);
    const qs = qp.toString();
    return req(`/api/schedules${qs ? `?${qs}` : ""}`);
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
  emitSchedule(id: string): Promise<{ ok: boolean }> {
    return req(`/api/schedules/${encodeURIComponent(id)}/emit`, { method: "POST" });
  },
  getSchedule(id: string): Promise<ScheduleDto> {
    return req(`/api/schedules/${encodeURIComponent(id)}`);
  },
  getScheduleHistory(
    id: string,
    params: { limit?: number; offset?: number } = {},
  ): Promise<ScheduleHistoryResponse> {
    const qp = new URLSearchParams();
    if (params.limit !== undefined) qp.set("limit", String(params.limit));
    if (params.offset !== undefined) qp.set("offset", String(params.offset));
    const qs = qp.toString();
    return req(`/api/schedules/${encodeURIComponent(id)}/history${qs ? `?${qs}` : ""}`);
  },
  getScheduleUpcoming(
    id: string,
    params: { count?: number } = {},
  ): Promise<ScheduleUpcomingResponse> {
    const qp = new URLSearchParams();
    if (params.count !== undefined) qp.set("count", String(params.count));
    const qs = qp.toString();
    return req(`/api/schedules/${encodeURIComponent(id)}/upcoming${qs ? `?${qs}` : ""}`);
  },

  // ---------------------------------------------------------------------
  // Agents — registry browsing + chat console
  // ---------------------------------------------------------------------
  listAgents(): Promise<AgentsListResponse> {
    return req<AgentsListResponse>(`/api/agents`);
  },
  getAgent(id: string): Promise<RegisteredAgent> {
    return req<RegisteredAgent>(`/api/agents/${encodeURIComponent(id)}`);
  },
  listAgentThreads(
    id: string,
    params: { namespaceId: string; resourceId?: string; limit?: number },
  ): Promise<AgentThreadsResponse> {
    const qp = new URLSearchParams({ namespaceId: params.namespaceId });
    if (params.resourceId) qp.set("resourceId", params.resourceId);
    if (params.limit !== undefined) qp.set("limit", String(params.limit));
    return req<AgentThreadsResponse>(`/api/agents/${encodeURIComponent(id)}/threads?${qp}`);
  },
  listAgentThreadMessages(
    id: string,
    threadId: string,
    params: { namespaceId: string; resourceId?: string; limit?: number },
  ): Promise<ThreadMessagesResponse> {
    const qp = new URLSearchParams({ namespaceId: params.namespaceId });
    if (params.resourceId) qp.set("resourceId", params.resourceId);
    if (params.limit !== undefined) qp.set("limit", String(params.limit));
    return req<ThreadMessagesResponse>(
      `/api/agents/${encodeURIComponent(id)}/threads/${encodeURIComponent(threadId)}/messages?${qp}`,
    );
  },
  sendAgentThreadMessage(
    id: string,
    threadId: string,
    body: { task: string; namespaceId: string; resourceId?: string },
  ): Promise<ThreadInvokeResponse> {
    return req<ThreadInvokeResponse>(
      `/api/agents/${encodeURIComponent(id)}/threads/${encodeURIComponent(threadId)}`,
      {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(body),
      },
    );
  },
  /**
   * POST /api/agents/:id/threads/:threadId/stream — SSE streaming turn.
   *
   * EventSource doesn't support POST or custom headers (auth), so this uses
   * fetch + manual SSE parsing — same pattern ChatGPT/Claude.ai use.
   * `onDelta` fires per text chunk; `onFinish` once at completion.
   * Returns an AbortController so callers can cancel mid-stream.
   */
  streamAgentThread(
    id: string,
    threadId: string,
    body: { task: string; namespaceId: string; resourceId?: string },
    handlers: {
      onThread?: (info: { threadId: string; isNew: boolean }) => void;
      onDelta: (delta: string) => void;
      onFinish?: (info: {
        text: string;
        finishReason: string;
        usage: { inputTokens: number; outputTokens: number };
      }) => void;
      onError?: (message: string) => void;
    },
  ): { abort: () => void; done: Promise<void> } {
    const ctrl = new AbortController();
    const url = `${BASE}/api/agents/${encodeURIComponent(id)}/threads/${encodeURIComponent(threadId)}/stream`;

    const done = (async () => {
      try {
        const res = await fetch(url, {
          method: "POST",
          headers: {
            "content-type": "application/json",
            accept: "text/event-stream",
            ...authHeader(),
          },
          body: JSON.stringify(body),
          signal: ctrl.signal,
        });
        if (!res.ok || !res.body) {
          handlers.onError?.(`HTTP ${res.status}`);
          return;
        }
        const reader = res.body.getReader();
        const decoder = new TextDecoder();
        let buffer = "";
        while (true) {
          const { value, done: streamDone } = await reader.read();
          if (streamDone) break;
          buffer += decoder.decode(value, { stream: true });
          // SSE frames are separated by a blank line. Process whole frames
          // and keep any trailing partial frame in the buffer.
          let sepIndex: number;
          while ((sepIndex = buffer.indexOf("\n\n")) !== -1) {
            const frame = buffer.slice(0, sepIndex);
            buffer = buffer.slice(sepIndex + 2);
            const parsed = parseSseFrame(frame);
            if (!parsed) continue;
            const { event, data } = parsed;
            try {
              const payload = JSON.parse(data);
              if (event === "thread") {
                handlers.onThread?.(payload);
              } else if (event === "finish") {
                handlers.onFinish?.(payload);
              } else if (event === "error") {
                handlers.onError?.(String(payload?.message ?? "stream error"));
              } else if (typeof payload?.delta === "string") {
                handlers.onDelta(payload.delta);
              }
            } catch {
              // Skip malformed frames.
            }
          }
        }
      } catch (e) {
        if ((e as Error).name === "AbortError") return;
        handlers.onError?.((e as Error).message);
      }
    })();

    return { abort: () => ctrl.abort(), done };
  },
};

function parseSseFrame(frame: string): { event: string; data: string } | null {
  let event = "message";
  const dataLines: string[] = [];
  for (const line of frame.split("\n")) {
    if (line.startsWith(":")) continue; // comment
    if (line.startsWith("event:")) event = line.slice(6).trim();
    else if (line.startsWith("data:")) dataLines.push(line.slice(5).replace(/^ /, ""));
  }
  if (dataLines.length === 0) return null;
  return { event, data: dataLines.join("\n") };
}
