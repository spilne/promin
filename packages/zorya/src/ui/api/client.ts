import type {
  ApiError as ApiErrorBody,
  ApiIssue,
  RerunResponse,
  ResumeRunResponse,
  RunActionResponse,
  RunDto,
  RunListResponse,
  RunListQuery,
  MetricsDto,
  WorkersResponse,
  SignalDeliveryResponse,
  SignalRequest,
  TriggerRunRequest,
  TriggerRunResponse,
} from "../../server/api-types.ts";
import type {
  ScheduleCreateRequest,
  ScheduleEmitResponse,
  SchedulePatchRequest,
  SchedulesResponse,
  ScheduleDto,
  ScheduleHistoryResponse,
  ScheduleUpcomingResponse,
} from "../../server/routes/schedules.ts";
import type { NamespacesResponse } from "../../server/routes/namespaces.ts";
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
  AgentVersionsResponse,
  AgentSourcesResponse,
  AgentThreadsResponse,
  RegisteredAgent,
  ThreadApprovalsResponse,
  ThreadInvokeResponse,
  ThreadMessagesResponse,
} from "../../server/routes/agents/agent-gateway-deps.ts";
import type {
  RegisteredSkill,
  SkillsListResponse,
  SkillSourcesResponse,
  SkillVersionsResponse,
} from "../../server/routes/skills.ts";
import type {
  FragmentDto,
  FragmentsListResponse,
  FragmentSourcesResponse,
} from "../../server/routes/fragments.ts";
import type {
  RegisteredRole,
  RoleDefinition,
  RolesListResponse,
  RoleVersionsResponse,
} from "../../server/routes/roles.ts";
import type {
  FragmentsCatalogResponse,
  RetrieverCatalogEntry,
  RetrieversCatalogResponse,
  SkillsCatalogResponse,
} from "../../server/routes/agent-catalog.ts";
import type { MemoryInspectResponse } from "../../server/routes/memory.ts";
import type { McpServersCatalogResponse } from "../../server/services/mcp/zorya-mcp-servers.ts";

export type { McpServersCatalogResponse };
import type {
  KnowledgeBase,
  KnowledgeBaseCreateRequest,
  KnowledgeBasePatchRequest,
  KnowledgeBasesListResponse,
  KnowledgeChunksResponse,
  KnowledgeSearchRequest,
  KnowledgeSearchResponse,
  KnowledgeSourceAdaptersResponse,
  KnowledgeSourceCreateRequest,
  KnowledgeSourceImportRequest,
  KnowledgeSourcesImportResponse,
  KnowledgeSourcesResponse,
  KnowledgeSourceSummary,
} from "../../server/routes/knowledge-bases.ts";
import type {
  BuilderActivitiesResponse,
  BuilderValidateResponse,
  BuilderWorkflow,
  BuilderWorkflowPublishRequest,
  BuilderWorkflowSaveRequest,
  BuilderWorkflowsResponse,
  PublishResult,
} from "../../server/routes/workflow-builder.ts";
import type { SignalsResponse } from "../../server/routes/signals.ts";
import type {
  CompleteTokenResponse,
  DescribeSignalTokenResponse,
  MintTokenResponse,
} from "../../server/routes/signal-tokens.ts";
import type { WorkflowVersionDto } from "../../server/routes/workflow-versions.ts";
import { readResumableSse, SseHttpError, subscribeSse, type SseMessage } from "./sse.ts";

const BASE = ""; // served from same origin

/** Attempts a GET gets in all before its failure surfaces. */
const GET_ATTEMPTS = 3;
/** Answers a server gives while it restarts or a gateway can't reach it. */
const RETRYABLE_STATUSES: ReadonlySet<number> = new Set([502, 503, 504]);

/** Backoff before retry `attempt + 1`: 50ms, then 100ms. */
function retryDelay(attempt: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, 50 * 2 ** (attempt - 1)));
}

/** The dashboard's stored operator key as an `authorization` header, if any. */
export function authHeader(): Record<string, string> {
  const k = localStorage.getItem("zorya_api_key");
  return k ? { authorization: `Bearer ${k}` } : {};
}

/**
 * Fetch JSON from the API. The stored operator key is sent unless the
 * caller passes its own `authorization` (the signal-token routes send the
 * token's bearer, which an operator key must never replace). 204 and empty
 * bodies resolve to `undefined`.
 *
 * A GET is retried (up to `GET_ATTEMPTS` in all, with a short backoff) on a
 * network failure or a gateway-style 502/503/504, which a restarting or
 * hot-reloading server answers for a moment. Other methods are never
 * replayed from the browser.
 */
export async function req<T>(path: string, init?: RequestInit): Promise<T> {
  const headers = new Headers(authHeader());
  new Headers(init?.headers).forEach((value, key) => headers.set(key, value));
  const res = await fetchWithRetry({ url: `${BASE}${path}`, init: { ...init, headers } });
  if (!res.ok) throw new ApiError(res.status, await res.text());
  if (res.status === 204) return undefined as T;
  const text = await res.text();
  if (text.length === 0) return undefined as T;
  return JSON.parse(text) as T;
}

/** `fetch`, with a GET retried on a network failure or a 502/503/504. */
async function fetchWithRetry(params: { url: string; init: RequestInit }): Promise<Response> {
  const method = (params.init.method ?? "GET").toUpperCase();
  const attempts = method === "GET" ? GET_ATTEMPTS : 1;
  for (let attempt = 1; ; attempt++) {
    const last = attempt >= attempts;
    let res: Response;
    try {
      res = await fetch(params.url, params.init);
    } catch (err) {
      // fetch rejects with a TypeError when the request never got an answer.
      if (last || !(err instanceof TypeError)) throw err;
      await retryDelay(attempt);
      continue;
    }
    if (last || !RETRYABLE_STATUSES.has(res.status)) return res;
    // Release the discarded answer's body before asking again.
    await res.body?.cancel();
    await retryDelay(attempt);
  }
}

/**
 * A non-2xx answer. The server's error envelope (`{ error, message?,
 * issues? }`) is parsed into `code`, `detail` and `issues`; the message
 * reads as the server's explanation when there is one.
 */
export class ApiError extends Error {
  /** The envelope's `error` code (e.g. `run_ended`), or `undefined` for a non-JSON body. */
  readonly code?: string;
  /** The envelope's `message`. */
  readonly detail?: string;
  /** Field-level problems of an `invalid_request`. */
  readonly issues?: ReadonlyArray<ApiIssue>;

  constructor(
    public status: number,
    body: string,
  ) {
    const envelope = parseEnvelope(body);
    super(
      envelope
        ? `HTTP ${status}: ${envelope.message ?? envelope.error}`
        : `HTTP ${status}: ${body}`,
    );
    this.name = "ApiError";
    if (envelope) {
      this.code = envelope.error;
      if (envelope.message !== undefined) this.detail = envelope.message;
      if (envelope.issues !== undefined) this.issues = envelope.issues;
    }
  }
}

function parseEnvelope(body: string): ApiErrorBody | undefined {
  try {
    const parsed = JSON.parse(body) as unknown;
    if (
      parsed &&
      typeof parsed === "object" &&
      typeof (parsed as ApiErrorBody).error === "string"
    ) {
      return parsed as ApiErrorBody;
    }
  } catch {
    // not JSON
  }
  return undefined;
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
    // a column is explicitly chosen — server defaults to startedAt:desc.
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
  /** Cancel an in-flight run; `cascade` cancels its in-flight descendants too. */
  cancelRun(id: string, opts?: { cascade?: boolean }): Promise<RunActionResponse> {
    return req(`/api/runs/${encodeURIComponent(id)}/cancel`, {
      method: "POST",
      headers: opts?.cascade ? { "content-type": "application/json" } : undefined,
      body: opts?.cascade ? JSON.stringify({ cascade: true }) : undefined,
    });
  },
  /** Deliver a signal (202). Last-wins unless `ifAbsent` is set. */
  signalRun(id: string, body: SignalRequest): Promise<SignalDeliveryResponse> {
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
  /**
   * Live run events over fetch-based SSE (carries the operator key, which
   * `EventSource` cannot). Reconnects until the run's `end` event or until
   * the returned function is called.
   */
  subscribeRunEvents<T extends { type?: string }>(
    id: string,
    onEvent: (ev: T) => void,
  ): () => void {
    let ended = false;
    return subscribeSse({
      url: api.eventsUrl(id),
      headers: authHeader,
      shouldReconnect: () => !ended,
      onMessage: (message) => {
        let ev: T;
        try {
          ev = JSON.parse(message.data) as T;
        } catch {
          return; // malformed frame
        }
        if (ev.type === "end") ended = true;
        onEvent(ev);
      },
    });
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
  markRunSuccess(id: string): Promise<RunActionResponse> {
    return req(`/api/runs/${encodeURIComponent(id)}/mark-success`, { method: "POST" });
  },
  markRunFailed(id: string, reason?: string): Promise<RunActionResponse> {
    return req(`/api/runs/${encodeURIComponent(id)}/mark-failed`, {
      method: "POST",
      headers: reason ? { "content-type": "application/json" } : undefined,
      body: reason ? JSON.stringify({ reason }) : undefined,
    });
  },
  rerunRun(id: string): Promise<RerunResponse> {
    return req(`/api/runs/${encodeURIComponent(id)}/rerun`, { method: "POST" });
  },
  /** Continue an ended run from `fromStep`, keeping the results of the steps before it (202). */
  resumeRun(id: string, fromStep: string): Promise<ResumeRunResponse> {
    return req(`/api/runs/${encodeURIComponent(id)}/resume`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ fromStep }),
    });
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
  /** Start a run (202), or name the existing run with that `workflowId` (200, `existing`). */
  triggerWorkflow(name: string, body: TriggerRunRequest): Promise<TriggerRunResponse> {
    return req(`/api/workflows/${encodeURIComponent(name)}/runs`, {
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
  /** The namespace registry; `configured: false` when the server has none. */
  listNamespaces(params: { status?: "active" | "archived" } = {}): Promise<NamespacesResponse> {
    const qs = params.status ? `?status=${params.status}` : "";
    return req(`/api/namespaces${qs}`);
  },
  listWorkflowVersions(name: string): Promise<{ versions: WorkflowVersionDto[] }> {
    return req(`/api/workflows/${encodeURIComponent(name)}/versions`);
  },
  getActiveWorkflowVersion(name: string): Promise<WorkflowVersionDto> {
    return req(`/api/workflows/${encodeURIComponent(name)}/versions/active`);
  },
  promoteWorkflowVersion(name: string, version: string): Promise<{ version: WorkflowVersionDto }> {
    return req(
      `/api/workflows/${encodeURIComponent(name)}/versions/${encodeURIComponent(version)}/promote`,
      { method: "POST" },
    );
  },
  rollbackWorkflow(
    name: string,
    toVersion: string,
  ): Promise<{ previous: WorkflowVersionDto; active: WorkflowVersionDto }> {
    return req(`/api/workflows/${encodeURIComponent(name)}/rollback`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ toVersion }),
    });
  },
  listSignals(params: { namespace?: string; limit?: number } = {}): Promise<SignalsResponse> {
    const qp = new URLSearchParams();
    if (params.namespace) qp.set("namespace", params.namespace);
    if (params.limit !== undefined) qp.set("limit", String(params.limit));
    const qs = qp.toString();
    return req(`/api/signals${qs ? `?${qs}` : ""}`);
  },
  /**
   * Deliver a decision from the inbox. Sent with `ifAbsent`, so a value
   * another operator already delivered is not overwritten: that answers
   * 409 `signal_already_delivered`.
   */
  sendSignal(
    workflowId: string,
    signalName: string,
    payload: unknown,
  ): Promise<SignalDeliveryResponse> {
    return req(`/api/runs/${encodeURIComponent(workflowId)}/signal`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ signalName, payload, ifAbsent: true } satisfies SignalRequest),
    });
  },
  /**
   * Fetch a signal token's metadata for the public share page. Bearer-authed
   * (passed in `Authorization`), doesn't require dashboard credentials —
   * used by the shared form at /#/share/:combined.
   */
  describeSignalToken(tokenId: string, bearer: string): Promise<DescribeSignalTokenResponse> {
    return req(`/api/signal-tokens/${encodeURIComponent(tokenId)}/describe`, {
      method: "GET",
      headers: { authorization: `Bearer ${bearer}` },
    });
  },
  /**
   * Deliver a signal via a public bearer token — the no-dashboard path.
   * 202 on first delivery, 200 (`alreadyCompleted`) on a re-submission;
   * rejects with 410 `token_expired`, 409 `run_ended` or
   * `token_already_completed`.
   */
  completeSignalToken(
    tokenId: string,
    bearer: string,
    value: unknown,
  ): Promise<CompleteTokenResponse> {
    return req(`/api/signal-tokens/${encodeURIComponent(tokenId)}/complete`, {
      method: "POST",
      headers: {
        authorization: `Bearer ${bearer}`,
        "content-type": "application/json",
      },
      body: JSON.stringify({ value }),
    });
  },
  /**
   * Mint a public bearer token that lets a non-dashboard user deliver the
   * named signal — surfaces in /signals as the Share-link affordance.
   * Default TTL is 24 hours; tags are free-form labels for ops filtering.
   */
  mintSignalToken(
    workflowId: string,
    signalName: string,
    opts: { expiresInMs?: number; tags?: ReadonlyArray<string> } = {},
  ): Promise<MintTokenResponse> {
    return req(
      `/api/runs/${encodeURIComponent(workflowId)}/signals/${encodeURIComponent(signalName)}/token`,
      {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          expiresInMs: opts.expiresInMs ?? 24 * 60 * 60 * 1000,
          ...(opts.tags !== undefined && { tags: [...opts.tags] }),
        }),
      },
    );
  },
  listSchedules(
    params: {
      namespace?: string;
      /** Filter by `enabled` flag. Omit for "show both active + cancelled". */
      enabled?: boolean;
      /**
       * Metadata containment filter. Server JSON-decodes and pushes to
       * storage; nested-path lookups become indexed queries on backends
       * with native JSON support. Common shapes:
       *   - `{ agentTrigger: true }`              — kind = agent
       *   - `{ agentTrigger: true, threadId: t }` — chat per-thread drawer
       */
      metadata?: Record<string, unknown>;
    } = {},
  ): Promise<SchedulesResponse & { configured?: boolean }> {
    const qp = new URLSearchParams();
    if (params.namespace) qp.set("namespace", params.namespace);
    if (params.enabled !== undefined) qp.set("enabled", String(params.enabled));
    if (params.metadata && Object.keys(params.metadata).length > 0) {
      qp.set("metadata", JSON.stringify(params.metadata));
    }
    const qs = qp.toString();
    return req(`/api/schedules${qs ? `?${qs}` : ""}`);
  },
  /** Create a schedule (201); 409 `schedule_exists` when the id is taken. */
  createSchedule(body: ScheduleCreateRequest): Promise<ScheduleDto> {
    return req(`/api/schedules`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
  },
  /** Change any mutable field; `null` clears an optional one. */
  patchSchedule(id: string, body: SchedulePatchRequest): Promise<ScheduleDto> {
    return req(`/api/schedules/${encodeURIComponent(id)}`, {
      method: "PATCH",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
  },
  deleteSchedule(id: string): Promise<void> {
    return req<void>(`/api/schedules/${encodeURIComponent(id)}`, { method: "DELETE" });
  },
  /** Fire one tick now (202): names the tick and the run it started. */
  emitSchedule(id: string): Promise<ScheduleEmitResponse> {
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
  /**
   * One row per agent (its active version + version count), sorted by id.
   * `total` counts every matching agent; page with `limit` / `offset`.
   */
  listAgents(
    opts: { capability?: string; tag?: string; limit?: number; offset?: number } = {},
  ): Promise<AgentsListResponse> {
    const qs = new URLSearchParams();
    if (opts.capability) qs.set("capability", opts.capability);
    if (opts.tag) qs.set("tag", opts.tag);
    if (opts.limit !== undefined) qs.set("limit", String(opts.limit));
    if (opts.offset !== undefined) qs.set("offset", String(opts.offset));
    const q = qs.toString();
    return req<AgentsListResponse>(`/api/agents${q ? `?${q}` : ""}`);
  },
  getAgent(id: string): Promise<RegisteredAgent> {
    return req<RegisteredAgent>(`/api/agents/${encodeURIComponent(id)}`);
  },
  // Recipe CRUD (Designer UI wiring)
  createAgent(body: {
    id: string;
    version?: string;
    backend: RegisteredAgent["backend"];
    metadata?: Partial<RegisteredAgent["metadata"]>;
  }): Promise<RegisteredAgent> {
    return req<RegisteredAgent>(`/api/agents`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
  },
  updateAgent(
    id: string,
    body: {
      version?: string;
      backend?: RegisteredAgent["backend"];
      metadata?: Partial<RegisteredAgent["metadata"]>;
    },
  ): Promise<RegisteredAgent> {
    return req<RegisteredAgent>(`/api/agents/${encodeURIComponent(id)}`, {
      method: "PATCH",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
  },
  deleteAgent(id: string, version?: string): Promise<void> {
    const qp = version ? `?version=${encodeURIComponent(version)}` : "";
    return req<void>(`/api/agents/${encodeURIComponent(id)}${qp}`, { method: "DELETE" });
  },
  listAgentVersions(id: string): Promise<AgentVersionsResponse> {
    return req<AgentVersionsResponse>(`/api/agents/${encodeURIComponent(id)}/versions`);
  },
  cloneAgent(
    id: string,
    body: {
      targetId: string;
      targetVersion?: string;
      secrets?: Record<string, string>;
      /** Where supplied secrets are stored. Full scope union — the
       *  server accepts global / namespace / resource. */
      secretsScope?: SecretScopeWire;
    },
  ): Promise<{ recipe: RegisteredAgent; acceptedSecrets: string[] }> {
    return req<{ recipe: RegisteredAgent; acceptedSecrets: string[] }>(
      `/api/agents/${encodeURIComponent(id)}/clone`,
      {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(body),
      },
    );
  },
  /**
   * Register an ephemeral draft recipe (`__draft__`-prefixed id) — used
   * to chat a recipe edit before committing it. Pair with `deleteDraft`
   * on modal close; orphans are TTL-swept server-side.
   */
  createDraft(body: {
    backend: RegisteredAgent["backend"];
    metadata?: Partial<RegisteredAgent["metadata"]>;
    sourceId?: string;
  }): Promise<{ recipe: RegisteredAgent }> {
    return req<{ recipe: RegisteredAgent }>(`/api/agents/_draft`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
  },
  deleteDraft(id: string): Promise<void> {
    return req<void>(`/api/agents/_draft/${encodeURIComponent(id)}`, { method: "DELETE" });
  },
  listCatalogTools(): Promise<ToolCatalogDto> {
    return req<ToolCatalogDto>(`/api/agents/_catalog/tools`);
  },
  /** The operator's MCP allowlist for the agent editor's MCP picker. */
  listCatalogMcpServers(): Promise<McpServersCatalogResponse> {
    return req<McpServersCatalogResponse>(`/api/agents/_catalog/mcp-servers`);
  },
  listAgentSources(): Promise<AgentSourcesResponse> {
    return req<AgentSourcesResponse>(`/api/agents/_sources`);
  },
  getToolCatalogHealth(): Promise<ToolCatalogHealthDto> {
    return req<ToolCatalogHealthDto>(`/api/agents/_catalog/tools/health`);
  },
  listCatalogModels(): Promise<{ models: ModelCatalogEntryDto[] }> {
    return req<{ models: ModelCatalogEntryDto[] }>(`/api/agents/_catalog/models`);
  },
  /** Host-wired retrievers for the agent editor's knowledge picker. */
  listCatalogRetrievers(): Promise<RetrieversCatalogResponse> {
    return req<RetrieversCatalogResponse>(`/api/agents/_catalog/retrievers`);
  },

  // ---------------------------------------------------------------------
  // Skills — registry CRUD + the agent editor's picker catalog
  // ---------------------------------------------------------------------
  listSkills(): Promise<SkillsListResponse> {
    return req<SkillsListResponse>(`/api/skills`);
  },
  getSkill(id: string, version?: string): Promise<RegisteredSkill> {
    const qp = version ? `?version=${encodeURIComponent(version)}` : "";
    return req<RegisteredSkill>(`/api/skills/${encodeURIComponent(id)}${qp}`);
  },
  listSkillVersions(id: string): Promise<SkillVersionsResponse> {
    return req<SkillVersionsResponse>(`/api/skills/${encodeURIComponent(id)}/versions`);
  },
  createSkill(body: {
    id: string;
    version?: string;
    description: string;
    whenToUse?: string;
    body: string;
    metadata?: RegisteredSkill["metadata"];
  }): Promise<RegisteredSkill> {
    return req<RegisteredSkill>(`/api/skills`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
  },
  updateSkill(
    id: string,
    body: {
      version?: string;
      description?: string;
      whenToUse?: string;
      body?: string;
      metadata?: RegisteredSkill["metadata"];
    },
  ): Promise<RegisteredSkill> {
    return req<RegisteredSkill>(`/api/skills/${encodeURIComponent(id)}`, {
      method: "PATCH",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
  },
  deleteSkill(id: string, version?: string): Promise<void> {
    const qp = version ? `?version=${encodeURIComponent(version)}` : "";
    return req<void>(`/api/skills/${encodeURIComponent(id)}${qp}`, { method: "DELETE" });
  },
  listCatalogSkills(): Promise<SkillsCatalogResponse> {
    return req<SkillsCatalogResponse>(`/api/agents/_catalog/skills`);
  },
  listCatalogFragments(): Promise<FragmentsCatalogResponse> {
    return req<FragmentsCatalogResponse>(`/api/agents/_catalog/fragments`);
  },

  // ---------------------------------------------------------------------
  // Fragments — registry CRUD (parallel to skills)
  // ---------------------------------------------------------------------
  listFragments(): Promise<FragmentsListResponse> {
    return req<FragmentsListResponse>(`/api/fragments`);
  },
  getFragment(key: string): Promise<FragmentDto> {
    return req<FragmentDto>(`/api/fragments/${encodeURIComponent(key)}`);
  },
  createFragment(body: { key: string; content: string }): Promise<FragmentDto> {
    return req<FragmentDto>(`/api/fragments`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
  },
  updateFragment(key: string, body: { content: string }): Promise<FragmentDto> {
    return req<FragmentDto>(`/api/fragments/${encodeURIComponent(key)}`, {
      method: "PATCH",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
  },
  deleteFragment(key: string): Promise<void> {
    return req<void>(`/api/fragments/${encodeURIComponent(key)}`, { method: "DELETE" });
  },
  listFragmentSources(): Promise<FragmentSourcesResponse> {
    return req<FragmentSourcesResponse>(`/api/fragments/_sources`);
  },

  // ---------------------------------------------------------------------
  // Knowledge bases — managed retrievers agents bind in backend.knowledge
  // ---------------------------------------------------------------------
  listKnowledgeBases(): Promise<KnowledgeBasesListResponse> {
    return req<KnowledgeBasesListResponse>(`/api/knowledge-bases`);
  },
  listKnowledgeSourceAdapters(): Promise<KnowledgeSourceAdaptersResponse> {
    return req<KnowledgeSourceAdaptersResponse>(`/api/knowledge-bases/_adapters`);
  },
  createKnowledgeBase(body: KnowledgeBaseCreateRequest): Promise<KnowledgeBase> {
    return req<KnowledgeBase>(`/api/knowledge-bases`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
  },
  updateKnowledgeBase(id: string, body: KnowledgeBasePatchRequest): Promise<KnowledgeBase> {
    return req<KnowledgeBase>(`/api/knowledge-bases/${encodeURIComponent(id)}`, {
      method: "PATCH",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
  },
  deleteKnowledgeBase(id: string, opts: { force?: boolean } = {}): Promise<void> {
    const qs = opts.force ? "?force=true" : "";
    return req<void>(`/api/knowledge-bases/${encodeURIComponent(id)}${qs}`, { method: "DELETE" });
  },
  listKnowledgeSources(id: string): Promise<KnowledgeSourcesResponse> {
    return req<KnowledgeSourcesResponse>(`/api/knowledge-bases/${encodeURIComponent(id)}/sources`);
  },
  ingestKnowledgeSource(
    id: string,
    body: KnowledgeSourceCreateRequest,
  ): Promise<KnowledgeSourceSummary> {
    return req<KnowledgeSourceSummary>(`/api/knowledge-bases/${encodeURIComponent(id)}/sources`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
  },
  importKnowledgeSources(
    id: string,
    body: KnowledgeSourceImportRequest,
  ): Promise<KnowledgeSourcesImportResponse> {
    return req<KnowledgeSourcesImportResponse>(
      `/api/knowledge-bases/${encodeURIComponent(id)}/sources/import`,
      {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(body),
      },
    );
  },
  reindexKnowledgeSource(id: string, sourceId: string): Promise<KnowledgeSourceSummary> {
    return req<KnowledgeSourceSummary>(
      `/api/knowledge-bases/${encodeURIComponent(id)}/sources/${encodeURIComponent(sourceId)}/reindex`,
      { method: "POST" },
    );
  },
  deleteKnowledgeSource(id: string, sourceId: string): Promise<void> {
    return req<void>(
      `/api/knowledge-bases/${encodeURIComponent(id)}/sources/${encodeURIComponent(sourceId)}`,
      { method: "DELETE" },
    );
  },
  listKnowledgeChunks(
    id: string,
    params: { sourceId?: string; limit?: number; offset?: number } = {},
  ): Promise<KnowledgeChunksResponse> {
    const qs = new URLSearchParams();
    if (params.sourceId !== undefined) qs.set("sourceId", params.sourceId);
    if (params.limit !== undefined) qs.set("limit", String(params.limit));
    if (params.offset !== undefined) qs.set("offset", String(params.offset));
    const suffix = qs.toString() ? `?${qs.toString()}` : "";
    return req<KnowledgeChunksResponse>(
      `/api/knowledge-bases/${encodeURIComponent(id)}/chunks${suffix}`,
    );
  },
  searchKnowledgeBase(id: string, body: KnowledgeSearchRequest): Promise<KnowledgeSearchResponse> {
    return req<KnowledgeSearchResponse>(`/api/knowledge-bases/${encodeURIComponent(id)}/search`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
  },

  // ---------------------------------------------------------------------
  // Workflow builder — workflows authored as JSON on the Builder page
  // ---------------------------------------------------------------------
  listBuilderActivities(): Promise<BuilderActivitiesResponse> {
    return req<BuilderActivitiesResponse>(`/api/workflow-builder/activities`);
  },
  validateBuilderWorkflow(schema: unknown): Promise<BuilderValidateResponse> {
    return req<BuilderValidateResponse>(`/api/workflow-builder/validate`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ schema }),
    });
  },
  listBuilderWorkflows(): Promise<BuilderWorkflowsResponse> {
    return req<BuilderWorkflowsResponse>(`/api/workflow-builder/workflows`);
  },
  getBuilderWorkflow(name: string): Promise<BuilderWorkflow> {
    return req<BuilderWorkflow>(`/api/workflow-builder/workflows/${encodeURIComponent(name)}`);
  },
  createBuilderWorkflow(schema: unknown): Promise<BuilderWorkflow> {
    return req<BuilderWorkflow>(`/api/workflow-builder/workflows`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ schema }),
    });
  },
  saveBuilderWorkflow(name: string, body: BuilderWorkflowSaveRequest): Promise<BuilderWorkflow> {
    return req<BuilderWorkflow>(`/api/workflow-builder/workflows/${encodeURIComponent(name)}`, {
      method: "PUT",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
  },
  publishBuilderWorkflow(
    name: string,
    body: BuilderWorkflowPublishRequest = {},
  ): Promise<PublishResult> {
    return req<PublishResult>(
      `/api/workflow-builder/workflows/${encodeURIComponent(name)}/publish`,
      {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(body),
      },
    );
  },
  deleteBuilderWorkflow(name: string): Promise<void> {
    return req<void>(`/api/workflow-builder/workflows/${encodeURIComponent(name)}`, {
      method: "DELETE",
    });
  },
  listSkillSources(): Promise<SkillSourcesResponse> {
    return req<SkillSourcesResponse>(`/api/skills/_sources`);
  },

  // ---------------------------------------------------------------------
  // Roles — registry CRUD (the behavioral bundle an agent binds)
  // ---------------------------------------------------------------------
  listRoles(): Promise<RolesListResponse> {
    return req<RolesListResponse>(`/api/roles`);
  },
  getRole(id: string, version?: string): Promise<RegisteredRole> {
    const qp = version ? `?version=${encodeURIComponent(version)}` : "";
    return req<RegisteredRole>(`/api/roles/${encodeURIComponent(id)}${qp}`);
  },
  listRoleVersions(id: string): Promise<RoleVersionsResponse> {
    return req<RoleVersionsResponse>(`/api/roles/${encodeURIComponent(id)}/versions`);
  },
  createRole(body: {
    id: string;
    version?: string;
    definition: RoleDefinition;
    metadata?: RegisteredRole["metadata"];
  }): Promise<RegisteredRole> {
    return req<RegisteredRole>(`/api/roles`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
  },
  updateRole(
    id: string,
    body: {
      version?: string;
      definition?: RoleDefinition;
      metadata?: RegisteredRole["metadata"];
    },
  ): Promise<RegisteredRole> {
    return req<RegisteredRole>(`/api/roles/${encodeURIComponent(id)}`, {
      method: "PATCH",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
  },
  deleteRole(id: string, version?: string): Promise<void> {
    const qp = version ? `?version=${encodeURIComponent(version)}` : "";
    return req<void>(`/api/roles/${encodeURIComponent(id)}${qp}`, { method: "DELETE" });
  },
  /** Lift an agent's inline role into the registry and rebind it to a ref. */
  extractRole(
    agentId: string,
    body: { roleId: string; roleVersion?: string; metadata?: RegisteredRole["metadata"] },
  ): Promise<{ role: RegisteredRole; agent: RegisteredAgent }> {
    return req(`/api/agents/${encodeURIComponent(agentId)}/extract-role`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
  },

  // Agentic DAG endpoints
  listDags(params?: { tag?: string }): Promise<{ dags: DagDto[] }> {
    const qp = new URLSearchParams();
    if (params?.tag) qp.set("tag", params.tag);
    const q = qp.toString();
    return req<{ dags: DagDto[] }>(`/api/dags${q ? `?${q}` : ""}`);
  },
  getDag(id: string, version?: string): Promise<DagDto> {
    const qp = version ? `?version=${encodeURIComponent(version)}` : "";
    return req<DagDto>(`/api/dags/${encodeURIComponent(id)}${qp}`);
  },
  listDagVersions(id: string): Promise<{ versions: DagDto[] }> {
    return req<{ versions: DagDto[] }>(`/api/dags/${encodeURIComponent(id)}/versions`);
  },
  runDag(
    id: string,
    body: {
      namespaceId: string;
      resourceId: string;
      version?: string;
      initialInput?: Record<string, unknown>;
      workflowId?: string;
    },
  ): Promise<DagRunStartedDto> {
    return req<DagRunStartedDto>(`/api/dags/${encodeURIComponent(id)}/run`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
  },
  getThreadTrace(
    id: string,
    threadId: string,
    params: { namespaceId: string; resourceId?: string },
  ): Promise<{ threadId: string; trace: AgentTraceDto }> {
    const qp = new URLSearchParams({ namespaceId: params.namespaceId });
    if (params.resourceId) qp.set("resourceId", params.resourceId);
    return req(
      `/api/agents/${encodeURIComponent(id)}/threads/${encodeURIComponent(threadId)}/trace?${qp}`,
    );
  },
  listAgentThreads(
    id: string,
    params: { namespaceId: string; resourceId?: string; limit?: number; q?: string },
  ): Promise<AgentThreadsResponse> {
    const qp = new URLSearchParams({ namespaceId: params.namespaceId });
    if (params.resourceId) qp.set("resourceId", params.resourceId);
    if (params.limit !== undefined) qp.set("limit", String(params.limit));
    if (params.q && params.q.length > 0) qp.set("q", params.q);
    return req<AgentThreadsResponse>(`/api/agents/${encodeURIComponent(id)}/threads?${qp}`);
  },
  renameAgentThread(
    id: string,
    threadId: string,
    body: { namespaceId: string; resourceId?: string; title: string | null },
  ): Promise<{ threadId: string; title: string | null; metadata: Record<string, unknown> }> {
    return req(`/api/agents/${encodeURIComponent(id)}/threads/${encodeURIComponent(threadId)}`, {
      method: "PATCH",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
  },
  archiveAgentThread(
    id: string,
    threadId: string,
    body: { namespaceId: string; resourceId?: string; archivedAt: number | null },
  ): Promise<{ threadId: string; archivedAt: number | null }> {
    return req(
      `/api/agents/${encodeURIComponent(id)}/threads/${encodeURIComponent(threadId)}/archive`,
      {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(body),
      },
    );
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
  /** Tool approvals the thread's turns are suspended on (the pending-approval index). */
  listAgentThreadApprovals(
    id: string,
    threadId: string,
    params: { namespaceId: string; resourceId?: string },
  ): Promise<ThreadApprovalsResponse> {
    const qp = new URLSearchParams({ namespaceId: params.namespaceId });
    if (params.resourceId) qp.set("resourceId", params.resourceId);
    return req<ThreadApprovalsResponse>(
      `/api/agents/${encodeURIComponent(id)}/threads/${encodeURIComponent(threadId)}/approvals?${qp}`,
    );
  },
  distillAgentThread(
    id: string,
    threadId: string,
    body: { namespaceId: string; resourceId: string; force?: boolean },
  ): Promise<{ episode: { id: string; summary: string; salience: number } }> {
    return req(
      `/api/agents/${encodeURIComponent(id)}/threads/${encodeURIComponent(threadId)}/distill`,
      {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(body),
      },
    );
  },
  compactAgentThread(
    id: string,
    threadId: string,
    body: { namespaceId: string; resourceId?: string; keepRecent?: number },
  ): Promise<{ episode: { id: string; summary: string; salience: number } }> {
    return req(
      `/api/agents/${encodeURIComponent(id)}/threads/${encodeURIComponent(threadId)}/compact`,
      {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(body),
      },
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
   * `onDelta` fires per text chunk; `onFinish` once at completion. A
   * dropped connection is resumed with `Last-Event-ID`, so no delta is
   * delivered twice. `abort()` only detaches: the turn keeps running on the
   * server (`cancelAgentThreadTurn` stops it).
   */
  streamAgentThread(
    id: string,
    threadId: string,
    body: { task: string; namespaceId: string; resourceId?: string },
    handlers: AgentStreamHandlers,
  ): { abort: () => void; done: Promise<void> } {
    const url = `${BASE}/api/agents/${encodeURIComponent(id)}/threads/${encodeURIComponent(threadId)}/stream`;
    return startSseStream({
      url,
      body,
      resumeUrl: threadStreamResumeUrl({ id, threadId, scope: body }),
      handlers,
    });
  },

  /**
   * POST /api/agents/:id/threads/:threadId/cancel — cancel the thread's
   * running turn. Resolves false when no turn runs (404).
   */
  async cancelAgentThreadTurn(
    id: string,
    threadId: string,
    body: { namespaceId: string; resourceId?: string },
  ): Promise<boolean> {
    const res = await fetch(
      `${BASE}/api/agents/${encodeURIComponent(id)}/threads/${encodeURIComponent(threadId)}/cancel`,
      {
        method: "POST",
        headers: { "content-type": "application/json", ...authHeader() },
        body: JSON.stringify(body),
      },
    );
    if (res.status === 404) return false;
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return true;
  },

  /**
   * Resume a thread that suspended on an approval gate. Streams the
   * resumed turn back over the same SSE protocol as `streamAgentThread`,
   * so the chat panel can splice the new deltas into the same
   * conversation.
   */
  streamThreadApproval(
    id: string,
    threadId: string,
    body: {
      toolCallId: string;
      approved: boolean;
      reason?: string;
      namespaceId: string;
      resourceId?: string;
    },
    handlers: AgentStreamHandlers,
  ): { abort: () => void; done: Promise<void> } {
    const url = `${BASE}/api/agents/${encodeURIComponent(id)}/threads/${encodeURIComponent(threadId)}/approve`;
    return startSseStream({
      url,
      body,
      resumeUrl: threadStreamResumeUrl({ id, threadId, scope: body }),
      handlers,
    });
  },
};

// ---------------------------------------------------------------------------
// Secrets API — scoped vault CRUD
// ---------------------------------------------------------------------------

// Wire shape for /api/agents/_catalog/tools. Keep loose-typed so the
// UI doesn't drag in @promin/agent at the boundary.
export type ToolCatalogSourceDto =
  | { kind: "in-process" }
  | { kind: "file"; path?: string }
  | { kind: "mcp"; server: string; tool: string; transport: "stdio" | "http" };

export interface ToolCatalogEntryDto {
  name: string;
  description: string;
  parameters: Record<string, unknown>;
  source: ToolCatalogSourceDto;
  enabled: boolean;
  /** Whether each call waits for a human approval. */
  requireApproval: boolean;
  /** Whether the engine may re-run the call after a crash. */
  idempotent: boolean;
  /** Secret-store keys the tool needs to run. Empty when none declared. */
  requiredSecrets: string[];
  /** True when the tool declares scoped memory. */
  usesMemory: boolean;
}

/** A tool source the catalog could not (fully) list, e.g. an unreachable MCP server. */
export interface ToolCatalogErrorDto {
  kind: "mcp";
  server: string;
  tool?: string;
  /** Stable reason code; the detail stays in the server log. */
  code: string;
  message: string;
}

/** Wire shape for /api/agents/_catalog/tools. */
export interface ToolCatalogDto {
  tools: ToolCatalogEntryDto[];
  errors: ToolCatalogErrorDto[];
}

/** Wire shape for /api/agents/_catalog/tools/health. */
export interface ToolCatalogHealthDto {
  recipes: Array<{
    recipeId: string;
    version: string;
    resolved: string[];
    missing: string[];
  }>;
  orphans: Array<{
    toolName: string;
    recipes: Array<{ id: string; version: string }>;
  }>;
}

/**
 * Wire shape for /api/agents/:id/threads/:threadId/trace.
 * Mirrors `AgentTrace` from @promin/agent — kept loose-typed at the
 * boundary so the UI doesn't drag the whole agent package in.
 */
export interface AgentTraceDto {
  turns: Array<{
    kind: "turn";
    turnIndex: number;
    fromSeq: number;
    toSeq: number;
    children: Array<TraceChildDto>;
    /** The engine's timed trace of the turn, when it recorded one. */
    timing?: TurnTimingDto;
  }>;
  orphanSystem: Array<{ kind: "system"; seq: number; content: string }>;
  summary: {
    turns: number;
    assistantMoves: number;
    toolCalls: number;
    toolFailures: number;
    orphanedToolCalls: number;
    orphanedToolResults: number;
  };
}
/** Token usage of a think step or a whole turn. */
export interface TraceUsageDto {
  inputTokens: number;
  outputTokens: number;
  cacheReadTokens?: number;
  cacheWriteTokens?: number;
}
export type TurnSpanDto =
  | { kind: "think"; step: number; startedAt: number; durationMs: number; usage?: TraceUsageDto }
  | {
      kind: "tool";
      step: number;
      name: string;
      toolCallId: string;
      failed: boolean;
      delegated?: true;
      startedAt?: number;
      durationMs?: number;
    }
  | {
      kind: "approval";
      step: number;
      toolCallId: string;
      toolName: string;
      approved: boolean;
      startedAt: number;
      durationMs: number;
    };
export interface TurnTimingDto {
  workflowId: string;
  startedAt: number;
  endedAt: number;
  durationMs: number;
  finishReason: string;
  steps: number;
  usage: TraceUsageDto;
  spans: TurnSpanDto[];
}
export type TraceChildDto =
  | { kind: "user"; seq: number; content: string; metadata?: Record<string, unknown> }
  | {
      kind: "assistant";
      seq: number;
      content: string | null;
      thinkingBlocks?: unknown[];
      toolCalls: Array<TraceToolCallDto>;
    }
  | TraceToolCallDto
  | { kind: "system"; seq: number; content: string };
export interface TraceToolCallDto {
  kind: "tool-call";
  id: string;
  name: string;
  input: unknown;
  callSeq: number;
  result?: { seq: number; content: string; failed: boolean };
  /**
   * Sub-agent run trace — present on `callAgent` tool calls. Lets the
   * graph view expand the call into the peer's turns. Recursive: a
   * peer that itself called `callAgent` carries its own `childTrace`.
   */
  childTrace?: AgentTraceDto;
}

// ---------------------------------------------------------------------------
// Agentic DAG wire shapes. Loose-typed at the boundary so
// the UI doesn't import @promin/agent.
// ---------------------------------------------------------------------------

export type DagNodeInputSourceDto =
  | { kind: "initial"; path: string }
  | { kind: "node"; nodeId: string; path: string }
  | { kind: "literal"; value: unknown };

export interface DagNodeDto {
  id: string;
  agentId: string;
  inputs: Record<string, DagNodeInputSourceDto>;
  outputPath?: string;
  onError?: "abort" | "skip";
}

export interface DagEdgeDto {
  from: string;
  to: string;
  condition?: { kind: "equals"; path: string; value: string };
}

export interface DagDto {
  id: string;
  version: string;
  nodes: DagNodeDto[];
  edges: DagEdgeDto[];
  entry: string[];
  terminals: string[];
  metadata?: {
    description?: string;
    tags?: string[];
  };
  createdAt: number;
  updatedAt: number;
}

/** Answer of POST /api/dags/:id/run: the run it started (or found under `workflowId`). */
export interface DagRunStartedDto {
  workflowId: string;
  existing?: boolean;
}

/** A finished DAG run: its id and the DAG workflow's result. */
export interface DagRunResultDto {
  workflowId: string;
  result: {
    outputs: Record<string, unknown>;
    ok: boolean;
    nodeOutputs: Record<string, unknown>;
    errors: Record<string, string>;
    skipped: string[];
  };
}

/** Wire shape for /api/agents/_catalog/models — mirrors SerializedModelCatalogItem. */
export interface ModelCatalogEntryDto {
  provider: string;
  id: string;
  displayName?: string;
  contextLimit?: number;
  capabilities?: string[];
  costTier?: string;
}

/** Wire shape for /api/agents/_catalog/retrievers — retriever metadata, never the retriever. */
export type RetrieverCatalogEntryDto = RetrieverCatalogEntry;

export type SecretScopeWire =
  | { kind: "global" }
  | { kind: "namespace"; namespaceId: string }
  | { kind: "resource"; namespaceId: string; resourceId: string };

export interface SecretsListResponse {
  scope: SecretScopeWire;
  keys: string[];
}

export interface SecretsCreateResponse {
  scope: SecretScopeWire;
  key: string;
}

function scopeToQuery(scope: SecretScopeWire): string {
  const qp = new URLSearchParams();
  qp.set("scope", scope.kind);
  if (scope.kind !== "global") qp.set("namespaceId", scope.namespaceId);
  if (scope.kind === "resource") qp.set("resourceId", scope.resourceId);
  return qp.toString();
}

export const secretsApi = {
  list(scope: SecretScopeWire): Promise<SecretsListResponse> {
    return req<SecretsListResponse>(`/api/secrets?${scopeToQuery(scope)}`);
  },
  create(body: {
    scope: SecretScopeWire;
    key: string;
    value: string;
  }): Promise<SecretsCreateResponse> {
    return req<SecretsCreateResponse>(`/api/secrets`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
  },
  delete(scope: SecretScopeWire, key: string): Promise<void> {
    return req<void>(`/api/secrets/${encodeURIComponent(key)}?${scopeToQuery(scope)}`, {
      method: "DELETE",
    });
  },
};

export interface AgentStreamHandlers {
  onThread?: (info: {
    threadId: string;
    isNew: boolean;
    instanceId?: string;
    /** The turn's id: event ids are `<turnId>:<seq>`. */
    turnId?: string;
  }) => void;
  onDelta: (delta: string) => void;
  /** Workflow suspended on an approval gate — banner should render. */
  onApprovalRequested?: (info: { toolCallId: string; toolName: string; input?: unknown }) => void;
  /** Convenience: equivalent to `onSuspended` but typed for the resume side. */
  onSuspended?: (info: { toolCallId?: string; toolName?: string }) => void;
  onFinish?: (info: {
    text: string;
    finishReason: string;
    usage: {
      inputTokens: number;
      outputTokens: number;
      cacheReadTokens?: number;
      cacheWriteTokens?: number;
    };
  }) => void;
  onError?: (message: string) => void;
}

/** `GET .../threads/:threadId/stream` — where a dropped turn stream is resumed. */
function threadStreamResumeUrl(params: {
  id: string;
  threadId: string;
  scope: { namespaceId: string; resourceId?: string };
}): string {
  const qp = new URLSearchParams({ namespaceId: params.scope.namespaceId });
  if (params.scope.resourceId) qp.set("resourceId", params.scope.resourceId);
  return `${BASE}/api/agents/${encodeURIComponent(params.id)}/threads/${encodeURIComponent(params.threadId)}/stream?${qp}`;
}

/** Turn-ending events: the stream is complete once one arrives. */
const TERMINAL_STREAM_EVENTS = new Set(["finish", "suspended", "error"]);

/** Deliver one agent stream message to its handler. */
export function dispatchAgentStreamMessage(params: {
  message: SseMessage;
  handlers: AgentStreamHandlers;
}): void {
  const { message, handlers } = params;
  let payload: Record<string, unknown> | null;
  try {
    payload = JSON.parse(message.data) as Record<string, unknown> | null;
  } catch {
    return; // Skip malformed frames.
  }
  const event = message.event;
  if (event === "thread") {
    handlers.onThread?.(payload as Parameters<NonNullable<AgentStreamHandlers["onThread"]>>[0]);
  } else if (event === "finish") {
    handlers.onFinish?.(payload as Parameters<NonNullable<AgentStreamHandlers["onFinish"]>>[0]);
  } else if (event === "error") {
    handlers.onError?.(String(payload?.message ?? payload?.error ?? "stream error"));
  } else if (event === "approval-requested") {
    handlers.onApprovalRequested?.(
      payload as Parameters<NonNullable<AgentStreamHandlers["onApprovalRequested"]>>[0],
    );
  } else if (event === "suspended") {
    handlers.onSuspended?.(
      payload as Parameters<NonNullable<AgentStreamHandlers["onSuspended"]>>[0],
    );
  } else if (typeof payload?.delta === "string") {
    handlers.onDelta(payload.delta);
  }
}

function startSseStream<TBody>(params: {
  url: string;
  body: TBody;
  /** Where the stream is resumed with `Last-Event-ID` after a network blip. */
  resumeUrl: string;
  handlers: AgentStreamHandlers;
}): { abort: () => void; done: Promise<void> } {
  const { handlers } = params;
  const ctrl = new AbortController();

  const done = (async () => {
    try {
      const outcome = await readResumableSse({
        start: {
          url: params.url,
          init: {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify(params.body),
          },
        },
        resumeUrl: params.resumeUrl,
        headers: authHeader,
        signal: ctrl.signal,
        isTerminal: (message) => TERMINAL_STREAM_EVENTS.has(message.event),
        onMessage: (message) => dispatchAgentStreamMessage({ message, handlers }),
      });
      // Gave up resuming: the turn may still finish on the server, and the
      // thread's messages will show it.
      if (outcome === "lost") handlers.onError?.("connection lost; reload the thread");
    } catch (e) {
      if ((e as Error).name === "AbortError") return;
      handlers.onError?.(e instanceof SseHttpError ? e.message : (e as Error).message);
    }
  })();

  return { abort: () => ctrl.abort(), done };
}

// ---------------------------------------------------------------------------
// Memory inspector — read-only snapshot of the three-scope cascade.
// Lives outside the `api` object literal because it was added after the
// agent block was sealed by the SSE helpers below; same `req` / authHeader
// helpers used inside the literal.
// ---------------------------------------------------------------------------
export const memoryApi = {
  inspect(params: {
    namespaceId: string;
    resourceId?: string;
    threadId?: string;
    agentId?: string;
  }): Promise<MemoryInspectResponse> {
    const qp = new URLSearchParams({ namespaceId: params.namespaceId });
    if (params.resourceId) qp.set("resourceId", params.resourceId);
    if (params.threadId) qp.set("threadId", params.threadId);
    if (params.agentId) qp.set("agentId", params.agentId);
    return req<MemoryInspectResponse>(`/api/memory/inspect?${qp}`);
  },
  patchNamespace(
    namespaceId: string,
    patch: { staticRules?: string | null; workingMemory?: string | null },
  ): Promise<{ row: unknown }> {
    return req(`/api/memory/namespace/${encodeURIComponent(namespaceId)}`, {
      method: "PATCH",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(patch),
    });
  },
  addNamespaceFact(namespaceId: string, text: string): Promise<{ fact: unknown }> {
    return req(`/api/memory/namespace/${encodeURIComponent(namespaceId)}/facts`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ text }),
    });
  },
  deleteNamespaceFact(namespaceId: string, factId: string): Promise<{ ok: true }> {
    return req(
      `/api/memory/namespace/${encodeURIComponent(namespaceId)}/facts/${encodeURIComponent(factId)}`,
      { method: "DELETE" },
    );
  },
};

// ---------------------------------------------------------------------------
// Agent instances — long-lived per-(agent, namespace, owner) records.
// `ownerId` is opaque (user, team, project, device, ...).
// ---------------------------------------------------------------------------

export interface AgentInstanceDto {
  id: string;
  registeredAgentId: string;
  namespaceId: string;
  ownerId: string;
  displayName: string | null;
  metadata: Record<string, unknown>;
  createdAt: number;
}

export const instancesApi = {
  list(
    params: {
      namespaceId?: string;
      ownerId?: string;
      limit?: number;
    } = {},
  ): Promise<{ instances: AgentInstanceDto[] }> {
    const qp = new URLSearchParams();
    if (params.namespaceId) qp.set("namespaceId", params.namespaceId);
    if (params.ownerId) qp.set("ownerId", params.ownerId);
    if (params.limit !== undefined) qp.set("limit", String(params.limit));
    const qs = qp.toString();
    return req(`/api/instances${qs ? `?${qs}` : ""}`);
  },
  listForAgent(
    agentId: string,
    params: { namespaceId?: string; ownerId?: string; limit?: number } = {},
  ): Promise<{ instances: AgentInstanceDto[] }> {
    const qp = new URLSearchParams();
    if (params.namespaceId) qp.set("namespaceId", params.namespaceId);
    if (params.ownerId) qp.set("ownerId", params.ownerId);
    if (params.limit !== undefined) qp.set("limit", String(params.limit));
    const qs = qp.toString();
    return req(`/api/agents/${encodeURIComponent(agentId)}/instances${qs ? `?${qs}` : ""}`);
  },
  rename(
    agentId: string,
    instanceId: string,
    displayName: string | null,
  ): Promise<{ instance: AgentInstanceDto }> {
    return req(
      `/api/agents/${encodeURIComponent(agentId)}/instances/${encodeURIComponent(instanceId)}`,
      {
        method: "PATCH",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ displayName }),
      },
    );
  },
  resolveOrCreate(
    agentId: string,
    body: { namespaceId: string; ownerId: string; displayName?: string },
  ): Promise<{ instance: AgentInstanceDto }> {
    return req(`/api/agents/${encodeURIComponent(agentId)}/instances`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
  },
  wipe(
    agentId: string,
    instanceId: string,
  ): Promise<{
    instanceId: string;
    threadsDeleted: number;
    factsDeleted: number;
    episodesDeleted: number;
  }> {
    return req(
      `/api/agents/${encodeURIComponent(agentId)}/instances/${encodeURIComponent(instanceId)}`,
      { method: "DELETE" },
    );
  },
};
