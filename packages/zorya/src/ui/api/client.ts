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
  AgentSourcesResponse,
  AgentThreadsResponse,
  RegisteredAgent,
  ThreadInvokeResponse,
  ThreadMessagesResponse,
} from "../../server/routes/agents.ts";
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
  SkillsCatalogResponse,
} from "../../server/routes/agent-catalog.ts";
import type { MemoryInspectResponse } from "../../server/routes/memory.ts";
import type { SignalsResponse } from "../../server/routes/signals.ts";
import type {
  DescribeSignalTokenResponse,
  MintTokenResponse,
} from "../../server/routes/signal-tokens.ts";
import type { WorkflowVersionDto } from "../../server/routes/workflow-versions.ts";

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
  /** Deliver a signal payload to a suspended workflow. */
  sendSignal(workflowId: string, signalName: string, payload: unknown): Promise<{ ok: true }> {
    return req(`/api/runs/${encodeURIComponent(workflowId)}/signal`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ signalName, payload }),
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
   * Returns the resume status (201 first delivery, 200 idempotent
   * re-submission, 410 already consumed, 408 expired).
   */
  completeSignalToken(
    tokenId: string,
    bearer: string,
    value: unknown,
  ): Promise<{ ok: boolean; value?: unknown; alreadyCompleted?: boolean }> {
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
  // Recipe CRUD — gsze Phase 2 (Designer UI wiring)
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
  listAgentVersions(id: string): Promise<{ versions: RegisteredAgent[] }> {
    return req<{ versions: RegisteredAgent[] }>(`/api/agents/${encodeURIComponent(id)}/versions`);
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
  listCatalogTools(): Promise<{ tools: ToolCatalogEntryDto[] }> {
    return req<{ tools: ToolCatalogEntryDto[] }>(`/api/agents/_catalog/tools`);
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

  // Agentic DAG endpoints (promin-li95)
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
    body: { version?: string; initialInput?: Record<string, unknown>; workflowId?: string },
  ): Promise<DagRunResultDto> {
    return req<DagRunResultDto>(`/api/dags/${encodeURIComponent(id)}/run`, {
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
   * `onDelta` fires per text chunk; `onFinish` once at completion.
   * Returns an AbortController so callers can cancel mid-stream.
   */
  streamAgentThread(
    id: string,
    threadId: string,
    body: { task: string; namespaceId: string; resourceId?: string },
    handlers: AgentStreamHandlers,
  ): { abort: () => void; done: Promise<void> } {
    const url = `${BASE}/api/agents/${encodeURIComponent(id)}/threads/${encodeURIComponent(threadId)}/stream`;
    return startSseStream(url, body, handlers);
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
    return startSseStream(url, body, handlers);
  },
};

// ---------------------------------------------------------------------------
// Secrets API — scoped vault CRUD (h1st Phase 2/3)
// ---------------------------------------------------------------------------

// Wire shape for /api/agents/_catalog/tools. Keep loose-typed so the
// UI doesn't drag in @promin/agent at the boundary.
export type ToolCatalogSourceDto =
  | { kind: "in-process" }
  | { kind: "file"; path?: string }
  | { kind: "mcp"; server: string };

export interface ToolCatalogEntryDto {
  name: string;
  description: string;
  parameters: Record<string, unknown>;
  source: ToolCatalogSourceDto;
  enabled: boolean;
  /** Secret-store keys the tool needs to run. Empty when none declared. */
  requiredSecrets: string[];
  /** True when the tool declares scoped memory. */
  usesMemory: boolean;
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
// Agentic DAG wire shapes (promin-li95). Loose-typed at the boundary so
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
  onThread?: (info: { threadId: string; isNew: boolean; instanceId?: string }) => void;
  onDelta: (delta: string) => void;
  /** Workflow suspended on an approval gate — banner should render. */
  onApprovalRequested?: (info: { toolCallId: string; toolName: string }) => void;
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

function startSseStream<TBody>(
  url: string,
  body: TBody,
  handlers: AgentStreamHandlers,
): { abort: () => void; done: Promise<void> } {
  const ctrl = new AbortController();

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
            } else if (event === "approval-requested") {
              handlers.onApprovalRequested?.(payload);
            } else if (event === "suspended") {
              handlers.onSuspended?.(payload);
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
