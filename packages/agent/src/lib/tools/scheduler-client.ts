// ---------------------------------------------------------------------------
// SchedulerClient — the agent's view of "schedule a thing on the server".
// Decouples the scheduler tool from any specific storage backend so the
// same tool works whether the agent runs in-process with the storage
// (demo, single-pod) or in a separate worker that talks to the server
// over HTTP.
//
// Two ships:
//   - `inProcessSchedulerClient(storage, scope)` — wraps SchedulerStorage
//     directly. For demos / single-process REPLs / tests.
//   - `httpSchedulerClient({ baseUrl, ... })` — POSTs / GETs / DELETEs
//     against the existing `/api/schedules` routes in the server.
//
// The tool consumes the interface; consumers pick which impl to wire.
// ---------------------------------------------------------------------------

import type { DurableScheduleConfig, SchedulerStorage } from "@promin/workflow";

/** Minimal scope label every client carries — used to gate ownership. */
export interface SchedulerClientScope {
  readonly namespaceId: string;
  readonly resourceId?: string;
  readonly threadId?: string;
  /** Recipe id of the agent the tool is bound to. */
  readonly agentId: string;
}

export interface SchedulerCreateInput {
  readonly id: string;
  readonly name?: string;
  readonly cron?: string;
  readonly intervalMs?: number;
  readonly rrule?: string;
  readonly timezone?: string;
  readonly startAt?: Date;
  readonly endAt?: Date;
  readonly metadata: Readonly<Record<string, unknown>>;
}

export interface SchedulerSummary {
  readonly id: string;
  readonly name: string | null;
  readonly cron: string | null;
  readonly intervalMs: number | null;
  readonly rrule: string | null;
  readonly enabled: boolean;
  readonly metadata: Readonly<Record<string, unknown>>;
}

export interface SchedulerClient {
  /**
   * Persist a new schedule. Implementations validate ownership +
   * tenant boundary at the boundary (the in-process client trusts the
   * tool; the HTTP client gets server-side enforcement).
   */
  create(input: SchedulerCreateInput): Promise<{ id: string }>;
  /**
   * Return schedules that belong to this client's scope. Filtering is
   * the implementation's responsibility — the in-process client does
   * a metadata-based filter; the HTTP client relies on the server's
   * tenant + thread checks.
   */
  list(): Promise<SchedulerSummary[]>;
  /**
   * Delete a schedule. Returns the deleted id on success. Implementations
   * MUST refuse to cancel a schedule that doesn't belong to this scope.
   */
  cancel(id: string): Promise<{ ok: true } | { ok: false; error: string }>;
}

// ---------------------------------------------------------------------------
// In-process client — wraps SchedulerStorage directly. Used by demos and
// any deployment where the agent runtime + storage live in one process.
// ---------------------------------------------------------------------------

export interface InProcessSchedulerClientConfig {
  readonly storage: SchedulerStorage;
  readonly scope: SchedulerClientScope;
}

export function inProcessSchedulerClient(config: InProcessSchedulerClientConfig): SchedulerClient {
  const { storage, scope } = config;
  return {
    async create(input) {
      const dsConfig: DurableScheduleConfig = {
        id: input.id,
        ...(input.name !== undefined && { name: input.name }),
        namespace: scope.namespaceId,
        ...(input.cron !== undefined && { cron: input.cron }),
        ...(input.intervalMs !== undefined && { intervalMs: input.intervalMs }),
        ...(input.rrule !== undefined && { rrule: input.rrule }),
        ...(input.timezone !== undefined && { timezone: input.timezone }),
        ...(input.startAt !== undefined && { startAt: input.startAt }),
        ...(input.endAt !== undefined && { endAt: input.endAt }),
        enabled: true,
        metadata: input.metadata,
      };
      await storage.upsertSchedule(dsConfig);
      return { id: input.id };
    },

    async list() {
      const all = await storage.listSchedules({
        namespace: scope.namespaceId,
        limit: 1000,
      });
      return all
        .filter((s) => belongsToScope(s, scope))
        .map((s) => ({
          id: s.id,
          name: s.name ?? null,
          cron: s.cron ?? null,
          intervalMs: s.intervalMs ?? null,
          rrule: s.rrule ?? null,
          enabled: s.enabled !== false,
          metadata: s.metadata ?? {},
        }));
    },

    async cancel(id) {
      const found = await storage.loadSchedule(id);
      if (!found) return { ok: false, error: `no schedule with id "${id}"` };
      if (!belongsToScope(found, scope)) {
        return { ok: false, error: `schedule "${id}" doesn't belong to this thread` };
      }
      await storage.deleteSchedule(id);
      return { ok: true };
    },
  };
}

function belongsToScope(s: DurableScheduleConfig, scope: SchedulerClientScope): boolean {
  if (s.namespace !== scope.namespaceId) return false;
  if (scope.threadId !== undefined && s.metadata?.threadId !== scope.threadId) return false;
  return true;
}

// ---------------------------------------------------------------------------
// HTTP client — hits the server's existing `/api/schedules` routes. Used
// by agent workers running in a separate process from the server.
//
// Wire shape matches the existing ScheduleCreateRequest body and the
// SchedulesResponse shape — anything the dashboard's UI client uses,
// this client uses too.
// ---------------------------------------------------------------------------

export interface HttpSchedulerClientConfig {
  readonly baseUrl: string;
  readonly scope: SchedulerClientScope;
  /**
   * Optional fetch override (testing, custom auth). Defaults to global
   * fetch. Same shape as `globalThis.fetch`.
   */
  readonly fetch?: typeof globalThis.fetch;
  /** Authorization header builder. Common: bearer token, mTLS-friendly headers. */
  readonly authHeader?: () => Record<string, string>;
}

interface HttpSchedulesResponse {
  schedules: Array<{
    id: string;
    name?: string;
    namespace?: string;
    cron?: string;
    intervalMs?: number;
    rrule?: string;
    enabled?: boolean;
    metadata?: Record<string, unknown>;
  }>;
  total: number;
}

export function httpSchedulerClient(config: HttpSchedulerClientConfig): SchedulerClient {
  const fetchImpl = config.fetch ?? globalThis.fetch;
  const headers = (extra: Record<string, string> = {}): Record<string, string> => ({
    "content-type": "application/json",
    ...config.authHeader?.(),
    ...extra,
  });
  const url = (path: string): string => `${config.baseUrl.replace(/\/$/, "")}${path}`;

  return {
    async create(input) {
      const body = {
        id: input.id,
        ...(input.name !== undefined && { name: input.name }),
        namespace: config.scope.namespaceId,
        ...(input.cron !== undefined && { cron: input.cron }),
        ...(input.intervalMs !== undefined && { intervalMs: input.intervalMs }),
        ...(input.rrule !== undefined && { rrule: input.rrule }),
        ...(input.timezone !== undefined && { timezone: input.timezone }),
        ...(input.startAt !== undefined && { startAt: input.startAt.toISOString() }),
        ...(input.endAt !== undefined && { endAt: input.endAt.toISOString() }),
        enabled: true,
        metadata: input.metadata,
      };
      const res = await fetchImpl(url("/api/schedules"), {
        method: "POST",
        headers: headers(),
        body: JSON.stringify(body),
      });
      if (!res.ok) {
        throw new Error(`POST /api/schedules failed: ${res.status} ${await res.text()}`);
      }
      const dto = (await res.json()) as { id: string };
      return { id: dto.id };
    },

    async list() {
      const qp = new URLSearchParams({ namespace: config.scope.namespaceId });
      const res = await fetchImpl(url(`/api/schedules?${qp}`), {
        method: "GET",
        headers: headers(),
      });
      if (!res.ok) {
        throw new Error(`GET /api/schedules failed: ${res.status} ${await res.text()}`);
      }
      const body = (await res.json()) as HttpSchedulesResponse;
      // Server doesn't filter by thread — we mirror the in-process
      // client's filter at the wire boundary so the agent only sees
      // its own schedules. Server-side filtering is a follow-up if
      // the response volume becomes a concern.
      return body.schedules
        .filter((s) => {
          if (config.scope.threadId === undefined) return true;
          return s.metadata?.threadId === config.scope.threadId;
        })
        .map((s) => ({
          id: s.id,
          name: s.name ?? null,
          cron: s.cron ?? null,
          intervalMs: s.intervalMs ?? null,
          rrule: s.rrule ?? null,
          enabled: s.enabled ?? true,
          metadata: s.metadata ?? {},
        }));
    },

    async cancel(id) {
      // Defense-in-depth: confirm the schedule belongs to this scope
      // before issuing the DELETE. The server doesn't enforce thread
      // ownership today (operator UI deletes any schedule by id), so
      // the client gates this. Server-side enforcement is a follow-up.
      const own = await this.list();
      if (!own.some((s) => s.id === id)) {
        return { ok: false, error: `schedule "${id}" doesn't belong to this thread` };
      }
      const res = await fetchImpl(url(`/api/schedules/${encodeURIComponent(id)}`), {
        method: "DELETE",
        headers: headers(),
      });
      if (!res.ok) {
        if (res.status === 404) return { ok: false, error: `no schedule with id "${id}"` };
        throw new Error(`DELETE failed: ${res.status} ${await res.text()}`);
      }
      return { ok: true };
    },
  };
}
