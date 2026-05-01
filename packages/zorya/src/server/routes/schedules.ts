// ---------------------------------------------------------------------------
// Schedules route — /api/schedules
//
// Exposes a pluggable SchedulerStorage (from @promin/workflow) behind a thin
// DTO layer. Zorya itself doesn't own scheduler state — the embedder passes
// their InMemorySchedulerStorage / PgSchedulerStorage / etc.
// ---------------------------------------------------------------------------

import type { DurableScheduleConfig, SchedulerStorage, WorkflowStorage } from "@promin/workflow";
import { computeNextRun, isTickLogStorage } from "@promin/workflow";
import type { Clock } from "@promin/core";
import { json, jsonError, readJson } from "../router.ts";

export interface ScheduleDto {
  id: string;
  name?: string;
  namespace?: string;
  cron?: string;
  rrule?: string;
  intervalMs?: number;
  timezone?: string;
  enabled: boolean;
  startAt?: string;
  endAt?: string;
  jitterMs?: number;
  metadata?: Record<string, unknown>;
  overlapPolicy?: "skip" | "queue" | "cancel_previous" | "allow";
  maxCatchUp?: number;
  /** ISO timestamp of last fire. */
  lastFiredAt?: string;
  /** Running fire counter. */
  tickCount?: number;
  /** ISO timestamp of the next scheduled fire. Null when no next run. */
  nextRunAt?: string;
}

export interface SchedulesResponse {
  schedules: ScheduleDto[];
  total: number;
}

export interface ScheduleTickHistoryDto {
  /** Tick number parsed from `${scheduleId}.${tickNumber}`; undefined if id doesn't fit the pattern. */
  tickNumber?: number;
  workflowId: string;
  workflowName: string;
  status: string;
  /**
   * What kind of execution this tick produced.
   * - `"workflow"` (default): a normal workflow run; `status` reflects the
   *   workflow row's lifecycle.
   * - `"agent"`: dispatched directly to an in-process agent via
   *   `dispatchAgentSchedule`; there is no workflow row, so `status` is
   *   `"completed"` (tick fired) and the row's lag/duration columns are
   *   not meaningful. UI renders an agent-specific badge.
   */
  kind?: "workflow" | "agent";
  /** Wall-clock fire time recorded by the dispatcher (ISO). */
  firedAt?: string;
  /** Nominal scheduled time (cron / interval-derived) — may differ from firedAt under jitter. */
  scheduledAt?: string;
  startedAt?: string;
  completedAt?: string;
  /**
   * Dispatch lag: `startedAt - firedAt`. How long after the scheduler fired
   * the trigger did a worker actually start the run. Captures queue wait +
   * worker pickup time. Undefined when the run hasn't started yet, or when
   * the workflow row was reused from a prior fire (`startedAt` < `firedAt`).
   */
  lagMs?: number;
  /**
   * Run duration: `completedAt - firedAt`. End-to-end elapsed since the
   * fire. Undefined while the run is still in progress.
   */
  durationMs?: number;
  namespace?: string;
}

export interface ScheduleHistoryResponse {
  history: ScheduleTickHistoryDto[];
  total: number;
}

export interface ScheduleUpcomingTickDto {
  tickNumber: number;
  scheduledAt: string;
}

export interface ScheduleUpcomingResponse {
  upcoming: ScheduleUpcomingTickDto[];
  /** Cron / RRULE / interval already played out — no further fires. */
  exhausted: boolean;
}

export interface SchedulePatchRequest {
  enabled?: boolean;
}

export interface ScheduleCreateRequest {
  id: string;
  name?: string;
  namespace?: string;
  cron?: string;
  rrule?: string;
  intervalMs?: number;
  timezone?: string;
  enabled?: boolean;
  startAt?: string;
  endAt?: string;
  jitterMs?: number;
  overlapPolicy?: "skip" | "queue" | "cancel_previous" | "allow";
  maxCatchUp?: number;
  /** Optional workflow name to link. Stored in metadata.workflowName. */
  workflowName?: string;
  /** Optional input payload for triggered runs. Stored in metadata.input. */
  input?: unknown;
  metadata?: Record<string, unknown>;
}

function iso(d?: Date | null): string | undefined {
  return d ? d.toISOString() : undefined;
}

function fakeClockAt(when: Date): Clock {
  return {
    currentTimeMs: () => when.getTime(),
    now: () => new Date(when.getTime()),
    setTimeout: (fn, ms) => {
      const h = setTimeout(fn, ms);
      return { clear: () => clearTimeout(h) };
    },
    setInterval: (fn, ms) => {
      const h = setInterval(fn, ms);
      return { clear: () => clearInterval(h) };
    },
  };
}

function nextRunIso(config: DurableScheduleConfig): string | undefined {
  if (!(config.enabled ?? true)) return undefined;
  try {
    const next = computeNextRun(config, fakeClockAt(new Date()));
    return next ? next.toISOString() : undefined;
  } catch {
    return undefined;
  }
}

function toDto(
  config: DurableScheduleConfig,
  state?: { lastFired: Date | null; tickCount: number } | null,
): ScheduleDto {
  return {
    id: config.id,
    name: config.name,
    namespace: config.namespace,
    cron: config.cron,
    rrule: config.rrule,
    intervalMs: config.intervalMs,
    timezone: config.timezone,
    enabled: config.enabled ?? true,
    startAt: iso(config.startAt),
    endAt: iso(config.endAt),
    jitterMs: config.jitterMs,
    metadata: config.metadata,
    overlapPolicy: config.overlapPolicy,
    maxCatchUp: config.maxCatchUp,
    lastFiredAt: state?.lastFired ? iso(state.lastFired) : undefined,
    tickCount: state?.tickCount,
    nextRunAt: nextRunIso(config),
  };
}

export function listSchedules(storage: SchedulerStorage) {
  return async (req: Request): Promise<Response> => {
    const url = new URL(req.url);
    const enabled = parseBool(url.searchParams.get("enabled"));
    const namespace = url.searchParams.get("namespace") ?? undefined;
    const limit = parseInt10(url.searchParams.get("limit")) ?? 100;
    const offset = parseInt10(url.searchParams.get("offset")) ?? 0;
    // Metadata filter — JSON-encoded for nested-path queries (the chat
    // per-thread drawer sends `{ agentTrigger: true, threadId: "..." }`,
    // the dashboard's "kind = agent" chip sends `{ agentTrigger: true }`).
    // Containment semantics ride through to the storage layer; backends
    // with native JSON support push it to the database.
    const metadataRaw = url.searchParams.get("metadata");
    let metadata: Record<string, unknown> | undefined;
    if (metadataRaw) {
      try {
        const parsed = JSON.parse(metadataRaw) as unknown;
        if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
          metadata = parsed as Record<string, unknown>;
        }
      } catch {
        return jsonError(400, "invalid_metadata", "metadata must be JSON-encoded object");
      }
    }

    const [configs, total] = await Promise.all([
      storage.listSchedules({ enabled, namespace, limit, offset, metadata }),
      storage.countSchedules({ enabled, namespace, metadata }),
    ]);

    const ids = configs.map((c) => c.id);
    const states = ids.length > 0 ? await storage.loadScheduleStates(ids) : new Map();
    const schedules = configs.map((c) => toDto(c, states.get(c.id) ?? null));
    const response: SchedulesResponse = { schedules, total };
    return json(200, response);
  };
}

export function getSchedule(storage: SchedulerStorage) {
  return async (_req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");
    const config = await storage.loadSchedule(id);
    if (!config) return jsonError(404, "not_found");
    const state = await storage.loadScheduleState(id);
    return json(200, toDto(config, state));
  };
}

export function createSchedule(storage: SchedulerStorage) {
  return async (req: Request): Promise<Response> => {
    const body = await readJson<ScheduleCreateRequest>(req);
    if (!body || !body.id) return jsonError(400, "missing_id");

    const triggerCount =
      (body.cron ? 1 : 0) + (body.rrule ? 1 : 0) + (body.intervalMs !== undefined ? 1 : 0);
    if (triggerCount !== 1) {
      return jsonError(
        400,
        "invalid_trigger",
        "Exactly one of cron, rrule, intervalMs is required",
      );
    }

    const metadata: Record<string, unknown> = { ...(body.metadata ?? {}) };
    if (body.workflowName) metadata.workflowName = body.workflowName;
    if (body.input !== undefined) metadata.input = body.input;

    const config: DurableScheduleConfig = {
      id: body.id,
      name: body.name,
      namespace: body.namespace,
      cron: body.cron,
      rrule: body.rrule,
      intervalMs: body.intervalMs,
      timezone: body.timezone,
      enabled: body.enabled ?? true,
      startAt: body.startAt ? new Date(body.startAt) : undefined,
      endAt: body.endAt ? new Date(body.endAt) : undefined,
      jitterMs: body.jitterMs,
      metadata: Object.keys(metadata).length > 0 ? metadata : undefined,
      overlapPolicy: body.overlapPolicy,
      maxCatchUp: body.maxCatchUp,
    };

    try {
      await storage.upsertSchedule(config);
      // `upsertSchedule` writes config columns only — `next_run` stays
      // NULL on insert, which makes the row invisible to `findDue` and
      // the SchedulerLoop never fires it. Match
      // `DurableScheduler.registerAsync`'s pattern: seed nextRun = now
      // so the first poll picks it up (computeDueTicks fires one boot
      // tick at `now` when `lastFired` is null, then commitPoll advances
      // nextRun onto the natural cron / interval cadence).
      await storage.setNextRun(config.id, new Date());
      const state = await storage.loadScheduleState(config.id);
      return json(200, toDto(config, state));
    } catch (err) {
      return jsonError(400, "upsert_failed", err instanceof Error ? err.message : String(err));
    }
  };
}

export function patchSchedule(storage: SchedulerStorage) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");
    const body = await readJson<SchedulePatchRequest>(req);
    if (!body) return jsonError(400, "invalid_body");
    if (typeof body.enabled === "boolean") {
      await storage.setEnabled(id, body.enabled);
    }
    const config = await storage.loadSchedule(id);
    if (!config) return jsonError(404, "not_found");
    const state = await storage.loadScheduleState(id);
    return json(200, toDto(config, state));
  };
}

export function deleteSchedule(storage: SchedulerStorage) {
  return async (_req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");
    await storage.deleteSchedule(id);
    return json(200, { ok: true });
  };
}

// ---------------------------------------------------------------------------
// Emit now — fires a schedule on the next poll cycle by setting its
// `nextRun` to now. Goes through the same dispatch path as a normal
// firing — same workflowId convention, same tick log, same metadata
// stamping. Useful for testing without waiting on cron / interval.
//
// We deliberately don't bypass the SchedulerLoop and call the trigger
// directly: that would skip leader election, skip commitPoll's atomic
// state advance, and break the invariant that tickCount === count of
// logged ticks. One unified path keeps the model coherent.
// ---------------------------------------------------------------------------

export function emitScheduleNow(
  storage: SchedulerStorage,
  fire: ((scheduleId: string) => Promise<unknown>) | undefined,
) {
  return async (_req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");
    const config = await storage.loadSchedule(id);
    if (!config) return jsonError(404, "not_found");
    if (config.enabled === false) {
      return jsonError(409, "schedule_paused", "Resume the schedule before emitting.");
    }
    if (!fire) {
      // Without an embedded scheduler loop, simply bumping `nextRun` won't
      // fire interval schedules (computeIntervalDue's `lastFired + ms > now`
      // guard skips them). Tell the operator clearly instead of silently
      // failing.
      return jsonError(
        503,
        "scheduler_not_running",
        "Embedded scheduler is not running on this server — set scheduling.enabled to use emit-now.",
      );
    }
    await fire(id);
    return json(200, { ok: true });
  };
}

// ---------------------------------------------------------------------------
// History — past fires for a schedule.
//
// Derived from the workflow rows produced by `SchedulerLoop.dispatch`, which
// stamps every triggered run with `metadata.scheduleId`, `metadata.scheduleTick`,
// `metadata.scheduledAt`, and `metadata.firedAt`. So we don't need a separate
// ticks table — `WorkflowStorage.listWorkflows({ metadata: { scheduleId } })`
// is the source of truth. Custom `scheduling.fire` callbacks that route ticks
// outside the default trigger won't appear here; that's the tradeoff for the
// metadata-derived approach.
// ---------------------------------------------------------------------------

export function getScheduleHistory(
  schedulerStorage: SchedulerStorage,
  workflowStorage: WorkflowStorage,
) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");
    const config = await schedulerStorage.loadSchedule(id);
    if (!config) return jsonError(404, "not_found");

    const url = new URL(req.url);
    const limit = parseInt10(url.searchParams.get("limit")) ?? 20;
    const offset = parseInt10(url.searchParams.get("offset")) ?? 0;

    // Primary path: scheduler-side tick log. Authoritative — written in the
    // same transaction as the tickCount advance, so the history is always
    // a complete record of every fire, independent of whether the resulting
    // workflow row preserved its metadata across replays / retries.
    // Agent schedules dispatch directly to an in-process agent and never
    // produce a workflow row. Skip the workflow lookup entirely and report
    // each fired tick as a completed agent execution.
    const isAgentTrigger =
      (config.metadata as { agentTrigger?: unknown } | undefined)?.agentTrigger === true;

    if (isTickLogStorage(schedulerStorage)) {
      const [ticks, total] = await Promise.all([
        schedulerStorage.listTicks({ scheduleId: id, limit, offset }),
        schedulerStorage.countTicks({ scheduleId: id }),
      ]);

      if (isAgentTrigger) {
        const history: ScheduleTickHistoryDto[] = ticks.map((t) => {
          const wfId = `${id}.${t.tickNumber}`;
          return {
            tickNumber: t.tickNumber,
            workflowId: wfId,
            workflowName: (config.metadata?.["agentId"] as string) ?? "agent",
            status: "completed",
            kind: "agent",
            firedAt: t.firedAt.toISOString(),
            scheduledAt: t.scheduledAt.toISOString(),
            namespace: config.namespace ?? undefined,
          };
        });
        return json(200, { history, total } satisfies ScheduleHistoryResponse);
      }

      // Enrich each tick with status / duration from the matching workflow
      // row, joined on the deterministic `${scheduleId}.${tickNumber}` id.
      // One bulk query per page, not N+1.
      const workflowIds = ticks.map((t) => `${id}.${t.tickNumber}`);
      const wfStates = await Promise.all(
        workflowIds.map((wfId) => workflowStorage.loadWorkflow(wfId)),
      );
      const wfById = new Map(wfStates.filter((s) => s != null).map((s) => [s!.workflowId, s!]));

      const history: ScheduleTickHistoryDto[] = ticks.map((t) => {
        const wfId = `${id}.${t.tickNumber}`;
        const wf = wfById.get(wfId);
        const firedMs = t.firedAt.getTime();
        // Both lag and duration are measured against this fire's
        // `firedAt`, not the workflow row's `startedAt` / `completedAt`
        // alone. The deterministic id `${scheduleId}.${tick}` can be
        // reused across reboots when scheduler state resets but workflow
        // rows persist; in that case `startedAt` is from a prior fire and
        // would produce nonsense like "12h 47m" for a 6s-ago tick. The
        // tick log's `firedAt` is freshly written every fire, so we
        // require any derived latency to live AFTER it.
        const lagMs =
          wf?.startedAt && wf.startedAt.getTime() >= firedMs
            ? wf.startedAt.getTime() - firedMs
            : undefined;
        const durationMs =
          wf?.completedAt && wf.completedAt.getTime() >= firedMs
            ? wf.completedAt.getTime() - firedMs
            : undefined;
        return {
          tickNumber: t.tickNumber,
          workflowId: wfId,
          workflowName: wf?.workflowName ?? (config.metadata?.["workflowName"] as string) ?? "",
          status: wf?.status ?? "pending",
          kind: "workflow" as const,
          firedAt: t.firedAt.toISOString(),
          scheduledAt: t.scheduledAt.toISOString(),
          startedAt: iso(wf?.startedAt),
          completedAt: iso(wf?.completedAt),
          lagMs,
          durationMs,
          namespace: wf?.namespace ?? config.namespace ?? undefined,
        };
      });
      return json(200, { history, total } satisfies ScheduleHistoryResponse);
    }

    // Fallback: derive history from workflow metadata. Loses fidelity on
    // backends that don't yet implement a tick log (Postgres, Redis), but
    // keeps the endpoint working without a hard dependency.
    const all = await workflowStorage.listWorkflows({
      metadata: { scheduleId: id },
      orderBy: "createdAt",
      orderDir: "desc",
      limit: limit + offset,
    });
    const slice = all.slice(offset, offset + limit);
    const history: ScheduleTickHistoryDto[] = slice.map((wf) => {
      const meta = wf.metadata ?? {};
      const tickNumberRaw = meta["scheduleTick"];
      const tickNumber =
        typeof tickNumberRaw === "number"
          ? tickNumberRaw
          : typeof tickNumberRaw === "string"
            ? Number.parseInt(tickNumberRaw, 10)
            : Number.NaN;
      const firedAt = typeof meta["firedAt"] === "string" ? (meta["firedAt"] as string) : undefined;
      const scheduledAt =
        typeof meta["scheduledAt"] === "string" ? (meta["scheduledAt"] as string) : undefined;
      const firedMs = firedAt ? new Date(firedAt).getTime() : undefined;
      const lagMs =
        firedMs !== undefined && wf.startedAt && wf.startedAt.getTime() >= firedMs
          ? wf.startedAt.getTime() - firedMs
          : undefined;
      const durationMs =
        firedMs !== undefined && wf.completedAt && wf.completedAt.getTime() >= firedMs
          ? wf.completedAt.getTime() - firedMs
          : undefined;
      return {
        tickNumber: Number.isFinite(tickNumber) ? tickNumber : undefined,
        workflowId: wf.workflowId,
        workflowName: wf.workflowName,
        status: wf.status,
        firedAt,
        scheduledAt,
        startedAt: iso(wf.startedAt),
        completedAt: iso(wf.completedAt),
        lagMs,
        durationMs,
        namespace: wf.namespace ?? undefined,
      };
    });
    const total = all.length < limit + offset ? all.length : offset + slice.length;
    return json(200, { history, total } satisfies ScheduleHistoryResponse);
  };
}

// ---------------------------------------------------------------------------
// Upcoming — next N planned ticks. Pure: derives from cron/rrule/interval by
// stepping `computeNextRun` forward, advancing a fake clock between calls.
// ---------------------------------------------------------------------------

export function getScheduleUpcoming(storage: SchedulerStorage) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");
    const config = await storage.loadSchedule(id);
    if (!config) return jsonError(404, "not_found");

    const url = new URL(req.url);
    const count = Math.max(1, Math.min(parseInt10(url.searchParams.get("count")) ?? 10, 100));

    // Disabled schedules have no upcoming fires by definition; surface the
    // empty list with `exhausted: false` so the UI distinguishes "nothing
    // planned because paused" from "schedule reached endAt."
    if (!(config.enabled ?? true)) {
      return json(200, { upcoming: [], exhausted: false } satisfies ScheduleUpcomingResponse);
    }

    const state = await storage.loadScheduleState(id);
    const startTickNumber = state?.tickCount ?? 0;

    const upcoming: ScheduleUpcomingTickDto[] = [];
    let cursor = new Date();
    let exhausted = false;
    for (let i = 0; i < count; i++) {
      const next = computeNextRun(config, fakeClockAt(cursor));
      if (!next) {
        exhausted = true;
        break;
      }
      upcoming.push({
        tickNumber: startTickNumber + i,
        scheduledAt: next.toISOString(),
      });
      // Advance one ms past the just-computed fire so the next call returns
      // the run after this one rather than re-emitting the same time.
      cursor = new Date(next.getTime() + 1);
    }
    return json(200, { upcoming, exhausted } satisfies ScheduleUpcomingResponse);
  };
}

function parseBool(s: string | null): boolean | undefined {
  if (s === null || s === "") return undefined;
  if (s === "true" || s === "1") return true;
  if (s === "false" || s === "0") return false;
  return undefined;
}

function parseInt10(s: string | null): number | undefined {
  if (s === null) return undefined;
  const n = Number.parseInt(s, 10);
  return Number.isFinite(n) ? n : undefined;
}
