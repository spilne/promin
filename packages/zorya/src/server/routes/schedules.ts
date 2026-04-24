// ---------------------------------------------------------------------------
// Schedules route — /api/schedules
//
// Exposes a pluggable SchedulerStorage (from @promin/workflow) behind a thin
// DTO layer. Zorya itself doesn't own scheduler state — the embedder passes
// their InMemorySchedulerStorage / PgSchedulerStorage / etc.
// ---------------------------------------------------------------------------

import type { DurableScheduleConfig, SchedulerStorage } from "@promin/workflow";
import { computeNextRun } from "@promin/workflow";
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

    const [configs, total] = await Promise.all([
      storage.listSchedules({ enabled, namespace, limit, offset }),
      storage.countSchedules({ enabled, namespace }),
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
