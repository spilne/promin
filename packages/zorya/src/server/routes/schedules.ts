// ---------------------------------------------------------------------------
// Schedules route — /api/schedules
//
// Exposes a pluggable SchedulerStorage (from @promin/workflow) behind a thin
// DTO layer. Zorya itself doesn't own scheduler state — the embedder passes
// their InMemorySchedulerStorage / PgSchedulerStorage / etc.
// ---------------------------------------------------------------------------

import type { WorkflowStorage } from "@promin/workflow";
import type {
  DurableScheduleConfig,
  ScheduleState,
  ScheduleTick,
  SchedulerStorage,
} from "@promin/workflow/scheduler";
import {
  computeNextRun,
  isTickLogStorage,
  scheduleTickRunId,
  validateScheduleConfig,
} from "@promin/workflow/scheduler";
import { SystemWallClock, hasCapability, type WallClock } from "@promin/workflow";
import { z } from "zod";
import { accepted, isoOrNull, json, jsonError, noContent } from "../router.ts";
import { BooleanQuery, MetadataQuery, OffsetQuery, limitQuery } from "../api-types.ts";
import { parseBody, parseParams, parseQuery } from "../request-validation.ts";
import { assertIfMatch, contentEtag, withEtag } from "../preconditions.ts";
import { runLocation } from "./runs.ts";

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
  maxCatchUp?: number;
  /** ISO timestamp of last fire. */
  lastFiredAt?: string;
  /** Running fire counter. */
  tickCount?: number;
  /** ISO timestamp of the next scheduled fire. Null when no next run. */
  nextRunAt?: string;
  /**
   * Token of this incarnation of the schedule id: new when the schedule is
   * deleted and created again. Part of every tick's run id.
   */
  incarnation?: string;
  /**
   * The schedule's last failure to fire — a tick whose run could not be
   * started (e.g. its `workflowName` resolves to no workflow) or a config
   * the scheduler can't evaluate. Absent once a fire goes through, or after
   * the schedule is edited. A failing schedule stays due and is retried
   * with backoff.
   */
  lastError?: { message: string; at: string };
}

export interface SchedulesResponse {
  schedules: ScheduleDto[];
  total: number;
}

export interface ScheduleTickHistoryDto {
  /** The tick's number within its schedule incarnation; undefined when unknown. */
  tickNumber?: number;
  workflowId: string;
  workflowName: string;
  status: string;
  /**
   * What kind of execution this tick produced — derived from the schedule's
   * `metadata.agentTrigger`, not from inspecting the run row. Both kinds
   * produce a workflow row keyed by `scheduleTickRunId(...)`; `kind` is
   * just provenance for the UI badge. `status`, `lagMs`, `durationMs`
   * come from the joined workflow row in both cases.
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

/** `202` from `POST /api/schedules/:id/emit`: the tick fired and its run started. */
export interface ScheduleEmitResponse {
  scheduleId: string;
  tickNumber: number;
  /** Run the tick started (`scheduleTickRunId`); poll `/api/runs/:id`. */
  workflowId: string;
}

/** `null` and `""` mean "not set" — the dashboard form sends either. */
function optional<T extends z.ZodTypeAny>(schema: T) {
  return z.preprocess((v) => (v === null || v === "" ? undefined : v), schema.optional());
}

const IsoDateSchema = z
  .string()
  .max(64)
  .refine((v) => Number.isFinite(Date.parse(v)), { message: "must be an ISO-8601 date" });

const TimezoneSchema = z
  .string()
  .max(64)
  .refine(
    (tz) => {
      try {
        new Intl.DateTimeFormat("en-US", { timeZone: tz });
        return true;
      } catch {
        return false;
      }
    },
    { message: "must be an IANA time zone" },
  );

const ScheduleName = z.string().max(256);
const Cron = z.string().min(1).max(256);
const RRule = z.string().min(1).max(4096);
const IntervalMs = z.number().int().positive().max(Number.MAX_SAFE_INTEGER);
const JitterMs = z.number().int().nonnegative().max(Number.MAX_SAFE_INTEGER);
const MaxCatchUp = z.number().int().nonnegative().max(100_000);

/**
 * Body of `POST /api/schedules` (create-only: 409 `schedule_exists` when
 * the id is taken). Exactly one of `cron`, `rrule`, `intervalMs`.
 * `namespace` must be one this server's scheduler polls; it defaults to
 * the scheduler's own namespace.
 */
export const ScheduleCreateRequestSchema = z
  .object({
    id: z.string().min(1).max(256),
    name: optional(ScheduleName),
    namespace: optional(z.string().min(1).max(256)),
    cron: optional(Cron),
    rrule: optional(RRule),
    intervalMs: optional(IntervalMs),
    timezone: optional(TimezoneSchema),
    enabled: optional(z.boolean()),
    startAt: optional(IsoDateSchema),
    endAt: optional(IsoDateSchema),
    jitterMs: optional(JitterMs),
    maxCatchUp: optional(MaxCatchUp),
    /** Workflow to trigger on each tick. Stored in metadata.workflowName. */
    workflowName: optional(z.string().min(1).max(256)),
    /** Input for triggered runs. Stored in metadata.input. */
    input: z.unknown().optional(),
    metadata: optional(z.record(z.unknown())),
  })
  .strict();

export type ScheduleCreateRequest = z.infer<typeof ScheduleCreateRequestSchema>;

/**
 * Body of `PATCH /api/schedules/:id`: any mutable field; omitted fields
 * keep their value and `null` clears an optional one. Sending one of
 * `cron` / `rrule` / `intervalMs` replaces the trigger (the other two are
 * cleared). `metadata` is merged key by key (`null` removes a key). `id`
 * and `namespace` are immutable — delete and re-create to move a schedule.
 */
export const SchedulePatchRequestSchema = z
  .object({
    name: ScheduleName.nullable().optional(),
    cron: Cron.optional(),
    rrule: RRule.optional(),
    intervalMs: IntervalMs.optional(),
    timezone: TimezoneSchema.nullable().optional(),
    enabled: z.boolean().optional(),
    startAt: IsoDateSchema.nullable().optional(),
    endAt: IsoDateSchema.nullable().optional(),
    jitterMs: JitterMs.nullable().optional(),
    maxCatchUp: MaxCatchUp.nullable().optional(),
    workflowName: z.string().min(1).max(256).nullable().optional(),
    input: z.unknown().optional(),
    metadata: z.record(z.unknown()).optional(),
  })
  .strict()
  .refine(
    (b) =>
      (b.cron !== undefined ? 1 : 0) +
        (b.rrule !== undefined ? 1 : 0) +
        (b.intervalMs !== undefined ? 1 : 0) <=
      1,
    { message: "Send at most one of cron, rrule, intervalMs" },
  );

export type SchedulePatchRequest = z.infer<typeof SchedulePatchRequestSchema>;

/** Query of `GET /api/schedules`. */
export const ScheduleListQuerySchema = z.object({
  enabled: BooleanQuery,
  namespace: z.preprocess((v) => (v === "" ? undefined : v), z.string().max(256).optional()),
  /**
   * Containment filter, JSON-encoded. The chat per-thread drawer sends
   * `{ agentTrigger: true, threadId }`, the "kind = agent" chip
   * `{ agentTrigger: true }`.
   */
  metadata: MetadataQuery,
  limit: limitQuery({ fallback: 100 }),
  offset: OffsetQuery,
});

/** Query of `GET /api/schedules/:id/history`. */
export const ScheduleHistoryQuerySchema = z.object({
  limit: limitQuery({ fallback: 20 }),
  offset: OffsetQuery,
});

/** Query of `GET /api/schedules/:id/upcoming`. */
export const ScheduleUpcomingQuerySchema = z.object({
  count: limitQuery({ fallback: 10, max: 100 }),
});

const ScheduleParamsSchema = z.object({ id: z.string().min(1).max(256) });

/** ISO string, or `undefined` for a missing or unreadable stored date. */
function iso(d?: Date | null): string | undefined {
  return isoOrNull(d) ?? undefined;
}

/**
 * Dependencies shared by the schedule routes that read the current time
 * (next-run projection, upcoming ticks, the create-time nextRun seed).
 */
export interface ScheduleRoutesDeps {
  readonly storage: SchedulerStorage;
  /**
   * Time source for "now". Pass the scheduler loop's clock so seeded and
   * projected times line up with its due checks. Default: `SystemWallClock`.
   */
  readonly clock?: WallClock;
  /**
   * Whether a scheduler polls `namespace` (`undefined` is the global
   * namespace). Creates elsewhere are refused: nothing would fire them.
   * Default: only the global namespace.
   */
  readonly servesNamespace?: (namespace: string | undefined) => boolean;
  /** Namespace of a schedule created without one. Default: global. */
  readonly defaultNamespace?: string;
}

/** A clock frozen at `when` — `computeNextRun` only reads the time. */
function frozenClockAt(params: { when: Date; base: WallClock }): WallClock {
  const { when, base } = params;
  return {
    currentTimeMs: () => when.getTime(),
    now: () => new Date(when.getTime()),
    setTimeout: (fn, ms) => base.setTimeout(fn, ms),
    setInterval: (fn, ms) => base.setInterval(fn, ms),
  };
}

function nextRunIso(params: {
  config: DurableScheduleConfig;
  clock: WallClock;
}): string | undefined {
  const { config, clock } = params;
  if (!(config.enabled ?? true)) return undefined;
  try {
    const next = computeNextRun({
      config,
      clock: frozenClockAt({ when: clock.now(), base: clock }),
    });
    return next ? next.toISOString() : undefined;
  } catch {
    return undefined;
  }
}

function toDto(params: {
  config: DurableScheduleConfig;
  state?: ScheduleState | null;
  clock: WallClock;
}): ScheduleDto {
  const { config, state, clock } = params;
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
    maxCatchUp: config.maxCatchUp,
    lastFiredAt: state?.lastFired ? iso(state.lastFired) : undefined,
    tickCount: state?.tickCount,
    nextRunAt: nextRunIso({ config, clock }),
    incarnation: state?.incarnation,
    lastError: state?.lastError
      ? { message: state.lastError.message, at: iso(state.lastError.at) ?? "" }
      : undefined,
  };
}

/**
 * Entity tag of a schedule: its stored configuration and incarnation. Fire
 * state (`lastFiredAt`, `tickCount`, `nextRunAt`, `lastError`) is left out,
 * so a tick firing between a read and a PATCH doesn't fail the PATCH's
 * `If-Match`; a schedule deleted and created again under the same id does.
 */
function scheduleEtag(dto: ScheduleDto): string {
  const {
    lastFiredAt: _fired,
    tickCount: _ticks,
    nextRunAt: _next,
    lastError: _error,
    ...config
  } = dto;
  return contentEtag(config);
}

/** A schedule with its entity tag (`If-Match` on PATCH compares against it). */
function scheduleResponse(status: number, dto: ScheduleDto): Response {
  return withEtag(json(status, dto), scheduleEtag(dto));
}

export function listSchedules(deps: ScheduleRoutesDeps) {
  const { storage } = deps;
  const clock = deps.clock ?? SystemWallClock;
  return async (req: Request): Promise<Response> => {
    const { enabled, namespace, metadata, limit, offset } = parseQuery({
      req,
      schema: ScheduleListQuerySchema,
    });
    const [configs, total] = await Promise.all([
      storage.listSchedules({ enabled, namespace, limit, offset, metadata }),
      storage.countSchedules({ enabled, namespace, metadata }),
    ]);

    const ids = configs.map((c) => c.id);
    const states = ids.length > 0 ? await storage.loadScheduleStates(ids) : new Map();
    const schedules = configs.map((c) =>
      toDto({ config: c, state: states.get(c.id) ?? null, clock }),
    );
    const response: SchedulesResponse = { schedules, total };
    return json(200, response);
  };
}

export function getSchedule(deps: ScheduleRoutesDeps) {
  const { storage } = deps;
  const clock = deps.clock ?? SystemWallClock;
  return async (_req: Request, params: Record<string, string>): Promise<Response> => {
    const { id } = parseParams({ params, schema: ScheduleParamsSchema });
    const config = await storage.loadSchedule(id);
    if (!config) return jsonError(404, "not_found");
    const state = await storage.loadScheduleState(id);
    return scheduleResponse(200, toDto({ config, state, clock }));
  };
}

/**
 * POST /api/schedules — create a schedule: 201 + the schedule. Create-only:
 * an existing id is a 409 `schedule_exists` (change it with PATCH). The
 * namespace must be one this server's scheduler polls (400
 * `namespace_not_served`), so every created schedule can fire.
 */
export function createSchedule(deps: ScheduleRoutesDeps) {
  const { storage } = deps;
  const clock = deps.clock ?? SystemWallClock;
  const serves = deps.servesNamespace ?? ((ns: string | undefined) => ns === undefined);
  return async (req: Request): Promise<Response> => {
    const body = await parseBody({ req, schema: ScheduleCreateRequestSchema });
    const namespace = body.namespace ?? deps.defaultNamespace;
    if (!serves(namespace)) {
      return jsonError(
        400,
        "namespace_not_served",
        `No scheduler on this server polls namespace ${namespace === undefined ? "(global)" : `"${namespace}"`}`,
      );
    }

    const metadata: Record<string, unknown> = { ...(body.metadata ?? {}) };
    if (body.workflowName) metadata.workflowName = body.workflowName;
    if (body.input !== undefined) metadata.input = body.input;

    const config: DurableScheduleConfig = {
      id: body.id,
      name: body.name,
      namespace,
      cron: body.cron,
      rrule: body.rrule,
      intervalMs: body.intervalMs,
      timezone: body.timezone,
      enabled: body.enabled ?? true,
      startAt: body.startAt ? new Date(body.startAt) : undefined,
      endAt: body.endAt ? new Date(body.endAt) : undefined,
      jitterMs: body.jitterMs,
      metadata: Object.keys(metadata).length > 0 ? metadata : undefined,
      maxCatchUp: body.maxCatchUp,
    };
    const invalid = invalidConfig(config);
    if (invalid !== undefined) return jsonError(400, "invalid_schedule", invalid);

    if (await storage.loadSchedule(config.id)) {
      return jsonError(409, "schedule_exists", `Schedule "${config.id}" already exists`);
    }
    await storage.upsertSchedule(config);
    // Match `DurableScheduler.register`: an enabled schedule is due now,
    // so the first poll picks it up (computeDueTicks fires one boot tick
    // at `now` when `lastFired` is null, then commitPoll advances nextRun
    // onto the natural cron / interval cadence). A disabled one stays out
    // of due-tracking — the upsert already cleared it.
    if (config.enabled !== false) await storage.setNextRun(config.id, clock.now());
    const state = await storage.loadScheduleState(config.id);
    return scheduleResponse(201, toDto({ config, state, clock }));
  };
}

/**
 * PATCH /api/schedules/:id — change any mutable field (see
 * `SchedulePatchRequestSchema`): 200 + the schedule. The merged config is
 * validated before it is stored, and a change to when the schedule fires
 * (trigger, timezone, window) recomputes its next run. Pausing takes it
 * out of due-tracking; resuming makes it due now, as after a restart.
 * `If-Match` (the GET `etag`) makes the edit conditional: 412
 * `precondition_failed` when the configuration changed since it was read.
 */
export function patchSchedule(deps: ScheduleRoutesDeps) {
  const { storage } = deps;
  const clock = deps.clock ?? SystemWallClock;
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const { id } = parseParams({ params, schema: ScheduleParamsSchema });
    const patch = await parseBody({ req, schema: SchedulePatchRequestSchema });
    const current = await storage.loadSchedule(id);
    if (!current) return jsonError(404, "not_found");
    const currentState = await storage.loadScheduleState(id);
    assertIfMatch({
      req,
      etag: scheduleEtag(toDto({ config: current, state: currentState, clock })),
      what: "schedule",
    });

    const merged = applySchedulePatch({ current, patch });
    const invalid = invalidConfig(merged);
    if (invalid !== undefined) return jsonError(400, "invalid_schedule", invalid);

    await storage.upsertSchedule(merged);
    const timingChanged =
      patch.cron !== undefined ||
      patch.rrule !== undefined ||
      patch.intervalMs !== undefined ||
      patch.timezone !== undefined ||
      patch.startAt !== undefined ||
      patch.endAt !== undefined;
    const resumed = current.enabled === false && merged.enabled !== false;
    if (merged.enabled !== false && timingChanged && !resumed) {
      // Same as `DurableScheduler.update`: the next fire follows the new timing.
      await storage.setNextRun(id, computeNextRun({ config: merged, clock }));
    }
    const state = await storage.loadScheduleState(id);
    const stored = (await storage.loadSchedule(id)) ?? merged;
    return scheduleResponse(200, toDto({ config: stored, state, clock }));
  };
}

/** The stored config with `patch` applied (see `SchedulePatchRequestSchema`). */
function applySchedulePatch(params: {
  current: DurableScheduleConfig;
  patch: SchedulePatchRequest;
}): DurableScheduleConfig {
  const { current, patch } = params;
  const next: { -readonly [K in keyof DurableScheduleConfig]: DurableScheduleConfig[K] } = {
    ...current,
  };
  const set = <K extends keyof DurableScheduleConfig>(
    key: K,
    value: DurableScheduleConfig[K] | null | undefined,
  ): void => {
    if (value === undefined) return;
    if (value === null) delete next[key];
    else next[key] = value;
  };

  if (patch.cron !== undefined || patch.rrule !== undefined || patch.intervalMs !== undefined) {
    delete next.cron;
    delete next.rrule;
    delete next.intervalMs;
    set("cron", patch.cron);
    set("rrule", patch.rrule);
    set("intervalMs", patch.intervalMs);
  }
  set("name", patch.name);
  set("timezone", patch.timezone);
  set("enabled", patch.enabled);
  set("startAt", patch.startAt === undefined ? undefined : toDate(patch.startAt));
  set("endAt", patch.endAt === undefined ? undefined : toDate(patch.endAt));
  set("jitterMs", patch.jitterMs);
  set("maxCatchUp", patch.maxCatchUp);

  const metadata: Record<string, unknown> = { ...(current.metadata ?? {}) };
  for (const [key, value] of Object.entries(patch.metadata ?? {})) {
    if (value === null) delete metadata[key];
    else metadata[key] = value;
  }
  if (patch.workflowName === null) delete metadata.workflowName;
  else if (patch.workflowName !== undefined) metadata.workflowName = patch.workflowName;
  if ("input" in patch) metadata.input = patch.input;
  if (Object.keys(metadata).length > 0) next.metadata = metadata;
  else delete next.metadata;
  return next;
}

function toDate(iso: string | null): Date | null {
  return iso === null ? null : new Date(iso);
}

/**
 * Why the scheduler couldn't evaluate `config` (bad cron / RRULE, no or
 * several triggers, an inverted window), or `undefined` when it can.
 */
function invalidConfig(config: DurableScheduleConfig): string | undefined {
  if (config.startAt && config.endAt && config.startAt.getTime() > config.endAt.getTime()) {
    return "startAt must not be after endAt";
  }
  try {
    validateScheduleConfig(config);
    return undefined;
  } catch (err) {
    return err instanceof Error ? err.message : String(err);
  }
}

/** DELETE /api/schedules/:id — 204; 404 when there is no such schedule. */
export function deleteSchedule(storage: SchedulerStorage) {
  return async (_req: Request, params: Record<string, string>): Promise<Response> => {
    const { id } = parseParams({ params, schema: ScheduleParamsSchema });
    if (!(await storage.loadSchedule(id))) return jsonError(404, "not_found");
    await storage.deleteSchedule(id);
    return noContent();
  };
}

// ---------------------------------------------------------------------------
// Emit now — fire one tick of a schedule right away, through the scheduler
// loop's `fireOnce`: the same tick numbering, tick log, metadata stamping
// and dispatch as a polled fire, without waiting on cron / interval.
//
// 202 `{ scheduleId, tickNumber, workflowId }` once the tick's run has
// started; 409 `schedule_paused` for a paused schedule; 502
// `dispatch_failed` when the tick was recorded but its run could not start
// (emit again to fire a new tick); 503 when no scheduler loop runs here.
// ---------------------------------------------------------------------------

export function emitScheduleNow(
  storage: SchedulerStorage,
  fire: ((scheduleId: string) => Promise<ScheduleTick | null>) | undefined,
) {
  return async (_req: Request, params: Record<string, string>): Promise<Response> => {
    const { id } = parseParams({ params, schema: ScheduleParamsSchema });
    const config = await storage.loadSchedule(id);
    if (!config) return jsonError(404, "not_found");
    if (config.enabled === false) {
      return jsonError(409, "schedule_paused", "Resume the schedule before emitting.");
    }
    if (!fire) {
      return jsonError(
        503,
        "scheduler_not_running",
        "Embedded scheduler is not running on this server — set scheduling.enabled to use emit-now.",
      );
    }
    let tick: ScheduleTick | null;
    try {
      tick = await fire(id);
    } catch (err) {
      // The tick was recorded but its run did not start: say so instead of
      // reporting a fire that never happened.
      return jsonError(502, "dispatch_failed", err instanceof Error ? err.message : String(err));
    }
    if (!tick) {
      // Paused or deleted between the check above and the fire.
      return jsonError(409, "schedule_paused", "The schedule was paused or removed.");
    }
    const workflowId = scheduleTickRunId(tick);
    const response: ScheduleEmitResponse = {
      scheduleId: id,
      tickNumber: tick.tickNumber,
      workflowId,
    };
    return accepted(response, runLocation(workflowId));
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
    const { id } = parseParams({ params, schema: ScheduleParamsSchema });
    const { limit, offset } = parseQuery({ req, schema: ScheduleHistoryQuerySchema });
    const config = await schedulerStorage.loadSchedule(id);
    if (!config) return jsonError(404, "not_found");

    // Primary path: scheduler-side tick log. Authoritative — written in the
    // same transaction as the tickCount advance, so the history is always
    // a complete record of every fire, independent of whether the resulting
    // workflow row preserved its metadata across replays / retries.
    //
    // Both workflow- and agent-targeted schedules produce a workflow row
    // with id `scheduleTickRunId(tick)` — workflow ones
    // via `scheduler-loop`'s deterministic `workflowId`, agent ones via
    // `dispatchAgentSchedule` passing `runId` into `agent.invoke`. Same
    // join either way; the `kind` chip just reflects schedule provenance.
    const isAgentTrigger =
      (config.metadata as { agentTrigger?: unknown } | undefined)?.agentTrigger === true;
    const fallbackName = isAgentTrigger
      ? ((config.metadata?.["agentId"] as string) ?? "agent")
      : ((config.metadata?.["workflowName"] as string) ?? "");
    const kind: "agent" | "workflow" = isAgentTrigger ? "agent" : "workflow";

    if (isTickLogStorage(schedulerStorage)) {
      const [ticks, total] = await Promise.all([
        schedulerStorage.listTicks({ scheduleId: id, limit, offset }),
        schedulerStorage.countTicks({ scheduleId: id }),
      ]);

      // Enrich each tick with status / duration from the matching workflow
      // row, joined on the deterministic `scheduleTickRunId` id: one read
      // per tick on the page, so `limit` (at most 500) bounds the reads.
      // Each logged tick carries its incarnation, so a schedule deleted
      // and created again never joins the old schedule's runs.
      const workflowIds = ticks.map((t) => scheduleTickRunId(t));
      const wfStates = await Promise.all(
        workflowIds.map((wfId) => workflowStorage.loadWorkflow(wfId)),
      );
      const wfById = new Map(wfStates.filter((s) => s != null).map((s) => [s!.workflowId, s!]));

      const history: ScheduleTickHistoryDto[] = ticks.map((t) => {
        const wfId = scheduleTickRunId(t);
        const wf = wfById.get(wfId);
        const firedMs = t.firedAt.getTime();
        // Both lag and duration are measured against this fire's
        // `firedAt`, not the workflow row's `startedAt` / `completedAt`
        // alone. The deterministic id can be reused across reboots when
        // scheduler state resets but workflow rows persist; in that case
        // `startedAt` is from a prior fire and would produce nonsense
        // like "12h 47m" for a 6s-ago tick. The tick log's `firedAt` is
        // freshly written every fire, so we require any derived latency
        // to live AFTER it.
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
          workflowName: wf?.workflowName ?? fallbackName,
          status: wf?.status ?? "pending",
          kind,
          firedAt: iso(t.firedAt),
          scheduledAt: iso(t.scheduledAt),
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
    // Only this incarnation's runs: the dispatcher stamps it next to the id.
    const state = await schedulerStorage.loadScheduleState(id);
    const filter = {
      metadata: {
        scheduleId: id,
        ...(state && { scheduleIncarnation: state.incarnation }),
      },
    };
    const [slice, counted] = await Promise.all([
      workflowStorage.listWorkflows({
        ...filter,
        orderBy: "createdAt",
        orderDir: "desc",
        limit,
        offset,
      }),
      hasCapability(workflowStorage, "countWorkflows")
        ? workflowStorage.countWorkflows(filter)
        : undefined,
    ]);
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
        kind,
        namespace: wf.namespace ?? undefined,
      };
    });
    // Without a count, a full page means "at least one more may follow".
    const total = counted ?? offset + slice.length + (slice.length === limit ? 1 : 0);
    return json(200, { history, total } satisfies ScheduleHistoryResponse);
  };
}

// ---------------------------------------------------------------------------
// Upcoming — next N planned ticks. Pure: derives from cron/rrule/interval by
// stepping `computeNextRun` forward, re-freezing the clock one ms past each fire.
// ---------------------------------------------------------------------------

export function getScheduleUpcoming(deps: ScheduleRoutesDeps) {
  const { storage } = deps;
  const clock = deps.clock ?? SystemWallClock;
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const { id } = parseParams({ params, schema: ScheduleParamsSchema });
    const { count } = parseQuery({ req, schema: ScheduleUpcomingQuerySchema });
    const config = await storage.loadSchedule(id);
    if (!config) return jsonError(404, "not_found");

    // Disabled schedules have no upcoming fires by definition; surface the
    // empty list with `exhausted: false` so the UI distinguishes "nothing
    // planned because paused" from "schedule reached endAt."
    if (!(config.enabled ?? true)) {
      return json(200, { upcoming: [], exhausted: false } satisfies ScheduleUpcomingResponse);
    }

    const state = await storage.loadScheduleState(id);
    const startTickNumber = state?.tickCount ?? 0;

    const upcoming: ScheduleUpcomingTickDto[] = [];
    let cursor = clock.now();
    let exhausted = false;
    for (let i = 0; i < count; i++) {
      let next: Date | null;
      try {
        next = computeNextRun({ config, clock: frozenClockAt({ when: cursor, base: clock }) });
      } catch {
        // A stored config the scheduler can't evaluate has no upcoming fires.
        next = null;
      }
      if (!next || !Number.isFinite(next.getTime())) {
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
