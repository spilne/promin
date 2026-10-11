// ---------------------------------------------------------------------------
// Schedules route — /api/schedules
//
// Exposes a pluggable SchedulerStorage (from @promin/workflow) behind a thin
// DTO layer. Zorya itself doesn't own scheduler state — the embedder passes
// their InMemorySchedulerStorage / PgSchedulerStorage / etc.
// ---------------------------------------------------------------------------

import type { WorkflowStorage } from "@promin/workflow";
import type { DurableScheduleConfig, SchedulerStorage } from "@promin/workflow/scheduler";
import {
  computeNextRun,
  isTickLogStorage,
  scheduleTickRunId,
  validateScheduleConfig,
} from "@promin/workflow/scheduler";
import { SystemWallClock, type WallClock } from "@promin/workflow";
import { z } from "zod";
import { isoOrNull, json, jsonError, parseIntParam, readJson } from "../router.ts";

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

export interface SchedulePatchRequest {
  enabled?: boolean;
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

/** Body of `POST /api/schedules`. Validated before anything is stored. */
export const ScheduleCreateRequestSchema = z.object({
  id: z.string().min(1).max(256),
  name: optional(z.string().max(256)),
  namespace: optional(z.string().max(256)),
  cron: optional(z.string().max(256)),
  rrule: optional(z.string().max(4096)),
  intervalMs: optional(z.number().finite().positive().max(Number.MAX_SAFE_INTEGER)),
  timezone: optional(TimezoneSchema),
  enabled: optional(z.boolean()),
  startAt: optional(IsoDateSchema),
  endAt: optional(IsoDateSchema),
  jitterMs: optional(z.number().finite().nonnegative().max(Number.MAX_SAFE_INTEGER)),
  maxCatchUp: optional(z.number().int().nonnegative().max(100_000)),
  /** Optional workflow name to link. Stored in metadata.workflowName. */
  workflowName: optional(z.string().max(256)),
  /** Optional input payload for triggered runs. Stored in metadata.input. */
  input: z.unknown().optional(),
  metadata: optional(z.record(z.unknown())),
});

export type ScheduleCreateRequest = z.infer<typeof ScheduleCreateRequestSchema>;

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
  state?: { lastFired: Date | null; tickCount: number } | null;
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
  };
}

export function listSchedules(deps: ScheduleRoutesDeps) {
  const { storage } = deps;
  const clock = deps.clock ?? SystemWallClock;
  return async (req: Request): Promise<Response> => {
    const url = new URL(req.url);
    const enabled = parseBool(url.searchParams.get("enabled"));
    const namespace = url.searchParams.get("namespace") ?? undefined;
    const limit = parseIntParam({ raw: url.searchParams.get("limit"), min: 1, max: 1_000 }) ?? 100;
    const offset =
      parseIntParam({ raw: url.searchParams.get("offset"), min: 0, max: 1_000_000 }) ?? 0;
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
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");
    const config = await storage.loadSchedule(id);
    if (!config) return jsonError(404, "not_found");
    const state = await storage.loadScheduleState(id);
    return json(200, toDto({ config, state, clock }));
  };
}

export function createSchedule(deps: ScheduleRoutesDeps) {
  const { storage } = deps;
  const clock = deps.clock ?? SystemWallClock;
  return async (req: Request): Promise<Response> => {
    const raw = await readJson<unknown>(req);
    if (!raw || typeof raw !== "object" || !(raw as { id?: unknown }).id) {
      return jsonError(400, "missing_id");
    }
    const parsed = ScheduleCreateRequestSchema.safeParse(raw);
    if (!parsed.success) {
      return jsonError(
        400,
        "invalid_schedule",
        parsed.error.issues.map((i) => `${i.path.join(".") || "body"}: ${i.message}`).join("; "),
      );
    }
    const body = parsed.data;
    if (body.startAt && body.endAt && Date.parse(body.startAt) > Date.parse(body.endAt)) {
      return jsonError(400, "invalid_schedule", "startAt must not be after endAt");
    }

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
      maxCatchUp: body.maxCatchUp,
    };

    // Reject what the scheduler couldn't evaluate (bad cron/RRULE, a
    // non-positive interval, ...) before it reaches storage.
    try {
      validateScheduleConfig(config);
    } catch (err) {
      return jsonError(400, "invalid_schedule", err instanceof Error ? err.message : String(err));
    }

    try {
      await storage.upsertSchedule(config);
      // Match `DurableScheduler.register`: an enabled schedule is due now,
      // so the first poll picks it up (computeDueTicks fires one boot tick
      // at `now` when `lastFired` is null, then commitPoll advances nextRun
      // onto the natural cron / interval cadence). A disabled one stays out
      // of due-tracking — the upsert already cleared it.
      if (config.enabled !== false) await storage.setNextRun(config.id, clock.now());
      const state = await storage.loadScheduleState(config.id);
      return json(200, toDto({ config, state, clock }));
    } catch (err) {
      return jsonError(400, "upsert_failed", err instanceof Error ? err.message : String(err));
    }
  };
}

export function patchSchedule(deps: ScheduleRoutesDeps) {
  const { storage } = deps;
  const clock = deps.clock ?? SystemWallClock;
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
    return json(200, toDto({ config, state, clock }));
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
    try {
      await fire(id);
    } catch (err) {
      // The tick was recorded but its run did not start: say so instead of
      // reporting a fire that never happened.
      return jsonError(502, "dispatch_failed", err instanceof Error ? err.message : String(err));
    }
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
    const limit = parseIntParam({ raw: url.searchParams.get("limit"), min: 1, max: 500 }) ?? 20;
    const offset =
      parseIntParam({ raw: url.searchParams.get("offset"), min: 0, max: 1_000_000 }) ?? 0;

    // Primary path: scheduler-side tick log. Authoritative — written in the
    // same transaction as the tickCount advance, so the history is always
    // a complete record of every fire, independent of whether the resulting
    // workflow row preserved its metadata across replays / retries.
    //
    // Both workflow- and agent-targeted schedules produce a workflow row
    // with id `scheduleTickRunId(scheduleId, tickNumber)` — workflow ones
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
      // row, joined on the deterministic `scheduleTickRunId` id. One bulk
      // query per page, not N+1.
      const workflowIds = ticks.map((t) =>
        scheduleTickRunId({ scheduleId: id, tickNumber: t.tickNumber }),
      );
      const wfStates = await Promise.all(
        workflowIds.map((wfId) => workflowStorage.loadWorkflow(wfId)),
      );
      const wfById = new Map(wfStates.filter((s) => s != null).map((s) => [s!.workflowId, s!]));

      const history: ScheduleTickHistoryDto[] = ticks.map((t) => {
        const wfId = scheduleTickRunId({ scheduleId: id, tickNumber: t.tickNumber });
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
        kind,
        namespace: wf.namespace ?? undefined,
      };
    });
    const total = all.length < limit + offset ? all.length : offset + slice.length;
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
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");
    const config = await storage.loadSchedule(id);
    if (!config) return jsonError(404, "not_found");

    const url = new URL(req.url);
    const count = parseIntParam({ raw: url.searchParams.get("count"), min: 1, max: 100 }) ?? 10;

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

function parseBool(s: string | null): boolean | undefined {
  if (s === null || s === "") return undefined;
  if (s === "true" || s === "1") return true;
  if (s === "false" || s === "0") return false;
  return undefined;
}
