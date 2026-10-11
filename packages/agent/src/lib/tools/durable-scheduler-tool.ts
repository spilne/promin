// ---------------------------------------------------------------------------
// `createDurableSchedulerTools` — three flat agent tools (create / list /
// cancel) for durable schedules that re-fire an agent on cron / interval /
// rrule triggers.
//
// Why three tools, not one discriminated union
// --------------------------------------------
// Small tool-tuned models (qwen2.5:3b, llama3.2:3b) struggle with the
// "pick one of three" decision when fused with field extraction. We
// observed (on qwen2.5:3b in the live demo):
//   - "list my schedules"          → scheduler({}) — drops `command`
//   - "cancel abc123"              → scheduler({ id: "abc123" }) — drops it too
//   - "schedule X every 30s"       → scheduler({ command: "list", task: "...", cron: "..." }) — wrong command
//
// 7B+ models handle the union fine, but tool-name selection is what every
// LLM is best at. Three flat tools play to that strength: the model picks
// `schedulerCreate` / `schedulerList` / `schedulerCancel` by name; field
// extraction never has to share its budget with discriminator selection.
//
// Use case
// --------
// "Every hour, check Twitter for AI-related posts and summarise anything
// useful." The agent calls scheduler.create({ task: "Check Twitter ...",
// intervalMs: 3_600_000 }) and a row lands in SchedulerStorage. The
// host's scheduler-loop fires the tick later, the dispatch hook reads
// the metadata, and runs `agent.invoke({ task })` for the same scope —
// fresh turn, agent does its thing, the result posts back wherever the
// host wired it.
//
// Schedules live in SchedulerStorage, survive restart, and dispatch via
// the host's scheduler-loop.
//
// Talks to a `SchedulerClient` (interface in `./scheduler-client.ts`) —
// `inProcessSchedulerClient` for demos / single-process REPLs, or
// `httpSchedulerClient` for agents running outside the server process.
// The tool never touches `SchedulerStorage` directly so server-side
// details stay on the server.
//
// What the tool DOES NOT do
// -------------------------
// It writes schedule rows. It does NOT dispatch them — that's the
// host's scheduler-loop. To wire the firing path, the host registers a
// `fireOverride` (or the equivalent in their dispatch plumbing) that:
//
//   1. Reads metadata.agentTrigger — only acts on agent-targeted ticks
//   2. Resolves the recipe from the agent registry by metadata.agentId
//   3. Materialises the agent + .withScope({ namespaceId, resourceId })
//   4. Calls .invoke({ task: metadata.task })
//
// The metadata fields stamped here are the contract the host implements
// against. See `dispatchAgentSchedule` (sibling helper) for the
// reference implementation.
//
// Safety
// ------
// Every schedule is stamped with the caller's (namespaceId, resourceId,
// threadId, agentId) in `metadata`. `list` and `cancel` filter by
// thread so one tenant can't see / cancel another's schedules even if
// they guess the schedule id — wire `inProcessSchedulerClient` /
// `httpSchedulerClient` with the same scope as the tool.
//
// A schedule keeps acting long after the conversation that made it, so
// `schedulerCreate`:
//   - asks for human approval by default (`requireApproval`);
//   - refuses cadences faster than `minIntervalMs` (cron seconds fields
//     and SECONDLY rrules included) and unparseable triggers / dates;
//   - fires only the calling agent unless `authorizeDelegation` allows a
//     peer — the model must not pick which agent runs in this scope;
//   - caps schedules per thread atomically: ids are `maxPerThread` fixed
//     slots derived from the scope, and the store inserts a slot id only
//     if it is free, so concurrent creates can't overshoot the cap.
// ---------------------------------------------------------------------------

import { createHash } from "node:crypto";
import { z } from "zod";
import { SystemWallClock, type WallClock } from "@promin/workflow";
import { shortestFireGapMs, validateScheduleConfig } from "@promin/workflow/scheduler";
import { tool } from "../tool.ts";
import type { AgentTool, ToolScope } from "../tool.ts";
import type { SchedulerClient, SchedulerClientScope } from "./scheduler-client.ts";

export interface DurableSchedulerToolDeps {
  /**
   * Per-call backend client factory. Receives the live caller scope
   * (namespace, resource, thread, agentId) read from `ctx.scope` and
   * returns a `SchedulerClient` bound to that scope. Hosts wrap their
   * concrete impl once:
   *
   *   getClient: (scope) => inProcessSchedulerClient({ storage, scope })
   *   getClient: (scope) => httpSchedulerClient({ baseUrl, scope })
   *
   * Per-call construction is cheap (the client is just a thin wrapper)
   * and lets the tool be a normal user-registered tool — no per-tool
   * special-casing inside the agent runtime.
   */
  readonly getClient: (scope: SchedulerClientScope) => SchedulerClient;
  /**
   * Maximum schedules per thread (per namespace + resource when the
   * scope has no thread). Enforced atomically by the store. Default: 20.
   */
  readonly maxPerThread?: number;
  /**
   * Shortest allowed gap between two fires, in ms. Applies to intervals,
   * cron expressions (incl. a seconds field) and RRULEs. Default: 60 000.
   */
  readonly minIntervalMs?: number;
  /** Ask a human before each `schedulerCreate`. Default: `true`. */
  readonly requireApproval?: boolean;
  /**
   * Decide whether the calling agent may schedule a different agent
   * (`agentId` other than its own). Without this hook delegation is
   * refused: the model never chooses which agent runs in a tenant's scope
   * on its own.
   */
  readonly authorizeDelegation?: (params: {
    readonly scope: SchedulerClientScope;
    readonly targetAgentId: string;
  }) => boolean | Promise<boolean>;
  /** Time source for validating `startAt` / `endAt`. Default: `SystemWallClock`. */
  readonly clock?: WallClock;
}

const DEFAULT_MAX_PER_THREAD = 20;
const DEFAULT_MIN_INTERVAL_MS = 60_000;

const CREATE_SCHEMA = z.object({
  task: z
    .string()
    .min(1)
    .describe(
      "What you want to do when the schedule fires — phrased as a fresh task. Will be replayed as if the user typed it. Example: 'Check Twitter for AI-related posts and summarize anything useful.'",
    ),
  agentId: z
    .string()
    .optional()
    .describe(
      "Recipe id of the agent to invoke. Defaults to the calling agent (you re-fire yourself). Other agents only when the host allows it.",
    ),
  cron: z.string().optional().describe("Cron expression, e.g. '0 14 * * 5' (Friday 14:00)."),
  intervalMs: z
    .number()
    .int()
    .positive()
    .optional()
    .describe("Fixed interval in ms. Mutually exclusive with cron + rrule."),
  rrule: z.string().optional().describe("iCalendar RRULE string (RFC 5545)."),
  name: z.string().optional().describe("Human-readable label shown in the schedule list."),
  timezone: z.string().optional().describe("IANA timezone for cron / rrule."),
  startAt: z
    .string()
    .optional()
    .describe("ISO 8601 timestamp with timezone, in the future. Don't fire before this time."),
  endAt: z
    .string()
    .optional()
    .describe("ISO 8601 timestamp with timezone, in the future. Stop firing after this time."),
});

const LIST_SCHEMA = z.object({});

const CANCEL_SCHEMA = z.object({
  id: z.string().min(1).describe("Id returned from schedulerCreate."),
});

interface CreateOk {
  ok: true;
  id: string;
  schedule: {
    task: string;
    agentId: string;
    cron?: string;
    intervalMs?: number;
    rrule?: string;
  };
}
interface ToolError {
  ok: false;
  error: string;
}
interface ListOk {
  ok: true;
  schedules: Array<{
    id: string;
    name: string | null;
    task: string;
    agentId: string;
    enabled: boolean;
    cron: string | null;
    intervalMs: number | null;
    rrule: string | null;
    /** ISO timestamp of last fire. Null when never fired. */
    lastFiredAt: string | null;
    /** How many times this schedule has fired. */
    tickCount: number;
  }>;
}

type CreateInput = z.infer<typeof CREATE_SCHEMA>;
type ListInput = z.infer<typeof LIST_SCHEMA>;
type CancelInput = z.infer<typeof CANCEL_SCHEMA>;

export interface DurableSchedulerTools {
  schedulerCreate: AgentTool<CreateInput, CreateOk | ToolError>;
  schedulerList: AgentTool<ListInput, ListOk>;
  schedulerCancel: AgentTool<CancelInput, { ok: true } | ToolError>;
}

/**
 * Build the three durable-scheduler tools. Register all three on any
 * agent that needs to manage its own recurring tasks:
 *
 * ```ts
 * const tools = createDurableSchedulerTools({ getClient: ... });
 * // recipe.backend.tools = ["schedulerCreate", "schedulerList", "schedulerCancel"]
 * agent: { tools: { ...tools, ...otherTools } }
 * ```
 */
export function createDurableSchedulerTools(deps: DurableSchedulerToolDeps): DurableSchedulerTools {
  const policy: CreatePolicy = {
    maxPerThread: deps.maxPerThread ?? DEFAULT_MAX_PER_THREAD,
    minIntervalMs: deps.minIntervalMs ?? DEFAULT_MIN_INTERVAL_MS,
    clock: deps.clock ?? SystemWallClock,
    ...(deps.authorizeDelegation !== undefined && {
      authorizeDelegation: deps.authorizeDelegation,
    }),
  };

  const schedulerCreate = tool({
    name: "schedulerCreate",
    description:
      "Schedule a recurring agent invocation. Each fire re-runs an agent with the given `task`. Survives server restart. Pass exactly one of `cron` (most common: '0 9 * * MON' = Monday 9am UTC; use `timezone` for other zones), `intervalMs`, or `rrule`. " +
      `Fires at most once every ${Math.round(policy.minIntervalMs / 1_000)} s. Defaults to firing the SAME agent that called this tool.`,
    parameters: CREATE_SCHEMA,
    requireApproval: deps.requireApproval ?? true,
    execute: async (input, ctx): Promise<CreateOk | ToolError> => {
      const scope = resolveSchedulerScope(ctx?.scope);
      if ("error" in scope) return scope;
      return handleCreate({ input, client: deps.getClient(scope), scope, policy });
    },
  });

  const schedulerList = tool({
    name: "schedulerList",
    description:
      "List durable schedules you've created in this conversation. Returns id, task, cadence (cron / intervalMs / rrule), enabled flag, last fire time, and tick count. No arguments — call as `schedulerList({})`.",
    parameters: LIST_SCHEMA,
    execute: async (_input, ctx): Promise<ListOk> => {
      const scope = resolveSchedulerScope(ctx?.scope);
      if ("error" in scope) return { ok: true, schedules: [] };
      return handleList(deps.getClient(scope));
    },
  });

  const schedulerCancel = tool({
    name: "schedulerCancel",
    description:
      "Cancel a durable schedule by its id. The id is the value returned from `schedulerCreate` (or shown in `schedulerList`).",
    parameters: CANCEL_SCHEMA,
    execute: async (input, ctx): Promise<{ ok: true } | ToolError> => {
      const scope = resolveSchedulerScope(ctx?.scope);
      if ("error" in scope) return scope;
      return handleCancel(input, deps.getClient(scope));
    },
  });

  return { schedulerCreate, schedulerList, schedulerCancel };
}

interface CreatePolicy {
  readonly maxPerThread: number;
  readonly minIntervalMs: number;
  readonly clock: WallClock;
  readonly authorizeDelegation?: DurableSchedulerToolDeps["authorizeDelegation"];
}

/**
 * Validate that the agent runtime populated `ctx.scope` with the fields
 * the scheduler tool needs (namespaceId + agentId at minimum). Returns
 * the typed scope on success, or a ToolError pointing at the specific
 * missing piece so test harnesses + misconfigured hosts get a clear
 * message instead of a silent crash inside `getClient`.
 */
function resolveSchedulerScope(scope: ToolScope | undefined): SchedulerClientScope | ToolError {
  if (!scope) {
    return {
      ok: false,
      error:
        "scheduler tool: ctx.scope is missing. The host must run this tool through an agent runtime that populates ctx.scope.",
    };
  }
  if (!scope.namespaceId) {
    return { ok: false, error: "scheduler tool: ctx.scope.namespaceId is required." };
  }
  if (!scope.agentId) {
    return { ok: false, error: "scheduler tool: ctx.scope.agentId is required." };
  }
  return {
    namespaceId: scope.namespaceId,
    ...(scope.resourceId !== undefined && { resourceId: scope.resourceId }),
    ...(scope.threadId !== undefined && { threadId: scope.threadId }),
    agentId: scope.agentId,
  };
}

/**
 * The fixed schedule ids one scope may occupy: `maxPerThread` slots under
 * a prefix derived from (namespace, resource, thread).
 */
function slotIds(params: { scope: SchedulerClientScope; maxPerThread: number }): string[] {
  const { scope } = params;
  const key = createHash("sha256")
    .update(JSON.stringify([scope.namespaceId, scope.resourceId ?? null, scope.threadId ?? null]))
    .digest("hex")
    .slice(0, 16);
  return Array.from({ length: params.maxPerThread }, (_, i) => `agent-schedule-${key}-${i + 1}`);
}

/** Parse an ISO timestamp the model sent; `undefined` when absent, an error string when bad. */
function parseInstant(params: {
  field: string;
  raw: string | undefined;
}): Date | string | undefined {
  if (params.raw === undefined) return undefined;
  const iso = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(:\d{2}(\.\d+)?)?(Z|[+-]\d{2}:?\d{2})$/i;
  const at = new Date(params.raw);
  if (!iso.test(params.raw) || Number.isNaN(at.getTime())) {
    return `${params.field} must be an ISO 8601 timestamp with a timezone, e.g. 2026-05-01T09:00:00Z`;
  }
  return at;
}

async function handleCreate(params: {
  input: CreateInput;
  client: SchedulerClient;
  scope: SchedulerClientScope;
  policy: CreatePolicy;
}): Promise<CreateOk | ToolError> {
  const { input, client, scope, policy } = params;
  const fail = (error: string): ToolError => ({ ok: false, error });

  const triggers = [input.cron, input.intervalMs, input.rrule].filter((v) => v !== undefined);
  if (triggers.length !== 1)
    return fail("exactly one of cron / intervalMs / rrule must be provided");

  if (input.timezone !== undefined) {
    try {
      new Intl.DateTimeFormat("en-US", { timeZone: input.timezone });
    } catch {
      return fail(`unknown timezone "${input.timezone}"`);
    }
  }

  const now = policy.clock.now();
  const startAt = parseInstant({ field: "startAt", raw: input.startAt });
  if (typeof startAt === "string") return fail(startAt);
  const endAt = parseInstant({ field: "endAt", raw: input.endAt });
  if (typeof endAt === "string") return fail(endAt);
  if (startAt && startAt.getTime() <= now.getTime()) {
    return fail("startAt must be in the future; omit it to start now");
  }
  if (endAt && endAt.getTime() <= now.getTime()) return fail("endAt must be in the future");
  if (startAt && endAt && endAt.getTime() <= startAt.getTime()) {
    return fail("endAt must be after startAt");
  }

  const trigger = {
    ...(input.cron !== undefined && { cron: input.cron }),
    ...(input.intervalMs !== undefined && { intervalMs: input.intervalMs }),
    ...(input.rrule !== undefined && { rrule: input.rrule }),
    ...(input.timezone !== undefined && { timezone: input.timezone }),
  };
  try {
    validateScheduleConfig({ id: "candidate", ...trigger });
  } catch (err) {
    return fail(err instanceof Error ? err.message : String(err));
  }
  const gap = shortestFireGapMs({ config: trigger, from: startAt ?? now });
  if (gap !== null && gap < policy.minIntervalMs) {
    return fail(
      `this schedule would fire every ${Math.round(gap / 1_000)} s; the minimum is ${Math.round(policy.minIntervalMs / 1_000)} s`,
    );
  }

  const targetAgentId = input.agentId ?? scope.agentId;
  if (targetAgentId !== scope.agentId) {
    const allowed = policy.authorizeDelegation
      ? await policy.authorizeDelegation({ scope, targetAgentId })
      : false;
    if (!allowed) {
      return fail(
        `you may not schedule agent "${targetAgentId}"; omit agentId to schedule yourself`,
      );
    }
  }

  const metadata: Record<string, unknown> = {
    // Dispatch contract — read by the host's scheduler-loop fireOverride.
    agentTrigger: true,
    agentId: targetAgentId,
    task: input.task,
    // Routing — the firing invocation runs against this scope.
    namespaceId: scope.namespaceId,
    ...(scope.resourceId !== undefined && { resourceId: scope.resourceId }),
    ...(scope.threadId !== undefined && { threadId: scope.threadId }),
    // Provenance — the agent that created this schedule (may differ
    // from agentId if the agent scheduled a peer to fire).
    createdByAgent: scope.agentId,
  };

  // Claim the first free slot. The listing only skips slots known to be
  // taken; the store's insert-if-absent is what decides, so two concurrent
  // creates never land in one slot and never exceed `maxPerThread`.
  const taken = new Set((await client.list()).map((s) => s.id));
  for (const id of slotIds({ scope, maxPerThread: policy.maxPerThread })) {
    if (taken.has(id)) continue;
    const result = await client.create({
      id,
      ...(input.name !== undefined && { name: input.name }),
      ...trigger,
      ...(startAt !== undefined && { startAt }),
      ...(endAt !== undefined && { endAt }),
      metadata,
    });
    if (!result.created) continue;
    const schedule: CreateOk["schedule"] = { task: input.task, agentId: targetAgentId };
    if (input.cron !== undefined) schedule.cron = input.cron;
    if (input.intervalMs !== undefined) schedule.intervalMs = input.intervalMs;
    if (input.rrule !== undefined) schedule.rrule = input.rrule;
    return { ok: true, id: result.id, schedule };
  }
  return fail(
    `you already have ${policy.maxPerThread} schedules in this thread (cap ${policy.maxPerThread}). Cancel one first.`,
  );
}

async function handleList(client: SchedulerClient): Promise<ListOk> {
  const all = await client.list();
  const ours = all.filter((s) => s.metadata?.agentTrigger === true);
  return {
    ok: true,
    schedules: ours.map((s) => ({
      id: s.id,
      name: s.name,
      task: typeof s.metadata?.task === "string" ? s.metadata.task : "?",
      agentId: typeof s.metadata?.agentId === "string" ? s.metadata.agentId : "?",
      enabled: s.enabled,
      cron: s.cron,
      intervalMs: s.intervalMs,
      rrule: s.rrule,
      lastFiredAt: s.lastFiredAt,
      tickCount: s.tickCount,
    })),
  };
}

async function handleCancel(
  input: z.infer<typeof CANCEL_SCHEMA>,
  client: SchedulerClient,
): Promise<{ ok: true } | ToolError> {
  // The client enforces scope ownership + returns a typed error when
  // the id doesn't belong to this scope (or doesn't exist).
  const result = await client.cancel(input.id);
  if (result.ok) return { ok: true };
  return { ok: false, error: result.error };
}
