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
// vs `createSchedulerTools` (sibling file)
// ----------------------------------------
// `createSchedulerTools` is for in-process reminders that re-enter the
// SAME agent session via an onTick callback — fine for single-pod chat
// REPLs but not durable across restarts. This tool is the durable
// counterpart: schedules live in SchedulerStorage, survive restart,
// and dispatch via the host's scheduler-loop.
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
// Multi-tenant safety
// -------------------
// Every schedule is stamped with the caller's (namespaceId, resourceId,
// threadId, agentId) in `metadata`. `list` and `cancel` filter by
// thread so one tenant can't see / cancel another's schedules even if
// they guess the schedule id. The cap check below also relies on the
// client filtering correctly — wire `inProcessSchedulerClient` /
// `httpSchedulerClient` with the same scope as the tool.
// ---------------------------------------------------------------------------

import { z } from "zod";
import { randomUUID } from "node:crypto";
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
   * Override id generation. Default: `${threadId ?? agentId}-${uuid()}`
   * so ids are roughly thread-scoped + globally unique.
   */
  readonly generateId?: () => string;
  /**
   * Maximum schedules an agent can create per thread. Soft cap — the
   * tool returns an error to the model when hit so it can ask the user
   * which existing schedule to cancel. Default: 20.
   */
  readonly maxPerThread?: number;
}

const DEFAULT_MAX_PER_THREAD = 20;

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
      "Recipe id of the agent to invoke. Defaults to the calling agent (you re-fire yourself).",
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
  startAt: z.string().optional().describe("ISO timestamp. Don't fire before this time."),
  endAt: z.string().optional().describe("ISO timestamp. Stop firing after this time."),
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
  const maxPerThread = deps.maxPerThread ?? DEFAULT_MAX_PER_THREAD;
  const generateIdFor = (scope: SchedulerClientScope) =>
    deps.generateId ?? (() => `${scope.threadId ?? scope.agentId}-${randomUUID().slice(0, 8)}`);

  const schedulerCreate = tool({
    name: "schedulerCreate",
    description:
      "Schedule a recurring agent invocation. Each fire re-runs an agent with the given `task`. Survives server restart. Pass exactly one of `cron` (most common: '0 9 * * MON' = Monday 9am UTC; use `timezone` for other zones), `intervalMs`, or `rrule`. Defaults to firing the SAME agent that called this tool — pass `agentId` only to delegate to a peer.",
    parameters: CREATE_SCHEMA,
    execute: async (input, ctx): Promise<CreateOk | ToolError> => {
      const scope = resolveSchedulerScope(ctx?.scope);
      if ("error" in scope) return scope;
      return handleCreate(input, deps.getClient(scope), scope, generateIdFor(scope), maxPerThread);
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

// ---------------------------------------------------------------------------
// Unified `scheduler` tool — same handlers, different surface
// ---------------------------------------------------------------------------
//
// Big tool-tuned models (Claude, GPT-4) handle a 3-way discriminated union
// fine, and one tool name is a few hundred bytes lighter in the system
// prompt than three. For agents with already-large tool registries that's
// a real cost. This unified variant exists for those callers.
//
// Implementation note: this builds on the SAME `handleCreate` / `handleList`
// / `handleCancel` helpers as the split version. Adding a new command
// (e.g. `pause`) lands in one place — the handler — and both surfaces pick
// it up. No drift risk between the two API shapes.

const UNIFIED_CREATE_SCHEMA = CREATE_SCHEMA.extend({
  command: z.literal("create"),
});
const UNIFIED_LIST_SCHEMA = z.object({ command: z.literal("list") });
const UNIFIED_CANCEL_SCHEMA = CANCEL_SCHEMA.extend({
  command: z.literal("cancel"),
});

type UnifiedSchedulerInput =
  | z.infer<typeof UNIFIED_CREATE_SCHEMA>
  | z.infer<typeof UNIFIED_LIST_SCHEMA>
  | z.infer<typeof UNIFIED_CANCEL_SCHEMA>;
type UnifiedSchedulerOutput = CreateOk | ListOk | ToolError | { ok: true };

/**
 * Build a single `scheduler` tool that takes a `command` discriminator.
 * Prefer `createDurableSchedulerTools()` for small models — the unified
 * shape trips up sub-7B models on the discriminator pick.
 */
export function createDurableSchedulerTool(
  deps: DurableSchedulerToolDeps,
): AgentTool<UnifiedSchedulerInput, UnifiedSchedulerOutput> {
  const maxPerThread = deps.maxPerThread ?? DEFAULT_MAX_PER_THREAD;
  const generateIdFor = (scope: SchedulerClientScope) =>
    deps.generateId ?? (() => `${scope.threadId ?? scope.agentId}-${randomUUID().slice(0, 8)}`);

  return tool({
    name: "scheduler",
    description:
      "Create / list / cancel durable scheduled agent invocations. Pick `command`:\n" +
      "  - create({ task, cron|intervalMs|rrule, agentId?, name?, timezone?, startAt?, endAt? }) — schedule a recurring fire\n" +
      "  - list() — your schedules in this conversation\n" +
      "  - cancel({ id }) — delete a schedule\n\n" +
      "Pick exactly one of cron / intervalMs / rrule. Cron is most common ('0 9 * * MON' = Monday 9am UTC; pass `timezone` for other zones). Defaults to firing the SAME agent — pass `agentId` only to delegate to a peer.",
    parameters: z.discriminatedUnion("command", [
      UNIFIED_CREATE_SCHEMA,
      UNIFIED_LIST_SCHEMA,
      UNIFIED_CANCEL_SCHEMA,
    ]),
    execute: async (input, ctx): Promise<UnifiedSchedulerOutput> => {
      const scope = resolveSchedulerScope(ctx?.scope);
      if ("error" in scope) return scope;
      const client = deps.getClient(scope);
      switch (input.command) {
        case "create":
          return handleCreate(input, client, scope, generateIdFor(scope), maxPerThread);
        case "list":
          return handleList(client);
        case "cancel":
          return handleCancel(input, client);
      }
    },
  });
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

async function handleCreate(
  input: z.infer<typeof CREATE_SCHEMA>,
  client: SchedulerClient,
  scope: SchedulerClientScope,
  generateId: () => string,
  maxPerThread: number,
): Promise<CreateOk | ToolError> {
  const triggers = [input.cron, input.intervalMs, input.rrule].filter((v) => v !== undefined);
  if (triggers.length !== 1) {
    return {
      ok: false,
      error: "exactly one of cron / intervalMs / rrule must be provided",
    };
  }

  // Cap check — count this thread's existing agent-created schedules.
  // Relies on the client filtering by scope (it does — both
  // inProcessSchedulerClient and httpSchedulerClient honour threadId).
  if (scope.threadId) {
    const ours = await client.list();
    const agentOwned = ours.filter((s) => s.metadata?.agentTrigger === true);
    if (agentOwned.length >= maxPerThread) {
      return {
        ok: false,
        error: `you already have ${agentOwned.length} schedules in this thread (cap ${maxPerThread}). Cancel one first.`,
      };
    }
  }

  const id = generateId();
  const targetAgentId = input.agentId ?? scope.agentId;

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

  await client.create({
    id,
    ...(input.name !== undefined && { name: input.name }),
    ...(input.cron !== undefined && { cron: input.cron }),
    ...(input.intervalMs !== undefined && { intervalMs: input.intervalMs }),
    ...(input.rrule !== undefined && { rrule: input.rrule }),
    ...(input.timezone !== undefined && { timezone: input.timezone }),
    ...(input.startAt !== undefined && { startAt: new Date(input.startAt) }),
    ...(input.endAt !== undefined && { endAt: new Date(input.endAt) }),
    metadata,
  });

  const schedule: CreateOk["schedule"] = { task: input.task, agentId: targetAgentId };
  if (input.cron !== undefined) schedule.cron = input.cron;
  if (input.intervalMs !== undefined) schedule.intervalMs = input.intervalMs;
  if (input.rrule !== undefined) schedule.rrule = input.rrule;
  return { ok: true, id, schedule };
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
