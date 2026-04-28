// ---------------------------------------------------------------------------
// `createDurableSchedulerTool` — agent-callable scheduler that re-fires
// the agent (or a peer agent) on a cron / interval / rrule trigger.
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
import type { AgentTool } from "../tool.ts";
import type { SchedulerClient } from "./scheduler-client.ts";

export interface DurableSchedulerToolDeps {
  /**
   * Backend-agnostic scheduler view. The client carries the caller scope
   * (namespace, resource, thread, agentId) — the tool reads it to stamp
   * metadata, default fields, and apply per-thread caps.
   */
  readonly client: SchedulerClient;
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
  command: z.literal("create"),
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

const LIST_SCHEMA = z.object({ command: z.literal("list") });

const CANCEL_SCHEMA = z.object({
  command: z.literal("cancel"),
  id: z.string().min(1).describe("Id returned from create."),
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
  }>;
}

type Output = CreateOk | ListOk | ToolError | { ok: true };

type SchedulerInput =
  | z.infer<typeof CREATE_SCHEMA>
  | z.infer<typeof LIST_SCHEMA>
  | z.infer<typeof CANCEL_SCHEMA>;

export function createDurableSchedulerTool(
  deps: DurableSchedulerToolDeps,
): AgentTool<SchedulerInput, Output> {
  const maxPerThread = deps.maxPerThread ?? DEFAULT_MAX_PER_THREAD;
  const scope = deps.client.scope;
  const generateId =
    deps.generateId ?? (() => `${scope.threadId ?? scope.agentId}-${randomUUID().slice(0, 8)}`);

  return tool({
    name: "scheduler",
    description:
      "Create / list / cancel durable scheduled tasks. Each schedule re-fires an agent with a task on a cron / interval / rrule trigger. Survives server restart. Three commands:\n" +
      "  - create({ task, agentId?, cron|intervalMs|rrule, timezone?, startAt?, endAt?, name? }) — schedule a recurring agent invocation\n" +
      "  - list() — schedules you've created in this conversation\n" +
      "  - cancel({ id }) — delete a schedule\n\n" +
      "Pick exactly one of cron / intervalMs / rrule. Cron is most common ('0 9 * * MON' for Monday 9am UTC; pass `timezone` for other zones). Defaults to firing the SAME agent — pass `agentId` only if you want to delegate to a peer.",
    parameters: z.discriminatedUnion("command", [CREATE_SCHEMA, LIST_SCHEMA, CANCEL_SCHEMA]),
    execute: async (input): Promise<Output> => {
      switch (input.command) {
        case "create":
          return await handleCreate(input, deps, generateId, maxPerThread);
        case "list":
          return await handleList(deps);
        case "cancel":
          return await handleCancel(input, deps);
      }
    },
  });
}

async function handleCreate(
  input: z.infer<typeof CREATE_SCHEMA>,
  deps: DurableSchedulerToolDeps,
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

  const scope = deps.client.scope;

  // Cap check — count this thread's existing agent-created schedules.
  // Relies on the client filtering by scope (it does — both
  // inProcessSchedulerClient and httpSchedulerClient honour threadId).
  if (scope.threadId) {
    const ours = await deps.client.list();
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

  await deps.client.create({
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

async function handleList(deps: DurableSchedulerToolDeps): Promise<ListOk> {
  const all = await deps.client.list();
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
    })),
  };
}

async function handleCancel(
  input: z.infer<typeof CANCEL_SCHEMA>,
  deps: DurableSchedulerToolDeps,
): Promise<{ ok: true } | ToolError> {
  // The client enforces scope ownership + returns a typed error when
  // the id doesn't belong to this scope (or doesn't exist).
  const result = await deps.client.cancel(input.id);
  if (result.ok) return { ok: true };
  return { ok: false, error: result.error };
}
