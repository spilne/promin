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
// they guess the schedule id.
// ---------------------------------------------------------------------------

import { z } from "zod";
import { randomUUID } from "node:crypto";
import { tool } from "../tool.ts";
import type { AgentTool } from "../tool.ts";
import type { DurableScheduleConfig, SchedulerStorage } from "@promin/workflow";

export interface DurableSchedulerToolDeps {
  readonly storage: SchedulerStorage;
  /**
   * Caller scope. The tool stamps every schedule with this so listing
   * + cancellation can filter by thread; the firing dispatch hook
   * reads metadata to re-enter the agent for the same scope.
   */
  readonly scope: {
    readonly namespaceId: string;
    readonly resourceId?: string;
    readonly threadId?: string;
    /** Recipe id of the agent that's calling this tool. */
    readonly agentId: string;
  };
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
  const generateId =
    deps.generateId ??
    (() => `${deps.scope.threadId ?? deps.scope.agentId}-${randomUUID().slice(0, 8)}`);

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

  // Cap check — count this thread's existing agent-created schedules.
  if (deps.scope.threadId) {
    const existing = await deps.storage.listSchedules({
      namespace: deps.scope.namespaceId,
      limit: 1000,
    });
    const ours = existing.filter((s) => s.metadata?.threadId === deps.scope.threadId);
    if (ours.length >= maxPerThread) {
      return {
        ok: false,
        error: `you already have ${ours.length} schedules in this thread (cap ${maxPerThread}). Cancel one first.`,
      };
    }
  }

  const id = generateId();
  const targetAgentId = input.agentId ?? deps.scope.agentId;

  const config: DurableScheduleConfig = {
    id,
    ...(input.name !== undefined && { name: input.name }),
    namespace: deps.scope.namespaceId,
    ...(input.cron !== undefined && { cron: input.cron }),
    ...(input.intervalMs !== undefined && { intervalMs: input.intervalMs }),
    ...(input.rrule !== undefined && { rrule: input.rrule }),
    ...(input.timezone !== undefined && { timezone: input.timezone }),
    ...(input.startAt !== undefined && { startAt: new Date(input.startAt) }),
    ...(input.endAt !== undefined && { endAt: new Date(input.endAt) }),
    enabled: true,
    metadata: {
      // Dispatch contract — read by the host's scheduler-loop fireOverride.
      agentTrigger: true,
      agentId: targetAgentId,
      task: input.task,
      // Routing — the firing invocation runs against this scope.
      namespaceId: deps.scope.namespaceId,
      ...(deps.scope.resourceId !== undefined && { resourceId: deps.scope.resourceId }),
      ...(deps.scope.threadId !== undefined && { threadId: deps.scope.threadId }),
      // Provenance — the agent that created this schedule (may differ
      // from agentId if the agent scheduled a peer to fire).
      createdByAgent: deps.scope.agentId,
    },
  };

  await deps.storage.upsertSchedule(config);

  const schedule: CreateOk["schedule"] = { task: input.task, agentId: targetAgentId };
  if (input.cron !== undefined) schedule.cron = input.cron;
  if (input.intervalMs !== undefined) schedule.intervalMs = input.intervalMs;
  if (input.rrule !== undefined) schedule.rrule = input.rrule;
  return { ok: true, id, schedule };
}

async function handleList(deps: DurableSchedulerToolDeps): Promise<ListOk> {
  const all = await deps.storage.listSchedules({
    namespace: deps.scope.namespaceId,
    limit: 1000,
  });
  const ours = all.filter((s) => {
    if (!s.metadata?.agentTrigger) return false;
    if (deps.scope.threadId !== undefined && s.metadata.threadId !== deps.scope.threadId) {
      return false;
    }
    return true;
  });
  return {
    ok: true,
    schedules: ours.map((s) => ({
      id: s.id,
      name: s.name ?? null,
      task: typeof s.metadata?.task === "string" ? s.metadata.task : "?",
      agentId: typeof s.metadata?.agentId === "string" ? s.metadata.agentId : "?",
      enabled: s.enabled !== false,
      cron: s.cron ?? null,
      intervalMs: s.intervalMs ?? null,
      rrule: s.rrule ?? null,
    })),
  };
}

async function handleCancel(
  input: z.infer<typeof CANCEL_SCHEMA>,
  deps: DurableSchedulerToolDeps,
): Promise<{ ok: true } | ToolError> {
  const found = await deps.storage.loadSchedule(input.id);
  if (!found) {
    return { ok: false, error: `no schedule with id "${input.id}"` };
  }
  // Ownership check — caller can only cancel schedules from their own
  // thread. Defends against the LLM hallucinating ids that belong to
  // another tenant's row.
  if (deps.scope.threadId !== undefined && found.metadata?.threadId !== deps.scope.threadId) {
    return { ok: false, error: `schedule "${input.id}" doesn't belong to this thread` };
  }
  await deps.storage.deleteSchedule(input.id);
  return { ok: true };
}
