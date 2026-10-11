// ---------------------------------------------------------------------------
// listPendingSignals — read-only helper over WorkflowStorage primitives
// that surfaces EVERY workflow currently suspended on `waiting_for_signal`,
// for an "everything pending external delivery" inbox the dashboard can
// show. No parallel side-table: the workflow journal is authoritative.
//
// When the signal is an agent's `approve:<callId>` tool-approval gate, the
// tool metadata (toolName / toolInput) is read back from the
// `*-approval-<callId>-start` activity the agent journals right before
// suspending (`step-<n>-approval-<callId>-start` in agentAction). That needs the storage's `journal` capability; without it
// the row is still listed, with toolName / toolInput undefined.
// ---------------------------------------------------------------------------

import type { JournalStore, JournalEntry, WorkflowStorage } from "@promin/workflow";
import { hasCapability } from "@promin/workflow";
import { parseApprovalSignal } from "./approve-signal.ts";

const APPROVAL_START_SUFFIX = "-start";

export interface PendingSignal {
  readonly workflowId: string;
  readonly workflowName: string;
  readonly namespace: string | undefined;
  /** Step suspended on the signal — pass to `loadJournal` / `deliverSignal`. */
  readonly stepName: string;
  /** Exact signal name the step is waiting for. */
  readonly signalName: string;
  /**
   * True when the signal name matches the tool-call approval convention
   * (`approve:<callId>`). Consumers should branch on this, NOT on the
   * prefix string — see `parseApprovalSignal` in `./approve-signal.ts`.
   */
  readonly isApproval: boolean;
  readonly suspendedAt: Date | undefined;
  /**
   * JSON Schema snapshot the suspend point waited on, populated when the
   * workflow used `ctx.validatedSignal` / `ctx.approval`. Plain `ctx.signal`
   * callers leave this undefined (server falls back to pass-through). The
   * dashboard form renderer keys schema-driven UI off this field.
   */
  readonly signalJsonSchema?: unknown;
  /** Tool-call id parsed out of an `approve:<id>` signal — undefined otherwise. */
  readonly toolCallId?: string;
  /** Tool name (only set for `approve:` signals where the agent loop wrote it). */
  readonly toolName?: string;
  /** Tool input (only set for `approve:` signals where the agent loop wrote it). */
  readonly toolInput?: unknown;
  /**
   * Set when the approval belongs to an agent thread turn. Decide it through
   * the agent (`thread.resume`, the gateway's approve route), which records
   * it in the pending-approval index and continues the turn — never by
   * delivering the signal directly.
   */
  readonly agentThread?: AgentApprovalThread;
}

/** The thread an agent turn's approval belongs to. */
export interface AgentApprovalThread {
  readonly namespaceId: string;
  readonly threadId: string;
  readonly agentId?: string;
}

export interface ListPendingSignalsParams {
  readonly namespace?: string;
  /** Cap on suspended workflows scanned. Default 200. */
  readonly limit?: number;
}

/**
 * Every workflow currently suspended on any signal, most-recent first.
 * One entry per workflow — when a workflow has multiple
 * `waiting_for_signal` steps the first one is returned (agent loops only
 * suspend on one at a time; generic workflows typically do too).
 */
export async function listPendingSignals(
  storage: WorkflowStorage,
  params: ListPendingSignalsParams = {},
): Promise<PendingSignal[]> {
  const limit = params.limit ?? 200;
  const suspended = await storage.listWorkflows({
    status: "suspended",
    ...(params.namespace !== undefined && { namespace: params.namespace }),
    limit,
    orderBy: "startedAt",
    orderDir: "desc",
  });

  const journalStorage = hasCapability(storage, "journal") ? storage : undefined;

  const out: PendingSignal[] = [];
  for (const wf of suspended) {
    let waiting: {
      stepName: string;
      signalName: string;
      startedAt: Date | undefined;
      signalJsonSchema: unknown;
    } | null = null;
    for (const step of Object.values(wf.steps)) {
      if (step.status === "waiting_for_signal" && step.signalName !== undefined) {
        waiting = {
          stepName: step.stepName,
          signalName: step.signalName,
          startedAt: step.startedAt,
          signalJsonSchema: step.signalJsonSchema,
        };
        break;
      }
    }
    if (!waiting) continue;

    const parsed = parseApprovalSignal(waiting.signalName);
    const meta =
      parsed && journalStorage
        ? await readApprovalStartMetadata(
            journalStorage,
            wf.workflowId,
            waiting.stepName,
            parsed.toolCallId,
          )
        : { toolName: undefined, toolInput: undefined, agentThread: undefined };

    out.push({
      workflowId: wf.workflowId,
      workflowName: wf.workflowName,
      namespace: wf.namespace,
      stepName: waiting.stepName,
      signalName: waiting.signalName,
      isApproval: parsed !== null,
      suspendedAt: waiting.startedAt,
      ...(waiting.signalJsonSchema !== undefined && {
        signalJsonSchema: waiting.signalJsonSchema,
      }),
      ...(parsed && { toolCallId: parsed.toolCallId }),
      ...(meta.toolName !== undefined && { toolName: meta.toolName }),
      ...(meta.toolInput !== undefined && { toolInput: meta.toolInput }),
      ...(meta.agentThread !== undefined && { agentThread: meta.agentThread }),
    });
  }
  return out;
}

/**
 * The agent thread an `approve:<callId>` signal of `workflowId` belongs to,
 * or null when the run is not an agent thread turn (or the storage keeps no
 * journal). Signal-delivery routes use it to send agent approvals through
 * the agent instead of delivering the signal directly.
 */
export async function findAgentApprovalThread(params: {
  readonly storage: WorkflowStorage;
  readonly workflowId: string;
  readonly signalName: string;
}): Promise<AgentApprovalThread | null> {
  const parsed = parseApprovalSignal(params.signalName);
  if (!parsed || !hasCapability(params.storage, "journal")) return null;
  const wf = await params.storage.loadWorkflow(params.workflowId);
  if (!wf) return null;
  for (const step of Object.values(wf.steps)) {
    const meta = await readApprovalStartMetadata(
      params.storage,
      params.workflowId,
      step.stepName,
      parsed.toolCallId,
    );
    if (meta.agentThread) return meta.agentThread;
  }
  return null;
}

async function readApprovalStartMetadata(
  storage: JournalStore,
  workflowId: string,
  stepName: string,
  toolCallId: string,
): Promise<{
  toolName: string | undefined;
  toolInput: unknown;
  agentThread: AgentApprovalThread | undefined;
}> {
  let entries: JournalEntry[];
  try {
    entries = await storage.loadJournal({ workflowId, stepName });
  } catch {
    return { toolName: undefined, toolInput: undefined, agentThread: undefined };
  }
  const startName = `-approval-${toolCallId}${APPROVAL_START_SUFFIX}`;
  // The latest matching record: the step that is suspended now is the
  // last one to have asked for this approval.
  const entry = entries.findLast(
    (e) => e.activityName.endsWith(startName) && e.phase !== "pending" && e.exit?.tag === "Success",
  );
  const value = entry?.exit && "value" in entry.exit ? entry.exit.value : undefined;
  if (!value || typeof value !== "object") {
    return { toolName: undefined, toolInput: undefined, agentThread: undefined };
  }
  const { toolName, toolInput, thread } = value as {
    toolName?: unknown;
    toolInput?: unknown;
    thread?: { namespaceId?: unknown; threadId?: unknown; agentId?: unknown };
  };
  const agentThread =
    thread && typeof thread.namespaceId === "string" && typeof thread.threadId === "string"
      ? {
          namespaceId: thread.namespaceId,
          threadId: thread.threadId,
          ...(typeof thread.agentId === "string" && { agentId: thread.agentId }),
        }
      : undefined;
  return {
    toolName: typeof toolName === "string" ? toolName : undefined,
    toolInput,
    agentThread,
  };
}
