// ---------------------------------------------------------------------------
// Child-turn wiring — how a delegated turn's approvals and events reach the
// turn at the root of the delegation chain.
//
// A sub-agent's tool that needs approval suspends the child turn, which
// parks every caller up to the root turn. The approval is carried up: the
// child records it in the ROOT turn's approval index (a thread turn's
// pending-approval index), and its approval and tool events go to the root
// turn's bus, so the user sees and decides it where they talk to the root
// agent. A child's tool-call ids can repeat its caller's, so every id the
// root sees is scoped to the child's workflow: `<child workflow id>:<id>`
// (`delegatedApprovalId`). Deeper children record and emit straight to the
// root, scoped to their own workflow.
// ---------------------------------------------------------------------------

import type { TurnApprovalIndex } from "../agent-action.ts";
import { SessionEventBus, type SessionEvent } from "../session-logger.ts";

/** Where a delegated child turn's approvals and events go. */
export interface ChildTurnWiring {
  /** Receives the child's approval and tool events, ids scoped to the child. */
  readonly bus?: SessionEventBus;
  /** Records the child's approvals in the root turn's index, ids scoped to the child. */
  readonly approvalIndex?: TurnApprovalIndex;
}

/** The id a root turn sees for a tool call of the delegated child `workflowId`. */
export function delegatedApprovalId(params: {
  readonly workflowId: string;
  readonly toolCallId: string;
}): string {
  return `${params.workflowId}:${params.toolCallId}`;
}

/**
 * The child's own tool-call id behind an approval id of the child
 * `workflowId` — the id its `approve:<id>` signal carries. An id that is
 * not scoped to `workflowId` is returned as is (the root turn's own call).
 */
export function childToolCallId(params: {
  readonly approvalId: string;
  readonly workflowId: string;
}): string {
  const prefix = `${params.workflowId}:`;
  return params.approvalId.startsWith(prefix)
    ? params.approvalId.slice(prefix.length)
    : params.approvalId;
}

/** Root bus / index behind a child's wiring, so deeper children skip the middle. */
const ROOT_BUS = new WeakMap<SessionEventBus, SessionEventBus>();
const ROOT_INDEX = new WeakMap<TurnApprovalIndex, TurnApprovalIndex>();

/** Events a child forwards to the root: the ones the user acts on or watches. */
const FORWARDED: ReadonlySet<SessionEvent["type"]> = new Set([
  "tool.start",
  "tool.progress",
  "tool.end",
  "approval.requested",
  "approval.decision",
]);

/** The wiring of the child `childWorkflowId` of a turn wired with `bus` / `approvalIndex`. */
export function childTurnWiring(params: {
  readonly childWorkflowId: string;
  readonly bus?: SessionEventBus;
  readonly approvalIndex?: TurnApprovalIndex;
}): ChildTurnWiring {
  const { childWorkflowId } = params;
  let bus: SessionEventBus | undefined;
  if (params.bus) {
    const rootBus = ROOT_BUS.get(params.bus) ?? params.bus;
    bus = new SessionEventBus();
    bus.subscribe((event) => {
      if (!FORWARDED.has(event.type) || !("toolCallId" in event)) return;
      rootBus.emit({
        ...event,
        toolCallId: delegatedApprovalId({
          workflowId: childWorkflowId,
          toolCallId: event.toolCallId,
        }),
      } as SessionEvent);
    });
    ROOT_BUS.set(bus, rootBus);
  }
  let approvalIndex: TurnApprovalIndex | undefined;
  if (params.approvalIndex) {
    const rootIndex = ROOT_INDEX.get(params.approvalIndex) ?? params.approvalIndex;
    approvalIndex = {
      ...(rootIndex.thread && { thread: rootIndex.thread }),
      requested: ({ workflowId, call }) =>
        rootIndex.requested({
          workflowId,
          call: { ...call, id: delegatedApprovalId({ workflowId, toolCallId: call.id }) },
        }),
    };
    ROOT_INDEX.set(approvalIndex, rootIndex);
  }
  return { ...(bus && { bus }), ...(approvalIndex && { approvalIndex }) };
}
