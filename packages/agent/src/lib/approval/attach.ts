// ---------------------------------------------------------------------------
// `attachApprovalStorage` — bridge between SessionEventBus and
// ApprovalStorage. Hosts that want a queryable inbox + audit trail call
// this once per session (or once at the loop level if they want to
// capture across all sessions on a runner) and the storage tracks every
// approval request + decision automatically.
//
// Why a bridge instead of pushing storage into the agent loop directly
// -------------------------------------------------------------------
// The loop is already fully replay-safe via the journaled approval
// activity. Adding a second write inside the activity would mean
// approving a dual-write contract (what if storage fails but the
// activity succeeded?). Keeping storage as a side-table downstream of
// the bus event keeps the loop simple — if storage drops a write, the
// journal is still authoritative. The trade-off: storage is eventually
// consistent with the journal (microseconds in practice).
// ---------------------------------------------------------------------------

import type { SessionEventBus, SessionEvent } from "../session-logger.ts";
import type { ApprovalStorage, CreateApprovalRequestInput } from "./types.ts";

export interface AttachApprovalStorageConfig {
  readonly bus: SessionEventBus;
  readonly storage: ApprovalStorage;
  /**
   * Workflow id this session is running under — used to label persisted
   * requests so operators know which run to deliver the signal to.
   */
  readonly workflowId: string;
  /**
   * Tenant the session is bound to. Persisted on every row for the
   * "list approvals in namespace X" query.
   */
  readonly namespaceId: string;
  /** Optional resource id (user / agent instance) for the inbox label. */
  readonly resourceId?: string;
  /**
   * Build the operator-facing summary from the bus event. Default:
   * `${toolName}(${JSON.stringify(input)})` — fine for debugging,
   * worth overriding to render a domain-specific string.
   */
  readonly summarize?: (event: ApprovalRequestedEvent) => string;
  /**
   * Default expiry per request in ms. Hosts that want a wall-clock
   * "decide within 24h or it expires" cap set this; otherwise pending
   * rows live forever until decided.
   */
  readonly defaultExpiryMs?: number;
  /**
   * Operator id used when the decision came in via the agent loop's
   * inline `onApprovalRequired` hook (which doesn't know who decided).
   * Defaults to `"system:hook"` — recognisable in audit reports.
   */
  readonly hookDecidedBy?: string;
  /** Optional logger for unexpected storage failures. */
  readonly onError?: (err: unknown, event: SessionEvent) => void;
}

type ApprovalRequestedEvent = Extract<SessionEvent, { type: "approval.requested" }>;

export interface AttachApprovalStorageHandle {
  /** Stop subscribing. Idempotent. */
  detach(): void;
}

/**
 * Subscribe to approval.* bus events and mirror them into ApprovalStorage.
 * Returns a handle whose `detach()` unsubscribes — call on session.close()
 * if you want clean teardown.
 *
 * The wire is one-directional: bus → storage. Decisions must still be
 * delivered via the existing `session.approve(callId)` / `reject` path
 * (which signals the workflow). This helper just records what's
 * happening, it doesn't drive it.
 */
export function attachApprovalStorage(
  config: AttachApprovalStorageConfig,
): AttachApprovalStorageHandle {
  // Track the current toolName + input per callId so the decision event
  // (which doesn't carry toolName) can look back. We also need this to
  // build the summary at create() time — bus events are split between
  // approval.requested (has toolName) and tool.start (has input).
  const pending = new Map<string, { toolName: string; input: unknown }>();

  // tool.start fires before approval.requested for the same callId,
  // capturing the input. We index by toolCallId so we can join.
  const onToolStart = (e: Extract<SessionEvent, { type: "tool.start" }>) => {
    // SessionEventBody doesn't carry the toolCallId in tool.start —
    // only the name. So we fall back to the toolName as the key, which
    // is fine for one-tool-call-per-turn but ambiguous when the model
    // calls the same tool twice in one turn. The approval.requested
    // event DOES carry toolCallId, so we re-key on that when the
    // request fires.
    pending.set(`tool:${e.name}`, { toolName: e.name, input: e.input });
  };

  const onRequested = async (e: ApprovalRequestedEvent) => {
    const cached = pending.get(`tool:${e.toolName}`);
    pending.delete(`tool:${e.toolName}`);
    pending.set(`call:${e.toolCallId}`, {
      toolName: e.toolName,
      input: cached?.input,
    });
    const summary =
      config.summarize?.(e) ?? `${e.toolName}(${cached ? JSON.stringify(cached.input) : "?"})`;
    const now = Date.now();
    const input: CreateApprovalRequestInput = {
      requestId: requestIdFor(config.workflowId, e.toolCallId),
      workflowId: config.workflowId,
      signalName: `approve:${e.toolCallId}`,
      namespaceId: config.namespaceId,
      ...(config.resourceId !== undefined ? { resourceId: config.resourceId } : {}),
      toolName: e.toolName,
      toolInput: cached?.input,
      summary,
      ...(config.defaultExpiryMs !== undefined ? { expiresAt: now + config.defaultExpiryMs } : {}),
    };
    try {
      await config.storage.create(input);
    } catch (err) {
      config.onError?.(err, e);
    }
  };

  const onDecision = async (e: Extract<SessionEvent, { type: "approval.decision" }>) => {
    pending.delete(`call:${e.toolCallId}`);
    try {
      await config.storage.decide({
        requestId: requestIdFor(config.workflowId, e.toolCallId),
        decision: e.approved ? "approved" : "rejected",
        decidedBy: config.hookDecidedBy ?? "system:hook",
      });
    } catch (err) {
      // Decision conflict (already decided) is benign — happens when the
      // operator delivered the signal manually and the bus event is just
      // confirming. Errors below that bar bubble through onError.
      const code = (err as { code?: string }).code;
      if (code === "approval_decision_conflict") return;
      config.onError?.(err, e);
    }
  };

  const unsubscribe = config.bus.subscribe((ev: SessionEvent) => {
    switch (ev.type) {
      case "tool.start":
        onToolStart(ev);
        break;
      case "approval.requested":
        void onRequested(ev);
        break;
      case "approval.decision":
        void onDecision(ev);
        break;
    }
  });

  let detached = false;
  return {
    detach() {
      if (detached) return;
      detached = true;
      unsubscribe();
    },
  };
}

/**
 * Compose a stable requestId from (workflowId, toolCallId). Lets the
 * `decide` call resolve the row without the operator tracking a
 * separate id. Same shape every time → idempotent retries.
 */
export function requestIdFor(workflowId: string, toolCallId: string): string {
  return `${workflowId}::${toolCallId}`;
}
