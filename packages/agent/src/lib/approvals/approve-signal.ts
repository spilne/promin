// ---------------------------------------------------------------------------
// Approval-signal wire format — the one place the `approve:<callId>`
// convention lives.
//
// `agentLoop` registers a `ctx.signal("approve:<toolCallId>")` wait when a
// tool with `requireApproval: true` is invoked. listPendingApprovals
// filters on this prefix; listPendingSignals tags rows as approvals via
// the same parse; the dashboard's /signals UI keys its shortcut Approve /
// Reject buttons off the result; the agent loop's resume path constructs
// the name. Five callers, one string — centralised here so a rename is a
// one-file change.
// ---------------------------------------------------------------------------

/** Wire-format prefix every tool-call approval signal name starts with. */
export const APPROVE_SIGNAL_PREFIX = "approve:";

/** True when `signalName` follows the tool-call approval convention. */
export function isApprovalSignal(signalName: string): boolean {
  return (
    signalName.startsWith(APPROVE_SIGNAL_PREFIX) && signalName.length > APPROVE_SIGNAL_PREFIX.length
  );
}

/** Compose the signal name `agentLoop` registers for a tool-call approval. */
export function composeApprovalSignal(toolCallId: string): string {
  return `${APPROVE_SIGNAL_PREFIX}${toolCallId}`;
}

/**
 * Pull the tool-call id out of an approval signal name. Returns null when
 * the name doesn't match the convention (wrong prefix or empty id).
 */
export function parseApprovalSignal(signalName: string): { toolCallId: string } | null {
  if (!signalName.startsWith(APPROVE_SIGNAL_PREFIX)) return null;
  const toolCallId = signalName.slice(APPROVE_SIGNAL_PREFIX.length);
  if (toolCallId.length === 0) return null;
  return { toolCallId };
}
