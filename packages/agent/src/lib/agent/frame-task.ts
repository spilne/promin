// ---------------------------------------------------------------------------
// frameTask — produces the user-message text the agent actually sees
// from an `AgentInput`. When `source` is set to anything other than
// `user`, prepends a single-line header that signals "this isn't a
// live user typing" to the model.
//
// Centralised here so changes to the wording (or new source kinds)
// don't need updates in N callers. The durable scheduler dispatch +
// webhook ingress + peer-agent delegation all funnel through this.
// ---------------------------------------------------------------------------

import type { AgentInput, AgentInputSource } from "./types.ts";

/**
 * Returns the framed task string for the model. Returns the bare task
 * unchanged when `input.source` is absent or `kind: "user"`.
 */
export function frameTask(input: AgentInput): string {
  if (!input.source || input.source.kind === "user") return input.task;
  return `${headerFor(input.source)} ${input.task}`;
}

function headerFor(source: Exclude<AgentInputSource, { kind: "user" }>): string {
  switch (source.kind) {
    case "scheduled": {
      const id = source.scheduleId ? ` ${source.scheduleId}` : "";
      return `[Scheduled trigger${id} at ${source.firedAt.toISOString()}]`;
    }
    case "webhook": {
      const origin = source.origin ? ` from ${source.origin}` : "";
      return `[Webhook event${origin} at ${source.receivedAt.toISOString()}]`;
    }
    case "agent":
      return `[Delegated by agent ${source.callerAgentId}]`;
  }
}
