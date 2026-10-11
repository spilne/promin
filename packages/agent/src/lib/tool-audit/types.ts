// ---------------------------------------------------------------------------
// Audit log — durable record of elevated (cross-scope) tool invocations.
//
// `createElevatedTool` already enforces that every elevated tool calls
// `ctx.audit()` per invocation; the `ToolAuditLogger` is what turns that call
// into a durable record a security review can read. Injected on the agent
// config — absent → audit calls are still enforced, just not persisted.
// ---------------------------------------------------------------------------

/**
 * Stage of an audited action:
 * - `intent` — written when the tool calls `ctx.audit()`, before it acts.
 * - `completed` — the tool body returned.
 * - `failed` — the tool body threw (`error` carries the message).
 *
 * An `intent` with no outcome record means the process died mid-action:
 * the action may or may not have happened.
 */
export type ToolAuditPhase = "intent" | "completed" | "failed";

/**
 * One audit record describing a cross-scope action taken by an elevated
 * tool. Each `ctx.audit()` call writes an `intent` record at once and an
 * outcome record (`completed` / `failed`) when the body settles —
 * `action` / `target` / `meta` come from that call, the scope fields,
 * `toolName` and `toolCallId` are filled by the factory.
 */
export interface ToolAuditEntry {
  /** Caller namespace the elevated tool ran under. */
  readonly namespaceId: string;
  /** Caller resource (user / persona). */
  readonly resourceId: string;
  /** Recipe id of the agent the tool executed under, when known. */
  readonly agentId?: string;
  /** Tool that performed the action. */
  readonly toolName: string;
  /** What cross-scope action was taken — free-form, tool-defined. */
  readonly action: string;
  /** Optional target of the action (an id, a path, a scope key). */
  readonly target?: string;
  /** Optional structured detail. */
  readonly meta?: Readonly<Record<string, unknown>>;
  /** Model tool-call id the action ran for; links intent and outcome records. */
  readonly toolCallId?: string;
  readonly phase: ToolAuditPhase;
  /** Error message of a `failed` outcome. */
  readonly error?: string;
}

/** A persisted audit entry — `ToolAuditEntry` plus the logger-assigned timestamp. */
export interface ToolAuditRecord extends ToolAuditEntry {
  /** Epoch ms the entry was recorded. Assigned by the logger. */
  readonly timestamp: number;
}

/**
 * Sink for elevated-tool audit entries. Injected on the agent config;
 * `createElevatedTool` records an `intent` per `ctx.audit()` call before
 * the action and an outcome record per intent after the body. A
 * `record()` rejection fails the tool call — an unrecorded cross-scope
 * action is a compliance gap, not a silent skip.
 */
export interface ToolAuditLogger {
  record(entry: ToolAuditEntry): Promise<void>;
}
