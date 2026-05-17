// ---------------------------------------------------------------------------
// Audit log — durable record of elevated (cross-scope) tool invocations.
//
// `createElevatedTool` already enforces that every elevated tool calls
// `ctx.audit()` per invocation; the `AuditLogger` is what turns that call
// into a durable record a security review can read. Injected on the agent
// config — absent → audit calls are still enforced, just not persisted.
// ---------------------------------------------------------------------------

/**
 * One audit record describing a cross-scope action taken by an elevated
 * tool. Emitted once per `ctx.audit()` call inside the tool's `execute`
 * body — `action` / `target` / `meta` come from that call, the scope
 * fields and `toolName` are filled by the factory.
 */
export interface AuditEntry {
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
}

/** A persisted audit entry — `AuditEntry` plus the logger-assigned timestamp. */
export interface AuditRecord extends AuditEntry {
  /** Epoch ms the entry was recorded. Assigned by the logger. */
  readonly timestamp: number;
}

/**
 * Sink for elevated-tool audit entries. Injected on the agent config;
 * `createElevatedTool` emits one `record()` per `ctx.audit()` call once
 * the tool body completes. A `record()` rejection fails the tool call —
 * an unrecorded cross-scope action is a compliance gap, not a silent skip.
 */
export interface AuditLogger {
  record(entry: AuditEntry): Promise<void>;
}
