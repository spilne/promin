// ---------------------------------------------------------------------------
// Typed errors raised by `Agent` backends, so callers (gateways, tools)
// can branch on the failure kind instead of matching message text.
// ---------------------------------------------------------------------------

/**
 * The thread does not exist in the caller's scope: it was never created
 * (and `createIfMissing: false` was passed), or it exists but belongs to a
 * different resource. The two cases are deliberately indistinguishable so
 * a caller cannot probe for other resources' thread ids.
 */
export class ThreadNotFoundError extends Error {
  readonly _tag = "ThreadNotFound";
  readonly namespaceId: string;
  readonly threadId: string;

  constructor(params: { namespaceId: string; threadId: string }) {
    super(`Thread not found: ${params.namespaceId}/${params.threadId}`);
    this.name = "ThreadNotFoundError";
    this.namespaceId = params.namespaceId;
    this.threadId = params.threadId;
  }
}

/**
 * A turn key (or run id) was reused for a different task. The key names one
 * turn: a retry must send the same task, a new message needs a new key.
 */
export class TurnKeyConflictError extends Error {
  readonly _tag = "TurnKeyConflict";
  readonly workflowId: string;

  constructor(params: { workflowId: string }) {
    super(
      `Turn "${params.workflowId}" already ran with a different task; ` +
        "use a new turn key (or run id) for a new message.",
    );
    this.name = "TurnKeyConflictError";
    this.workflowId = params.workflowId;
  }
}
