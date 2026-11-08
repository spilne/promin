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

/** `createThread` found a thread with that id already stored in the namespace. */
export class ThreadExistsError extends Error {
  readonly _tag = "ThreadExists";
  readonly namespaceId: string;
  readonly threadId: string;

  constructor(params: { namespaceId: string; threadId: string }) {
    super(`Thread already exists: ${params.namespaceId}/${params.threadId}`);
    this.name = "ThreadExistsError";
    this.namespaceId = params.namespaceId;
    this.threadId = params.threadId;
  }
}

/**
 * A fork point that cannot yield a valid transcript: `atSeq` names no retained
 * message, or it falls between an assistant tool call and its result. Forks
 * are rejected, not snapped, so the caller picks the boundary it means.
 */
export class InvalidForkPointError extends RangeError {
  readonly _tag = "InvalidForkPoint";

  constructor(message: string) {
    super(message);
    this.name = "InvalidForkPointError";
  }
}
