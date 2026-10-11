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
