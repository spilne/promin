// ---------------------------------------------------------------------------
// Keyset cursors of `MemoryStore.listThreads`. A cursor names the last
// thread of a page by its sort key (last activity or creation time, per the
// listing's order) and its id, so the next page starts right after it
// whatever was inserted or deleted meanwhile — no OFFSET scan.
// ---------------------------------------------------------------------------

import { Buffer } from "node:buffer";
import type { ListThreadsParams, ThreadSummary } from "./types.ts";

/** Position a `listThreads` page continues after. */
export interface ThreadCursorPosition {
  /** The thread's sort key: `lastActiveAt` or `createdAt`, per the order. */
  readonly key: number;
  readonly threadId: string;
}

type ThreadOrder = NonNullable<ListThreadsParams["order"]>;

/**
 * The cursor of the page after `thread`, for a listing in `order`
 * (default `lastActiveDesc`). Pass it as `ListThreadsParams.cursor`.
 */
export function nextThreadCursor(params: {
  readonly thread: Pick<ThreadSummary, "threadId" | "lastActiveAt" | "createdAt">;
  readonly order?: ThreadOrder;
}): string {
  const { thread } = params;
  const key =
    (params.order ?? "lastActiveDesc") === "lastActiveDesc"
      ? thread.lastActiveAt
      : thread.createdAt;
  return Buffer.from(JSON.stringify([key, thread.threadId])).toString("base64url");
}

/** The position a cursor names; `null` when absent or malformed (the first page). */
export function parseThreadCursor(cursor: string | undefined): ThreadCursorPosition | null {
  if (!cursor) return null;
  try {
    const value: unknown = JSON.parse(Buffer.from(cursor, "base64url").toString("utf8"));
    if (
      Array.isArray(value) &&
      value.length === 2 &&
      typeof value[0] === "number" &&
      Number.isFinite(value[0]) &&
      typeof value[1] === "string"
    ) {
      return { key: value[0], threadId: value[1] };
    }
  } catch {
    // Not a cursor: start from the first page.
  }
  return null;
}

/**
 * Whether a thread with `key` / `threadId` sorts after the cursor position
 * in `order` (ties on the key go by thread id, ascending).
 */
export function isAfterThreadCursor(params: {
  readonly position: ThreadCursorPosition;
  readonly key: number;
  readonly threadId: string;
  readonly order: ThreadOrder;
}): boolean {
  const { position, key, threadId, order } = params;
  if (key === position.key) return threadId > position.threadId;
  return order === "createdAsc" ? key > position.key : key < position.key;
}
