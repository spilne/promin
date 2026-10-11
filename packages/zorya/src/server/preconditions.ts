// ---------------------------------------------------------------------------
// Optimistic concurrency — entity tags and `If-Match`.
//
// Every editable registry resource (agents, roles, skills, fragments,
// schedules) answers its GET / POST / PATCH with an `etag` header. A PATCH
// that sends `If-Match` is applied only while the stored resource still
// carries that tag; otherwise it answers 412 `precondition_failed` and the
// caller re-reads before retrying. A PATCH without `If-Match` (or with
// `If-Match: *`) applies unconditionally.
//
// Tags are weak (`W/"…"`) and derived from the resource's canonical JSON,
// so any stored change — content or `updatedAt` — yields a new tag, and two
// stores that return the same row with differently ordered keys agree.
// ---------------------------------------------------------------------------

import { createHash } from "node:crypto";
import { HttpError } from "./router.ts";

/** Weak entity tag over the canonical JSON of `value`. */
export function contentEtag(value: unknown): string {
  const digest = createHash("sha256").update(canonicalJson(value)).digest("base64url");
  return `W/"${digest.slice(0, 27)}"`;
}

/**
 * Throw 412 `precondition_failed` when the request carries `If-Match` and
 * none of its tags is `etag` (`*` matches any existing resource). Weak and
 * strong forms of the same tag compare equal.
 */
export function assertIfMatch(params: { req: Request; etag: string; what: string }): void {
  const header = params.req.headers.get("if-match");
  if (header === null) return;
  const wanted = opaqueTag(params.etag);
  const tags = header.split(",").map((t) => t.trim());
  if (tags.some((t) => t === "*" || opaqueTag(t) === wanted)) return;
  throw new HttpError({
    status: 412,
    code: "precondition_failed",
    message: `The ${params.what} changed since it was read; fetch it again and retry.`,
  });
}

/** Set the `etag` header on `res` and return it. */
export function withEtag(res: Response, etag: string): Response {
  res.headers.set("etag", etag);
  return res;
}

function opaqueTag(tag: string): string {
  return tag.startsWith("W/") ? tag.slice(2) : tag;
}

function canonicalJson(value: unknown): string {
  return JSON.stringify(value, (_key, v: unknown) => {
    if (v === null || typeof v !== "object" || Array.isArray(v)) return v;
    const sorted: Record<string, unknown> = {};
    for (const k of Object.keys(v).sort()) sorted[k] = (v as Record<string, unknown>)[k];
    return sorted;
  });
}
