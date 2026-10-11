// ---------------------------------------------------------------------------
// Request validation — Zod-checked bodies, query strings and path params.
//
// Every parser throws `HttpError`, which the router's crash barrier turns
// into the shared error envelope:
//   400 { error: "invalid_json", message }           body is not JSON
//   415 { error: "unsupported_media_type", message } body is not sent as JSON
//   400 { error: "invalid_request", message, issues } schema mismatch
// An empty body is `undefined` to the schema, so a route whose body is
// optional accepts it and one that needs a body rejects it — a body is
// never silently replaced with `{}`. The schemas live in `api-types.ts`.
// ---------------------------------------------------------------------------

import type { z } from "zod";
import { HttpError, type ApiIssue } from "./router.ts";

/** Parse a JSON request body against `schema`. */
export async function parseBody<S extends z.ZodTypeAny>(params: {
  req: Request;
  schema: S;
}): Promise<z.output<S>> {
  const { req, schema } = params;
  const text = await req.text();
  let raw: unknown;
  if (text.trim().length > 0) {
    const contentType = req.headers.get("content-type") ?? "";
    if (!/^application\/(?:[\w.-]+\+)?json\b/i.test(contentType.trim())) {
      throw new HttpError({
        status: 415,
        code: "unsupported_media_type",
        message: "Send the body as application/json",
      });
    }
    try {
      raw = JSON.parse(text);
    } catch {
      throw new HttpError({ status: 400, code: "invalid_json", message: "Body is not valid JSON" });
    }
  }
  return check({ schema, value: raw });
}

/**
 * Parse the query string against `schema`. Each parameter arrives as a
 * string (the last one wins when repeated); schemas coerce.
 */
export function parseQuery<S extends z.ZodTypeAny>(params: {
  req: Request;
  schema: S;
}): z.output<S> {
  const url = new URL(params.req.url);
  const raw: Record<string, string> = {};
  url.searchParams.forEach((value, key) => {
    raw[key] = value;
  });
  return check({ schema: params.schema, value: raw });
}

/** Parse decoded path params against `schema`. */
export function parseParams<S extends z.ZodTypeAny>(params: {
  params: Record<string, string>;
  schema: S;
}): z.output<S> {
  return check({ schema: params.schema, value: params.params });
}

function check<S extends z.ZodTypeAny>(params: { schema: S; value: unknown }): z.output<S> {
  const result = params.schema.safeParse(params.value);
  if (result.success) return result.data;
  const issues: ApiIssue[] = result.error.issues.map((i) => ({
    path: i.path.join("."),
    message: i.message,
  }));
  throw new HttpError({
    status: 400,
    code: "invalid_request",
    message: issues.map((i) => (i.path ? `${i.path}: ${i.message}` : i.message)).join("; "),
    issues,
  });
}
