// ---------------------------------------------------------------------------
// Router — minimal pattern-matching router for Bun.serve.
//
// Path patterns support colon-prefixed parameters: "/api/runs/:id". The
// matched params are passed to the handler alongside the Request.
//
// `handle` is the server's crash barrier: a handler that throws never
// reaches Bun's error page. `HttpError` maps to its status; anything else
// becomes a JSON 500 `{ error: "internal_error", requestId }` with the
// detail logged under that request id and never returned to the caller.
// ---------------------------------------------------------------------------

export type Handler = (
  req: Request,
  params: Record<string, string>,
) => Response | Promise<Response>;

interface CompiledRoute {
  method: string;
  regex: RegExp;
  keys: string[];
  handler: Handler;
}

/** Sink for errors the crash barrier catches. */
export interface RouterErrorLogger {
  error(message: string, ...args: unknown[]): void;
}

export interface RouterConfig {
  /** Where unexpected handler errors are reported. Default: console. */
  logger?: RouterErrorLogger;
}

/**
 * Throw from a handler to answer with a JSON error of a given status. The
 * `message` is returned to the caller, so it must not carry internals.
 */
export class HttpError extends Error {
  readonly status: number;
  readonly code: string;

  constructor(params: { status: number; code: string; message?: string }) {
    super(params.message ?? params.code);
    this.name = "HttpError";
    this.status = params.status;
    this.code = params.code;
  }
}

export class Router {
  private readonly routes: CompiledRoute[] = [];
  private readonly logger: RouterErrorLogger;
  private staticHandler?: Handler;
  private notFoundHandler: Handler = () => notFound();

  constructor(config: RouterConfig = {}) {
    this.logger = config.logger ?? console;
  }

  add(method: string, pattern: string, handler: Handler): this {
    const keys: string[] = [];
    const regexSrc = pattern.replace(/:([^/]+)/g, (_m, k) => {
      keys.push(k);
      return "([^/]+)";
    });
    const regex = new RegExp(`^${regexSrc}$`);
    this.routes.push({ method: method.toUpperCase(), regex, keys, handler });
    return this;
  }

  get(pattern: string, handler: Handler): this {
    return this.add("GET", pattern, handler);
  }
  post(pattern: string, handler: Handler): this {
    return this.add("POST", pattern, handler);
  }
  delete(pattern: string, handler: Handler): this {
    return this.add("DELETE", pattern, handler);
  }
  patch(pattern: string, handler: Handler): this {
    return this.add("PATCH", pattern, handler);
  }

  /** Fallback handler when no route matches (e.g. to serve static UI). */
  setStatic(handler: Handler): this {
    this.staticHandler = handler;
    return this;
  }

  setNotFound(handler: Handler): this {
    this.notFoundHandler = handler;
    return this;
  }

  async handle(req: Request): Promise<Response> {
    try {
      return await this.dispatch(req);
    } catch (err) {
      return errorResponse({ err, req, logger: this.logger });
    }
  }

  private async dispatch(req: Request): Promise<Response> {
    const path = new URL(req.url).pathname;
    for (const r of this.routes) {
      if (r.method !== req.method) continue;
      const m = r.regex.exec(path);
      if (!m) continue;
      const params: Record<string, string> = {};
      for (let i = 0; i < r.keys.length; i++) {
        const decoded = safeDecode(m[i + 1]!);
        if (decoded === undefined) return badRequest("bad_path", "Malformed URL encoding");
        params[r.keys[i]!] = decoded;
      }
      return await r.handler(req, params);
    }
    if (this.staticHandler) return await this.staticHandler(req, {});
    return await this.notFoundHandler(req, {});
  }
}

/**
 * Map a thrown value to a response: `HttpError` keeps its status and code;
 * anything else is logged under a fresh request id and answered with a
 * bare JSON 500.
 */
export function errorResponse(params: {
  err: unknown;
  /** The failing request, when known; its method and path are logged. */
  req?: Request;
  logger: RouterErrorLogger;
}): Response {
  const { err, req, logger } = params;
  if (err instanceof HttpError) return jsonError(err.status, err.code, err.message);
  const requestId = crypto.randomUUID();
  let where = "";
  if (req) {
    let path = req.url;
    try {
      path = new URL(req.url).pathname;
    } catch {
      // keep the raw url
    }
    where = ` ${req.method} ${path}`;
  }
  try {
    logger.error(`[zorya] unhandled error requestId=${requestId}${where}`, err);
  } catch {
    // a broken logger must not break the barrier
  }
  return internal(requestId);
}

function safeDecode(raw: string): string | undefined {
  try {
    return decodeURIComponent(raw);
  } catch {
    return undefined;
  }
}

// ---------------------------------------------------------------------------
// Response helpers. Errors share one envelope: `{ error, message? }`.
// ---------------------------------------------------------------------------

export function json(status: number, body: unknown): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

export function jsonError(status: number, error: string, message?: string): Response {
  return json(status, { error, message });
}

/** 200 with a JSON body. */
export function ok(body: unknown): Response {
  return json(200, body);
}

/** 201 with the created resource. */
export function created(body: unknown): Response {
  return json(201, body);
}

/** 204, no body. */
export function noContent(): Response {
  return new Response(null, { status: 204 });
}

export function badRequest(error = "bad_request", message?: string): Response {
  return jsonError(400, error, message);
}

export function notFound(error = "not_found", message?: string): Response {
  return jsonError(404, error, message);
}

export function conflict(error = "conflict", message?: string): Response {
  return jsonError(409, error, message);
}

export function payloadTooLarge(maxBytes: number): Response {
  return jsonError(413, "payload_too_large", `Request body exceeds ${maxBytes} bytes`);
}

/** 500 that names only the request id the detail was logged under. */
export function internal(requestId: string): Response {
  return json(500, { error: "internal_error", requestId });
}

/**
 * Parse a JSON request body. Returns `null` when the content type is not
 * JSON or the body is not valid JSON; callers answer that with a 400.
 */
export async function readJson<T>(req: Request): Promise<T | null> {
  const ct = req.headers.get("content-type") ?? "";
  if (!ct.includes("application/json")) return null;
  try {
    return (await req.json()) as T;
  } catch {
    return null;
  }
}

/**
 * Parse an integer query parameter clamped to `[min, max]`. Missing or
 * non-numeric input yields `undefined`, so the caller's default applies.
 */
export function parseIntParam(params: {
  raw: string | null;
  min: number;
  max: number;
}): number | undefined {
  const { raw, min, max } = params;
  if (raw === null || raw === "") return undefined;
  const n = Number.parseInt(raw, 10);
  if (!Number.isFinite(n)) return undefined;
  return Math.min(max, Math.max(min, n));
}

/** ISO string for a valid date, `null` for a missing or invalid one. */
export function isoOrNull(d: Date | null | undefined): string | null {
  if (!d) return null;
  const t = d.getTime();
  return Number.isFinite(t) ? d.toISOString() : null;
}
