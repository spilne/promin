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
//
// A path that matches a route under another method answers 405 with an
// `Allow` header; the static fallback only sees paths no route matches.
// ---------------------------------------------------------------------------

import {
  SECURITY_ACTIONS,
  type SecurityPermissionFor,
  type SecurityResourceKind,
  type SecurityResourceRef,
} from "./security/security.ts";

export type Handler = (
  req: Request,
  params: Record<string, string>,
) => Response | Promise<Response>;

export interface RouteTarget<K extends SecurityResourceKind = SecurityResourceKind> {
  readonly resource: SecurityResourceRef<K>;
  /** A resolver may preload the protected object so the handler need not read it twice. */
  readonly value?: unknown;
}

export interface RouteTargetRequest {
  readonly request: Request;
  readonly params: Readonly<Record<string, string>>;
}

export type RouteTargetResolver<K extends SecurityResourceKind> = (
  request: RouteTargetRequest,
) => RouteTarget<K> | Promise<RouteTarget<K>>;

/** A protected declaration keeps its permission and resolved resource kind correlated. */
export type ProtectedRouteAccess = {
  readonly [K in SecurityResourceKind]: {
    readonly kind: "protected";
    readonly permission: SecurityPermissionFor<K>;
    readonly resolveTarget: RouteTargetResolver<K>;
  };
}[SecurityResourceKind];

/**
 * Every declared route is either protected by RBAC or names the other
 * credential boundary that admits it. Nothing is implicitly public.
 */
export type RouteAccess =
  | ProtectedRouteAccess
  | {
      readonly kind: "public";
      readonly purpose: "lifecycle" | "login" | "static-ui";
    }
  | { readonly kind: "worker" }
  | { readonly kind: "signed-webhook" }
  | { readonly kind: "signal-token" };

export type ResolvedProtectedRouteAccess = {
  readonly [K in SecurityResourceKind]: {
    readonly kind: "protected";
    readonly permission: SecurityPermissionFor<K>;
    readonly target: RouteTarget<K>;
  };
}[SecurityResourceKind];

export type ResolvedRouteAccess =
  | ResolvedProtectedRouteAccess
  | Exclude<RouteAccess, ProtectedRouteAccess>;

export type DeclaredRouteHandler = (
  req: Request,
  params: Record<string, string>,
  access: ResolvedRouteAccess,
) => Response | Promise<Response>;

export interface RouteDefinition {
  readonly method: string;
  readonly path: string;
  readonly access: RouteAccess;
  readonly handler: DeclaredRouteHandler;
}

interface CompiledRoute {
  method: string;
  pattern: string;
  regex: RegExp;
  keys: string[];
  handler: Handler | DeclaredRouteHandler;
  access?: RouteAccess;
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
  /** Field-level problems, returned as `issues` (request validation). */
  readonly issues?: ReadonlyArray<ApiIssue>;

  constructor(params: {
    status: number;
    code: string;
    message?: string;
    issues?: ReadonlyArray<ApiIssue>;
  }) {
    super(params.message ?? params.code);
    this.name = "HttpError";
    this.status = params.status;
    this.code = params.code;
    if (params.issues !== undefined) this.issues = params.issues;
  }
}

/** One field-level validation problem: where (`path`) and what (`message`). */
export interface ApiIssue {
  readonly path: string;
  readonly message: string;
}

export class Router {
  private readonly routes: CompiledRoute[] = [];
  private readonly logger: RouterErrorLogger;
  private staticHandler?: Handler | DeclaredRouteHandler;
  private staticAccess?: RouteAccess;
  private notFoundHandler: Handler = () => notFound();

  constructor(config: RouterConfig = {}) {
    this.logger = config.logger ?? console;
  }

  /**
   * Register a route with mandatory, runtime-validated access metadata.
   * New production routes should use this API.
   */
  route(definition: RouteDefinition): this {
    validateRouteDefinition(definition);
    return this.compile({
      method: definition.method,
      pattern: definition.path,
      handler: definition.handler,
      access: definition.access,
    });
  }

  /**
   * Compatibility registration for route groups not migrated yet. It is
   * deliberately visible to `uncoveredRoutes`/`assertSecurityCoverage`.
   */
  addLegacy(method: string, pattern: string, handler: Handler): this {
    return this.compile({ method, pattern, handler });
  }

  /** @deprecated Migrate to `route`; this registration is reported as uncovered. */
  add(method: string, pattern: string, handler: Handler): this {
    return this.addLegacy(method, pattern, handler);
  }

  private compile(params: {
    readonly method: string;
    readonly pattern: string;
    readonly handler: Handler | DeclaredRouteHandler;
    readonly access?: RouteAccess;
  }): this {
    const keys: string[] = [];
    const regexSrc = params.pattern.replace(/:([^/]+)/g, (_m, k) => {
      keys.push(k);
      return "([^/]+)";
    });
    const regex = new RegExp(`^${regexSrc}$`);
    this.routes.push({
      method: params.method.toUpperCase(),
      pattern: params.pattern,
      regex,
      keys,
      handler: params.handler,
      ...(params.access !== undefined && { access: params.access }),
    });
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
  put(pattern: string, handler: Handler): this {
    return this.add("PUT", pattern, handler);
  }

  /** Sorted route identities still using compatibility registration. */
  uncoveredRoutes(): ReadonlyArray<string> {
    const routes = this.routes
      .filter((route) => route.access === undefined)
      .map((route) => `${route.method} ${route.pattern}`);
    if (this.staticHandler && this.staticAccess === undefined) routes.push("STATIC *");
    return routes.sort();
  }

  /** Refuse startup once strict coverage is enabled and any route is undeclared. */
  assertSecurityCoverage(): void {
    const uncovered = this.uncoveredRoutes();
    if (uncovered.length === 0) return;
    throw new Error(`Routes missing access declarations:\n${uncovered.join("\n")}`);
  }

  /** Fallback handler when no route matches (e.g. to serve static UI). */
  setStatic(handler: Handler): this {
    this.staticHandler = handler;
    this.staticAccess = undefined;
    return this;
  }

  /** Declared static fallback; normally explicit public `static-ui` access. */
  setStaticRoute(access: RouteAccess, handler: DeclaredRouteHandler): this {
    validateRouteAccess(access);
    this.staticHandler = handler;
    this.staticAccess = access;
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
    const allowed = new Set<string>();
    for (const r of this.routes) {
      const m = r.regex.exec(path);
      if (!m) continue;
      if (r.method !== req.method) {
        allowed.add(r.method);
        continue;
      }
      const params: Record<string, string> = {};
      for (let i = 0; i < r.keys.length; i++) {
        const decoded = safeDecode(m[i + 1]!);
        if (decoded === undefined) return badRequest("bad_path", "Malformed URL encoding");
        params[r.keys[i]!] = decoded;
      }
      if (r.access === undefined) return await (r.handler as Handler)(req, params);
      const access = await resolveRouteAccess({
        access: r.access,
        request: req,
        params,
      });
      return await (r.handler as DeclaredRouteHandler)(req, params, access);
    }
    if (allowed.size > 0) return methodNotAllowed([...allowed]);
    if (this.staticHandler) {
      if (this.staticAccess === undefined) return await (this.staticHandler as Handler)(req, {});
      const access = await resolveRouteAccess({
        access: this.staticAccess,
        request: req,
        params: {},
      });
      return await (this.staticHandler as DeclaredRouteHandler)(req, {}, access);
    }
    return await this.notFoundHandler(req, {});
  }
}

function validateRouteDefinition(definition: RouteDefinition): void {
  if (!definition || typeof definition !== "object") {
    throw new TypeError("Route definition must be an object");
  }
  if (typeof definition.method !== "string" || definition.method.length === 0) {
    throw new TypeError("Route definition requires a method");
  }
  if (typeof definition.path !== "string" || !definition.path.startsWith("/")) {
    throw new TypeError("Route definition requires an absolute path");
  }
  if (typeof definition.handler !== "function") {
    throw new TypeError("Route definition requires a handler");
  }
  validateRouteAccess(definition.access);
}

function validateRouteAccess(access: RouteAccess): void {
  if (!access || typeof access !== "object" || typeof access.kind !== "string") {
    throw new TypeError("Route definition requires access metadata");
  }
  if (access.kind === "protected") {
    const permission = (access as Partial<ProtectedRouteAccess>).permission;
    if (!permission || typeof permission !== "object") {
      throw new TypeError("Protected route requires a permission");
    }
    const actions = Object.hasOwn(SECURITY_ACTIONS, permission.resource)
      ? SECURITY_ACTIONS[permission.resource as SecurityResourceKind]
      : undefined;
    if (!actions?.includes(permission.action as never)) {
      throw new TypeError("Protected route permission is not in the approved vocabulary");
    }
    if (typeof (access as Partial<ProtectedRouteAccess>).resolveTarget !== "function") {
      throw new TypeError("Protected route requires a target resolver");
    }
    return;
  }
  if (access.kind === "public") {
    if (!(["lifecycle", "login", "static-ui"] as const).includes(access.purpose)) {
      throw new TypeError("Public route requires an approved purpose");
    }
    return;
  }
  if (
    access.kind !== "worker" &&
    access.kind !== "signed-webhook" &&
    access.kind !== "signal-token"
  ) {
    throw new TypeError("Unknown route access kind");
  }
}

async function resolveRouteAccess(params: {
  readonly access: RouteAccess;
  readonly request: Request;
  readonly params: Readonly<Record<string, string>>;
}): Promise<ResolvedRouteAccess> {
  if (params.access.kind !== "protected") return params.access;
  const target = await params.access.resolveTarget({
    request: params.request,
    params: params.params,
  });
  if (
    !target ||
    typeof target !== "object" ||
    !target.resource ||
    target.resource.kind !== params.access.permission.resource ||
    typeof target.resource.namespaceId !== "string" ||
    target.resource.namespaceId.length === 0
  ) {
    throw new Error("Protected route target does not match its permission");
  }
  return {
    kind: "protected",
    permission: params.access.permission,
    target,
  } as ResolvedProtectedRouteAccess;
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
  if (err instanceof HttpError) {
    return json(err.status, {
      error: err.code,
      message: err.message,
      ...(err.issues !== undefined && { issues: err.issues }),
    });
  }
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
// Response helpers. Errors share one envelope: `{ error, message?, issues? }`.
// Success statuses: 200 for reads and actions (with the resulting state),
// 201 + the resource for creates, 202 for work started asynchronously, 204
// for deletes.
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

/**
 * 202 for work that was started, not finished. `location` names the
 * resource to poll for the outcome.
 */
export function accepted(body: unknown, location?: string): Response {
  return new Response(JSON.stringify(body), {
    status: 202,
    headers: {
      "content-type": "application/json",
      ...(location !== undefined && { location }),
    },
  });
}

/** 405 naming the methods the path does accept. */
export function methodNotAllowed(allow: ReadonlyArray<string>): Response {
  return new Response(
    JSON.stringify({
      error: "method_not_allowed",
      message: `Allowed: ${allow.join(", ")}`,
    }),
    {
      status: 405,
      headers: { "content-type": "application/json", allow: allow.join(", ") },
    },
  );
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
