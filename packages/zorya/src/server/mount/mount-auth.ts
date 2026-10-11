// ---------------------------------------------------------------------------
// Auth planes — which credentials each path needs.
//
// Every plane is authenticated; `insecure: true` is the only way to run a
// plane without keys:
//   - /api/*                    operator keys (`apiKeys`), except the public
//                               routes below
//   - GET /api/health, /api/ready  public (liveness / readiness probes)
//   - /api/signal-tokens/:id/describe, /complete
//                               public; the route checks the token bearer
//   - /ws/worker, /rpc/worker, /api/advertisements, /api/worker-protocol/*
//                               worker keys (`remoteWorkers.apiKeys`)
//   - /rpc/storage              worker keys, scoped to `RUNNER_STORAGE_METHODS`;
//                               operator keys get the full storage contract
//   - /webhooks/*               per-source HMAC signature (checked by the route)
// Keys travel only in `Authorization: Bearer <key>` (the worker WebSocket
// upgrade included) and are compared in constant time.
// ---------------------------------------------------------------------------

import { Auth, bearerToken } from "../auth.ts";
import { jsonError } from "../router.ts";
import { ZoryaInsecureConfigError, type Logger } from "../server-config.ts";

/** Who may call `/rpc/storage`: the full contract or the runner's methods only. */
export type StorageCaller = "operator" | "worker";

export class AuthPlanes {
  /** Operator plane: `/api/*`. */
  readonly operator: Auth;
  /** Worker plane: `/ws/worker`, `/rpc/*`, advertisements, worker protocol. */
  readonly worker: Auth;

  constructor(params: {
    apiKeys: ReadonlyArray<string> | undefined;
    workerApiKeys: ReadonlyArray<string> | undefined;
  }) {
    this.operator = new Auth({ apiKeys: params.apiKeys });
    this.worker = new Auth({ apiKeys: params.workerApiKeys });
  }

  /**
   * Throw `ZoryaInsecureConfigError` unless every mounted plane has keys or
   * the operator opted into insecure mode; in insecure mode, warn about
   * each open plane.
   */
  assertConfigured(params: { remoteWorkers: boolean; insecure: boolean; logger: Logger }): void {
    const open: string[] = [];
    if (!this.operator.isEnabled) open.push("/api (apiKeys)");
    if (params.remoteWorkers && !this.worker.isEnabled) {
      open.push("worker planes /ws/worker, /rpc/* (remoteWorkers.apiKeys)");
    }
    if (open.length === 0) return;
    if (!params.insecure) {
      throw new ZoryaInsecureConfigError(
        `ZoryaServer: no API keys configured for ${open.join(" and ")}. ` +
          "Configure keys, or pass `insecure: true` (ZORYA_INSECURE=1 for the bundled server) " +
          "to run unauthenticated for local development.",
      );
    }
    params.logger.warn(
      "[zorya] WARNING: INSECURE MODE. These planes accept requests without credentials: " +
        `${open.join(", ")}. Anyone who can reach this port can read and change every run. ` +
        "Never expose this server outside local development.",
    );
  }

  /**
   * The 401 for a request that lacks the credentials its plane requires,
   * or undefined when it may proceed.
   */
  authenticate(params: { req: Request; path: string }): Response | undefined {
    const { req, path } = params;
    if (isPublicRoute({ method: req.method, path })) return undefined;
    if (path === "/rpc/storage") {
      return this.storageCaller(req) ? undefined : jsonError(401, "unauthorized_worker");
    }
    if (isWorkerPath(path)) {
      return this.worker.check(req) ? undefined : jsonError(401, "unauthorized_worker");
    }
    if (path.startsWith("/api/")) {
      return this.operator.check(req) ? undefined : jsonError(401, "unauthorized");
    }
    return undefined;
  }

  /**
   * Who is calling `/rpc/storage`: an operator (full contract), a worker
   * (runner methods only), or nobody allowed (undefined).
   */
  storageCaller(req: Request): StorageCaller | undefined {
    const token = bearerToken(req);
    if (this.operator.matches(token)) return "operator";
    if (this.worker.isEnabled) return this.worker.matches(token) ? "worker" : undefined;
    // Insecure: the worker plane is open. Everything is open when the
    // operator plane is too; otherwise anonymous callers get worker scope.
    return this.operator.isEnabled ? "worker" : "operator";
  }
}

/** Liveness and readiness probes: public, and answered while shutting down. */
export function isProbePath(path: string): boolean {
  return path === "/api/health" || path === "/api/ready";
}

/** Routes that need no Zorya key: the probes and the bearer-checked signal-token routes. */
function isPublicRoute(params: { method: string; path: string }): boolean {
  const { method, path } = params;
  if (method === "GET" && isProbePath(path)) return true;
  if (method === "GET" && /^\/api\/signal-tokens\/[^/]+\/describe$/.test(path)) return true;
  if (method === "POST" && /^\/api\/signal-tokens\/[^/]+\/complete$/.test(path)) return true;
  return false;
}

/** Paths on the worker plane, gated by worker keys (`/rpc/storage` is handled separately). */
function isWorkerPath(path: string): boolean {
  return (
    path.startsWith("/rpc/") ||
    path === "/api/advertisements" ||
    path.startsWith("/api/advertisements/") ||
    path.startsWith("/api/worker-protocol/")
  );
}
