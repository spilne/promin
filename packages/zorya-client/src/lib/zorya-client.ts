// ---------------------------------------------------------------------------
// ZoryaClient — connects a worker process / admin tool to a remote Zorya
// server. Wraps RemoteWorkflowStorage + RemoteWorkerRegistry behind a single
// ergonomic object, knows how to advertise workflow definitions, and speaks
// the start-queue worker protocol.
//
// Requests are retried, with capped backoff, while the server can't be
// reached (connection refused) or answers that it is shutting down (503):
// neither reached a route, so nothing was applied. A request whose
// connection dropped mid-flight (the server died while answering) may have
// been applied, so it is resent only when applying it twice is proven
// harmless — durably, by the store's contract, not by a response cache:
// storage calls per `isRetrySafeStorageCall` (`RemoteWorkflowStorage`
// stamps the attempt / chunk ids that make locks and appends repeatable),
// registry heartbeats, the start-queue protocol (claims carry a `claimId`),
// advertisements, and triggers with a `workflowId` (fixed before sending).
// A worker's writes therefore ride out a Zorya restart instead of failing
// the run.
// ---------------------------------------------------------------------------

import {
  SystemWallClock,
  createWorkflowRunner,
  type WallClock,
  type Workflow,
  type WorkflowHandle,
  type WorkflowRunner,
  type WorkflowStorage,
} from "@promin/workflow";
import type { WorkerRegistry } from "@promin/workflow/distributed";
import {
  RemoteWorkerRegistry,
  RemoteWorkflowStorage,
  isRetrySafeStorageCall,
  isRetrySafeWorkerCall,
  type FetchLike,
} from "@promin/workflow-remote";

export interface ZoryaClientConfig {
  /**
   * Base URL of the Zorya server, e.g. "https://zorya.internal:4100".
   * Client appends "/rpc/storage", "/rpc/worker", and "/api/advertisements"
   * as needed.
   */
  url: string;
  /**
   * Worker API key (one of the server's `remoteWorkers.apiKeys`). Sent as
   * `Authorization: Bearer …` on `/rpc/*`, the advertisement and
   * worker-protocol routes, and the `/ws/worker` upgrade.
   */
  apiKey?: string;
  /**
   * Operator API key (one of the server's `apiKeys`) for the `/api` calls
   * `start` and `startByName` make. Default: `apiKey`, which works when the
   * server lists the same key on both planes.
   */
  operatorApiKey?: string;
  /** Custom fetch (for tests or in-process wiring). Defaults to globalThis.fetch. */
  fetch?: FetchLike;
  /**
   * Retry of requests cut off by a server outage (connection refused or
   * dropped, 503 while shutting down). Default: up to 30 s in total, 100 ms
   * doubling to 2 s between attempts. `{ maxElapsedMs: 0 }` disables it.
   */
  retry?: { maxElapsedMs?: number; initialDelayMs?: number; maxDelayMs?: number };
  /** Time source for the retry backoff. Default: `SystemWallClock`. */
  clock?: WallClock;
}

export class ZoryaClient {
  readonly url: string;
  /**
   * Bearer token used for HTTP-RPC and the persistent WS upgrade. Public
   * so peers (e.g. `WorkerControlSocket`) can authenticate against the
   * same key without re-passing it through every layer.
   */
  readonly apiKey?: string;
  readonly storage: WorkflowStorage;
  readonly workerRegistry: WorkerRegistry;
  private readonly fetch: FetchLike;
  private readonly headers: Record<string, string>;
  private readonly operatorHeaders: Record<string, string>;
  // Built lazily on first start() / startByName() call. The runner is just
  // a thin wrapper around storage for handle construction — no orchestration
  // happens client-side; the server's worker fleet runs the workflow.
  private _runner?: WorkflowRunner;

  constructor(config: ZoryaClientConfig) {
    const base = config.url.replace(/\/$/, "");
    this.url = base;
    this.apiKey = config.apiKey;
    this.fetch = retryingFetch({
      fetch: config.fetch ?? ((req) => globalThis.fetch(req)),
      clock: config.clock ?? SystemWallClock,
      maxElapsedMs: config.retry?.maxElapsedMs ?? 30_000,
      initialDelayMs: config.retry?.initialDelayMs ?? 100,
      maxDelayMs: config.retry?.maxDelayMs ?? 2_000,
    });
    this.headers = {
      "content-type": "application/json",
      ...(config.apiKey ? { authorization: `Bearer ${config.apiKey}` } : {}),
    };
    const operatorKey = config.operatorApiKey ?? config.apiKey;
    this.operatorHeaders = {
      "content-type": "application/json",
      ...(operatorKey ? { authorization: `Bearer ${operatorKey}` } : {}),
    };
    this.storage = new RemoteWorkflowStorage({
      url: `${base}/rpc/storage`,
      fetch: this.fetch,
      headers: this.headers,
    });
    this.workerRegistry = new RemoteWorkerRegistry({
      url: `${base}/rpc/worker`,
      fetch: this.fetch,
      headers: this.headers,
    });
  }

  /**
   * Advertise the workflow definitions (and versions) this worker hosts:
   * the server routes starts by them and the dashboard's Workflows page
   * lists them. Idempotent — re-calling replaces the prior advertisement
   * for the same workerId and refreshes its TTL, so a worker re-advertises
   * periodically. Pass every definition the worker can run (see
   * `workflowClosure`).
   */
  async advertise(params: {
    workerId: string;
    workflows: ReadonlyArray<Workflow<unknown, unknown>>;
    sampleInput?: (workflowName: string) => unknown;
  }): Promise<void> {
    const { workerId, workflows, sampleInput } = params;
    const entries: Array<{
      name: string;
      version?: string;
      steps: Array<{
        name: string;
        kind: string;
        dependsOn: string[];
        needs?: string[];
        priority?: number;
      }>;
      sampleInput?: unknown;
    }> = [];
    const seen = new Set<string>();
    const add = (wf: Workflow<unknown, unknown>) => {
      const key = `${wf.name}@${wf.version ?? ""}`;
      if (seen.has(key)) return;
      seen.add(key);
      entries.push({
        name: wf.name,
        version: wf.version,
        steps: wf.dag.steps.map((s) => ({
          name: s.name,
          kind: s.kind,
          dependsOn: [...s.dependsOn],
          needs: s.needs ? [...s.needs] : undefined,
          priority: s.priority,
        })),
        sampleInput: sampleInput?.(wf.name),
      });
    };
    for (const wf of workflows) add(wf);
    const body = { workerId, workflows: entries };
    const req = new Request(`${this.url}/api/advertisements`, {
      method: "POST",
      headers: this.headers,
      body: JSON.stringify(body),
    });
    const res = await this.fetch(req);
    if (!res.ok) {
      throw new Error(`ZoryaClient.advertise failed: ${res.status} ${await res.text()}`);
    }
  }

  /** Clear this worker's advertisement on shutdown. */
  async unadvertise(workerId: string): Promise<void> {
    const req = new Request(`${this.url}/api/advertisements/${encodeURIComponent(workerId)}`, {
      method: "DELETE",
      headers: this.headers,
    });
    const res = await this.fetch(req);
    if (!res.ok && res.status !== 404) {
      throw new Error(`ZoryaClient.unadvertise failed: ${res.status}`);
    }
  }

  private get runner(): WorkflowRunner {
    if (!this._runner) {
      this._runner = createWorkflowRunner({ storage: this.storage });
    }
    return this._runner;
  }

  /**
   * Trigger a workflow run by name and return a typed `WorkflowHandle`.
   *
   * Prefer this overload when the caller has the `Workflow<I, O>` definition
   * imported — TypeScript checks `params.input` against the workflow's
   * declared `Input` and threads the `Output` generic into `handle.result()`.
   *
   * Routing on the server is by `name` (and optionally `version`); the
   * imported definition is used purely for type information, so it doesn't
   * have to be the exact instance the worker is running, only structurally
   * compatible.
   */
  async start<Input, Output>(
    workflow: Workflow<Input, Output>,
    params: {
      input: Input;
      workflowId?: string;
      namespace?: string;
      version?: string;
      metadata?: Record<string, unknown>;
    },
  ): Promise<WorkflowHandle<Output>> {
    const { workflowId } = await this._postTrigger(workflow.name, {
      input: params.input,
      workflowId: params.workflowId,
      namespace: params.namespace,
      version: params.version ?? workflow.version,
      metadata: params.metadata,
    });
    return this.runner.handle<Output>(workflowId);
  }

  /**
   * Trigger a workflow run by name without compile-time type info. Returns
   * an untyped handle (`WorkflowHandle<unknown>`). Used by the dashboard's
   * trigger modal and other dynamic-dispatch callers that don't have the
   * workflow definition on hand.
   */
  async startByName(
    name: string,
    params: {
      input?: unknown;
      workflowId?: string;
      namespace?: string;
      version?: string;
      metadata?: Record<string, unknown>;
    } = {},
  ): Promise<WorkflowHandle<unknown>> {
    const { workflowId } = await this._postTrigger(name, params);
    return this.runner.handle<unknown>(workflowId);
  }

  private async _postTrigger(
    name: string,
    body: {
      input?: unknown;
      workflowId?: string;
      namespace?: string;
      version?: string;
      metadata?: Record<string, unknown>;
    },
  ): Promise<{ workflowId: string }> {
    // 202 when the run started, 200 when `workflowId` names an existing run
    // of this workflow; either way the body names the run. The id is fixed
    // here, before the request, so a retry of a request cut off mid-flight
    // answers with the same run instead of starting a second one.
    const workflowId = body.workflowId ?? crypto.randomUUID();
    const req = new Request(`${this.url}/api/workflows/${encodeURIComponent(name)}/runs`, {
      method: "POST",
      headers: this.operatorHeaders,
      body: JSON.stringify({ ...body, workflowId, runSource: "api" }),
    });
    const res = await this.fetch(req);
    const text = await res.text();
    if (!res.ok) throw new Error(`workflow trigger failed: ${res.status} ${text}`);
    return JSON.parse(text) as { workflowId: string };
  }

  /**
   * Claim pending workflow-start requests the worker can serve. Pass
   * `workflowSpecs` listing the (name, versions) tuples the worker
   * advertises; the server only hands back starts whose name matches and
   * whose pinned version (if any) is in the worker's set.
   */
  async claimWorkflowStarts(params: {
    workflowSpecs: ReadonlyArray<{ name: string; versions: readonly string[] }>;
    workerId?: string;
    limit?: number;
  }): Promise<ReadonlyArray<WorkflowStartClaim>> {
    const req = new Request(`${this.url}/api/worker-protocol/claim-starts`, {
      method: "POST",
      headers: this.headers,
      body: JSON.stringify({
        workflowSpecs: params.workflowSpecs.map((s) => ({
          name: s.name,
          versions: [...s.versions],
        })),
        workerId: params.workerId,
        limit: params.limit ?? 10,
        // A resend of this call (its answer lost) gets the same starts back.
        claimId: crypto.randomUUID(),
      }),
    });
    const res = await this.fetch(req);
    const text = await res.text();
    if (!res.ok) throw new Error(`claimWorkflowStarts failed: ${res.status} ${text}`);
    const body = JSON.parse(text) as { starts?: WorkflowStartClaim[] };
    return body.starts ?? [];
  }

  /**
   * Keep a claimed workflow-start alive. The server re-queues a claim whose
   * last heartbeat is older than its reclaim window. Returns false when the
   * claim is no longer current (it went stale and was re-claimed).
   */
  async heartbeatWorkflowStart(params: { id: string; claimToken: string }): Promise<boolean> {
    return this.postStartClaim({ action: "heartbeat-start", ...params });
  }

  /**
   * Mark a claimed workflow-start as done. Returns false when the claim
   * token is stale: the start was re-claimed by another worker and is left
   * alone.
   */
  async completeWorkflowStart(params: { id: string; claimToken: string }): Promise<boolean> {
    return this.postStartClaim({ action: "complete-start", ...params });
  }

  /**
   * Give a claimed workflow-start back without completing it: claimable
   * again once `delayMs` has passed. Returns false when the claim token is
   * stale.
   */
  async releaseWorkflowStart(params: {
    id: string;
    claimToken: string;
    delayMs?: number;
  }): Promise<boolean> {
    return this.postStartClaim({ action: "release-start", ...params });
  }

  private async postStartClaim(params: {
    action: "heartbeat-start" | "complete-start" | "release-start";
    id: string;
    claimToken: string;
    delayMs?: number;
  }): Promise<boolean> {
    const req = new Request(
      `${this.url}/api/worker-protocol/${params.action}/${encodeURIComponent(params.id)}`,
      {
        method: "POST",
        headers: this.headers,
        body: JSON.stringify({
          claimToken: params.claimToken,
          ...(params.delayMs !== undefined && { delayMs: params.delayMs }),
        }),
      },
    );
    const res = await this.fetch(req);
    const text = await res.text();
    if (!res.ok) throw new Error(`${params.action} failed: ${res.status} ${text}`);
    const body = JSON.parse(text) as { ok?: boolean };
    return body.ok === true;
  }
}

/**
 * Wrap `fetch` so a request the server never handled — the connection was
 * refused, or the server answered 503 while shutting down — is sent again
 * with capped exponential backoff, for at most `maxElapsedMs` in total. A
 * request cut off mid-flight is resent only when `isRetrySafeRequest`
 * holds for it.
 */
function retryingFetch(params: {
  fetch: FetchLike;
  clock: WallClock;
  maxElapsedMs: number;
  initialDelayMs: number;
  maxDelayMs: number;
}): FetchLike {
  const { fetch, clock, maxElapsedMs, initialDelayMs, maxDelayMs } = params;
  if (maxElapsedMs <= 0) return fetch;
  return async (req) => {
    const startedAt = clock.currentTimeMs();
    let delayMs = initialDelayMs;
    let midFlightSafe: boolean | undefined;
    for (;;) {
      const attempt = req.clone() as Request;
      let res: Response | undefined;
      let error: unknown;
      try {
        res = await fetch(attempt);
      } catch (err) {
        if (isConnectRefused(err)) {
          error = err;
        } else if (isDroppedMidFlight(err)) {
          midFlightSafe ??= await isRetrySafeRequest(req.clone() as Request);
          if (!midFlightSafe) throw err;
          error = err;
        } else {
          throw err;
        }
      }
      if (res && !(res.status === 503 && (await isShuttingDown(res)))) return res;
      const elapsed = clock.currentTimeMs() - startedAt;
      if (elapsed + delayMs > maxElapsedMs) {
        if (res) return res;
        throw error;
      }
      await new Promise<void>((resolve) => clock.setTimeout(resolve, delayMs));
      delayMs = Math.min(delayMs * 2, maxDelayMs);
    }
  };
}

/** The connection was refused: the request never reached the server. */
function isConnectRefused(err: unknown): boolean {
  const code = (err as { code?: unknown } | null)?.code;
  if (code === "ConnectionRefused" || code === "ECONNREFUSED") return true;
  const message = (err as { message?: unknown } | null)?.message;
  return typeof message === "string" && /unable to connect|connection refused/i.test(message);
}

/** The connection dropped mid-request: the server may have applied it. */
function isDroppedMidFlight(err: unknown): boolean {
  const code = (err as { code?: unknown } | null)?.code;
  if (code === "ECONNRESET" || code === "ConnectionClosed") return true;
  const message = (err as { message?: unknown } | null)?.message;
  return (
    typeof message === "string" && /socket connection was closed|connection reset/i.test(message)
  );
}

/** Paths of the start-queue protocol: every call is idempotent (see the route). */
const START_PROTOCOL_PATH =
  /\/api\/worker-protocol\/(heartbeat-start|complete-start|release-start)\//;

/**
 * True when resending `req` after it may already have been applied is
 * harmless (see the header).
 */
async function isRetrySafeRequest(req: Request): Promise<boolean> {
  const path = new URL(req.url).pathname;
  let body: Record<string, unknown> = {};
  try {
    const text = await req.text();
    body = text ? (JSON.parse(text) as Record<string, unknown>) : {};
  } catch {
    return false;
  }
  if (path.endsWith("/rpc/storage")) {
    return isRetrySafeStorageCall({ method: String(body.method), params: body.params });
  }
  if (path.endsWith("/rpc/worker")) return isRetrySafeWorkerCall({ method: String(body.method) });
  if (path.endsWith("/api/worker-protocol/claim-starts")) return typeof body.claimId === "string";
  if (START_PROTOCOL_PATH.test(path)) return true;
  if (path.endsWith("/api/advertisements") || path.includes("/api/advertisements/")) return true;
  if (/\/api\/workflows\/[^/]+\/runs$/.test(path)) return typeof body.workflowId === "string";
  return false;
}

/** True for Zorya's own 503 while it stops (no route ran). */
async function isShuttingDown(res: Response): Promise<boolean> {
  try {
    const body = (await res.clone().json()) as { error?: unknown };
    return body.error === "shutting_down";
  } catch {
    return false;
  }
}

export interface WorkflowStartClaim {
  readonly id: string;
  readonly workflowId: string;
  readonly workflowName: string;
  readonly input: unknown;
  readonly metadata?: Record<string, unknown>;
  readonly version?: string;
  readonly enqueuedAt: number;
  /** Fencing token of this claim — pass it to heartbeat and complete. */
  readonly claimToken: string;
}
