// ---------------------------------------------------------------------------
// ZoryaServer — HTTP server that wraps a promin WorkflowStorage.
//
// Usage:
// ```ts
// const server = new ZoryaServer({
//   storage,
//   apiKeys: ["secret"],
//   trigger: async (name, input) => myRunner.start(name, input),
//   workers: myWorkerRegistry,
// });
// server.listen({ port: 4000 });
// ```
// ---------------------------------------------------------------------------

import type {
  WorkflowStorage,
  SchedulerStorage,
  StepQueue,
  Workflow,
  WorkerRegistry,
} from "@promin/workflow";
import { createWorkerApiHandler, createWorkflowStorageHandler } from "@promin/workflow-remote";
import { Auth, type AuthConfig } from "./auth.ts";
import { Router, jsonError } from "./router.ts";
import {
  InMemoryWorkflowAdvertisementRegistry,
  type WorkflowAdvertisementRegistry,
} from "./workflow-advertisements.ts";
import {
  listAdvertisements,
  removeAdvertisements,
  upsertAdvertisements,
} from "./routes/advertisements.ts";
import { RunEventBus } from "./run-event-bus.ts";
import {
  cancelRun,
  getRun,
  listRuns,
  listWorkflowNames,
  sendSignal,
  triggerRun,
  type RunTrigger,
} from "./routes/runs.ts";
import { streamRunEvents } from "./routes/sse.ts";
import { getMetrics, StorageMetricsProvider, type MetricsProvider } from "./routes/metrics.ts";
import { emptyWorkersProvider, listWorkers, type WorkersProvider } from "./routes/workers.ts";
import {
  createSchedule,
  deleteSchedule,
  getSchedule,
  listSchedules,
  patchSchedule,
} from "./routes/schedules.ts";
import {
  getRunAttempts,
  getRunChildren,
  getRunHistory,
  getRunSignals,
  markRunFailed,
  markRunSuccess,
  rerunRun,
} from "./routes/run-extras.ts";
import { getSparklines, getWorkflowGrid } from "./routes/grid.ts";
import { getWorkflowDef, listWorkflowDefs } from "./routes/workflow-defs.ts";

export interface ZoryaServerConfig extends AuthConfig {
  storage: WorkflowStorage;
  /** Function that starts a new run by name. Required for POST /api/runs/trigger/:name. */
  trigger?: RunTrigger;
  /** Override metrics with a backend-specific provider (e.g. PgWorkflowMetrics). */
  metrics?: MetricsProvider;
  /** Plug in a worker registry. When omitted, /api/workers returns []. */
  workers?: WorkersProvider;
  /** Plug in a scheduler storage. When omitted, /api/schedules returns a stub. */
  scheduler?: SchedulerStorage;
  /**
   * Registry of known workflow definitions, keyed by workflow name. Used by
   * `GET /api/runs/:id` to enrich the response with the full static step
   * list (including steps that haven't executed yet). Optional — without
   * it the UI still works, it just can't render planned steps.
   */
  workflows?: Readonly<Record<string, Workflow<unknown, unknown>>>;
  /**
   * Returns a sample/default input for a workflow by name. Used by the UI
   * to populate the trigger form with plausible values (so users can tweak
   * fields instead of typing raw JSON from scratch). Return `undefined`
   * for workflows with no default.
   */
  sampleInput?: (workflowName: string) => unknown;
  /**
   * Optional re-run hook. When present, `POST /api/runs/:id/rerun` calls
   * this after `startFreshRun` so the caller can actually drive the
   * workflow again. Without it the storage row is reset but nothing
   * executes (the storage row stays in `running`).
   */
  rerun?: (workflowId: string) => Promise<void>;
  /** Directory with compiled dashboard assets (index.html, app.js, app.css). */
  uiDir?: string;
  /** SSE watcher poll interval. Default 1000ms. */
  sseIntervalMs?: number;
  /**
   * Enables the remote worker protocol. When provided, the server mounts:
   *
   *   POST /rpc/storage            — createWorkflowStorageHandler
   *   POST /rpc/worker             — createWorkerApiHandler (steps + workers)
   *   POST /api/advertisements     — workers register their workflow defs
   *   DELETE /api/advertisements/:workerId
   *   GET  /api/advertisements     — debugging
   *
   * And the Workflows page starts pulling from the advertisement registry
   * in addition to the static `workflows` config.
   */
  workerProtocol?: {
    stepQueue: StepQueue;
    workerRegistry?: WorkerRegistry;
    advertisements?: WorkflowAdvertisementRegistry;
    /**
     * API keys required on /rpc/storage, /rpc/worker, and
     * /api/advertisements. When omitted, the worker surface is open —
     * safe behind a private network, risky on a public one. Kept
     * separate from the dashboard's top-level `apiKeys` so you can
     * rotate one without the other and issue worker-only keys.
     */
    apiKeys?: ReadonlyArray<string>;
  };
}

export interface ListenOptions {
  port?: number;
  hostname?: string;
}

export class ZoryaServer {
  readonly config: Required<Pick<ZoryaServerConfig, "storage">> & ZoryaServerConfig;
  private readonly auth: Auth;
  /** Separate auth for worker-protocol endpoints. Open when no keys set. */
  private readonly workerAuth: Auth;
  private readonly bus: RunEventBus;
  private readonly router: Router;
  private server?: { stop(): void; port: number; hostname: string };

  constructor(config: ZoryaServerConfig) {
    this.config = config;
    this.auth = new Auth(config);
    this.workerAuth = new Auth({ apiKeys: config.workerProtocol?.apiKeys });
    this.bus = new RunEventBus();

    const metrics = config.metrics ?? new StorageMetricsProvider(config.storage);
    const workers = config.workers ?? emptyWorkersProvider;
    const deps = {
      storage: config.storage,
      trigger: config.trigger,
      workflows: config.workflows,
    };

    // Auto-create an in-memory advertisement registry when the worker
    // protocol is on but no registry is passed — that's the common case.
    const advertisements: WorkflowAdvertisementRegistry | undefined = config.workerProtocol
      ? (config.workerProtocol.advertisements ?? new InMemoryWorkflowAdvertisementRegistry())
      : undefined;

    this.router = new Router()
      .get(
        "/api/health",
        () =>
          new Response(JSON.stringify({ ok: true }), {
            headers: { "content-type": "application/json" },
          }),
      )
      .get("/api/runs", listRuns(deps))
      .get("/api/workflows", listWorkflowNames(deps))
      .get("/api/runs/:id", getRun(deps))
      .get("/api/runs/:id/signals", getRunSignals(config.storage))
      .get("/api/runs/:id/attempts", getRunAttempts(config.storage))
      .get("/api/runs/:id/history", getRunHistory(config.storage))
      .get("/api/runs/:id/children", getRunChildren(config.storage))
      .post("/api/runs/:id/mark-success", markRunSuccess(config.storage))
      .post("/api/runs/:id/mark-failed", markRunFailed(config.storage))
      .post("/api/runs/:id/rerun", rerunRun(config.storage, config.rerun))
      .get("/api/workflows/sparklines", getSparklines(config.storage))
      .get(
        "/api/workflows/definitions",
        listWorkflowDefs({
          workflows: config.workflows,
          sampleInput: config.sampleInput,
          advertisements,
        }),
      )
      .get("/api/workflows/:name/grid", getWorkflowGrid(config.storage))
      .get(
        "/api/workflows/:name/definition",
        getWorkflowDef({
          workflows: config.workflows,
          sampleInput: config.sampleInput,
          advertisements,
        }),
      )
      .post("/api/runs/trigger/:name", triggerRun(deps))
      .post("/api/runs/:id/cancel", cancelRun(deps))
      .post("/api/runs/:id/signal", sendSignal(deps))
      .get(
        "/api/runs/:id/events",
        streamRunEvents({
          storage: config.storage,
          bus: this.bus,
          pollIntervalMs: config.sseIntervalMs,
        }),
      )
      .get("/api/workers", listWorkers(workers))
      .get("/api/metrics", getMetrics(metrics));

    if (config.scheduler) {
      const sch = config.scheduler;
      this.router
        .get("/api/schedules", listSchedules(sch))
        .post("/api/schedules", createSchedule(sch))
        .get("/api/schedules/:id", getSchedule(sch))
        .patch("/api/schedules/:id", patchSchedule(sch))
        .delete("/api/schedules/:id", deleteSchedule(sch));
    } else {
      // Stub response so the UI can tell "scheduler not configured" from
      // "scheduler configured but empty".
      this.router.get(
        "/api/schedules",
        () =>
          new Response(JSON.stringify({ schedules: [], total: 0, configured: false }), {
            headers: { "content-type": "application/json" },
          }),
      );
    }

    if (config.workerProtocol) {
      const { stepQueue, workerRegistry } = config.workerProtocol;
      const storageHandler = createWorkflowStorageHandler(config.storage);
      const workerHandler = createWorkerApiHandler({
        stepQueue,
        storage: config.storage,
        workerRegistry,
      });
      // Mount as catch-all handlers: RPC body is the source of truth, path
      // is just a mount point. Keep paths stable so client SDKs don't need
      // configuration.
      this.router.post("/rpc/storage", (req) => storageHandler(req));
      this.router.post("/rpc/worker", (req) => workerHandler(req));

      if (advertisements) {
        this.router
          .post("/api/advertisements", upsertAdvertisements(advertisements))
          .delete("/api/advertisements/:workerId", removeAdvertisements(advertisements))
          .get("/api/advertisements", listAdvertisements(advertisements));
      }
    }

    if (config.uiDir) {
      this.router.setStatic(this.buildStaticHandler(config.uiDir));
    }
  }

  /** Expose the router for tests or embedding. */
  async handle(req: Request): Promise<Response> {
    const url = new URL(req.url);
    const path = url.pathname;

    // Worker-protocol surface has its own keyset. /api/advertisements lives
    // under /api/ so strip it from the dashboard-auth branch too.
    const isWorkerPath = path.startsWith("/rpc/") || path.startsWith("/api/advertisements");

    if (isWorkerPath) {
      if (!this.workerAuth.check(req)) return jsonError(401, "unauthorized_worker");
    } else if (path.startsWith("/api/")) {
      if (!this.auth.check(req)) return jsonError(401, "unauthorized");
    }

    return this.router.handle(req);
  }

  listen(opts: ListenOptions = {}): { port: number; hostname: string; stop: () => void } {
    const port = opts.port ?? 4000;
    const hostname = opts.hostname ?? "0.0.0.0";
    const handle = this.handle.bind(this);
    // Use Bun.serve if available; otherwise throw a clear error.
    const BunGlobal = (globalThis as { Bun?: { serve: typeof Bun.serve } }).Bun;
    if (!BunGlobal) {
      throw new Error("ZoryaServer.listen requires Bun runtime");
    }
    const srv = BunGlobal.serve({
      port,
      hostname,
      fetch: (req) => handle(req),
    });
    const resolvedPort = typeof srv.port === "number" ? srv.port : port;
    const resolvedHost = typeof srv.hostname === "string" ? srv.hostname : hostname;
    this.server = { stop: () => srv.stop(), port: resolvedPort, hostname: resolvedHost };
    return { port: resolvedPort, hostname: resolvedHost, stop: () => srv.stop() };
  }

  stop(): void {
    this.server?.stop();
    this.server = undefined;
  }

  private buildStaticHandler(dir: string) {
    return async (req: Request): Promise<Response> => {
      const url = new URL(req.url);
      const path = url.pathname === "/" ? "/index.html" : url.pathname;
      const file = Bun.file(`${dir}${path}`);
      if (await file.exists()) return new Response(file);
      // Fallback to index.html for SPA routing.
      const indexFile = Bun.file(`${dir}/index.html`);
      if (await indexFile.exists()) return new Response(indexFile);
      return new Response("Not Found", { status: 404 });
    };
  }
}
