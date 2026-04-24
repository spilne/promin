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

import type { WorkflowStorage, SchedulerStorage, Workflow } from "@promin/workflow";
import { Auth, type AuthConfig } from "./auth.ts";
import { Router, jsonError } from "./router.ts";
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
} from "./routes/run-extras.ts";

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
  /** Directory with compiled dashboard assets (index.html, app.js, app.css). */
  uiDir?: string;
  /** SSE watcher poll interval. Default 1000ms. */
  sseIntervalMs?: number;
}

export interface ListenOptions {
  port?: number;
  hostname?: string;
}

export class ZoryaServer {
  readonly config: Required<Pick<ZoryaServerConfig, "storage">> & ZoryaServerConfig;
  private readonly auth: Auth;
  private readonly bus: RunEventBus;
  private readonly router: Router;
  private server?: { stop(): void; port: number; hostname: string };

  constructor(config: ZoryaServerConfig) {
    this.config = config;
    this.auth = new Auth(config);
    this.bus = new RunEventBus();

    const metrics = config.metrics ?? new StorageMetricsProvider(config.storage);
    const workers = config.workers ?? emptyWorkersProvider;
    const deps = {
      storage: config.storage,
      trigger: config.trigger,
      workflows: config.workflows,
    };

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

    if (config.uiDir) {
      this.router.setStatic(this.buildStaticHandler(config.uiDir));
    }
  }

  /** Expose the router for tests or embedding. */
  async handle(req: Request): Promise<Response> {
    const url = new URL(req.url);
    if (url.pathname.startsWith("/api/") && !this.auth.check(req)) {
      return jsonError(401, "unauthorized");
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
