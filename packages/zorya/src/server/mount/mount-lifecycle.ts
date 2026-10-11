// ---------------------------------------------------------------------------
// Lifecycle plumbing for ZoryaServer: the liveness / readiness probes, the
// background services it starts, and the trackers graceful shutdown uses
// (requests in flight, open event streams).
// ---------------------------------------------------------------------------

import type { WallClock, WorkflowStorage } from "@promin/workflow";
import type { Router } from "../router.ts";
import type { ManagedService, ServiceLifecycle } from "../service-lifecycle.ts";

/** Workflow id the readiness probe looks up; it never exists. */
const READINESS_PROBE_WORKFLOW_ID = "__zorya_readiness_probe__";

/**
 * `GET /api/health` (liveness: the process answers, touches nothing) and
 * `GET /api/ready` (readiness: storage answers and every background
 * service started). Both are public and answered while shutting down.
 */
export function mountProbes(params: {
  readonly router: Router;
  readonly storage: WorkflowStorage;
  readonly lifecycle: ServiceLifecycle;
  readonly clock: WallClock;
  readonly readinessTimeoutMs: number;
  readonly isStopping: () => boolean;
}): void {
  const { router, lifecycle } = params;
  router
    .get("/api/health", () => Response.json({ ok: true }))
    .get("/api/ready", async () => {
      const services = lifecycle.statuses();
      if (params.isStopping()) {
        return Response.json({ ready: false, reason: "stopping", services }, { status: 503 });
      }
      const storage = await probeStorage(params);
      const ready = storage.ok && lifecycle.isReady;
      return Response.json({ ready, storage, services }, { status: ready ? 200 : 503 });
    });
}

/** One cheap storage read, bounded by `readinessTimeoutMs`. */
async function probeStorage(params: {
  readonly storage: WorkflowStorage;
  readonly clock: WallClock;
  readonly readinessTimeoutMs: number;
}): Promise<{ ok: boolean; error?: string }> {
  const { clock, readinessTimeoutMs } = params;
  let timer: { clear(): void } | undefined;
  const timeout = new Promise<never>((_, reject) => {
    timer = clock.setTimeout(
      () => reject(new Error(`storage did not answer within ${readinessTimeoutMs}ms`)),
      readinessTimeoutMs,
    );
  });
  try {
    await Promise.race([params.storage.loadWorkflowStatus(READINESS_PROBE_WORKFLOW_ID), timeout]);
    return { ok: true };
  } catch (err) {
    return { ok: false, error: err instanceof Error ? err.message : String(err) };
  } finally {
    timer?.clear();
  }
}

interface Lifecycled {
  start(): Promise<void>;
  stop(): Promise<void>;
}

/** The background services the server starts, in start order. */
export function managedServices(params: {
  readonly workflows: Lifecycled;
  readonly scheduler: Lifecycled | undefined;
  readonly agents: Lifecycled | undefined;
  readonly skills: Lifecycled | undefined;
  readonly fragments: Lifecycled | undefined;
  readonly knowledgeBases: Lifecycled | undefined;
  readonly retention: Lifecycled | undefined;
  /** One-shot adoption of stored namespaces into the registry. */
  readonly namespaces: Lifecycled | undefined;
}): ManagedService[] {
  const out: ManagedService[] = [{ name: "workflows", ...bindLifecycle(params.workflows) }];
  if (params.namespaces) out.push({ name: "namespaces", ...bindLifecycle(params.namespaces) });
  if (params.scheduler) out.push({ name: "scheduler", ...bindLifecycle(params.scheduler) });
  if (params.agents) out.push({ name: "agents", ...bindLifecycle(params.agents) });
  if (params.skills) out.push({ name: "skills", ...bindLifecycle(params.skills) });
  if (params.fragments) out.push({ name: "fragments", ...bindLifecycle(params.fragments) });
  if (params.knowledgeBases) {
    out.push({ name: "knowledgeBases", ...bindLifecycle(params.knowledgeBases) });
  }
  if (params.retention) out.push({ name: "retention", ...bindLifecycle(params.retention) });
  return out;
}

function bindLifecycle(service: Lifecycled): Lifecycled {
  return { start: () => service.start(), stop: () => service.stop() };
}

/** Counts requests in flight so shutdown can wait for them to drain. */
export class InFlightRequests {
  private count = 0;
  private readonly idleWaiters = new Set<() => void>();

  constructor(private readonly clock: WallClock) {}

  enter(): void {
    this.count += 1;
  }

  leave(): void {
    this.count -= 1;
    if (this.count === 0) {
      for (const wake of this.idleWaiters) wake();
      this.idleWaiters.clear();
    }
  }

  /** Resolve when no request is in flight, or after `timeoutMs`. */
  async waitForIdle(timeoutMs: number): Promise<void> {
    if (this.count === 0) return;
    await new Promise<void>((resolve) => {
      const done = (): void => {
        timer.clear();
        this.idleWaiters.delete(done);
        resolve();
      };
      const timer = this.clock.setTimeout(done, timeoutMs);
      this.idleWaiters.add(done);
    });
  }
}

/** Event-stream responses still open, so shutdown can end them. */
export class LiveStreams {
  private readonly open = new Set<{ close(): void }>();

  /** Close every open stream. */
  closeAll(): void {
    for (const stream of Array.from(this.open)) stream.close();
    this.open.clear();
  }

  /**
   * Re-wrap an event-stream response so `closeAll()` can end it: closing
   * cancels the route's stream (running its teardown) and ends the body the
   * client reads. Other responses pass through.
   */
  track(res: Response): Response {
    if (!isEventStream(res)) return res;
    const source = res.body;
    if (!source) return res;
    const reader = source.getReader();
    let entry: { close(): void } | undefined;
    const forget = (): void => {
      if (entry) this.open.delete(entry);
    };
    const body = new ReadableStream<Uint8Array>({
      start: (controller) => {
        entry = {
          close: () => {
            forget();
            reader.cancel("server_shutdown").catch(() => {});
            try {
              controller.close();
            } catch {
              /* already closed */
            }
          },
        };
        this.open.add(entry);
      },
      pull: async (controller) => {
        try {
          const { value, done } = await reader.read();
          if (done) {
            forget();
            try {
              controller.close();
            } catch {
              /* closed by closeAll() */
            }
            return;
          }
          controller.enqueue(value);
        } catch (err) {
          forget();
          try {
            controller.error(err);
          } catch {
            /* closed by closeAll() */
          }
        }
      },
      cancel: async (reason) => {
        forget();
        await reader.cancel(reason).catch(() => {});
      },
    });
    return new Response(body, {
      status: res.status,
      statusText: res.statusText,
      headers: res.headers,
    });
  }
}

function isEventStream(res: Response): boolean {
  return (res.headers.get("content-type") ?? "").startsWith("text/event-stream");
}
