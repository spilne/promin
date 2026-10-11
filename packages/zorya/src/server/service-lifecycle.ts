// ---------------------------------------------------------------------------
// ServiceLifecycle — starts the server's background services and keeps
// retrying the ones whose start fails.
//
// A service whose `start()` rejects (storage down at boot, say) is retried
// with exponential backoff on the injected WallClock until it starts or the
// lifecycle stops. `/api/ready` reports each service's state, so a deploy
// that boots against a down database is visibly not ready instead of
// silently serving without its loops.
// ---------------------------------------------------------------------------

import { SystemWallClock, type TimerHandle, type WallClock } from "@promin/workflow";

/** A background service the server starts with `listen()` and stops with `stop()`. */
export interface ManagedService {
  readonly name: string;
  start(): Promise<void>;
  stop(): Promise<void>;
}

export type ServiceState = "pending" | "starting" | "retrying" | "started" | "stopped";

export interface ServiceStatus {
  name: string;
  state: ServiceState;
  /** Start attempts made so far. */
  attempts: number;
  /** Message of the most recent start failure, cleared once started. */
  lastError?: string;
}

export interface ServiceLifecycleConfig {
  services: ReadonlyArray<ManagedService>;
  /** Time source for retry backoff. Default: `SystemWallClock`. */
  clock?: WallClock;
  /** Delay before the first retry (ms). Doubles per failure. Default: 500. */
  initialRetryDelayMs?: number;
  /** Cap on the retry delay (ms). Default: 30_000. */
  maxRetryDelayMs?: number;
  /** Reports start and stop failures. */
  onError: (message: string, err: unknown) => void;
}

interface Entry {
  service: ManagedService;
  status: ServiceStatus;
  timer?: TimerHandle;
  /** The `start()` call in flight, if any. */
  attempt?: Promise<void>;
}

export class ServiceLifecycle {
  private readonly entries: Entry[];
  private readonly clock: WallClock;
  private readonly initialRetryDelayMs: number;
  private readonly maxRetryDelayMs: number;
  private readonly onError: (message: string, err: unknown) => void;
  private readonly readyWaiters = new Set<{ resolve: () => void; reject: (e: Error) => void }>();
  private started = false;
  private stopped = false;

  constructor(config: ServiceLifecycleConfig) {
    this.entries = config.services.map((service) => ({
      service,
      status: { name: service.name, state: "pending", attempts: 0 },
    }));
    this.clock = config.clock ?? SystemWallClock;
    this.initialRetryDelayMs = config.initialRetryDelayMs ?? 500;
    this.maxRetryDelayMs = config.maxRetryDelayMs ?? 30_000;
    this.onError = config.onError;
  }

  /** Start every service; failures retry in the background. Idempotent. */
  start(): void {
    if (this.started || this.stopped) return;
    this.started = true;
    for (const entry of this.entries) this.attempt(entry);
    this.settleWaiters();
  }

  /** True once every service has started. */
  get isReady(): boolean {
    return this.started && !this.stopped && this.entries.every((e) => e.status.state === "started");
  }

  /** Resolves once every service has started; rejects if the lifecycle stops first. */
  ready(): Promise<void> {
    if (this.isReady) return Promise.resolve();
    if (this.stopped) return Promise.reject(new Error("ZoryaServer stopped before it was ready"));
    return new Promise((resolve, reject) => this.readyWaiters.add({ resolve, reject }));
  }

  statuses(): ServiceStatus[] {
    return this.entries.map((e) => ({ ...e.status }));
  }

  /**
   * Cancel pending retries, wait for start attempts in flight, then stop
   * every service. Stop failures are reported, not thrown.
   */
  async stop(): Promise<void> {
    if (this.stopped) return;
    this.stopped = true;
    for (const entry of this.entries) {
      entry.timer?.clear();
      entry.timer = undefined;
    }
    this.settleWaiters();
    await Promise.allSettled(this.entries.map((e) => e.attempt));
    await Promise.all(
      this.entries.map(async (entry) => {
        try {
          await entry.service.stop();
        } catch (err) {
          this.report(`[zorya] service ${entry.service.name} failed to stop`, err);
        }
        entry.status.state = "stopped";
      }),
    );
  }

  private attempt(entry: Entry): void {
    if (this.stopped) return;
    entry.timer = undefined;
    entry.status.state = "starting";
    entry.status.attempts += 1;
    const attempt: Promise<void> = (async () => {
      try {
        await entry.service.start();
        if (this.stopped) return;
        entry.status.state = "started";
        delete entry.status.lastError;
        this.settleWaiters();
      } catch (err) {
        if (this.stopped) return;
        const delayMs = Math.min(
          this.maxRetryDelayMs,
          this.initialRetryDelayMs * 2 ** Math.min(entry.status.attempts - 1, 30),
        );
        entry.status.state = "retrying";
        entry.status.lastError = err instanceof Error ? err.message : String(err);
        this.report(
          `[zorya] service ${entry.service.name} failed to start (attempt ${entry.status.attempts}); retrying in ${delayMs}ms`,
          err,
        );
        entry.timer = this.clock.setTimeout(() => this.attempt(entry), delayMs);
      }
    })();
    entry.attempt = attempt;
    void attempt.finally(() => {
      if (entry.attempt === attempt) entry.attempt = undefined;
    });
  }

  private settleWaiters(): void {
    if (this.stopped) {
      const err = new Error("ZoryaServer stopped before it was ready");
      for (const w of this.readyWaiters) w.reject(err);
      this.readyWaiters.clear();
      return;
    }
    if (!this.isReady) return;
    for (const w of this.readyWaiters) w.resolve();
    this.readyWaiters.clear();
  }

  private report(message: string, err: unknown): void {
    try {
      this.onError(message, err);
    } catch {
      /* a broken reporter must not break the lifecycle */
    }
  }
}
