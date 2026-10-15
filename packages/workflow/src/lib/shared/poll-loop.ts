// ---------------------------------------------------------------------------
// PollLoop — the one background polling loop the distributed layer shares.
//
// Worker claim loop, coordinator sweep, sleep / signal scanners and the step
// executor's wait all run on it, so they get the same behaviour:
//
// - An iteration that throws never ends the loop. The error goes to
//   `onError` and the next iteration waits with capped exponential backoff.
// - `stop()` cancels a pending wait at once and resolves only after the
//   in-flight iteration (if any) has finished and the loop has exited.
// - `wake()` cuts the current wait short; called mid-iteration, it makes the
//   next wait a no-op, so a wake is never lost.
// - Every wait is a timer on the injected `WallClock`, so `FakeWallClock`
//   drives the cadence in tests.
// ---------------------------------------------------------------------------

import { SystemWallClock, type TimerHandle, type WallClock } from "./wall-clock.ts";

/**
 * What an iteration asks the loop to do next.
 * - `"again"` — run the next iteration immediately (e.g. a full batch came
 *   back, so more work is likely waiting).
 * - `"stop"` — exit the loop; `start()` resolves.
 * - `void` / `"idle"` — wait `intervalMs` (or until `wake()`).
 */
export type PollTickResult = "again" | "stop" | "idle" | void;

/** Context passed to `onError` for a failed iteration. */
export interface PollLoopErrorInfo {
  /** Loop name, for log prefixes. */
  readonly name: string;
  /** Failures in a row, including this one (1 on the first). */
  readonly consecutiveFailures: number;
  /** How long the loop waits before the next iteration. */
  readonly nextDelayMs: number;
}

export interface PollLoopConfig {
  /** Short name used in default log lines (`[name] …`). */
  readonly name: string;
  /** Wait between iterations when nothing failed (ms). */
  readonly intervalMs: number;
  /** One iteration. Throwing is reported and backed off, never fatal. */
  readonly tick: () => Promise<PollTickResult>;
  /**
   * Called when an iteration throws. It must not throw; if it does, that
   * error is swallowed so the loop keeps running. Default: `console.error`.
   */
  readonly onError?: (error: unknown, info: PollLoopErrorInfo) => void;
  /**
   * Upper bound on the wait after consecutive failures. The wait after the
   * n-th failure in a row is `min(maxBackoffMs, intervalMs × 2^(n-1))`, so the
   * first failure waits the usual interval and each further one doubles it.
   * Default: 30 000 (or `intervalMs`, if larger).
   */
  readonly maxBackoffMs?: number;
  /** Time source for every wait. Default: `SystemWallClock`. */
  readonly clock?: WallClock;
}

/**
 * Background poll loop with error backoff, prompt `stop()` and `wake()`.
 *
 * `start()` runs the loop and resolves when it exits (after `stop()` or an
 * iteration returning `"stop"`). Calling `start()` while it is running
 * returns the same promise.
 *
 * Do not `await stop()` from inside `tick` — it waits for that very
 * iteration. Return `"stop"` instead.
 */
export class PollLoop {
  private readonly name: string;
  private readonly intervalMs: number;
  private readonly tick: () => Promise<PollTickResult>;
  private readonly onError: (error: unknown, info: PollLoopErrorInfo) => void;
  private readonly maxBackoffMs: number;
  private readonly clock: WallClock;

  private stopping = false;
  private wakeRequested = false;
  private loopPromise?: Promise<void>;
  private sleepTimer?: TimerHandle;
  private sleepResolve?: () => void;

  constructor(config: PollLoopConfig) {
    this.name = config.name;
    this.intervalMs = config.intervalMs;
    this.tick = config.tick;
    this.onError = config.onError ?? defaultOnError;
    this.maxBackoffMs = config.maxBackoffMs ?? Math.max(30_000, config.intervalMs);
    this.clock = config.clock ?? SystemWallClock;
  }

  /** True between `start()` and the loop's exit. */
  get running(): boolean {
    return this.loopPromise !== undefined;
  }

  /** True once `stop()` was called and the loop has not restarted. */
  get stopRequested(): boolean {
    return this.stopping;
  }

  start(): Promise<void> {
    if (this.loopPromise) return this.loopPromise;
    this.stopping = false;
    this.wakeRequested = false;
    const p = this.run().finally(() => {
      if (this.loopPromise === p) this.loopPromise = undefined;
    });
    this.loopPromise = p;
    return p;
  }

  /**
   * Ask the loop to exit, cancel a pending wait, and resolve once the
   * in-flight iteration (if any) has finished and the loop has exited.
   */
  async stop(): Promise<void> {
    this.stopping = true;
    this.cancelSleep();
    await this.loopPromise;
  }

  /** Run the next iteration now instead of after the current wait. */
  wake(): void {
    if (this.sleepResolve) this.cancelSleep();
    else this.wakeRequested = true;
  }

  private async run(): Promise<void> {
    let consecutiveFailures = 0;
    while (!this.stopping) {
      let delayMs = this.intervalMs;
      try {
        const result = await this.tick();
        consecutiveFailures = 0;
        if (result === "stop") return;
        if (result === "again") continue;
      } catch (error) {
        consecutiveFailures += 1;
        delayMs = Math.min(this.maxBackoffMs, this.intervalMs * 2 ** (consecutiveFailures - 1));
        try {
          this.onError(error, { name: this.name, consecutiveFailures, nextDelayMs: delayMs });
        } catch {
          // A throwing error hook must not take the loop down with it.
        }
      }
      if (this.stopping) return;
      await this.sleep(delayMs);
    }
  }

  private sleep(ms: number): Promise<void> {
    if (this.wakeRequested) {
      this.wakeRequested = false;
      return Promise.resolve();
    }
    return new Promise<void>((resolve) => {
      this.sleepResolve = resolve;
      this.sleepTimer = this.clock.setTimeout(() => this.cancelSleep(), ms);
    });
  }

  private cancelSleep(): void {
    this.sleepTimer?.clear();
    this.sleepTimer = undefined;
    const resolve = this.sleepResolve;
    this.sleepResolve = undefined;
    resolve?.();
  }
}

function defaultOnError(error: unknown, info: PollLoopErrorInfo): void {
  console.error(
    `[${info.name}] poll iteration failed (${info.consecutiveFailures} in a row, ` +
      `retrying in ${info.nextDelayMs}ms):`,
    error,
  );
}

/**
 * Heuristic: is this error a transport / network blip rather than a bug?
 * Bun and Node throw a few canonical shapes; checks both `code` and the
 * message text. Used to pick terse vs loud logging.
 */
export function isNetworkError(err: unknown): boolean {
  const code = (err as { code?: string })?.code;
  if (code === "ConnectionRefused" || code === "ECONNREFUSED") return true;
  if (code === "ECONNRESET" || code === "ETIMEDOUT") return true;
  if (code === "ENOTFOUND" || code === "EHOSTUNREACH") return true;
  const msg = (err as { message?: string })?.message ?? "";
  return /unable to connect|connection refused|fetch failed|socket hang up/i.test(msg);
}

/** One-line description of an error for a log line, without the stack. */
export function describeError(err: unknown): string {
  const code = (err as { code?: string })?.code;
  const msg = (err as { message?: string })?.message ?? String(err);
  return code ? `${code}: ${msg.split("\n")[0]}` : (msg.split("\n")[0] ?? "");
}
