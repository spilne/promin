// ---------------------------------------------------------------------------
// Clock — injectable time source + scheduler for deterministic testing
// ---------------------------------------------------------------------------
//
// Unifies "what time is it?" and "call this in N ms" behind one interface
// so callers thread a single `clock` field through their config instead of
// pairing clock + timer. FakeClock.advance(ms) both moves the clock and
// fires any setTimeout/setInterval callbacks whose due time falls in the
// window — the same axis drives both.
// ---------------------------------------------------------------------------

/** Handle returned by `setInterval` / `setTimeout` — call `clear()` to cancel. */
export interface TimerHandle {
  clear(): void;
}

/**
 * Injectable time source + scheduler. Default: the real system clock.
 * Tests swap in `FakeClock` to drive time deterministically.
 */
export interface Clock {
  /** Current time as Date. */
  now(): Date;
  /** Current time as epoch milliseconds. */
  currentTimeMs(): number;
  /** Schedule `fn` to fire once in `ms` milliseconds. */
  setTimeout(fn: () => void, ms: number): TimerHandle;
  /** Schedule `fn` to fire every `ms` milliseconds, starting `ms` from now. */
  setInterval(fn: () => void, ms: number): TimerHandle;
}

/** Real system clock — delegates to `Date.now()` + global setTimeout/setInterval. */
export const SystemClock: Clock = {
  now: () => new Date(),
  currentTimeMs: () => Date.now(),
  setTimeout: (fn, ms) => {
    const id = globalThis.setTimeout(fn, ms);
    return { clear: () => globalThis.clearTimeout(id) };
  },
  setInterval: (fn, ms) => {
    const id = globalThis.setInterval(fn, ms);
    return { clear: () => globalThis.clearInterval(id) };
  },
};

/**
 * Deterministic clock + timer for tests. Time only advances when you tell
 * it to via `advance(ms)`; scheduled callbacks fire inside that advance
 * if their due time falls in the window.
 *
 * @example
 * ```ts
 * const clock = FakeClock.create("2026-01-01T00:00:00Z");
 * let ticks = 0;
 * clock.setInterval(() => ticks++, 100);
 * clock.advance(350);
 * // clock is now 2026-01-01T00:00:00.350Z
 * // ticks === 3 (fired at 100, 200, 300)
 * ```
 */
export class FakeClock implements Clock {
  private _ms: number;
  private nextId = 1;
  private pending = new Map<number, { dueMs: number; intervalMs?: number; fn: () => void }>();

  private constructor(ms: number) {
    this._ms = ms;
  }

  static create(startTime?: Date | string | number): FakeClock {
    if (startTime === undefined) return new FakeClock(0);
    if (typeof startTime === "number") return new FakeClock(startTime);
    if (typeof startTime === "string") return new FakeClock(new Date(startTime).getTime());
    return new FakeClock(startTime.getTime());
  }

  now(): Date {
    return new Date(this._ms);
  }

  currentTimeMs(): number {
    return this._ms;
  }

  /** Set time to a specific point. Does NOT fire pending callbacks. */
  set(time: Date | string | number): void {
    if (typeof time === "number") this._ms = time;
    else if (typeof time === "string") this._ms = new Date(time).getTime();
    else this._ms = time.getTime();
  }

  setTimeout(fn: () => void, ms: number): TimerHandle {
    const id = this.nextId++;
    this.pending.set(id, { dueMs: this._ms + ms, fn });
    return { clear: () => this.pending.delete(id) };
  }

  setInterval(fn: () => void, ms: number): TimerHandle {
    const id = this.nextId++;
    this.pending.set(id, { dueMs: this._ms + ms, intervalMs: ms, fn });
    return { clear: () => this.pending.delete(id) };
  }

  /**
   * Advance time by `ms`, firing any callbacks whose due time falls inside
   * the window, in due-time order. Interval callbacks re-arm for their
   * next firing within the same advance. The clock's `now()` during each
   * callback reflects that callback's own due time, not the final target.
   */
  advance(ms: number): void {
    const target = this._ms + ms;
    while (true) {
      let earliest: { id: number; dueMs: number } | null = null;
      for (const [id, entry] of this.pending) {
        if (entry.dueMs > target) continue;
        if (!earliest || entry.dueMs < earliest.dueMs) {
          earliest = { id, dueMs: entry.dueMs };
        }
      }
      if (!earliest) break;
      const entry = this.pending.get(earliest.id)!;
      this._ms = entry.dueMs;
      if (entry.intervalMs !== undefined) {
        entry.dueMs += entry.intervalMs;
      } else {
        this.pending.delete(earliest.id);
      }
      entry.fn();
    }
    if (this._ms < target) this._ms = target;
  }

  /** Pending callback count (for test introspection). */
  pendingCount(): number {
    return this.pending.size;
  }
}
