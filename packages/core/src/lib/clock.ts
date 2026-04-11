// ---------------------------------------------------------------------------
// Clock — injectable time source for deterministic testing
// ---------------------------------------------------------------------------

/** Injectable time source. Default: real system clock. */
export interface Clock {
  /** Current time as Date. */
  now(): Date;
  /** Current time as epoch milliseconds. */
  currentTimeMs(): number;
}

/** Real system clock — delegates to Date.now(). */
export const SystemClock: Clock = {
  now: () => new Date(),
  currentTimeMs: () => Date.now(),
};

/**
 * Fake clock for deterministic testing. Time only advances when you tell it to.
 *
 * @example
 * ```ts
 * const clock = FakeClock.create("2026-01-01T00:00:00Z");
 * clock.currentTimeMs(); // 1735689600000
 * clock.advance(5000);   // +5 seconds
 * clock.currentTimeMs(); // 1735689605000
 * clock.set("2026-06-01T00:00:00Z"); // jump to specific time
 * ```
 */
export class FakeClock implements Clock {
  private _ms: number;

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

  /** Advance time by the given milliseconds. */
  advance(ms: number): void {
    this._ms += ms;
  }

  /** Set time to a specific point. */
  set(time: Date | string | number): void {
    if (typeof time === "number") this._ms = time;
    else if (typeof time === "string") this._ms = new Date(time).getTime();
    else this._ms = time.getTime();
  }
}
