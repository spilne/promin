/**
 * Human-readable time duration. Eliminates magic millisecond numbers.
 *
 * @example
 * ```ts
 * Duration.hours(5)          // 18000000ms
 * Duration.minutes(30)       // 1800000ms
 * Duration.seconds(10)       // 10000ms
 * Duration.hours(1).plus(Duration.minutes(30))  // 5400000ms
 * Duration.parse("5m")       // 300000ms
 * ```
 */
export class Duration {
  private constructor(readonly ms: number) {}

  // --- Factories ---
  static millis(n: number): Duration {
    return new Duration(n);
  }
  static seconds(n: number): Duration {
    return new Duration(n * 1000);
  }
  static minutes(n: number): Duration {
    return new Duration(n * 60_000);
  }
  static hours(n: number): Duration {
    return new Duration(n * 3_600_000);
  }
  static days(n: number): Duration {
    return new Duration(n * 86_400_000);
  }
  static weeks(n: number): Duration {
    return new Duration(n * 604_800_000);
  }

  /** Parse a string like "5s", "30m", "2h", "1d", "1w" */
  static parse(s: string): Duration {
    const match = s.match(/^(\d+(?:\.\d+)?)\s*(ms|s|m|h|d|w)$/);
    if (!match) throw new Error(`Invalid duration: "${s}"`);
    const n = parseFloat(match[1]!);
    switch (match[2]) {
      case "ms":
        return Duration.millis(n);
      case "s":
        return Duration.seconds(n);
      case "m":
        return Duration.minutes(n);
      case "h":
        return Duration.hours(n);
      case "d":
        return Duration.days(n);
      case "w":
        return Duration.weeks(n);
      default:
        throw new Error(`Unknown unit: ${match[2]}`);
    }
  }

  /** Create from milliseconds or a Duration string. Convenience for APIs that accept both. */
  static from(value: number | string | Duration): Duration {
    if (value instanceof Duration) return value;
    if (typeof value === "number") return Duration.millis(value);
    return Duration.parse(value);
  }

  // --- Conversions ---
  toMilliseconds(): number {
    return this.ms;
  }
  toSeconds(): number {
    return this.ms / 1000;
  }
  toMinutes(): number {
    return this.ms / 60_000;
  }
  toHours(): number {
    return this.ms / 3_600_000;
  }
  toDays(): number {
    return this.ms / 86_400_000;
  }

  // --- Arithmetic ---
  plus(other: Duration): Duration {
    return new Duration(this.ms + other.ms);
  }
  minus(other: Duration): Duration {
    return new Duration(this.ms - other.ms);
  }
  times(factor: number): Duration {
    return new Duration(this.ms * factor);
  }

  // --- Comparison ---
  gt(other: Duration): boolean {
    return this.ms > other.ms;
  }
  gte(other: Duration): boolean {
    return this.ms >= other.ms;
  }
  lt(other: Duration): boolean {
    return this.ms < other.ms;
  }
  lte(other: Duration): boolean {
    return this.ms <= other.ms;
  }
  eq(other: Duration): boolean {
    return this.ms === other.ms;
  }

  // --- Display ---
  toString(): string {
    if (this.ms < 1000) return `${this.ms}ms`;
    if (this.ms < 60_000) return `${this.ms / 1000}s`;
    if (this.ms < 3_600_000) return `${this.ms / 60_000}m`;
    if (this.ms < 86_400_000) return `${this.ms / 3_600_000}h`;
    return `${this.ms / 86_400_000}d`;
  }
}

/** Convenience type: accepts milliseconds, Duration string, or Duration object. */
export type DurationInput = number | string | Duration;

/** Resolve a DurationInput to milliseconds. */
export function resolveMs(input: DurationInput): number {
  return Duration.from(input).ms;
}
