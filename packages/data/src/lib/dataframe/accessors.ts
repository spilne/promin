// ---------------------------------------------------------------------------
// String and Date accessors for DataFrame columns
// ---------------------------------------------------------------------------

import type { LogicalPlan } from "./logical-plan.ts";
import type { DataFrameExecutor } from "./executor.ts";
import { DataFrame } from "./dataframe.ts";

// ---------------------------------------------------------------------------
// StringAccessor
// ---------------------------------------------------------------------------

export class StringAccessor<T> {
  constructor(
    private readonly _plan: LogicalPlan,
    private readonly _column: string,
    private readonly _executor: DataFrameExecutor,
  ) {}

  /** Add a derived column `<col>_<suffix>` by mapping the original string value. */
  private _derive(
    suffix: string,
    fn: (val: string) => unknown,
  ): DataFrame<T & Record<string, unknown>> {
    return DataFrame._fromPlan(
      {
        _tag: "WithColumn",
        input: this._plan,
        name: `${this._column}_${suffix}`,
        fn: (row: any) => fn(String(row[this._column] ?? "")),
      },
      this._executor,
    ) as any;
  }

  /** Replace the column in place with a transformed string. */
  private _transform(fn: (val: string) => string): DataFrame<T> {
    return DataFrame._fromPlan(
      {
        _tag: "WithColumn",
        input: this._plan,
        name: this._column,
        fn: (row: any) => fn(String(row[this._column] ?? "")),
      },
      this._executor,
    );
  }

  // -------------------------------------------------------------------------
  // Predicates — emit boolean into `<col>_<op>` column
  // -------------------------------------------------------------------------

  contains(substr: string): DataFrame<T & Record<string, boolean>> {
    return this._derive("contains", (s) => s.includes(substr)) as any;
  }

  startsWith(prefix: string): DataFrame<T & Record<string, boolean>> {
    return this._derive("startsWith", (s) => s.startsWith(prefix)) as any;
  }

  endsWith(suffix: string): DataFrame<T & Record<string, boolean>> {
    return this._derive("endsWith", (s) => s.endsWith(suffix)) as any;
  }

  matches(regex: RegExp): DataFrame<T & Record<string, boolean>> {
    return this._derive("matches", (s) => regex.test(s)) as any;
  }

  isEmpty(): DataFrame<T & Record<string, boolean>> {
    return this._derive("isEmpty", (s) => s.length === 0) as any;
  }

  isNumeric(): DataFrame<T & Record<string, boolean>> {
    return this._derive("isNumeric", (s) => s.length > 0 && /^-?\d+(?:\.\d+)?$/.test(s)) as any;
  }

  isAlpha(): DataFrame<T & Record<string, boolean>> {
    return this._derive("isAlpha", (s) => s.length > 0 && /^[\p{L}]+$/u.test(s)) as any;
  }

  isAlphaNumeric(): DataFrame<T & Record<string, boolean>> {
    return this._derive("isAlphaNumeric", (s) => s.length > 0 && /^[\p{L}\p{N}]+$/u.test(s)) as any;
  }

  // -------------------------------------------------------------------------
  // Derivations — emit value into `<col>_<op>` column
  // -------------------------------------------------------------------------

  length(): DataFrame<T & Record<string, number>> {
    return this._derive("len", (s) => s.length) as any;
  }

  /** Extract the first capture group (or full match if no group) of `regex`. */
  extract(regex: RegExp): DataFrame<T & Record<string, string | null>> {
    return this._derive("extract", (s) => {
      const m = s.match(regex);
      if (!m) return null;
      return m[1] ?? m[0];
    }) as any;
  }

  countMatches(pattern: string | RegExp): DataFrame<T & Record<string, number>> {
    return this._derive("countMatches", (s) => {
      if (typeof pattern === "string") {
        if (pattern.length === 0) return 0;
        let count = 0;
        let i = s.indexOf(pattern);
        while (i !== -1) {
          count++;
          i = s.indexOf(pattern, i + pattern.length);
        }
        return count;
      }
      const flags = pattern.flags.includes("g") ? pattern.flags : `${pattern.flags}g`;
      const re = new RegExp(pattern.source, flags);
      return (s.match(re) ?? []).length;
    }) as any;
  }

  indexOf(needle: string): DataFrame<T & Record<string, number>> {
    return this._derive("indexOf", (s) => s.indexOf(needle)) as any;
  }

  // -------------------------------------------------------------------------
  // In-place transforms — replace the column
  // -------------------------------------------------------------------------

  toUpperCase(): DataFrame<T> {
    return this._transform((s) => s.toUpperCase());
  }

  toLowerCase(): DataFrame<T> {
    return this._transform((s) => s.toLowerCase());
  }

  trim(): DataFrame<T> {
    return this._transform((s) => s.trim());
  }

  replace(search: string | RegExp, replacement: string): DataFrame<T> {
    return this._transform((s) => s.replace(search, replacement));
  }

  replaceAll(search: string | RegExp, replacement: string): DataFrame<T> {
    return this._transform((s) => {
      if (typeof search === "string") return s.split(search).join(replacement);
      const flags = search.flags.includes("g") ? search.flags : `${search.flags}g`;
      return s.replace(new RegExp(search.source, flags), replacement);
    });
  }

  split(separator: string): DataFrame<T> {
    return DataFrame._fromPlan(
      {
        _tag: "WithColumn",
        input: this._plan,
        name: this._column,
        fn: (row: any) => String(row[this._column] ?? "").split(separator),
      },
      this._executor,
    );
  }

  slice(start: number, end?: number): DataFrame<T> {
    return this._transform((s) => s.slice(start, end));
  }

  substring(start: number, length?: number): DataFrame<T> {
    return this._transform((s) =>
      length === undefined ? s.slice(start) : s.slice(start, start + length),
    );
  }

  head(n: number): DataFrame<T> {
    return this._transform((s) => s.slice(0, n));
  }

  tail(n: number): DataFrame<T> {
    return this._transform((s) => (n <= 0 ? "" : s.slice(-n)));
  }

  zfill(width: number): DataFrame<T> {
    return this._transform((s) => {
      if (s.length >= width) return s;
      const sign = s.startsWith("-") || s.startsWith("+") ? s[0]! : "";
      const body = sign ? s.slice(1) : s;
      return sign + body.padStart(width - sign.length, "0");
    });
  }

  pad(
    width: number,
    params: { side?: "left" | "right" | "both"; fill?: string } = {},
  ): DataFrame<T> {
    const side = params.side ?? "left";
    const fill = params.fill ?? " ";
    if (fill.length !== 1) throw new Error("pad: fill must be a single character");
    return this._transform((s) => {
      if (s.length >= width) return s;
      const need = width - s.length;
      switch (side) {
        case "left":
          return fill.repeat(need) + s;
        case "right":
          return s + fill.repeat(need);
        case "both": {
          const left = Math.floor(need / 2);
          const right = need - left;
          return fill.repeat(left) + s + fill.repeat(right);
        }
      }
    });
  }

  strip(chars?: string): DataFrame<T> {
    return this._transform((s) => stripChars(s, chars, "both"));
  }

  lstrip(chars?: string): DataFrame<T> {
    return this._transform((s) => stripChars(s, chars, "left"));
  }

  rstrip(chars?: string): DataFrame<T> {
    return this._transform((s) => stripChars(s, chars, "right"));
  }

  repeat(n: number): DataFrame<T> {
    return this._transform((s) => (n < 0 ? "" : s.repeat(n)));
  }

  reverse(): DataFrame<T> {
    return this._transform((s) => [...s].reverse().join(""));
  }

  capitalize(): DataFrame<T> {
    return this._transform((s) =>
      s.length === 0 ? s : s[0]!.toUpperCase() + s.slice(1).toLowerCase(),
    );
  }

  titleCase(): DataFrame<T> {
    return this._transform((s) =>
      s.replace(/\w\S*/g, (w) => w[0]!.toUpperCase() + w.slice(1).toLowerCase()),
    );
  }

  removePrefix(prefix: string): DataFrame<T> {
    return this._transform((s) => (s.startsWith(prefix) ? s.slice(prefix.length) : s));
  }

  removeSuffix(suffix: string): DataFrame<T> {
    return this._transform((s) => (s.endsWith(suffix) ? s.slice(0, s.length - suffix.length) : s));
  }
}

function stripChars(s: string, chars: string | undefined, side: "left" | "right" | "both"): string {
  if (chars === undefined) {
    if (side === "left") return s.replace(/^\s+/, "");
    if (side === "right") return s.replace(/\s+$/, "");
    return s.trim();
  }
  const set = new Set(chars);
  let start = 0;
  let end = s.length;
  if (side !== "right") while (start < end && set.has(s[start]!)) start++;
  if (side !== "left") while (end > start && set.has(s[end - 1]!)) end--;
  return s.slice(start, end);
}

// ---------------------------------------------------------------------------
// DateAccessor
// ---------------------------------------------------------------------------

/** Parse an interval string like "1d", "3h", "2w" into milliseconds. */
function parseDuration(s: string): number {
  const m = s.match(/^(-?\d+)\s*(ms|s|m|h|d|w)$/);
  if (!m) throw new Error(`Invalid duration: ${s}`);
  const n = parseInt(m[1]!, 10);
  switch (m[2]) {
    case "ms":
      return n;
    case "s":
      return n * 1000;
    case "m":
      return n * 60_000;
    case "h":
      return n * 3_600_000;
    case "d":
      return n * 86_400_000;
    case "w":
      return n * 604_800_000;
    default:
      throw new Error(`Unknown unit: ${m[2]}`);
  }
}

function isLeapYear(year: number): boolean {
  return (year % 4 === 0 && year % 100 !== 0) || year % 400 === 0;
}

function daysInMonth(year: number, month1: number): number {
  // month1 is 1-based
  return new Date(year, month1, 0).getDate();
}

/** ISO-8601 week number. Returns 1..53. */
function isoWeek(d: Date): number {
  const target = new Date(Date.UTC(d.getFullYear(), d.getMonth(), d.getDate()));
  const dow = target.getUTCDay() || 7;
  target.setUTCDate(target.getUTCDate() + 4 - dow);
  const yearStart = new Date(Date.UTC(target.getUTCFullYear(), 0, 1));
  return Math.ceil(((target.getTime() - yearStart.getTime()) / 86_400_000 + 1) / 7);
}

export class DateAccessor<T> {
  constructor(
    private readonly _plan: LogicalPlan,
    private readonly _column: string,
    private readonly _executor: DataFrameExecutor,
  ) {}

  private _toDate(val: unknown): Date {
    return val instanceof Date ? val : new Date(val as string | number);
  }

  /** Add a derived column `<col>_<suffix>`. */
  private _derive(
    suffix: string,
    fn: (d: Date) => unknown,
  ): DataFrame<T & Record<string, unknown>> {
    return DataFrame._fromPlan(
      {
        _tag: "WithColumn",
        input: this._plan,
        name: `${this._column}_${suffix}`,
        fn: (row: any) => fn(this._toDate(row[this._column])),
      },
      this._executor,
    ) as any;
  }

  /** Replace the column in place with a transformed Date (serialised as ISO string). */
  private _transform(fn: (d: Date) => Date): DataFrame<T> {
    return DataFrame._fromPlan(
      {
        _tag: "WithColumn",
        input: this._plan,
        name: this._column,
        fn: (row: any) => fn(this._toDate(row[this._column])).toISOString(),
      },
      this._executor,
    );
  }

  // -------------------------------------------------------------------------
  // Component accessors
  // -------------------------------------------------------------------------

  year(): DataFrame<T & Record<string, number>> {
    return this._derive("year", (d) => d.getFullYear()) as any;
  }

  month(): DataFrame<T & Record<string, number>> {
    return this._derive("month", (d) => d.getMonth() + 1) as any;
  }

  day(): DataFrame<T & Record<string, number>> {
    return this._derive("day", (d) => d.getDate()) as any;
  }

  hour(): DataFrame<T & Record<string, number>> {
    return this._derive("hour", (d) => d.getHours()) as any;
  }

  minute(): DataFrame<T & Record<string, number>> {
    return this._derive("minute", (d) => d.getMinutes()) as any;
  }

  second(): DataFrame<T & Record<string, number>> {
    return this._derive("second", (d) => d.getSeconds()) as any;
  }

  millisecond(): DataFrame<T & Record<string, number>> {
    return this._derive("ms", (d) => d.getMilliseconds()) as any;
  }

  /** Day of week: 0 (Sunday) .. 6 (Saturday) — matches Date.getDay(). */
  dayOfWeek(): DataFrame<T & Record<string, number>> {
    return this._derive("dow", (d) => d.getDay()) as any;
  }

  /** ISO day of week: 1 (Monday) .. 7 (Sunday). */
  isoDayOfWeek(): DataFrame<T & Record<string, number>> {
    return this._derive("isoDow", (d) => d.getDay() || 7) as any;
  }

  timestamp(): DataFrame<T & Record<string, number>> {
    return this._derive("ts", (d) => d.getTime()) as any;
  }

  // -------------------------------------------------------------------------
  // Calendar derivations
  // -------------------------------------------------------------------------

  /** ISO week-of-year number (1..53). */
  week(): DataFrame<T & Record<string, number>> {
    return this._derive("week", isoWeek) as any;
  }

  isoWeek(): DataFrame<T & Record<string, number>> {
    return this._derive("isoWeek", isoWeek) as any;
  }

  quarter(): DataFrame<T & Record<string, number>> {
    return this._derive("quarter", (d) => Math.floor(d.getMonth() / 3) + 1) as any;
  }

  /** Day-of-year, 1..366. */
  ordinalDay(): DataFrame<T & Record<string, number>> {
    return this._derive("doy", (d) => {
      const start = new Date(d.getFullYear(), 0, 0);
      return Math.floor((d.getTime() - start.getTime()) / 86_400_000);
    }) as any;
  }

  daysInMonth(): DataFrame<T & Record<string, number>> {
    return this._derive("daysInMonth", (d) =>
      daysInMonth(d.getFullYear(), d.getMonth() + 1),
    ) as any;
  }

  isLeapYear(): DataFrame<T & Record<string, boolean>> {
    return this._derive("isLeapYear", (d) => isLeapYear(d.getFullYear())) as any;
  }

  isWeekend(): DataFrame<T & Record<string, boolean>> {
    return this._derive("isWeekend", (d) => {
      const dow = d.getDay();
      return dow === 0 || dow === 6;
    }) as any;
  }

  // -------------------------------------------------------------------------
  // Epoch conversions
  // -------------------------------------------------------------------------

  epochDays(): DataFrame<T & Record<string, number>> {
    return this._derive("epochDays", (d) => Math.floor(d.getTime() / 86_400_000)) as any;
  }

  epochSeconds(): DataFrame<T & Record<string, number>> {
    return this._derive("epochSeconds", (d) => Math.floor(d.getTime() / 1000)) as any;
  }

  /** Seconds since midnight for the given date (0..86399). */
  totalSeconds(): DataFrame<T & Record<string, number>> {
    return this._derive(
      "totalSeconds",
      (d) => d.getHours() * 3600 + d.getMinutes() * 60 + d.getSeconds(),
    ) as any;
  }

  /** Minutes since midnight for the given date (0..1439). */
  totalMinutes(): DataFrame<T & Record<string, number>> {
    return this._derive("totalMinutes", (d) => d.getHours() * 60 + d.getMinutes()) as any;
  }

  // -------------------------------------------------------------------------
  // In-place date transforms
  // -------------------------------------------------------------------------

  truncate(unit: "year" | "month" | "day" | "hour"): DataFrame<T> {
    return this._transform((d) => {
      switch (unit) {
        case "year":
          return new Date(d.getFullYear(), 0, 1);
        case "month":
          return new Date(d.getFullYear(), d.getMonth(), 1);
        case "day":
          return new Date(d.getFullYear(), d.getMonth(), d.getDate());
        case "hour":
          return new Date(d.getFullYear(), d.getMonth(), d.getDate(), d.getHours());
      }
    });
  }

  /** Shift the date by a duration. Accepts a string ("3d", "-12h") or milliseconds. */
  offsetBy(duration: string | number): DataFrame<T> {
    const ms = typeof duration === "number" ? duration : parseDuration(duration);
    return this._transform((d) => new Date(d.getTime() + ms));
  }

  addDays(n: number): DataFrame<T> {
    return this._transform((d) => {
      const r = new Date(d);
      r.setDate(r.getDate() + n);
      return r;
    });
  }

  addMonths(n: number): DataFrame<T> {
    return this._transform((d) => {
      const r = new Date(d);
      r.setMonth(r.getMonth() + n);
      return r;
    });
  }

  addYears(n: number): DataFrame<T> {
    return this._transform((d) => {
      const r = new Date(d);
      r.setFullYear(r.getFullYear() + n);
      return r;
    });
  }

  startOfMonth(): DataFrame<T> {
    return this.truncate("month");
  }

  startOfYear(): DataFrame<T> {
    return this.truncate("year");
  }

  /** Last day of the month at 00:00 local time. */
  endOfMonth(): DataFrame<T> {
    return this._transform(
      (d) =>
        new Date(d.getFullYear(), d.getMonth(), daysInMonth(d.getFullYear(), d.getMonth() + 1)),
    );
  }
}
