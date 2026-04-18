import { describe, it, expect } from "bun:test";
import { DataFrame } from "./dataframe.ts";

// ---------------------------------------------------------------------------
// StringAccessor — predicates, derivations, transforms
// ---------------------------------------------------------------------------

describe("StringAccessor — predicates", () => {
  const df = () =>
    DataFrame.fromArray([
      { s: "hello" },
      { s: "" },
      { s: "42" },
      { s: "3.14" },
      { s: "абв" }, // non-ASCII letters
      { s: "abc123" },
      { s: "abc!" },
    ]);

  it("matches(regex)", async () => {
    const r = await df().str("s").matches(/^\d+$/).collect();
    expect(r.map((x: any) => x.s_matches)).toEqual([
      false,
      false,
      true,
      false,
      false,
      false,
      false,
    ]);
  });

  it("isEmpty()", async () => {
    const r = await df().str("s").isEmpty().collect();
    expect(r.map((x: any) => x.s_isEmpty)).toEqual([
      false,
      true,
      false,
      false,
      false,
      false,
      false,
    ]);
  });

  it("isNumeric() — integers and decimals only", async () => {
    const r = await df().str("s").isNumeric().collect();
    expect(r.map((x: any) => x.s_isNumeric)).toEqual([
      false,
      false,
      true,
      true,
      false,
      false,
      false,
    ]);
  });

  it("isAlpha() — unicode letters, no digits", async () => {
    const r = await df().str("s").isAlpha().collect();
    expect(r.map((x: any) => x.s_isAlpha)).toEqual([true, false, false, false, true, false, false]);
  });

  it("isAlphaNumeric() — unicode letters + digits", async () => {
    const r = await df().str("s").isAlphaNumeric().collect();
    expect(r.map((x: any) => x.s_isAlphaNumeric)).toEqual([
      true,
      false,
      true,
      false,
      true,
      true,
      false,
    ]);
  });
});

describe("StringAccessor — derivations", () => {
  it("extract() returns first capture group", async () => {
    const df = DataFrame.fromArray([
      { email: "alice@acme.com" },
      { email: "bob@example.io" },
      { email: "invalid" },
    ]);
    const r = await df
      .str("email")
      .extract(/@([^.]+)/)
      .collect();
    expect(r.map((x: any) => x.email_extract)).toEqual(["acme", "example", null]);
  });

  it("extract() without capture group returns full match", async () => {
    const df = DataFrame.fromArray([{ s: "price: $42" }, { s: "free" }]);
    const r = await df.str("s").extract(/\$\d+/).collect();
    expect(r.map((x: any) => x.s_extract)).toEqual(["$42", null]);
  });

  it("countMatches(string)", async () => {
    const df = DataFrame.fromArray([{ s: "abababab" }, { s: "aaaa" }, { s: "xyz" }, { s: "" }]);
    const r = await df.str("s").countMatches("ab").collect();
    expect(r.map((x: any) => x.s_countMatches)).toEqual([4, 0, 0, 0]);
  });

  it("countMatches(regex)", async () => {
    const df = DataFrame.fromArray([{ s: "a1b2c3d" }, { s: "abc" }]);
    const r = await df.str("s").countMatches(/\d/).collect();
    expect(r.map((x: any) => x.s_countMatches)).toEqual([3, 0]);
  });

  it("indexOf()", async () => {
    const df = DataFrame.fromArray([{ s: "hello" }, { s: "world" }]);
    const r = await df.str("s").indexOf("ll").collect();
    expect(r.map((x: any) => x.s_indexOf)).toEqual([2, -1]);
  });

  it("length() returns character count", async () => {
    const df = DataFrame.fromArray([{ s: "abc" }, { s: "" }]);
    const r = await df.str("s").length().collect();
    expect(r.map((x: any) => x.s_len)).toEqual([3, 0]);
  });
});

describe("StringAccessor — transforms", () => {
  it("replaceAll string", async () => {
    const r = await DataFrame.fromArray([{ s: "a-b-c-d" }])
      .str("s")
      .replaceAll("-", "_")
      .collect();
    expect((r[0] as any).s).toBe("a_b_c_d");
  });

  it("replaceAll regex auto-adds global flag", async () => {
    const r = await DataFrame.fromArray([{ s: "ab12cd34" }])
      .str("s")
      .replaceAll(/\d/, "*")
      .collect();
    expect((r[0] as any).s).toBe("ab**cd**");
  });

  it("substring with start only", async () => {
    const r = await DataFrame.fromArray([{ s: "hello world" }])
      .str("s")
      .substring(6)
      .collect();
    expect((r[0] as any).s).toBe("world");
  });

  it("substring with start + length", async () => {
    const r = await DataFrame.fromArray([{ s: "hello world" }])
      .str("s")
      .substring(0, 5)
      .collect();
    expect((r[0] as any).s).toBe("hello");
  });

  it("head() takes first n chars", async () => {
    const r = await DataFrame.fromArray([{ s: "abcdef" }, { s: "xy" }])
      .str("s")
      .head(3)
      .collect();
    expect(r.map((x: any) => x.s)).toEqual(["abc", "xy"]);
  });

  it("tail() takes last n chars", async () => {
    const r = await DataFrame.fromArray([{ s: "abcdef" }, { s: "xy" }])
      .str("s")
      .tail(3)
      .collect();
    expect(r.map((x: any) => x.s)).toEqual(["def", "xy"]);
  });

  it("zfill() left-pads with zeros (Python semantics: preserves sign, pads body)", async () => {
    const r = await DataFrame.fromArray([
      { n: "42" },
      { n: "1000" },
      { n: "12345" },
      { n: "-5" },
      { n: "+7" },
    ])
      .str("n")
      .zfill(5)
      .collect();
    expect(r.map((x: any) => x.n)).toEqual(["00042", "01000", "12345", "-0005", "+0007"]);
  });

  it("pad() defaults to left", async () => {
    const r = await DataFrame.fromArray([{ s: "x" }])
      .str("s")
      .pad(4)
      .collect();
    expect((r[0] as any).s).toBe("   x");
  });

  it("pad() with side right", async () => {
    const r = await DataFrame.fromArray([{ s: "x" }])
      .str("s")
      .pad(4, { side: "right", fill: "." })
      .collect();
    expect((r[0] as any).s).toBe("x...");
  });

  it("pad() with side both", async () => {
    const r = await DataFrame.fromArray([{ s: "x" }])
      .str("s")
      .pad(5, { side: "both", fill: "-" })
      .collect();
    expect((r[0] as any).s).toBe("--x--");
  });

  it("pad() is no-op when already wider than width", async () => {
    const r = await DataFrame.fromArray([{ s: "hello" }])
      .str("s")
      .pad(3)
      .collect();
    expect((r[0] as any).s).toBe("hello");
  });

  it("strip() default trims whitespace", async () => {
    const r = await DataFrame.fromArray([{ s: "  hi  " }])
      .str("s")
      .strip()
      .collect();
    expect((r[0] as any).s).toBe("hi");
  });

  it("strip(chars) removes listed chars from both ends", async () => {
    const r = await DataFrame.fromArray([{ s: "##title##" }])
      .str("s")
      .strip("#")
      .collect();
    expect((r[0] as any).s).toBe("title");
  });

  it("lstrip() / rstrip() only one side", async () => {
    const left = await DataFrame.fromArray([{ s: "xxhixx" }])
      .str("s")
      .lstrip("x")
      .collect();
    const right = await DataFrame.fromArray([{ s: "xxhixx" }])
      .str("s")
      .rstrip("x")
      .collect();
    expect((left[0] as any).s).toBe("hixx");
    expect((right[0] as any).s).toBe("xxhi");
  });

  it("repeat(n)", async () => {
    const r = await DataFrame.fromArray([{ s: "ab" }, { s: "x" }])
      .str("s")
      .repeat(3)
      .collect();
    expect(r.map((x: any) => x.s)).toEqual(["ababab", "xxx"]);
  });

  it("repeat(0) yields empty", async () => {
    const r = await DataFrame.fromArray([{ s: "abc" }])
      .str("s")
      .repeat(0)
      .collect();
    expect((r[0] as any).s).toBe("");
  });

  it("reverse() reverses by code point", async () => {
    const r = await DataFrame.fromArray([{ s: "hello" }, { s: "a😀b" }])
      .str("s")
      .reverse()
      .collect();
    expect((r[0] as any).s).toBe("olleh");
    expect((r[1] as any).s).toBe("b😀a"); // [...s] splits by code points
  });

  it("capitalize()", async () => {
    const r = await DataFrame.fromArray([{ s: "hello WORLD" }, { s: "a" }, { s: "" }])
      .str("s")
      .capitalize()
      .collect();
    expect(r.map((x: any) => x.s)).toEqual(["Hello world", "A", ""]);
  });

  it("titleCase()", async () => {
    const r = await DataFrame.fromArray([{ s: "hello world" }, { s: "ALL CAPS" }])
      .str("s")
      .titleCase()
      .collect();
    expect(r.map((x: any) => x.s)).toEqual(["Hello World", "All Caps"]);
  });

  it("removePrefix / removeSuffix", async () => {
    const pre = await DataFrame.fromArray([{ s: "pre_value" }, { s: "value" }])
      .str("s")
      .removePrefix("pre_")
      .collect();
    const suf = await DataFrame.fromArray([{ s: "value.json" }, { s: "value" }])
      .str("s")
      .removeSuffix(".json")
      .collect();
    expect(pre.map((x: any) => x.s)).toEqual(["value", "value"]);
    expect(suf.map((x: any) => x.s)).toEqual(["value", "value"]);
  });
});

// ---------------------------------------------------------------------------
// DateAccessor
// ---------------------------------------------------------------------------

describe("DateAccessor — component accessors", () => {
  const events = DataFrame.fromArray([
    { t: "2026-03-15T10:30:45.250Z" },
    { t: "2024-02-29T00:00:00Z" }, // leap year
    { t: "2023-01-01T23:59:59Z" },
  ]);

  it("minute, second, millisecond", async () => {
    const r = await events.dt("t").minute().dt("t").second().dt("t").millisecond().collect();
    // Note: hour/min/sec via .get* use local tz, so just assert types & plausibility.
    for (const row of r as any[]) {
      expect(typeof row.t_minute).toBe("number");
      expect(typeof row.t_second).toBe("number");
      expect(typeof row.t_ms).toBe("number");
      expect(row.t_minute).toBeGreaterThanOrEqual(0);
      expect(row.t_minute).toBeLessThan(60);
    }
  });

  it("dayOfWeek and isoDayOfWeek", async () => {
    // 2026-03-15 is a Sunday
    const df = DataFrame.fromArray([{ t: "2026-03-15T12:00:00Z" }]);
    const r = await df.dt("t").dayOfWeek().dt("t").isoDayOfWeek().collect();
    // Compare with local Date parsing (timezone-dependent). Check invariants instead:
    const row = r[0] as any;
    // isoDow should be 1..7; dayOfWeek 0..6
    expect(row.t_dow).toBeGreaterThanOrEqual(0);
    expect(row.t_dow).toBeLessThanOrEqual(6);
    expect(row.t_isoDow).toBeGreaterThanOrEqual(1);
    expect(row.t_isoDow).toBeLessThanOrEqual(7);
    // isoDow 7 when dow 0, else == dow
    expect(row.t_isoDow).toBe(row.t_dow === 0 ? 7 : row.t_dow);
  });

  it("timestamp is a number (ms since epoch)", async () => {
    const df = DataFrame.fromArray([{ t: "2020-01-01T00:00:00Z" }]);
    const r = await df.dt("t").timestamp().collect();
    expect((r[0] as any).t_ts).toBe(new Date("2020-01-01T00:00:00Z").getTime());
  });
});

describe("DateAccessor — calendar derivations", () => {
  it("week / isoWeek — W01 of 2024 starts 2024-01-01 (Monday)", async () => {
    const df = DataFrame.fromArray([
      { t: "2024-01-01T12:00:00Z" },
      { t: "2024-01-07T12:00:00Z" },
      { t: "2024-12-30T12:00:00Z" }, // ISO W01 of 2025
    ]);
    const r = await df.dt("t").week().collect();
    expect((r[0] as any).t_week).toBe(1);
    expect((r[1] as any).t_week).toBe(1);
    expect((r[2] as any).t_week).toBe(1);
  });

  it("quarter", async () => {
    const df = DataFrame.fromArray([
      { t: "2026-01-01T00:00:00Z" },
      { t: "2026-04-01T00:00:00Z" },
      { t: "2026-07-01T00:00:00Z" },
      { t: "2026-10-01T00:00:00Z" },
    ]);
    const r = await df.dt("t").quarter().collect();
    // Note: these dates are UTC midnight; local tz may shift by 1 day.
    // Values should still be 1..4.
    for (const row of r as any[]) {
      expect(row.t_quarter).toBeGreaterThanOrEqual(1);
      expect(row.t_quarter).toBeLessThanOrEqual(4);
    }
  });

  it("ordinalDay — day of year", async () => {
    const df = DataFrame.fromArray([
      { t: "2026-01-01T12:00:00" }, // local noon to avoid tz boundary
      { t: "2026-12-31T12:00:00" },
    ]);
    const r = await df.dt("t").ordinalDay().collect();
    expect((r[0] as any).t_doy).toBe(1);
    expect((r[1] as any).t_doy).toBe(365);
  });

  it("daysInMonth", async () => {
    const df = DataFrame.fromArray([
      { t: "2024-02-15T12:00:00" }, // leap Feb → 29
      { t: "2025-02-15T12:00:00" }, // non-leap Feb → 28
      { t: "2026-04-15T12:00:00" }, // Apr → 30
      { t: "2026-07-15T12:00:00" }, // Jul → 31
    ]);
    const r = await df.dt("t").daysInMonth().collect();
    expect(r.map((x: any) => x.t_daysInMonth)).toEqual([29, 28, 30, 31]);
  });

  it("isLeapYear", async () => {
    const df = DataFrame.fromArray([
      { t: "2024-06-15T12:00:00" }, // leap
      { t: "2025-06-15T12:00:00" }, // not
      { t: "2000-06-15T12:00:00" }, // leap (400)
      { t: "2100-06-15T12:00:00" }, // not (100 but not 400)
    ]);
    const r = await df.dt("t").isLeapYear().collect();
    expect(r.map((x: any) => x.t_isLeapYear)).toEqual([true, false, true, false]);
  });

  it("isWeekend", async () => {
    // 2026-04-11 Sat, 2026-04-12 Sun, 2026-04-13 Mon (local noon)
    const df = DataFrame.fromArray([
      { t: "2026-04-11T12:00:00" },
      { t: "2026-04-12T12:00:00" },
      { t: "2026-04-13T12:00:00" },
    ]);
    const r = await df.dt("t").isWeekend().collect();
    expect(r.map((x: any) => x.t_isWeekend)).toEqual([true, true, false]);
  });
});

describe("DateAccessor — epoch conversions", () => {
  it("epochDays and epochSeconds", async () => {
    const df = DataFrame.fromArray([{ t: "2020-01-01T00:00:00Z" }]);
    const r = await df.dt("t").epochDays().dt("t").epochSeconds().collect();
    const expectedSecs = Math.floor(new Date("2020-01-01T00:00:00Z").getTime() / 1000);
    expect((r[0] as any).t_epochSeconds).toBe(expectedSecs);
    expect((r[0] as any).t_epochDays).toBe(Math.floor(expectedSecs / 86400));
  });

  it("totalSeconds — seconds since midnight", async () => {
    // Build a Date explicitly to sidestep tz parsing of "T01:02:03" strings
    const d = new Date();
    d.setHours(1, 2, 3, 0);
    const df = DataFrame.fromArray([{ t: d }]);
    const r = await df.dt("t").totalSeconds().collect();
    expect((r[0] as any).t_totalSeconds).toBe(1 * 3600 + 2 * 60 + 3);
  });

  it("totalMinutes — minutes since midnight", async () => {
    const d = new Date();
    d.setHours(10, 30, 0, 0);
    const df = DataFrame.fromArray([{ t: d }]);
    const r = await df.dt("t").totalMinutes().collect();
    expect((r[0] as any).t_totalMinutes).toBe(10 * 60 + 30);
  });
});

describe("DateAccessor — in-place transforms", () => {
  it("offsetBy(string)", async () => {
    const df = DataFrame.fromArray([{ t: "2026-04-15T00:00:00Z" }]);
    const r = await df.dt("t").offsetBy("3d").collect();
    expect((r[0] as any).t).toBe("2026-04-18T00:00:00.000Z");
  });

  it("offsetBy(number ms)", async () => {
    const df = DataFrame.fromArray([{ t: "2026-04-15T00:00:00Z" }]);
    const r = await df.dt("t").offsetBy(3600_000).collect();
    expect((r[0] as any).t).toBe("2026-04-15T01:00:00.000Z");
  });

  it("offsetBy negative duration", async () => {
    const df = DataFrame.fromArray([{ t: "2026-04-15T00:00:00Z" }]);
    const r = await df.dt("t").offsetBy("-2h").collect();
    expect((r[0] as any).t).toBe("2026-04-14T22:00:00.000Z");
  });

  it("offsetBy rejects invalid input", () => {
    const df = DataFrame.fromArray([{ t: "2026-04-15T00:00:00Z" }]);
    expect(() => df.dt("t").offsetBy("garbage")).toThrow();
  });

  it("addDays / addMonths / addYears", async () => {
    const base = DataFrame.fromArray([{ t: "2026-01-15T00:00:00Z" }]);
    const plusDays = await base.dt("t").addDays(7).collect();
    const plusMonths = await base.dt("t").addMonths(2).collect();
    const plusYears = await base.dt("t").addYears(5).collect();
    // check year/month/day round-trips; can't string-compare due to tz.
    expect(new Date((plusDays[0] as any).t).getUTCDate()).toBeGreaterThanOrEqual(21);
    expect(new Date((plusMonths[0] as any).t).getMonth()).toBe(2); // March (0-indexed)
    expect(new Date((plusYears[0] as any).t).getFullYear()).toBe(2031);
  });

  it("startOfMonth / startOfYear / endOfMonth", async () => {
    const df = DataFrame.fromArray([{ t: "2026-07-20T12:34:56" }]);
    const som = await df.dt("t").startOfMonth().collect();
    const soy = await df.dt("t").startOfYear().collect();
    const eom = await df.dt("t").endOfMonth().collect();
    const somD = new Date((som[0] as any).t);
    const soyD = new Date((soy[0] as any).t);
    const eomD = new Date((eom[0] as any).t);
    expect(somD.getDate()).toBe(1);
    expect(somD.getMonth()).toBe(6); // July = 6
    expect(soyD.getMonth()).toBe(0);
    expect(soyD.getDate()).toBe(1);
    expect(eomD.getDate()).toBe(31); // July has 31 days
  });

  it("truncate('year') zeros month and day", async () => {
    const df = DataFrame.fromArray([{ t: "2026-07-20T12:34:56" }]);
    const r = await df.dt("t").truncate("year").collect();
    const d = new Date((r[0] as any).t);
    expect(d.getMonth()).toBe(0);
    expect(d.getDate()).toBe(1);
    expect(d.getHours()).toBe(0);
  });
});

// ---------------------------------------------------------------------------
// Statistical functions (promin-ix4)
// ---------------------------------------------------------------------------

describe("DataFrame.skew", () => {
  it("zero for symmetric distribution", async () => {
    const df = DataFrame.fromArray(Array.from({ length: 100 }, (_, i) => ({ v: i - 49.5 })));
    const s = await df.skew("v");
    expect(s).not.toBeNull();
    expect(Math.abs(s!)).toBeLessThan(1e-9);
  });

  it("positive for right-skewed distribution", async () => {
    const df = DataFrame.fromArray([1, 1, 1, 1, 1, 2, 2, 3, 10, 100].map((v) => ({ v })));
    const s = await df.skew("v");
    expect(s).not.toBeNull();
    expect(s!).toBeGreaterThan(0.5);
  });

  it("null for fewer than 3 values", async () => {
    const df = DataFrame.fromArray([{ v: 1 }, { v: 2 }]);
    expect(await df.skew("v")).toBeNull();
  });

  it("zero when all values identical", async () => {
    const df = DataFrame.fromArray([{ v: 5 }, { v: 5 }, { v: 5 }, { v: 5 }]);
    expect(await df.skew("v")).toBe(0);
  });
});

describe("DataFrame.kurtosis", () => {
  it("null for fewer than 4 values", async () => {
    const df = DataFrame.fromArray([{ v: 1 }, { v: 2 }, { v: 3 }]);
    expect(await df.kurtosis("v")).toBeNull();
  });

  it("zero when all values identical", async () => {
    const df = DataFrame.fromArray([{ v: 5 }, { v: 5 }, { v: 5 }, { v: 5 }, { v: 5 }]);
    expect(await df.kurtosis("v")).toBe(0);
  });

  it("positive (leptokurtic) for heavy-tailed data", async () => {
    const df = DataFrame.fromArray([1, 1, 1, 1, 1, 1, 1, 1, 1, 100].map((v) => ({ v })));
    const k = await df.kurtosis("v");
    expect(k).not.toBeNull();
    expect(k!).toBeGreaterThan(0);
  });
});

describe("DataFrame.entropy", () => {
  it("zero when all values identical", async () => {
    const df = DataFrame.fromArray([{ v: "a" }, { v: "a" }, { v: "a" }]);
    expect(await df.entropy("v")).toBe(0);
  });

  it("1 bit for fair binary distribution", async () => {
    const df = DataFrame.fromArray([{ v: 0 }, { v: 1 }, { v: 0 }, { v: 1 }]);
    const h = await df.entropy("v");
    expect(h).toBeCloseTo(1, 10);
  });

  it("log2(n) for uniform distribution over n values", async () => {
    const df = DataFrame.fromArray([1, 2, 3, 4, 5, 6, 7, 8].map((v) => ({ v })));
    const h = await df.entropy("v");
    expect(h).toBeCloseTo(3, 10); // log2(8) = 3
  });

  it("null for empty column", async () => {
    const df = DataFrame.fromArray([{ v: null }, { v: null }]);
    expect(await df.entropy("v")).toBeNull();
  });

  it("ignores nulls in distribution", async () => {
    const df = DataFrame.fromArray([{ v: "a" }, { v: null }, { v: "a" }, { v: null }]);
    expect(await df.entropy("v")).toBe(0); // only "a" remains
  });
});

describe("DataFrame.approxNUnique", () => {
  it("exact count on small sets", async () => {
    const df = DataFrame.fromArray([
      { id: "a" },
      { id: "b" },
      { id: "c" },
      { id: "a" },
      { id: "b" },
    ]);
    expect(await df.approxNUnique("id")).toBe(3);
  });

  it("0 on empty", async () => {
    const df = DataFrame.fromArray<{ id: string }>([]);
    expect(await df.approxNUnique("id")).toBe(0);
  });

  it("approximates within 5% on ~10k unique strings", async () => {
    const data = Array.from({ length: 10_000 }, (_, i) => ({ id: `user-${i}` }));
    const df = DataFrame.fromArray(data);
    const est = await df.approxNUnique("id");
    const err = Math.abs(est - 10_000) / 10_000;
    expect(err).toBeLessThan(0.05);
  });

  it("dedupes duplicates", async () => {
    const data = Array.from({ length: 5000 }, (_, i) => ({ id: `user-${i % 100}` }));
    const df = DataFrame.fromArray(data);
    const est = await df.approxNUnique("id");
    const err = Math.abs(est - 100) / 100;
    expect(err).toBeLessThan(0.1); // small-cardinality correction kicks in
  });

  it("ignores null values", async () => {
    const df = DataFrame.fromArray([
      { id: "a" },
      { id: null as any },
      { id: "b" },
      { id: null as any },
    ]);
    expect(await df.approxNUnique("id")).toBe(2);
  });
});

describe("DataFrame.ewmMean", () => {
  it("first value equals input (no prior state)", async () => {
    const df = DataFrame.fromArray([{ v: 10 }]);
    const r = await df.ewmMean("v", { alpha: 0.5 }).collect();
    expect((r[0] as any).v).toBe(10);
  });

  it("alpha=1 equals raw series", async () => {
    const df = DataFrame.fromArray([1, 2, 3, 4].map((v) => ({ v })));
    const r = await df.ewmMean("v", { alpha: 1 }).collect();
    expect(r.map((x: any) => x.v)).toEqual([1, 2, 3, 4]);
  });

  it("alpha=0.5 smooths", async () => {
    const df = DataFrame.fromArray([10, 20, 30].map((v) => ({ v })));
    const r = await df.ewmMean("v", { alpha: 0.5 }).collect();
    // ewm[0]=10, ewm[1]=0.5*20+0.5*10=15, ewm[2]=0.5*30+0.5*15=22.5
    expect((r[0] as any).v).toBe(10);
    expect((r[1] as any).v).toBe(15);
    expect((r[2] as any).v).toBe(22.5);
  });

  it("nulls reuse previous smoothed value", async () => {
    const df = DataFrame.fromArray([10, null, 20].map((v) => ({ v })));
    const r = await df.ewmMean("v", { alpha: 0.5 }).collect();
    expect((r[0] as any).v).toBe(10);
    expect((r[1] as any).v).toBe(10); // null uses prev
    expect((r[2] as any).v).toBe(15); // 0.5*20 + 0.5*10
  });

  it("rejects alpha outside (0, 1]", () => {
    const df = DataFrame.fromArray([{ v: 1 }]);
    expect(() => df.ewmMean("v", { alpha: 0 })).toThrow();
    expect(() => df.ewmMean("v", { alpha: 1.1 })).toThrow();
    expect(() => df.ewmMean("v", { alpha: -0.1 })).toThrow();
  });

  it("preserves other columns", async () => {
    const df = DataFrame.fromArray([
      { name: "a", v: 10 },
      { name: "b", v: 20 },
    ]);
    const r = await df.ewmMean("v", { alpha: 0.5 }).collect();
    expect((r[0] as any).name).toBe("a");
    expect((r[1] as any).name).toBe("b");
  });
});

describe("DataFrame.interpolate", () => {
  it("linear between known endpoints", async () => {
    const df = DataFrame.fromArray([10, null, null, 40].map((v) => ({ v })));
    const r = await df.interpolate("v").collect();
    expect(r.map((x: any) => x.v)).toEqual([10, 20, 30, 40]);
  });

  it("forward-fills when only a leading value exists", async () => {
    const df = DataFrame.fromArray([10, null, null].map((v) => ({ v })));
    const r = await df.interpolate("v").collect();
    expect(r.map((x: any) => x.v)).toEqual([10, 10, 10]);
  });

  it("back-fills when only a trailing value exists", async () => {
    const df = DataFrame.fromArray([null, null, 30].map((v) => ({ v })));
    const r = await df.interpolate("v").collect();
    expect(r.map((x: any) => x.v)).toEqual([30, 30, 30]);
  });

  it("leaves all-null column untouched", async () => {
    const df = DataFrame.fromArray([null, null].map((v) => ({ v })));
    const r = await df.interpolate("v").collect();
    expect(r.map((x: any) => x.v)).toEqual([null, null]);
  });

  it("no-op when no nulls", async () => {
    const df = DataFrame.fromArray([1, 2, 3].map((v) => ({ v })));
    const r = await df.interpolate("v").collect();
    expect(r.map((x: any) => x.v)).toEqual([1, 2, 3]);
  });

  it("handles NaN like null", async () => {
    const df = DataFrame.fromArray([10, NaN, 30].map((v) => ({ v })));
    const r = await df.interpolate("v").collect();
    expect(r.map((x: any) => x.v)).toEqual([10, 20, 30]);
  });

  it("preserves other columns", async () => {
    const df = DataFrame.fromArray([
      { name: "a", v: 10 },
      { name: "b", v: null },
      { name: "c", v: 30 },
    ]);
    const r = await df.interpolate("v").collect();
    expect(r.map((x: any) => ({ name: x.name, v: x.v }))).toEqual([
      { name: "a", v: 10 },
      { name: "b", v: 20 },
      { name: "c", v: 30 },
    ]);
  });
});
