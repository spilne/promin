import { describe, it, expect } from "bun:test";
import { DataFrame } from "../../dataframe/dataframe.ts";
import type { NumericProfile, StringProfile, BooleanProfile } from "../profile-types.ts";

// ---------------------------------------------------------------------------
// Test data
// ---------------------------------------------------------------------------

const users = [
  { id: 1, name: "Alice", age: 30, email: "alice@co.com", active: true, joinedAt: "2025-01-15" },
  { id: 2, name: "Bob", age: 25, email: "bob@co.com", active: false, joinedAt: "2025-03-20" },
  {
    id: 3,
    name: "Charlie",
    age: 35,
    email: "charlie@co.com",
    active: true,
    joinedAt: "2025-06-01",
  },
  { id: 4, name: "Diana", age: 28, email: null, active: true, joinedAt: "2025-08-10" },
  { id: 5, name: "Eve", age: 30, email: "eve@co.com", active: false, joinedAt: "2025-12-25" },
];

// ---------------------------------------------------------------------------
// DataFrame.profile()
// ---------------------------------------------------------------------------

describe("Data profiling — understand your dataset in one call", () => {
  it("returns row and column counts", async () => {
    const report = await DataFrame.fromArray(users).profile();

    expect(report.rowCount).toBe(5);
    expect(report.columnCount).toBe(6);
    expect(report.durationMs).toBeGreaterThanOrEqual(0);
  });

  it("detects duplicate rows", async () => {
    const data = [
      { x: 1, y: "a" },
      { x: 1, y: "a" }, // duplicate
      { x: 2, y: "b" },
    ];
    const report = await DataFrame.fromArray(data).profile();
    expect(report.duplicateRows).toBe(1);
  });

  it("profiles numeric columns — mean, median, std, percentiles", async () => {
    const report = await DataFrame.fromArray(users).profile();
    const age = report.columns["age"] as NumericProfile;

    expect(age.type).toBe("numeric");
    expect(age.nullCount).toBe(0);
    expect(age.mean).toBeCloseTo(29.6, 0);
    expect(age.min).toBe(25);
    expect(age.max).toBe(35);
    expect(age.std).toBeGreaterThan(0);
    expect(age.zeros).toBe(0);
    expect(age.negatives).toBe(0);
    expect(age.percentiles.p50).toBeDefined();
  });

  it("profiles string columns — lengths, top values, uniques", async () => {
    const report = await DataFrame.fromArray(users).profile();
    const name = report.columns["name"] as StringProfile;

    expect(name.type).toBe("string");
    expect(name.uniqueCount).toBe(5);
    expect(name.avgLength).toBeGreaterThan(0);
    expect(name.minLength).toBeGreaterThan(0);
    expect(name.topValues.length).toBeGreaterThan(0);
    expect(name.emptyStrings).toBe(0);
  });

  it("profiles boolean columns — true/false counts", async () => {
    const report = await DataFrame.fromArray(users).profile();
    const active = report.columns["active"] as BooleanProfile;

    expect(active.type).toBe("boolean");
    expect(active.trueCount).toBe(3);
    expect(active.falseCount).toBe(2);
    expect(active.truePct).toBe(60);
  });

  it("detects null values and percentages", async () => {
    const report = await DataFrame.fromArray(users).profile();
    const email = report.columns["email"] as StringProfile;

    expect(email.nullCount).toBe(1);
    expect(email.nullPct).toBe(20);
  });

  it("computes correlations between numeric columns", async () => {
    const data = Array.from({ length: 50 }, (_, i) => ({
      x: i,
      y: i * 2 + Math.random(), // strong positive correlation
      z: Math.random() * 100, // no correlation
    }));

    const report = await DataFrame.fromArray(data).profile();
    const xyCorr = report.correlations.find((c) => c.pair.includes("x") && c.pair.includes("y"));

    expect(xyCorr).toBeDefined();
    expect(xyCorr!.strength).toBe("strong");
    expect(xyCorr!.value).toBeGreaterThan(0.9);
  });

  it("skips correlations when disabled", async () => {
    const report = await DataFrame.fromArray(users).profile({ correlations: false });
    expect(report.correlations).toHaveLength(0);
  });
});

// ---------------------------------------------------------------------------
// Warnings
// ---------------------------------------------------------------------------

describe("Profile warnings — auto-detect data quality issues", () => {
  it("warns about high null percentage", async () => {
    const data = [{ val: 1 }, { val: null }, { val: null }, { val: null }, { val: 5 }];
    const report = await DataFrame.fromArray(data).profile();
    const nullWarning = report.warnings.find((w) => w.type === "high_nulls");

    expect(nullWarning).toBeDefined();
    expect(nullWarning!.column).toBe("val");
  });

  it("warns about constant columns", async () => {
    const data = [
      { id: 1, status: "active" },
      { id: 2, status: "active" },
      { id: 3, status: "active" },
    ];
    const report = await DataFrame.fromArray(data).profile();
    const constWarning = report.warnings.find((w) => w.type === "constant");

    expect(constWarning).toBeDefined();
    expect(constWarning!.column).toBe("status");
  });

  it("notes high cardinality columns (possible IDs)", async () => {
    const data = Array.from({ length: 20 }, (_, i) => ({
      id: `id_${i}`,
      category: i % 3 === 0 ? "A" : "B",
    }));
    const report = await DataFrame.fromArray(data).profile();
    const cardWarning = report.warnings.find((w) => w.type === "high_cardinality");

    expect(cardWarning).toBeDefined();
    expect(cardWarning!.column).toBe("id");
  });

  it("notes duplicate rows", async () => {
    const data = [
      { a: 1, b: 2 },
      { a: 1, b: 2 },
      { a: 3, b: 4 },
    ];
    const report = await DataFrame.fromArray(data).profile();
    const dupWarning = report.warnings.find((w) => w.type === "duplicates");

    expect(dupWarning).toBeDefined();
  });
});

// ---------------------------------------------------------------------------
// Edge cases
// ---------------------------------------------------------------------------

describe("Profiling edge cases", () => {
  it("handles empty dataset", async () => {
    const report = await DataFrame.fromArray([]).profile();
    expect(report.rowCount).toBe(0);
    expect(report.columnCount).toBe(0);
    expect(report.warnings).toHaveLength(0);
  });

  it("handles single row", async () => {
    const report = await DataFrame.fromArray([{ x: 1, y: "hello" }]).profile();
    expect(report.rowCount).toBe(1);
    expect(report.duplicateRows).toBe(0);
  });

  it("handles all-null column", async () => {
    const data = [{ val: null }, { val: null }, { val: null }];
    const report = await DataFrame.fromArray(data).profile();
    expect(report.columns["val"]!.nullPct).toBe(100);
  });
});
