import { describe, it, expect } from "bun:test";
import { DataFrame } from "../dataframe/dataframe.ts";
import { generators, generateRows } from "./generators.ts";
import { dataframeProperties } from "./properties.ts";

// ---------------------------------------------------------------------------
// Generators — deterministic fake data
// ---------------------------------------------------------------------------

describe("Test data generators — create realistic fixtures", () => {
  it("sequence generates incrementing IDs", () => {
    const rows = generateRows({ id: generators.sequence() }, 5);
    expect(rows.map((r) => r.id)).toEqual([1, 2, 3, 4, 5]);
  });

  it("pick selects from provided values", () => {
    const rows = generateRows({ status: generators.pick("a", "b", "c") }, 20, 42);
    const values = new Set(rows.map((r) => r.status));
    expect(values.size).toBeGreaterThan(1); // not all the same
    for (const v of values) expect(["a", "b", "c"]).toContain(v);
  });

  it("int generates within range", () => {
    const rows = generateRows({ n: generators.int(10, 20) }, 100, 42);
    for (const r of rows) {
      expect(r.n).toBeGreaterThanOrEqual(10);
      expect(r.n).toBeLessThanOrEqual(20);
    }
  });

  it("float generates within range", () => {
    const rows = generateRows({ price: generators.float(0, 100) }, 50, 42);
    for (const r of rows) {
      expect(r.price).toBeGreaterThanOrEqual(0);
      expect(r.price).toBeLessThanOrEqual(100);
    }
  });

  it("email generates valid-looking emails", () => {
    const rows = generateRows({ email: generators.email() }, 5, 42);
    for (const r of rows) {
      expect((r.email as string).includes("@")).toBe(true);
    }
  });

  it("nullable wraps a generator with random nulls", () => {
    const rows = generateRows({ val: generators.nullable(generators.int(1, 100), 0.5) }, 100, 42);
    const nullCount = rows.filter((r) => r.val === null).length;
    expect(nullCount).toBeGreaterThan(10);
    expect(nullCount).toBeLessThan(90);
  });

  it("same seed produces same data — deterministic", () => {
    const a = generateRows({ x: generators.int(0, 1000) }, 10, 123);
    const b = generateRows({ x: generators.int(0, 1000) }, 10, 123);
    expect(a).toEqual(b);
  });

  it("different seeds produce different data", () => {
    const a = generateRows({ x: generators.int(0, 1000) }, 10, 1);
    const b = generateRows({ x: generators.int(0, 1000) }, 10, 2);
    expect(a).not.toEqual(b);
  });

  it("generates a full table fixture", () => {
    const rows = generateRows(
      {
        id: generators.sequence(),
        name: generators.string(8),
        amount: generators.float(10, 500),
        status: generators.pick("pending", "completed", "cancelled"),
        active: generators.bool(0.8),
        createdAt: generators.date(new Date("2025-01-01"), new Date("2026-01-01")),
      },
      100,
      42,
    );

    expect(rows).toHaveLength(100);
    expect(rows[0]).toHaveProperty("id");
    expect(rows[0]).toHaveProperty("name");
    expect(rows[0]).toHaveProperty("amount");
    expect(rows[0]).toHaveProperty("status");
    expect(rows[0]).toHaveProperty("active");
    expect(rows[0]).toHaveProperty("createdAt");
  });
});

// ---------------------------------------------------------------------------
// Property-based assertions
// ---------------------------------------------------------------------------

describe("DataFrame properties — invariants that should always hold", () => {
  const orders = DataFrame.fromArray([
    { id: 1, region: "US", amount: 100 },
    { id: 2, region: "EU", amount: 200 },
    { id: 3, region: "US", amount: 150 },
    { id: 4, region: "AP", amount: 50 },
    { id: 5, region: "EU", amount: 300 },
  ]);

  it("filter never increases row count", async () => {
    expect(
      await dataframeProperties.filterReducesOrMaintains(orders, (r) => r.region === "US"),
    ).toBe(true);
  });

  it("sort preserves row count", async () => {
    expect(await dataframeProperties.sortPreservesCount(orders, "amount")).toBe(true);
  });

  it("distinct reduces or maintains row count", async () => {
    expect(await dataframeProperties.distinctReducesOrMaintains(orders)).toBe(true);
  });

  it("limit is bounded", async () => {
    expect(await dataframeProperties.limitBounded(orders, 3)).toBe(true);
  });

  it("select preserves row count", async () => {
    expect(await dataframeProperties.selectPreservesCount(orders, ["id", "region"])).toBe(true);
  });
});
