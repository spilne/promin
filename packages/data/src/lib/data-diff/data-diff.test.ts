import { describe, it, expect } from "bun:test";
import { DataFrame } from "../dataframe/dataframe.ts";

// ---------------------------------------------------------------------------
// Row-level diff
// ---------------------------------------------------------------------------

describe("Data diff — compare two versions of a dataset", () => {
  it("detects added rows after a deploy", async () => {
    const before = DataFrame.fromArray([
      { id: 1, name: "Alice", revenue: 100 },
      { id: 2, name: "Bob", revenue: 200 },
    ]);
    const after = DataFrame.fromArray([
      { id: 1, name: "Alice", revenue: 100 },
      { id: 2, name: "Bob", revenue: 200 },
      { id: 3, name: "Charlie", revenue: 150 },
    ]);

    const diff = await DataFrame.diff(before, after, { key: "id" });

    expect(diff.summary.added).toBe(1);
    expect(diff.summary.removed).toBe(0);
    expect(diff.summary.unchanged).toBe(2);
    expect(diff.addedRows[0]).toEqual({ id: 3, name: "Charlie", revenue: 150 });
  });

  it("detects removed rows — data was deleted", async () => {
    const before = DataFrame.fromArray([
      { id: 1, revenue: 100 },
      { id: 2, revenue: 200 },
      { id: 3, revenue: 300 },
    ]);
    const after = DataFrame.fromArray([
      { id: 1, revenue: 100 },
      { id: 3, revenue: 300 },
    ]);

    const diff = await DataFrame.diff(before, after, { key: "id" });

    expect(diff.summary.removed).toBe(1);
    expect(diff.removedRows[0]).toEqual({ id: 2, revenue: 200 });
  });

  it("detects modified values — revenue changed after ETL fix", async () => {
    const before = DataFrame.fromArray([
      { id: 1, region: "US", revenue: 100 },
      { id: 2, region: "EU", revenue: 200 },
    ]);
    const after = DataFrame.fromArray([
      { id: 1, region: "US", revenue: 150 }, // changed
      { id: 2, region: "EU", revenue: 200 }, // unchanged
    ]);

    const diff = await DataFrame.diff(before, after, { key: "id" });

    expect(diff.summary.modified).toBe(1);
    expect(diff.summary.unchanged).toBe(1);
    expect(diff.modifications[0]).toEqual({
      key: 1,
      column: "revenue",
      before: 100,
      after: 150,
    });
  });

  it("compare specific columns only — ignore unrelated changes", async () => {
    const before = DataFrame.fromArray([{ id: 1, revenue: 100, updatedAt: "2026-01-01" }]);
    const after = DataFrame.fromArray([
      { id: 1, revenue: 100, updatedAt: "2026-04-01" }, // timestamp changed but revenue didn't
    ]);

    const diff = await DataFrame.diff(before, after, {
      key: "id",
      columns: ["revenue"], // only compare revenue
    });

    expect(diff.summary.unchanged).toBe(1);
    expect(diff.summary.modified).toBe(0);
  });

  it("numeric tolerance — small floating point differences are OK", async () => {
    const before = DataFrame.fromArray([{ id: 1, score: 0.1 + 0.2 }]); // 0.30000000000000004
    const after = DataFrame.fromArray([{ id: 1, score: 0.3 }]);

    const exact = await DataFrame.diff(before, after, { key: "id", tolerance: 0 });
    expect(exact.summary.modified).toBe(1); // different without tolerance

    const tolerant = await DataFrame.diff(before, after, { key: "id", tolerance: 0.001 });
    expect(tolerant.summary.modified).toBe(0); // same with tolerance
  });

  it("combined: adds, removes, and modifications in one diff", async () => {
    const before = DataFrame.fromArray([
      { id: 1, status: "active" },
      { id: 2, status: "active" },
      { id: 3, status: "inactive" },
    ]);
    const after = DataFrame.fromArray([
      { id: 1, status: "active" }, // unchanged
      { id: 2, status: "inactive" }, // modified
      { id: 4, status: "active" }, // added (id 3 removed)
    ]);

    const diff = await DataFrame.diff(before, after, { key: "id" });

    expect(diff.summary.added).toBe(1);
    expect(diff.summary.removed).toBe(1);
    expect(diff.summary.modified).toBe(1);
    expect(diff.summary.unchanged).toBe(1);
  });

  it("identical datasets — no differences", async () => {
    const data = [
      { id: 1, value: "a" },
      { id: 2, value: "b" },
    ];

    const diff = await DataFrame.diff(DataFrame.fromArray(data), DataFrame.fromArray(data), {
      key: "id",
    });

    expect(diff.summary.added).toBe(0);
    expect(diff.summary.removed).toBe(0);
    expect(diff.summary.modified).toBe(0);
    expect(diff.summary.unchanged).toBe(2);
  });
});

// ---------------------------------------------------------------------------
// Schema diff
// ---------------------------------------------------------------------------

describe("Schema diff — detect column changes between versions", () => {
  it("detects added columns — non-breaking change", () => {
    const diff = DataFrame.schemaDiff(["id", "name", "email"], ["id", "name", "email", "phone"]);

    expect(diff.addedColumns).toEqual(["phone"]);
    expect(diff.removedColumns).toEqual([]);
    expect(diff.compatible).toBe(true); // only additions = non-breaking
  });

  it("detects removed columns — breaking change", () => {
    const diff = DataFrame.schemaDiff(["id", "name", "email", "phone"], ["id", "name", "email"]);

    expect(diff.removedColumns).toEqual(["phone"]);
    expect(diff.compatible).toBe(false); // removal = breaking
  });

  it("detects both added and removed — breaking", () => {
    const diff = DataFrame.schemaDiff(["id", "name", "old_field"], ["id", "name", "new_field"]);

    expect(diff.addedColumns).toEqual(["new_field"]);
    expect(diff.removedColumns).toEqual(["old_field"]);
    expect(diff.compatible).toBe(false);
  });

  it("identical schemas — no changes", () => {
    const diff = DataFrame.schemaDiff(["id", "name"], ["id", "name"]);

    expect(diff.addedColumns).toEqual([]);
    expect(diff.removedColumns).toEqual([]);
    expect(diff.compatible).toBe(true);
  });
});
