import { describe, it, expect } from "bun:test";
import { DataFrame } from "./dataframe.ts";
import { optimizePlan } from "./plan-optimizer.ts";
import { col } from "./expr.ts";
import type { LogicalPlan } from "./logical-plan.ts";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const SAMPLE = [
  { id: 1, a: 10, b: 100, region: "north", active: true },
  { id: 2, a: 20, b: 200, region: "south", active: false },
  { id: 3, a: 30, b: 300, region: "north", active: true },
  { id: 4, a: 40, b: 400, region: "east", active: true },
  { id: 5, a: 50, b: 500, region: "west", active: false },
];

/** Walk to the first node whose tag matches. */
function find(plan: LogicalPlan, tag: string): any {
  if ((plan as any)._tag === tag) return plan;
  if ("input" in plan) return find((plan as any).input, tag);
  return null;
}

function tagsInOrder(plan: LogicalPlan): string[] {
  const out: string[] = [];
  let current: any = plan;
  while (current) {
    out.push(current._tag);
    current = current.input ?? null;
  }
  return out;
}

// ---------------------------------------------------------------------------
// Filter combining
// ---------------------------------------------------------------------------

describe("optimizer — combine adjacent filters", () => {
  it("merges two consecutive filters into one with AND expr", () => {
    const df = DataFrame.fromArray(SAMPLE).filter(col("a").gt(15)).filter(col("b").lt(400));
    const optimized = optimizePlan((df as any).plan);
    // After combining, chain should be Filter → Source (one Filter only).
    expect(tagsInOrder(optimized)).toEqual(["Filter", "Source"]);
    const filter = optimized as any;
    expect(filter.expr).toBeDefined();
    expect(filter.expr.type).toBe("binary");
    expect(filter.expr.op).toBe("AND");
  });

  it("combined filter produces same rows as two separate filters", async () => {
    const df = DataFrame.fromArray(SAMPLE).filter(col("a").gt(15)).filter(col("b").lt(400));
    const rows = await df.collect();
    expect(rows.map((r: any) => r.id)).toEqual([2, 3]);
  });

  it("combines three filters via repeated application", async () => {
    const df = DataFrame.fromArray(SAMPLE)
      .filter(col("a").gte(10))
      .filter(col("b").lt(500))
      .filter(col("region").eq("north"));
    const optimized = optimizePlan((df as any).plan);
    expect(tagsInOrder(optimized)).toEqual(["Filter", "Source"]);
    const rows = await df.collect();
    expect(rows.map((r: any) => r.id)).toEqual([1, 3]);
  });

  it("opaque filters (no expr) still compose via function", async () => {
    const df = DataFrame.fromArray(SAMPLE)
      .filter((r: any) => r.a > 15)
      .filter((r: any) => r.b < 400);
    const optimized = optimizePlan((df as any).plan);
    expect(tagsInOrder(optimized)).toEqual(["Filter", "Source"]);
    const rows = await df.collect();
    expect(rows.map((r: any) => r.id)).toEqual([2, 3]);
  });
});

// ---------------------------------------------------------------------------
// Filter pushdown
// ---------------------------------------------------------------------------

describe("optimizer — filter pushdown", () => {
  it("pushes Filter past Sort", () => {
    const df = DataFrame.fromArray(SAMPLE).sort("a", "desc").filter(col("a").gt(20));
    const optimized = optimizePlan((df as any).plan);
    expect(tagsInOrder(optimized)).toEqual(["Sort", "Filter", "Source"]);
  });

  it("Filter past Sort produces same rows as unoptimized execution", async () => {
    const df = DataFrame.fromArray(SAMPLE).sort("a", "desc").filter(col("a").gt(20));
    const rows = await df.collect();
    expect(rows.map((r: any) => r.id)).toEqual([5, 4, 3]);
  });

  it("pushes Filter past Drop when no dropped column is referenced", () => {
    const df = DataFrame.fromArray(SAMPLE).drop("b").filter(col("a").gt(20));
    const optimized = optimizePlan((df as any).plan);
    expect(tagsInOrder(optimized)).toEqual(["Drop", "Filter", "Source"]);
  });

  it("does NOT push Filter past Drop when filter uses dropped column", () => {
    // Filter references "a", but "a" is dropped — cannot push below Drop.
    const df = DataFrame.fromArray(SAMPLE).drop("a").filter(col("a").gt(20));
    const optimized = optimizePlan((df as any).plan);
    // Plan should still have Filter above Drop (no pushdown).
    expect(optimized._tag).toBe("Filter");
    expect((optimized as any).input._tag).toBe("Drop");
  });

  it("does push Filter past Drop when filter uses a kept column", async () => {
    const df = DataFrame.fromArray(SAMPLE).drop("b").filter(col("a").gt(20));
    const rows = await df.collect();
    for (const r of rows as any[]) {
      expect("b" in r).toBe(false);
      expect(r.a).toBeGreaterThan(20);
    }
  });

  it("pushes Filter past Rename and rewrites column references", () => {
    const df = DataFrame.fromArray(SAMPLE).rename({ a: "alpha" }).filter(col("alpha").gt(20));
    const optimized = optimizePlan((df as any).plan);
    expect(tagsInOrder(optimized)).toEqual(["Rename", "Filter", "Source"]);
    const innerFilter = find(optimized, "Filter") as any;
    // Filter expr should reference original column "a" after rewrite
    expect(innerFilter.expr.left.name).toBe("a");
  });

  it("Filter past Rename produces correct rows", async () => {
    const df = DataFrame.fromArray(SAMPLE).rename({ a: "alpha" }).filter(col("alpha").gt(20));
    const rows = await df.collect();
    expect(rows.map((r: any) => r.alpha)).toEqual([30, 40, 50]);
  });

  it("pushes Filter past WithColumn when filter doesn't reference new column", () => {
    const df = DataFrame.fromArray(SAMPLE)
      .withColumn("bonus", col("a").mul(2))
      .filter(col("b").gt(200));
    const optimized = optimizePlan((df as any).plan);
    expect(tagsInOrder(optimized)).toEqual(["WithColumn", "Filter", "Source"]);
  });

  it("does NOT push Filter past WithColumn when filter references new column", () => {
    const df = DataFrame.fromArray(SAMPLE)
      .withColumn("bonus", col("a").mul(2))
      .filter(col("bonus").gt(50));
    const optimized = optimizePlan((df as any).plan);
    // Filter must stay above WithColumn
    expect(optimized._tag).toBe("Filter");
    const inner = (optimized as any).input;
    expect(inner._tag).toBe("WithColumn");
  });

  it("semantic equivalence: Filter past WithColumn preserves results", async () => {
    const df = DataFrame.fromArray(SAMPLE)
      .withColumn("bonus", col("a").mul(2))
      .filter(col("b").gt(200));
    const rows = await df.collect();
    expect(rows.map((r: any) => ({ id: r.id, bonus: r.bonus }))).toEqual([
      { id: 3, bonus: 60 },
      { id: 4, bonus: 80 },
      { id: 5, bonus: 100 },
    ]);
  });
});

// ---------------------------------------------------------------------------
// Filter split at Join
// ---------------------------------------------------------------------------

describe("optimizer — filter split at Join", () => {
  const leftData = [
    { k: 1, lv: "a" },
    { k: 2, lv: "b" },
    { k: 3, lv: "c" },
  ];
  const rightData = [
    { k: 1, rv: 100 },
    { k: 2, rv: 200 },
    { k: 3, rv: 300 },
  ];

  it("splits AND conjuncts: left-only goes to left branch, right-only to right", () => {
    const df = DataFrame.fromArray(leftData)
      .join(DataFrame.fromArray(rightData), { on: "k", type: "inner" })
      .filter(col("lv").eq("b").and(col("rv").gt(100)));
    const optimized = optimizePlan((df as any).plan);
    // Expect: Join(Filter(leftSource), Filter(rightSource)) — both branches filtered.
    const join = find(optimized, "Join");
    expect(join).not.toBeNull();
    expect(join.left._tag).toBe("Filter");
    expect(join.right._tag).toBe("Filter");
  });

  it("leaves cross-branch predicates above the Join", () => {
    // Single OR atom touches both lv (left-only) and rv (right-only).
    // splitAnd can't split OR, and the atom can't live on either side alone.
    const df = DataFrame.fromArray(leftData)
      .join(DataFrame.fromArray(rightData), { on: "k", type: "inner" })
      .filter(col("lv").eq("a").or(col("rv").gt(200)));
    const optimized = optimizePlan((df as any).plan);
    expect(optimized._tag).toBe("Filter");
    expect((optimized as any).input._tag).toBe("Join");
  });

  it("semantic equivalence after split", async () => {
    const df = DataFrame.fromArray(leftData)
      .join(DataFrame.fromArray(rightData), { on: "k", type: "inner" })
      .filter(col("lv").eq("b").and(col("rv").gt(100)));
    const rows = await df.collect();
    expect(rows).toEqual([{ k: 2, lv: "b", rv: 200 }]);
  });
});

// ---------------------------------------------------------------------------
// Limit pushdown
// ---------------------------------------------------------------------------

describe("optimizer — limit pushdown", () => {
  it("pushes Limit past Select", () => {
    const df = DataFrame.fromArray(SAMPLE).select("id", "a").limit(2);
    const optimized = optimizePlan((df as any).plan);
    // Limit should be below Select now; after limitIntoSource the whole
    // chain may collapse to Select(Source). Accept either form:
    const tags = tagsInOrder(optimized);
    expect(tags[0]).toBe("Select");
    // The resulting source has at most 2 rows
    const src = find(optimized, "Source");
    expect(src.data.length).toBeLessThanOrEqual(2);
  });

  it("pushes Limit past WithColumn", async () => {
    const df = DataFrame.fromArray(SAMPLE).withColumn("bonus", col("a").mul(2)).limit(2);
    const optimized = optimizePlan((df as any).plan);
    expect(optimized._tag).toBe("WithColumn");
    const rows = await df.collect();
    expect(rows.length).toBe(2);
    expect((rows[0] as any).bonus).toBe(20);
  });

  it("pushes Limit past Drop", async () => {
    const df = DataFrame.fromArray(SAMPLE).drop("b").limit(2);
    const optimized = optimizePlan((df as any).plan);
    expect(optimized._tag).toBe("Drop");
    const rows = await df.collect();
    expect(rows.length).toBe(2);
  });

  it("pushes Limit past Rename", async () => {
    const df = DataFrame.fromArray(SAMPLE).rename({ a: "alpha" }).limit(2);
    const optimized = optimizePlan((df as any).plan);
    expect(optimized._tag).toBe("Rename");
    const rows = await df.collect();
    expect(rows.length).toBe(2);
  });

  it("does NOT push Limit past Filter (row-removing op)", () => {
    const df = DataFrame.fromArray(SAMPLE).filter(col("a").gt(20)).limit(2);
    const optimized = optimizePlan((df as any).plan);
    // Root must still be Limit above the Filter
    expect(optimized._tag).toBe("Limit");
  });

  it("does NOT push Limit past Sort (would break top-N semantics)", () => {
    const df = DataFrame.fromArray(SAMPLE).sort("a", "desc").limit(2);
    const optimized = optimizePlan((df as any).plan);
    // Root must still be Limit (existing Sort+Limit optimization lives in the executor, not the plan)
    expect(optimized._tag).toBe("Limit");
  });

  it("end-to-end correctness — complex chain", async () => {
    const df = DataFrame.fromArray(SAMPLE)
      .withColumn("bonus", col("a").mul(3))
      .select("id", "bonus")
      .limit(3);
    const rows = await df.collect();
    expect(rows).toEqual([
      { id: 1, bonus: 30 },
      { id: 2, bonus: 60 },
      { id: 3, bonus: 90 },
    ]);
  });
});

// ---------------------------------------------------------------------------
// Limit into Source
// ---------------------------------------------------------------------------

describe("optimizer — limit into Source", () => {
  it("slices a concrete Source.data array when Limit is adjacent", () => {
    const df = DataFrame.fromArray(SAMPLE).limit(3);
    const optimized = optimizePlan((df as any).plan);
    expect(optimized._tag).toBe("Source");
    expect((optimized as any).data.length).toBe(3);
  });

  it("is a no-op when limit ≥ source size", () => {
    const df = DataFrame.fromArray(SAMPLE).limit(100);
    const optimized = optimizePlan((df as any).plan);
    // collapsed to the Source unchanged
    expect(optimized._tag).toBe("Source");
    expect((optimized as any).data.length).toBe(SAMPLE.length);
  });

  it("does NOT slice when Source has a loader and empty data", () => {
    const plan: LogicalPlan = {
      _tag: "Limit",
      n: 2,
      input: {
        _tag: "Source",
        data: [],
        load: async () => [{ x: 1 }, { x: 2 }, { x: 3 }],
      },
    };
    const optimized = optimizePlan(plan);
    // Must preserve the Limit above the lazy Source so the loader decides
    expect(optimized._tag).toBe("Limit");
  });
});

// ---------------------------------------------------------------------------
// CSE on WithColumn
// ---------------------------------------------------------------------------

describe("optimizer — CSE on WithColumn", () => {
  it("aliases the second WithColumn to the first when expressions match", () => {
    const df = DataFrame.fromArray(SAMPLE)
      .withColumn("x1", col("a").mul(2))
      .withColumn("x2", col("a").mul(2));
    const optimized = optimizePlan((df as any).plan);
    // Outer WithColumn should have expr of type "col" pointing at x1
    const outer = optimized as any;
    expect(outer._tag).toBe("WithColumn");
    expect(outer.name).toBe("x2");
    expect(outer.expr.type).toBe("col");
    expect(outer.expr.name).toBe("x1");
  });

  it("still produces correct row values after CSE", async () => {
    const df = DataFrame.fromArray(SAMPLE)
      .withColumn("x1", col("a").mul(2))
      .withColumn("x2", col("a").mul(2));
    const rows = await df.collect();
    for (const r of rows as any[]) {
      expect(r.x1).toBe(r.a * 2);
      expect(r.x2).toBe(r.a * 2);
      expect(r.x1).toBe(r.x2);
    }
  });

  it("does not trigger when expressions differ", () => {
    const df = DataFrame.fromArray(SAMPLE)
      .withColumn("x1", col("a").mul(2))
      .withColumn("x2", col("a").mul(3));
    const optimized = optimizePlan((df as any).plan);
    const outer = optimized as any;
    expect(outer._tag).toBe("WithColumn");
    expect(outer.name).toBe("x2");
    expect(outer.expr.type).toBe("binary"); // original expr, not rewritten
  });

  it("does not trigger when expressions reference different columns", () => {
    const df = DataFrame.fromArray(SAMPLE)
      .withColumn("x1", col("a").mul(2))
      .withColumn("x2", col("b").mul(2));
    const optimized = optimizePlan((df as any).plan);
    expect((optimized as any).expr.type).toBe("binary");
  });
});

// ---------------------------------------------------------------------------
// Fixed-point convergence
// ---------------------------------------------------------------------------

describe("optimizer — fixed-point convergence", () => {
  it("converges (no oscillation) for Filter above Sort above Source", () => {
    const df = DataFrame.fromArray(SAMPLE).sort("a", "asc").filter(col("a").gt(20));
    const once = optimizePlan((df as any).plan);
    const twice = optimizePlan(once);
    expect(JSON.stringify(stripFns(twice))).toBe(JSON.stringify(stripFns(once)));
  });

  it("converges for Select above Filter above Source", () => {
    const df = DataFrame.fromArray(SAMPLE).filter(col("a").gt(20)).select("id", "a");
    const once = optimizePlan((df as any).plan);
    const twice = optimizePlan(once);
    expect(JSON.stringify(stripFns(twice))).toBe(JSON.stringify(stripFns(once)));
  });

  it("converges for complex mixed plan", () => {
    const df = DataFrame.fromArray(SAMPLE)
      .withColumn("bonus", col("a").mul(2))
      .filter(col("b").gt(100))
      .sort("a", "desc")
      .limit(2)
      .select("id", "bonus");
    const once = optimizePlan((df as any).plan);
    const twice = optimizePlan(once);
    expect(JSON.stringify(stripFns(twice))).toBe(JSON.stringify(stripFns(once)));
  });
});

/** Deep-clone a plan stripping function properties so JSON.stringify is stable. */
function stripFns(plan: any): any {
  if (Array.isArray(plan)) return plan.map(stripFns);
  if (plan && typeof plan === "object") {
    const out: any = {};
    for (const [k, v] of Object.entries(plan)) {
      if (typeof v === "function") continue;
      out[k] = stripFns(v);
    }
    return out;
  }
  return plan;
}

// ---------------------------------------------------------------------------
// End-to-end correctness (optimized == unoptimized results)
// ---------------------------------------------------------------------------

describe("optimizer — end-to-end correctness", () => {
  it("filter + sort + select returns same rows as unoptimized", async () => {
    const df = DataFrame.fromArray(SAMPLE)
      .filter(col("a").gte(20))
      .sort("a", "asc")
      .select("id", "a");
    const rows = await df.collect();
    expect(rows).toEqual([
      { id: 2, a: 20 },
      { id: 3, a: 30 },
      { id: 4, a: 40 },
      { id: 5, a: 50 },
    ]);
  });

  it("withColumn + filter + limit + select", async () => {
    const df = DataFrame.fromArray(SAMPLE)
      .withColumn("bonus", col("a").mul(10))
      .filter(col("region").eq("north"))
      .limit(5)
      .select("id", "bonus");
    const rows = await df.collect();
    expect(rows).toEqual([
      { id: 1, bonus: 100 },
      { id: 3, bonus: 300 },
    ]);
  });

  it("chained filters + rename + drop", async () => {
    const df = DataFrame.fromArray(SAMPLE)
      .rename({ a: "alpha" })
      .drop("b")
      .filter(col("alpha").gt(15))
      .filter(col("active").eq(true));
    const rows = await df.collect();
    expect(rows.map((r: any) => r.id)).toEqual([3, 4]);
    for (const r of rows as any[]) {
      expect("b" in r).toBe(false);
      expect("alpha" in r).toBe(true);
    }
  });
});
