import { describe, it, expect } from "bun:test";
import { DataFrame } from "@promin/core";
import { DuckDBExecutor } from "./duckdb-executor.ts";

const duckdb = new DuckDBExecutor();

function df<T>(data: T[]) {
  return DataFrame.fromArray(data).withExecutor(duckdb);
}

describe("DuckDBExecutor", () => {
  describe("source + basic ops", () => {
    it("collects source data", async () => {
      const result = await df([{ id: 1 }, { id: 2 }, { id: 3 }]).collect();
      expect(result).toEqual([{ id: 1 }, { id: 2 }, { id: 3 }]);
    });

    it("select columns", async () => {
      const result = await df([
        { id: 1, name: "a", score: 10 },
        { id: 2, name: "b", score: 20 },
      ])
        .select("id", "name")
        .collect();
      expect(result).toEqual([
        { id: 1, name: "a" },
        { id: 2, name: "b" },
      ]);
    });

    it("drop columns", async () => {
      const result = await df([{ id: 1, name: "a", score: 10 }])
        .drop("score")
        .collect();
      expect(result).toEqual([{ id: 1, name: "a" }]);
    });
  });

  describe("sort", () => {
    it("sorts ascending", async () => {
      const result = await df([{ v: 3 }, { v: 1 }, { v: 2 }])
        .sort("v")
        .collect();
      expect(result).toEqual([{ v: 1 }, { v: 2 }, { v: 3 }]);
    });

    it("sorts descending", async () => {
      const result = await df([{ v: 3 }, { v: 1 }, { v: 2 }])
        .sort("v", "desc")
        .collect();
      expect(result).toEqual([{ v: 3 }, { v: 2 }, { v: 1 }]);
    });

    it("sort + limit", async () => {
      const result = await df([{ v: 5 }, { v: 3 }, { v: 1 }, { v: 4 }, { v: 2 }])
        .sort("v", "desc")
        .limit(3)
        .collect();
      expect(result).toEqual([{ v: 5 }, { v: 4 }, { v: 3 }]);
    });
  });

  describe("groupBy + agg", () => {
    const data = [
      { region: "north", revenue: 100 },
      { region: "south", revenue: 200 },
      { region: "north", revenue: 300 },
      { region: "south", revenue: 400 },
    ];

    it("sum", async () => {
      const result = await df(data)
        .groupBy("region")
        .agg({ revenue: "sum" })
        .sort("region")
        .collect();
      expect(result).toEqual([
        { region: "north", revenue_sum: 400 },
        { region: "south", revenue_sum: 600 },
      ]);
    });

    it("count", async () => {
      const result = await df(data)
        .groupBy("region")
        .agg({ revenue: "count" })
        .sort("region")
        .collect();
      expect(result).toEqual([
        { region: "north", revenue_count: 2 },
        { region: "south", revenue_count: 2 },
      ]);
    });

    it("avg", async () => {
      const result = await df(data)
        .groupBy("region")
        .agg({ revenue: "avg" })
        .sort("region")
        .collect();
      expect(result[0]!.revenue_avg).toBeCloseTo(200);
      expect(result[1]!.revenue_avg).toBeCloseTo(300);
    });
  });

  describe("join", () => {
    const left = [
      { id: 1, name: "alice" },
      { id: 2, name: "bob" },
      { id: 3, name: "charlie" },
    ];
    const right = [
      { id: 1, tier: "gold" },
      { id: 3, tier: "silver" },
    ];

    it("inner join", async () => {
      const result = await df(left)
        .join(DataFrame.fromArray(right).withExecutor(duckdb), { on: "id", type: "inner" })
        .sort("id")
        .collect();
      expect(result).toEqual([
        { id: 1, name: "alice", tier: "gold" },
        { id: 3, name: "charlie", tier: "silver" },
      ]);
    });

    it("semi join (intersection)", async () => {
      const result = await df(left)
        .join(DataFrame.fromArray(right).withExecutor(duckdb), { on: "id", type: "semi" })
        .sort("id")
        .collect();
      expect(result).toEqual([
        { id: 1, name: "alice" },
        { id: 3, name: "charlie" },
      ]);
    });

    it("anti join (difference)", async () => {
      const result = await df(left)
        .join(DataFrame.fromArray(right).withExecutor(duckdb), { on: "id", type: "anti" })
        .collect();
      expect(result).toEqual([{ id: 2, name: "bob" }]);
    });
  });

  describe("distinct", () => {
    it("distinct all columns", async () => {
      const result = await df([{ v: 1 }, { v: 2 }, { v: 1 }, { v: 3 }])
        .distinct()
        .sort("v")
        .collect();
      expect(result).toEqual([{ v: 1 }, { v: 2 }, { v: 3 }]);
    });
  });

  describe("concat + union", () => {
    it("concat", async () => {
      const a = df([{ id: 1 }, { id: 2 }]);
      const b = df([{ id: 3 }, { id: 4 }]);
      const result = await DataFrame.concat(a, b).withExecutor(duckdb).collect();
      expect(result).toEqual([{ id: 1 }, { id: 2 }, { id: 3 }, { id: 4 }]);
    });

    it("union deduplicates", async () => {
      const a = df([{ id: 1 }, { id: 2 }]);
      const b = df([{ id: 2 }, { id: 3 }]);
      const result = await a.union(b).sort("id").collect();
      expect(result).toEqual([{ id: 1 }, { id: 2 }, { id: 3 }]);
    });
  });

  describe("filter + map (JS fallback)", () => {
    it("filter with JS function", async () => {
      const result = await df([{ v: 1 }, { v: 2 }, { v: 3 }, { v: 4 }])
        .filter((r) => r.v > 2)
        .collect();
      expect(result).toEqual([{ v: 3 }, { v: 4 }]);
    });

    it("withColumn with JS function", async () => {
      const result = await df([{ v: 1 }, { v: 2 }])
        .withColumn("doubled", (r) => r.v * 2)
        .collect();
      expect(result).toEqual([
        { v: 1, doubled: 2 },
        { v: 2, doubled: 4 },
      ]);
    });
  });

  describe("chained operations", () => {
    it("filter → groupBy → sort", async () => {
      const data = [
        { region: "north", revenue: 100, active: true },
        { region: "south", revenue: 200, active: false },
        { region: "north", revenue: 300, active: true },
        { region: "south", revenue: 400, active: true },
      ];
      const result = await df(data)
        .filter((r) => r.active)
        .groupBy("region")
        .agg({ revenue: "sum" })
        .sort("region")
        .collect();
      expect(result).toEqual([
        { region: "north", revenue_sum: 400 },
        { region: "south", revenue_sum: 400 },
      ]);
    });
  });

  describe("table caching (load once, query many)", () => {
    it("second query on same source is faster (cached)", async () => {
      const data = Array.from({ length: 10_000 }, (_, i) => ({
        id: i,
        region: ["north", "south", "east", "west"][i % 4]!,
        revenue: i * 10,
      }));
      const base = df(data);

      // First query — loads data
      const start1 = performance.now();
      await base.groupBy("region").agg({ revenue: "sum" }).collect();
      const first = performance.now() - start1;

      // Second query — should reuse cached table
      const start2 = performance.now();
      await base.sort("revenue", "desc").limit(5).collect();
      const second = performance.now() - start2;

      // Second should be noticeably faster (no data loading)
      expect(second).toBeLessThan(first);
    });

    it("different sources get different tables", async () => {
      const a = df([{ v: 1 }, { v: 2 }]);
      const b = df([{ v: 10 }, { v: 20 }]);

      const resultA = await a.sort("v").collect();
      const resultB = await b.sort("v").collect();

      expect(resultA).toEqual([{ v: 1 }, { v: 2 }]);
      expect(resultB).toEqual([{ v: 10 }, { v: 20 }]);
    });
  });
});
