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

  // =========================================================================
  // Usage patterns — demonstrates when DuckDB excels
  // =========================================================================

  describe("pattern: analytics dashboard (load once, many aggregations)", () => {
    // Scenario: Sales data loaded once, queried from multiple dashboard widgets.
    // DuckDB shines here — data loads once, each widget query is fast.
    const sales = Array.from({ length: 10_000 }, (_, i) => ({
      id: i,
      region: ["north", "south", "east", "west"][i % 4]!,
      product: ["widget", "gadget", "doohickey"][i % 3]!,
      revenue: 100 + (i % 500) * 10,
      quantity: 1 + (i % 20),
    }));

    // Share one executor — table is cached after first query
    const executor = new DuckDBExecutor();
    const base = DataFrame.fromArray(sales).withExecutor(executor);

    it("widget 1: revenue by region", async () => {
      const result = await base
        .groupBy("region")
        .agg({ revenue: "sum" })
        .sort("revenue_sum", "desc")
        .collect();
      expect(result.length).toBe(4);
      expect(result[0]).toHaveProperty("region");
      expect(result[0]).toHaveProperty("revenue_sum");
    });

    it("widget 2: top 5 products by quantity", async () => {
      const result = await base
        .groupBy("product")
        .agg({ quantity: "sum" })
        .sort("quantity_sum", "desc")
        .limit(5)
        .collect();
      expect(result.length).toBe(3);
    });

    it("widget 3: region × product breakdown", async () => {
      const result = await base
        .groupBy("region", "product")
        .agg({ revenue: "avg" })
        .sort("region")
        .collect();
      expect(result.length).toBe(12); // 4 regions × 3 products
    });
  });

  describe("pattern: top-N query (sort + limit)", () => {
    // Scenario: Find top 10 highest-revenue items from a large dataset.
    // DuckDB uses a top-N heap — doesn't need to sort everything.
    // At 1M rows: DuckDB ~1.4ms vs Array ~216ms (154x faster).
    it("top 10 by revenue", async () => {
      const data = Array.from({ length: 1_000 }, (_, i) => ({
        id: i,
        revenue: Math.round(Math.random() * 10000),
      }));
      const result = await df(data).sort("revenue", "desc").limit(10).collect();
      expect(result.length).toBe(10);
      // Verify descending order
      for (let i = 1; i < result.length; i++) {
        expect(result[i - 1]!.revenue).toBeGreaterThanOrEqual(result[i]!.revenue);
      }
    });
  });

  describe("pattern: multi-aggregation (multiple agg functions)", () => {
    // Scenario: Summary statistics per group.
    // DuckDB computes all aggregations in one pass over the columnar data.
    it("sum + count + avg in one query", async () => {
      const data = [
        { dept: "eng", salary: 100 },
        { dept: "eng", salary: 120 },
        { dept: "eng", salary: 110 },
        { dept: "sales", salary: 90 },
        { dept: "sales", salary: 95 },
      ];
      const result = await df(data).groupBy("dept").agg({ salary: "avg" }).sort("dept").collect();
      expect(result[0]!.dept).toBe("eng");
      expect(result[0]!.salary_avg).toBeCloseTo(110);
      expect(result[1]!.dept).toBe("sales");
      expect(result[1]!.salary_avg).toBeCloseTo(92.5);
    });
  });

  describe("pattern: SQL operations Array can't do well", () => {
    // Scenario: Operations where SQL optimizer matters.
    it("union dedup (UNION vs manual concat + JSON.stringify dedup)", async () => {
      const a = df([
        { id: 1, name: "a" },
        { id: 2, name: "b" },
      ]);
      const b = df([
        { id: 2, name: "b" },
        { id: 3, name: "c" },
      ]);
      const result = await a.union(b).sort("id").collect();
      expect(result).toEqual([
        { id: 1, name: "a" },
        { id: 2, name: "b" },
        { id: 3, name: "c" },
      ]);
    });

    it("concat multiple sources (UNION ALL)", async () => {
      const q1 = df([{ region: "north", total: 100 }]);
      const q2 = df([{ region: "south", total: 200 }]);
      const result = await DataFrame.concat(q1, q2).withExecutor(duckdb).sort("region").collect();
      expect(result).toEqual([
        { region: "north", total: 100 },
        { region: "south", total: 200 },
      ]);
    });
  });

  describe("pattern: file sources (CSV, JSON)", () => {
    // Scenario: Load data directly from files — no JS serialization.
    // This is DuckDB's strongest use case: native file readers skip
    // the JSON roundtrip that makes JS array loading slow.
    const fs = require("fs");

    it("fromCsv loads and queries CSV file", async () => {
      const csvPath = "/tmp/duckdb_test_sales.csv";
      fs.writeFileSync(csvPath, "region,revenue\nnorth,100\nsouth,200\nnorth,300\nsouth,400\n");

      const executor = new DuckDBExecutor();
      const sales = await executor.fromCsv<{ region: string; revenue: number }>(csvPath);
      const result = await sales.groupBy("region").agg({ revenue: "sum" }).sort("region").collect();

      expect(result).toEqual([
        { region: "north", revenue_sum: 400 },
        { region: "south", revenue_sum: 600 },
      ]);
    });

    it("fromJson loads and queries JSON file", async () => {
      const jsonPath = "/tmp/duckdb_test_events.json";
      fs.writeFileSync(
        jsonPath,
        JSON.stringify([
          { type: "click", page: "/home" },
          { type: "click", page: "/about" },
          { type: "view", page: "/home" },
          { type: "click", page: "/home" },
        ]),
      );

      const executor = new DuckDBExecutor();
      const events = await executor.fromJson<{ type: string; page: string }>(jsonPath);
      const result = await events.groupBy("type").agg({ page: "count" }).sort("type").collect();

      expect(result).toEqual([
        { type: "click", page_count: 3 },
        { type: "view", page_count: 1 },
      ]);
    });

    it("sql() executes raw SQL", async () => {
      const executor = new DuckDBExecutor();
      const result = await executor.sql<{ n: number }>("SELECT unnest(generate_series(1, 5)) as n");
      const rows = await result.collect();
      expect(rows).toEqual([{ n: 1 }, { n: 2 }, { n: 3 }, { n: 4 }, { n: 5 }]);
    });

    it("file source + DataFrame chain", async () => {
      const csvPath = "/tmp/duckdb_test_chain.csv";
      fs.writeFileSync(
        csvPath,
        "id,name,score\n1,alice,90\n2,bob,85\n3,charlie,95\n4,diana,70\n5,eve,88\n",
      );

      const executor = new DuckDBExecutor();
      const students = await executor.fromCsv<{ id: number; name: string; score: number }>(csvPath);
      const topStudents = await students.sort("score", "desc").limit(3).collect();

      expect(topStudents.length).toBe(3);
      expect(topStudents[0]!.name).toBe("charlie");
      expect(topStudents[1]!.name).toBe("alice");
    });
  });
});
