import { describe, it, expect } from "bun:test";
import { DataFrame, CsvFile } from "@promin/core";
import { AutoExecutor } from "./auto-executor.ts";
import { writeFileSync } from "fs";

describe("AutoExecutor", () => {
  describe("basic selection", () => {
    it("uses Array for small data", async () => {
      const executor = new AutoExecutor();
      const result = await DataFrame.fromArray([{ id: 1 }, { id: 2 }])
        .withExecutor(executor)
        .collect();
      expect(result).toEqual([{ id: 1 }, { id: 2 }]);
    });

    it("uses DuckDB for file-backed sources", async () => {
      const csvPath = "/tmp/auto_test_file.csv";
      writeFileSync(csvPath, "name,score\nalice,90\nbob,85\n");

      const executor = new AutoExecutor();
      const result = await (DataFrame.fromFile(CsvFile(csvPath)) as DataFrame<any>)
        .withExecutor(executor)
        .sort("score", "desc")
        .collect();
      expect(result[0]!.name).toBe("alice");
    });

    it("collectSync always uses Array", () => {
      const executor = new AutoExecutor();
      const result = DataFrame.fromArray([{ v: 1 }])
        .withExecutor(executor)
        .collectSync();
      expect(result).toEqual([{ v: 1 }]);
    });
  });

  describe("smart plan-based selection", () => {
    const bigData = Array.from({ length: 20_000 }, (_, i) => ({
      id: i,
      region: ["north", "south", "east", "west"][i % 4]!,
      revenue: i * 10,
      score: i % 100,
    }));

    it("groupBy on large data → DuckDB", async () => {
      const executor = new AutoExecutor({ threshold: 10_000 });
      const result = await DataFrame.fromArray(bigData)
        .withExecutor(executor)
        .groupBy("region")
        .agg({ revenue: "sum" })
        .sort("region")
        .collect();
      expect(result.length).toBe(4);
    });

    it("sort + limit on large data → DuckDB", async () => {
      const executor = new AutoExecutor({ threshold: 10_000 });
      const result = await DataFrame.fromArray(bigData)
        .withExecutor(executor)
        .sort("revenue", "desc")
        .limit(5)
        .collect();
      expect(result.length).toBe(5);
      expect(result[0]!.revenue).toBeGreaterThan(result[4]!.revenue);
    });

    it("filter-only on large data → Array (pass-through)", async () => {
      const executor = new AutoExecutor({ threshold: 10_000 });
      // filter is a pass-through op — DuckDB falls back to JS anyway
      const result = await DataFrame.fromArray(bigData)
        .withExecutor(executor)
        .filter((r) => r.region === "north")
        .collect();
      expect(result.every((r) => r.region === "north")).toBe(true);
    });

    it("small data with groupBy → Array (size overrides)", async () => {
      const smallData = Array.from({ length: 100 }, (_, i) => ({
        region: ["a", "b"][i % 2]!,
        value: i,
      }));
      const executor = new AutoExecutor({ threshold: 10_000 });
      const result = await DataFrame.fromArray(smallData)
        .withExecutor(executor)
        .groupBy("region")
        .agg({ value: "sum" })
        .sort("region")
        .collect();
      expect(result.length).toBe(2);
    });

    it("distinct on large data → DuckDB", async () => {
      const executor = new AutoExecutor({ threshold: 10_000 });
      const result = await DataFrame.fromArray(bigData)
        .withExecutor(executor)
        .select("region")
        .distinct()
        .sort("region")
        .collect();
      expect(result.length).toBe(4);
    });
  });
});
