import { describe, it, expect } from "bun:test";
import { DataFrame, CsvFile } from "@promin/core";
import { AutoExecutor } from "./auto-executor.ts";
import { writeFileSync } from "fs";

describe("AutoExecutor", () => {
  it("uses ArrayExecutor for small data", async () => {
    const executor = new AutoExecutor({ threshold: 100 });
    const df = DataFrame.fromArray([{ id: 1 }, { id: 2 }, { id: 3 }]).withExecutor(executor);
    const result = await df.collect();
    expect(result).toEqual([{ id: 1 }, { id: 2 }, { id: 3 }]);
  });

  it("uses DuckDB for large data", async () => {
    const executor = new AutoExecutor({ threshold: 10 });
    const data = Array.from({ length: 100 }, (_, i) => ({
      id: i,
      region: ["north", "south"][i % 2]!,
      revenue: i * 10,
    }));
    const df = DataFrame.fromArray(data).withExecutor(executor);
    const result = await df.groupBy("region").agg({ revenue: "sum" }).sort("region").collect();
    expect(result.length).toBe(2);
    expect(result[0]!.region).toBe("north");
  });

  it("uses DuckDB for file-backed sources (has hint)", async () => {
    const csvPath = "/tmp/auto_executor_test.csv";
    writeFileSync(csvPath, "name,score\nalice,90\nbob,85\n");

    const executor = new AutoExecutor();
    const df = DataFrame.fromFile(CsvFile(csvPath)).withExecutor(executor);
    const result = await df.sort("score", "desc").collect();
    expect(result[0]!.name).toBe("alice");
  });

  it("collectSync uses ArrayExecutor", () => {
    const executor = new AutoExecutor();
    const df = DataFrame.fromArray([{ v: 1 }, { v: 2 }]).withExecutor(executor);
    const result = df.collectSync();
    expect(result).toEqual([{ v: 1 }, { v: 2 }]);
  });

  it("groupBy works with auto-selection", async () => {
    const executor = new AutoExecutor({ threshold: 5 });
    const data = Array.from({ length: 20 }, (_, i) => ({
      category: ["A", "B", "C", "D"][i % 4]!,
      value: i,
    }));
    const result = await DataFrame.fromArray(data)
      .withExecutor(executor)
      .groupBy("category")
      .agg({ value: "sum" })
      .sort("category")
      .collect();
    expect(result.length).toBe(4);
  });
});
