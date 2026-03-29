import { describe, it, expect } from "bun:test";
import { DataFrame } from "./dataframe.ts";
import { col, lit, when } from "./expr.ts";

describe("Expression builder", () => {
  const data = [
    { name: "alice", age: 30, score: 85, region: "north" },
    { name: "bob", age: 25, score: 92, region: "south" },
    { name: "charlie", age: 35, score: 78, region: "north" },
    { name: "diana", age: 28, score: 95, region: "east" },
    { name: "eve", age: 40, score: 60, region: "west" },
  ];
  const df = DataFrame.fromArray(data);

  describe("col() — column reference", () => {
    it("withColumn using col arithmetic", async () => {
      const result = await df
        .withColumn("bonus", col("score").mul(10))
        .select("name", "bonus")
        .collect();
      expect(result[0]).toEqual({ name: "alice", bonus: 850 });
      expect(result[1]).toEqual({ name: "bob", bonus: 920 });
    });

    it("withColumn using col add", async () => {
      const result = await df
        .withColumn("nextAge", col("age").add(1))
        .select("name", "nextAge")
        .head(2);
      expect(result).toEqual([
        { name: "alice", nextAge: 31 },
        { name: "bob", nextAge: 26 },
      ]);
    });

    it("withColumn combining two columns", async () => {
      const result = await df
        .withColumn("combined", col("age").add(col("score")))
        .select("name", "combined")
        .head(1);
      expect(result).toEqual([{ name: "alice", combined: 115 }]);
    });
  });

  describe("filter with expressions", () => {
    it("filter gt", async () => {
      const result = await df.filter(col("age").gt(30)).collect();
      expect(result.map((r) => r.name)).toEqual(["charlie", "eve"]);
    });

    it("filter eq", async () => {
      const result = await df.filter(col("region").eq("north")).collect();
      expect(result.map((r) => r.name)).toEqual(["alice", "charlie"]);
    });

    it("filter and", async () => {
      const result = await df.filter(col("age").gt(25).and(col("score").gt(80))).collect();
      expect(result.map((r) => r.name)).toEqual(["alice", "diana"]);
    });

    it("filter or", async () => {
      const result = await df.filter(col("age").lt(26).or(col("age").gt(38))).collect();
      expect(result.map((r) => r.name)).toEqual(["bob", "eve"]);
    });

    it("filter not", async () => {
      const result = await df.filter(col("region").eq("north").not()).collect();
      expect(result.length).toBe(3);
    });

    it("filter between", async () => {
      const result = await df.filter(col("age").between(28, 35)).collect();
      expect(result.map((r) => r.name)).toEqual(["alice", "charlie", "diana"]);
    });

    it("filter isIn", async () => {
      const result = await df.filter(col("region").isIn(["north", "east"])).collect();
      expect(result.map((r) => r.name)).toEqual(["alice", "charlie", "diana"]);
    });
  });

  describe("when() — conditional expressions", () => {
    it("simple when/otherwise", async () => {
      const result = await df
        .withColumn("tier", when(col("score").gt(90), "A").otherwise("B"))
        .select("name", "tier")
        .collect();
      expect(result).toEqual([
        { name: "alice", tier: "B" },
        { name: "bob", tier: "A" },
        { name: "charlie", tier: "B" },
        { name: "diana", tier: "A" },
        { name: "eve", tier: "B" },
      ]);
    });

    it("multiple when branches", async () => {
      const result = await df
        .withColumn(
          "grade",
          when(col("score").gt(90), "A")
            .when(col("score").gt(80), "B")
            .when(col("score").gt(70), "C")
            .otherwise("D"),
        )
        .select("name", "grade")
        .collect();
      expect(result).toEqual([
        { name: "alice", grade: "B" },
        { name: "bob", grade: "A" },
        { name: "charlie", grade: "C" },
        { name: "diana", grade: "A" },
        { name: "eve", grade: "D" },
      ]);
    });
  });

  describe("string operations on expressions", () => {
    it("upper", async () => {
      const result = await df
        .withColumn("upper_name", col("name").upper())
        .select("upper_name")
        .head(2);
      expect(result).toEqual([{ upper_name: "ALICE" }, { upper_name: "BOB" }]);
    });

    it("contains", async () => {
      const result = await df.filter(col("name").contains("li")).collect();
      expect(result.map((r) => r.name)).toEqual(["alice", "charlie"]);
    });
  });

  describe("lit() — literal values", () => {
    it("withColumn with literal", async () => {
      const result = await df.withColumn("constant", lit(42)).select("name", "constant").head(1);
      expect(result).toEqual([{ name: "alice", constant: 42 }]);
    });

    it("arithmetic with literal", async () => {
      const result = await df
        .withColumn("pct", col("score").div(lit(100)))
        .select("name", "pct")
        .head(1);
      expect(result).toEqual([{ name: "alice", pct: 0.85 }]);
    });
  });
});
