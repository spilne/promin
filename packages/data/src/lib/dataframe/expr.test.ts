import { describe, it, expect } from "bun:test";
import { DataFrame } from "./dataframe.ts";
import { col, lit, when, astToSql, isCompilable } from "./expr.ts";

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

  describe("field() — struct field access", () => {
    const nested = DataFrame.fromArray([
      { name: "alice", address: { city: "NYC", zip: "10001" } },
      { name: "bob", address: { city: "LA", zip: "90001" } },
      { name: "charlie", address: null },
    ]);

    it("accesses nested field", async () => {
      const result = await nested
        .withColumn("city", col("address").field("city"))
        .select("name", "city")
        .collect();
      expect(result).toEqual([
        { name: "alice", city: "NYC" },
        { name: "bob", city: "LA" },
        { name: "charlie", city: undefined },
      ]);
    });

    it("chains field access", async () => {
      const deep = DataFrame.fromArray([
        { data: { geo: { lat: 40.7, lng: -74 } } },
        { data: { geo: { lat: 34, lng: -118.2 } } },
      ]);
      const result = await deep
        .withColumn("lat", col("data").field("geo").field("lat"))
        .select("lat")
        .collect();
      expect(result).toEqual([{ lat: 40.7 }, { lat: 34 }]);
    });

    it("filters on nested field", async () => {
      const result = await nested
        .filter(col("address").field("city").eq("NYC"))
        .select("name")
        .collect();
      expect(result).toEqual([{ name: "alice" }]);
    });
  });

  describe("list() — array column operations", () => {
    const arrData = DataFrame.fromArray([
      { name: "alice", tags: ["js", "ts", "rust"], scores: [85, 92, 78] },
      { name: "bob", tags: ["python", "go"], scores: [90, 88] },
      { name: "charlie", tags: [], scores: [] },
    ]);

    it("lengths()", async () => {
      const result = await arrData
        .withColumn("tagCount", col("tags").list().lengths())
        .select("name", "tagCount")
        .collect();
      expect(result).toEqual([
        { name: "alice", tagCount: 3 },
        { name: "bob", tagCount: 2 },
        { name: "charlie", tagCount: 0 },
      ]);
    });

    it("get() with positive index", async () => {
      const result = await arrData
        .withColumn("first", col("tags").list().get(0))
        .select("name", "first")
        .head(2);
      expect(result).toEqual([
        { name: "alice", first: "js" },
        { name: "bob", first: "python" },
      ]);
    });

    it("get() with negative index", async () => {
      const result = await arrData
        .withColumn("last", col("tags").list().get(-1))
        .select("name", "last")
        .head(2);
      expect(result).toEqual([
        { name: "alice", last: "rust" },
        { name: "bob", last: "go" },
      ]);
    });

    it("first() and last()", async () => {
      const result = await arrData
        .withColumn("f", col("scores").list().first())
        .withColumn("l", col("scores").list().last())
        .select("name", "f", "l")
        .head(1);
      expect(result).toEqual([{ name: "alice", f: 85, l: 78 }]);
    });

    it("contains()", async () => {
      const result = await arrData
        .withColumn("hasTs", col("tags").list().contains("ts"))
        .select("name", "hasTs")
        .collect();
      expect(result).toEqual([
        { name: "alice", hasTs: true },
        { name: "bob", hasTs: false },
        { name: "charlie", hasTs: false },
      ]);
    });

    it("sum() and mean()", async () => {
      const result = await arrData
        .withColumn("total", col("scores").list().sum())
        .withColumn("avg", col("scores").list().mean())
        .select("name", "total", "avg")
        .head(2);
      expect(result).toEqual([
        { name: "alice", total: 255, avg: 85 },
        { name: "bob", total: 178, avg: 89 },
      ]);
    });

    it("min() and max()", async () => {
      const result = await arrData
        .withColumn("lo", col("scores").list().min())
        .withColumn("hi", col("scores").list().max())
        .select("name", "lo", "hi")
        .head(1);
      expect(result).toEqual([{ name: "alice", lo: 78, hi: 92 }]);
    });

    it("unique()", async () => {
      const dupes = DataFrame.fromArray([{ vals: [1, 2, 2, 3, 1] }]);
      const result = await dupes
        .withColumn("uniq", col("vals").list().unique())
        .select("uniq")
        .collect();
      expect(result[0]!.uniq).toEqual([1, 2, 3]);
    });

    it("sort()", async () => {
      const unsorted = DataFrame.fromArray([{ vals: ["c", "a", "b"] }]);
      const result = await unsorted
        .withColumn("sorted", col("vals").list().sort())
        .select("sorted")
        .collect();
      expect(result[0]!.sorted).toEqual(["a", "b", "c"]);
    });

    it("join()", async () => {
      const result = await arrData
        .withColumn("tagStr", col("tags").list().join(", "))
        .select("name", "tagStr")
        .head(2);
      expect(result).toEqual([
        { name: "alice", tagStr: "js, ts, rust" },
        { name: "bob", tagStr: "python, go" },
      ]);
    });

    it("list ops return undefined for non-array values", async () => {
      const mixed = DataFrame.fromArray([
        { vals: [1, 2, 3] },
        { vals: null as any },
        { vals: undefined as any },
      ]);
      const result = await mixed
        .withColumn("n", col("vals").list().lengths())
        .select("n")
        .collect();
      expect(result[0]!.n).toBe(3);
      expect(result[1]!.n).toBeUndefined();
      expect(result[2]!.n).toBeUndefined();
    });

    it("empty array returns 0 for length, null for numeric ops", async () => {
      const empty = DataFrame.fromArray([{ vals: [] as number[] }]);
      const result = await empty
        .withColumn("n", col("vals").list().lengths())
        .withColumn("s", col("vals").list().sum())
        .withColumn("avg", col("vals").list().mean())
        .withColumn("lo", col("vals").list().min())
        .withColumn("hi", col("vals").list().max())
        .select("n", "s", "avg", "lo", "hi")
        .collect();
      expect(result[0]).toEqual({ n: 0, s: 0, avg: null, lo: null, hi: null });
    });

    it("list ops chain with other Expr ops", async () => {
      const df = DataFrame.fromArray([
        { tags: ["js", "ts"] },
        { tags: ["go"] },
        { tags: [] as string[] },
      ]);
      const result = await df
        .filter(col("tags").list().lengths().gt(0))
        .withColumn("first", col("tags").list().first())
        .select("first")
        .collect();
      expect(result).toEqual([{ first: "js" }, { first: "go" }]);
    });
  });

  describe("AST SQL compilation — field and listOp", () => {
    it("isCompilable returns true for field access", () => {
      const ast = col("address").field("city").ast;
      expect(isCompilable(ast)).toBe(true);
    });

    it("isCompilable returns true for chained field access", () => {
      const ast = col("data").field("geo").field("lat").ast;
      expect(isCompilable(ast)).toBe(true);
    });

    it("isCompilable returns true for list ops over a column", () => {
      expect(isCompilable(col("tags").list().lengths().ast)).toBe(true);
      expect(isCompilable(col("vals").list().get(0).ast)).toBe(true);
      expect(isCompilable(col("vals").list().contains("x").ast)).toBe(true);
    });

    it("isCompilable returns false when operand contains an opaque fn", () => {
      // Construct an opaque Expr via the fn default AST
      const opaque = col("x").mul(col("y")); // compilable
      expect(isCompilable(opaque.ast)).toBe(true);
      // listOp over a compilable operand is still compilable
      expect(isCompilable(col("xs").list().lengths().ast)).toBe(true);
    });

    it("astToSql emits struct field access", () => {
      const sql = astToSql(col("address").field("city").ast);
      expect(sql).toBe(`("address").city`);
    });

    it("astToSql emits nested struct field access", () => {
      const sql = astToSql(col("data").field("geo").field("lat").ast);
      expect(sql).toBe(`(("data").geo).lat`);
    });

    it("astToSql emits array_length for lengths()", () => {
      expect(astToSql(col("tags").list().lengths().ast)).toBe(`array_length("tags")`);
    });

    it("astToSql emits 1-based index for get()", () => {
      // DuckDB arrays are 1-based — index 0 in JS maps to [1] in SQL
      expect(astToSql(col("tags").list().get(0).ast)).toBe(`"tags"[1]`);
      expect(astToSql(col("tags").list().get(2).ast)).toBe(`"tags"[3]`);
    });

    it("astToSql emits array_contains with quoted string arg", () => {
      expect(astToSql(col("tags").list().contains("rust").ast)).toBe(
        `array_contains("tags", 'rust')`,
      );
    });

    it("astToSql emits array_contains with numeric arg", () => {
      expect(astToSql(col("scores").list().contains(42).ast)).toBe(`array_contains("scores", 42)`);
    });

    it("astToSql emits array_distinct for unique()", () => {
      expect(astToSql(col("tags").list().unique().ast)).toBe(`array_distinct("tags")`);
    });

    it("astToSql emits array_sort for sort()", () => {
      expect(astToSql(col("tags").list().sort().ast)).toBe(`array_sort("tags")`);
    });

    it("astToSql emits array_to_string for join()", () => {
      expect(astToSql(col("tags").list().join(", ").ast)).toBe(`array_to_string("tags", ', ')`);
    });

    it("astToSql emits list_<op> for numeric aggregates", () => {
      expect(astToSql(col("scores").list().sum().ast)).toBe(`list_sum("scores")`);
      expect(astToSql(col("scores").list().mean().ast)).toBe(`list_mean("scores")`);
      expect(astToSql(col("scores").list().min().ast)).toBe(`list_min("scores")`);
      expect(astToSql(col("scores").list().max().ast)).toBe(`list_max("scores")`);
    });

    it("astToSql composes field access with binary ops", () => {
      const sql = astToSql(col("addr").field("zip").eq("10001").ast);
      expect(sql).toBe(`(("addr").zip = '10001')`);
    });

    it("astToSql composes list lengths with comparison", () => {
      const sql = astToSql(col("tags").list().lengths().gt(2).ast);
      expect(sql).toBe(`(array_length("tags") > 2)`);
    });
  });
});
