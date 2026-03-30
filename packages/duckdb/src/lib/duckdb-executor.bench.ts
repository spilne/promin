import { group, bench, run } from "mitata";
import { DataFrame } from "@promin/core";
import { DuckDBExecutor } from "./duckdb-executor.ts";

const duckdb = new DuckDBExecutor();

function generateRows(n: number) {
  const regions = ["north", "south", "east", "west"];
  const names = ["alice", "bob", "charlie", "diana", "eve"];
  return Array.from({ length: n }, (_, i) => ({
    id: i,
    name: names[i % names.length]!,
    region: regions[i % regions.length]!,
    age: 20 + (i % 50),
    revenue: Math.round(Math.random() * 10000),
    score: Math.round(Math.random() * 100),
  }));
}

// ---------------------------------------------------------------------------
// GroupBy + Agg — DuckDB's sweet spot
// ---------------------------------------------------------------------------

for (const size of [1_000, 10_000, 100_000]) {
  const label = size >= 100_000 ? `${size / 1_000}K` : `${(size / 1_000).toFixed(0)}K`;
  const data = generateRows(size);
  const dfArray = DataFrame.fromArray(data);
  const dfDuck = DataFrame.fromArray(data).withExecutor(duckdb);

  group(`groupBy + sum (${label} rows)`, () => {
    bench("ArrayExecutor", async () => {
      return dfArray.groupBy("region").agg({ revenue: "sum" }).collect();
    });

    bench("DuckDBExecutor", async () => {
      return dfDuck.groupBy("region").agg({ revenue: "sum" }).collect();
    });
  });
}

// ---------------------------------------------------------------------------
// Sort
// ---------------------------------------------------------------------------

for (const size of [1_000, 10_000, 100_000]) {
  const label = size >= 100_000 ? `${size / 1_000}K` : `${(size / 1_000).toFixed(0)}K`;
  const data = generateRows(size);
  const dfArray = DataFrame.fromArray(data);
  const dfDuck = DataFrame.fromArray(data).withExecutor(duckdb);

  group(`sort (${label} rows)`, () => {
    bench("ArrayExecutor", async () => {
      return dfArray.sort("revenue").collect();
    });

    bench("DuckDBExecutor", async () => {
      return dfDuck.sort("revenue").collect();
    });
  });
}

// ---------------------------------------------------------------------------
// Sort + Limit (top-N) — DuckDB should excel
// ---------------------------------------------------------------------------

for (const size of [10_000, 100_000]) {
  const label = size >= 100_000 ? `${size / 1_000}K` : `${(size / 1_000).toFixed(0)}K`;
  const data = generateRows(size);
  const dfArray = DataFrame.fromArray(data);
  const dfDuck = DataFrame.fromArray(data).withExecutor(duckdb);

  group(`sort + limit 10 (${label} rows)`, () => {
    bench("ArrayExecutor", async () => {
      return dfArray.sort("revenue", "desc").limit(10).collect();
    });

    bench("DuckDBExecutor", async () => {
      return dfDuck.sort("revenue", "desc").limit(10).collect();
    });
  });
}

// ---------------------------------------------------------------------------
// Distinct
// ---------------------------------------------------------------------------

for (const size of [10_000, 100_000]) {
  const label = size >= 100_000 ? `${size / 1_000}K` : `${(size / 1_000).toFixed(0)}K`;
  const data = generateRows(size);
  const dfArray = DataFrame.fromArray(data);
  const dfDuck = DataFrame.fromArray(data).withExecutor(duckdb);

  group(`distinct (${label} rows)`, () => {
    bench("ArrayExecutor", async () => {
      return dfArray.distinct().collect();
    });

    bench("DuckDBExecutor", async () => {
      return dfDuck.distinct().collect();
    });
  });
}

// ---------------------------------------------------------------------------
// Join
// ---------------------------------------------------------------------------

for (const size of [1_000, 10_000]) {
  const label = `${(size / 1_000).toFixed(0)}K`;
  const left = generateRows(size);
  const right = Array.from({ length: size }, (_, i) => ({
    id: i * 2,
    tier: i % 3 === 0 ? "gold" : "silver",
  }));

  const dfLeftArr = DataFrame.fromArray(left);
  const dfRightArr = DataFrame.fromArray(right);
  const dfLeftDuck = DataFrame.fromArray(left).withExecutor(duckdb);
  const dfRightDuck = DataFrame.fromArray(right).withExecutor(duckdb);

  group(`inner join (${label}×${label} rows)`, () => {
    bench("ArrayExecutor", async () => {
      return dfLeftArr.join(dfRightArr, { on: "id", type: "inner" }).collect();
    });

    bench("DuckDBExecutor", async () => {
      return dfLeftDuck.join(dfRightDuck, { on: "id", type: "inner" }).collect();
    });
  });
}

// ---------------------------------------------------------------------------
// Filter (JS function — DuckDB falls back to JS)
// ---------------------------------------------------------------------------

{
  const data = generateRows(100_000);
  const dfArray = DataFrame.fromArray(data);
  const dfDuck = DataFrame.fromArray(data).withExecutor(duckdb);

  group("filter with JS fn (100K rows)", () => {
    bench("ArrayExecutor", async () => {
      return dfArray.filter((r) => r.age > 35).collect();
    });

    bench("DuckDBExecutor (JS fallback)", async () => {
      return dfDuck.filter((r) => r.age > 35).collect();
    });
  });
}

// ---------------------------------------------------------------------------
// Chained: filter → groupBy → sort → limit
// ---------------------------------------------------------------------------

{
  const data = generateRows(100_000);
  const dfArray = DataFrame.fromArray(data);
  const dfDuck = DataFrame.fromArray(data).withExecutor(duckdb);

  group("chained: filter → groupBy → sort → limit (100K rows)", () => {
    bench("ArrayExecutor", async () => {
      return dfArray
        .filter((r) => r.age > 30)
        .groupBy("region")
        .agg({ revenue: "sum" })
        .sort("revenue_sum", "desc")
        .limit(3)
        .collect();
    });

    bench("DuckDBExecutor", async () => {
      return dfDuck
        .filter((r) => r.age > 30)
        .groupBy("region")
        .agg({ revenue: "sum" })
        .sort("revenue_sum", "desc")
        .limit(3)
        .collect();
    });
  });
}

await run();
