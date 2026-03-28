import { group, bench, run } from "mitata";
import { DataFrame } from "./dataframe.ts";

// ---------------------------------------------------------------------------
// Data generators
// ---------------------------------------------------------------------------

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
    active: i % 3 !== 0,
  }));
}

// ---------------------------------------------------------------------------
// Filter
// ---------------------------------------------------------------------------

for (const size of [1_000, 10_000, 100_000]) {
  const label = size >= 100_000 ? `${size / 1_000}K` : `${(size / 1_000).toFixed(0)}K`;
  const data = generateRows(size);
  const df = DataFrame.fromArray(data);

  group(`filter (${label} rows)`, () => {
    bench("Array.filter", () => {
      return data.filter((r) => r.age > 35);
    });

    bench("DataFrame.filter (async)", async () => {
      return df.filter((r) => r.age > 35).collect();
    });

    bench("DataFrame.filter (sync)", () => {
      return df.filter((r) => r.age > 35).collectSync();
    });
  });
}

// ---------------------------------------------------------------------------
// Map (withColumn)
// ---------------------------------------------------------------------------

for (const size of [1_000, 10_000, 100_000]) {
  const label = size >= 100_000 ? `${size / 1_000}K` : `${(size / 1_000).toFixed(0)}K`;
  const data = generateRows(size);
  const df = DataFrame.fromArray(data);

  group(`withColumn (${label} rows)`, () => {
    bench("Array.map", () => {
      return data.map((r) => ({ ...r, revenueX2: r.revenue * 2 }));
    });

    bench("DataFrame.withColumn", async () => {
      return df.withColumn("revenueX2", (r) => r.revenue * 2).collect();
    });
  });
}

// ---------------------------------------------------------------------------
// Sort
// ---------------------------------------------------------------------------

for (const size of [1_000, 10_000, 100_000]) {
  const label = size >= 100_000 ? `${size / 1_000}K` : `${(size / 1_000).toFixed(0)}K`;
  const data = generateRows(size);
  const df = DataFrame.fromArray(data);

  group(`sort (${label} rows)`, () => {
    bench("Array.sort", () => {
      return [...data].sort((a, b) => a.revenue - b.revenue);
    });

    bench("DataFrame.sort", async () => {
      return df.sort("revenue").collect();
    });
  });
}

// ---------------------------------------------------------------------------
// GroupBy + Agg
// ---------------------------------------------------------------------------

for (const size of [1_000, 10_000, 100_000]) {
  const label = size >= 100_000 ? `${size / 1_000}K` : `${(size / 1_000).toFixed(0)}K`;
  const data = generateRows(size);
  const df = DataFrame.fromArray(data);

  group(`groupBy + sum (${label} rows)`, () => {
    bench("manual groupBy", () => {
      const groups = new Map<string, number>();
      for (const r of data) {
        groups.set(r.region, (groups.get(r.region) ?? 0) + r.revenue);
      }
      return [...groups.entries()].map(([region, revenue_sum]) => ({ region, revenue_sum }));
    });

    bench("DataFrame.groupBy.agg (async)", async () => {
      return df.groupBy("region").agg({ revenue: "sum" }).collect();
    });

    bench("DataFrame.groupBy.agg (sync)", () => {
      return df.groupBy("region").agg({ revenue: "sum" }).collectSync();
    });
  });
}

// ---------------------------------------------------------------------------
// Join (inner)
// ---------------------------------------------------------------------------

for (const size of [1_000, 10_000]) {
  const label = `${(size / 1_000).toFixed(0)}K`;
  const left = generateRows(size);
  const right = Array.from({ length: size }, (_, i) => ({
    id: i * 2, // half overlap
    tier: i % 3 === 0 ? "gold" : "silver",
  }));
  const dfLeft = DataFrame.fromArray(left);
  const dfRight = DataFrame.fromArray(right);

  group(`inner join (${label}×${label} rows)`, () => {
    bench("manual nested loop join", () => {
      const rightIndex = new Map<number, (typeof right)[0]>();
      for (const r of right) rightIndex.set(r.id, r);
      return left
        .filter((l) => rightIndex.has(l.id))
        .map((l) => ({ ...l, ...rightIndex.get(l.id)! }));
    });

    bench("DataFrame.join", async () => {
      return dfLeft.join(dfRight, { on: "id", type: "inner" }).collect();
    });
  });
}

// ---------------------------------------------------------------------------
// Distinct
// ---------------------------------------------------------------------------

for (const size of [1_000, 10_000, 100_000]) {
  const label = size >= 100_000 ? `${size / 1_000}K` : `${(size / 1_000).toFixed(0)}K`;
  const data = generateRows(size);
  const df = DataFrame.fromArray(data);

  group(`distinct by region (${label} rows)`, () => {
    bench("manual Set", () => {
      const seen = new Set<string>();
      return data.filter((r) => {
        if (seen.has(r.region)) return false;
        seen.add(r.region);
        return true;
      });
    });

    bench("DataFrame.distinctBy (async)", async () => {
      return df.distinctBy("region").collect();
    });

    bench("DataFrame.distinctBy (sync)", () => {
      return df.distinctBy("region").collectSync();
    });
  });
}

// ---------------------------------------------------------------------------
// Window function (row_number)
// ---------------------------------------------------------------------------

for (const size of [1_000, 10_000]) {
  const label = `${(size / 1_000).toFixed(0)}K`;
  const data = generateRows(size);
  const df = DataFrame.fromArray(data);

  group(`window: row_number partitioned (${label} rows)`, () => {
    bench("DataFrame.withWindowColumn", async () => {
      return df
        .withWindowColumn("rn", {
          partitionBy: "region",
          orderBy: "revenue",
          fn: "row_number",
        })
        .collect();
    });
  });
}

// ---------------------------------------------------------------------------
// Statistics
// ---------------------------------------------------------------------------

{
  const data = generateRows(100_000);
  const df = DataFrame.fromArray(data);

  group("statistics (100K rows)", () => {
    bench("DataFrame.describe()", async () => {
      return df.describe();
    });

    bench("DataFrame.median", async () => {
      return df.median("revenue");
    });

    bench("DataFrame.variance", async () => {
      return df.variance("revenue");
    });

    bench("DataFrame.correlation", async () => {
      return df.correlation("revenue", "score");
    });

    bench("DataFrame.quantile(0.95)", async () => {
      return df.quantile("revenue", 0.95);
    });
  });
}

// ---------------------------------------------------------------------------
// Chained operations
// ---------------------------------------------------------------------------

{
  const data = generateRows(100_000);
  const df = DataFrame.fromArray(data);

  group("chained: filter → withColumn → sort → limit (100K rows)", () => {
    bench("manual Array chain", () => {
      return data
        .filter((r) => r.age > 30)
        .map((r) => ({ ...r, bonus: r.revenue * 0.1 }))
        .sort((a, b) => b.revenue - a.revenue)
        .slice(0, 100);
    });

    bench("DataFrame chain", async () => {
      return df
        .filter((r) => r.age > 30)
        .withColumn("bonus", (r) => r.revenue * 0.1)
        .sort("revenue", "desc")
        .limit(100)
        .collect();
    });
  });
}

await run();
