# DataFrame

Lazy, composable DataFrame with pluggable executors. Build query plans with a fluent API and expression builder, then execute on Array (in-memory) or DuckDB (analytical).

## Main Idea

Operations are lazy — they build a logical plan. Terminal methods (`.collect()`, `.count()`) execute the plan through a `DataFrameExecutor`. The default `ArrayExecutor` runs everything in JS. The `DuckDBExecutor` (in `@promin/duckdb`) compiles plans to SQL with predicate/projection pushdown.

## Examples

### Filter, group, aggregate

```typescript
import { DataFrame, col, lit } from "@promin/core";

const result = await DataFrame.fromRows(sales)
  .filter(col("amount").gt(lit(100)))
  .groupBy("region")
  .agg({
    total: { column: "amount", fn: "sum" },
    count: { column: "*", fn: "count" },
  })
  .sort("total", "desc")
  .limit(10)
  .collect();
```

### Expression builder with when/then

```typescript
const df = DataFrame.fromRows(users)
  .withColumn(
    "tier",
    when(col("score").gt(lit(90)))
      .then(lit("gold"))
      .when(col("score").gt(lit(70)))
      .then(lit("silver"))
      .otherwise(lit("bronze")),
  )
  .select("name", "tier");
```

### Joins

```typescript
const result = await orders
  .join(customers, { on: "customerId", kind: "left" })
  .select("orderId", "customerName", "amount")
  .collect();
```

### Window functions

```typescript
const ranked = await DataFrame.fromRows(scores)
  .withColumn(
    "rank",
    col("score")
      .rank()
      .over({ partitionBy: ["category"], orderBy: ["score"] }),
  )
  .collect();
```

### File sources

```typescript
import { CsvFile, ParquetFile } from "@promin/core";

// In-memory (hyparquet for Parquet)
const df = await DataFrame.from(ParquetFile("data.parquet"));

// With DuckDB (native file reading, much faster for large files)
import { DuckDBExecutor } from "@promin/duckdb";
const executor = await DuckDBExecutor.create();
const df = DataFrame.fromFile("data.parquet", { executor });
```

### Data profiling

```typescript
const report = await df.profile();
// Per-column: type, nulls, unique, min/max, mean/std, top values
// Cross-column: correlations, warnings (high nulls, low cardinality, etc.)
```

## Operations

- **Filter/Select**: filter, select, distinct, limit, sample, head, tail
- **Transform**: withColumn, rename, cast, explode, pivot, unpivot, drop
- **Aggregate**: groupBy + agg (sum, avg, min, max, count, first, last, collect)
- **Window**: rank, denseRank, rowNumber, lead, lag, ntile, cumSum, movingAvg, percentRank
- **Join**: inner, left, right, full, cross, semi, anti
- **Set ops**: union, concat, intersection, difference
- **Statistics**: median, std, variance, quantile, correlation, covariance
- **Rolling/Cumulative**: rolling aggregates, cumulative sum/min/max/product

## Executors

| Executor         | Best For                         | Notes                                              |
| ---------------- | -------------------------------- | -------------------------------------------------- |
| `ArrayExecutor`  | Small data (<100K rows), testing | Default, zero dependencies                         |
| `DuckDBExecutor` | Analytics, large files, SQL      | In `@promin/duckdb`, predicate/projection pushdown |
| `AutoExecutor`   | Mixed workloads                  | Routes to Array or DuckDB based on plan analysis   |

## Use Cases

- **ETL transforms** — clean, filter, join datasets before loading
- **Analytics dashboards** — aggregate metrics with groupBy + window functions
- **Data validation** — profile data quality, detect anomalies
- **Report generation** — pivot, aggregate, and export results
- **File processing** — read CSV/Parquet/JSON, transform, write back
