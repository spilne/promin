# @promin/data

Lazy DataFrame with expression builder, pluggable executors, data quality, profiling, and diff.

## Install

```bash
bun add @promin/data
```

For native analytical queries, add the DuckDB executor:

```bash
bun add @promin/duckdb
```

## Quick Example

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

Operations are lazy — they build a logical plan. Terminal methods (`.collect()`, `.count()`) execute the plan through a `DataFrameExecutor`. The default `ArrayExecutor` runs everything in JS; the `DuckDBExecutor` compiles plans to SQL with predicate/projection pushdown.

## Features

- **Lazy execution** — build query plans, execute on demand
- **Expression builder** — `col()`, `lit()`, `when/then/otherwise` for type-safe transforms
- **Pluggable executors** — ArrayExecutor (in-memory) or DuckDBExecutor (analytical)
- **Joins** — inner, left, right, full with fluent API
- **Data quality** — validation rules and profiling
- **Diff** — compare DataFrames row-by-row

## Documentation

Full docs and examples: [packages/data](https://github.com/spilne/promin/tree/main/packages/data)
