# @promin/duckdb

DuckDB executor for @promin/data DataFrame — SQL compilation, Arrow IPC, auto-routing.

## Install

```bash
bun add @promin/duckdb
```

## Quick Example

```typescript
import { DataFrame, col, lit } from "@promin/data";
import { DuckDBExecutor } from "@promin/duckdb";

const executor = new DuckDBExecutor();
const df = DataFrame.fromArray(bigData).withExecutor(executor);

// All queries compile to SQL and run in DuckDB's vectorized engine
await df
  .groupBy("region")
  .agg({ revenue: { column: "revenue", fn: "sum" } })
  .collect();
await df.sort("revenue", "desc").limit(10).collect();
await df.distinct().collect();
```

Source data is cached in DuckDB tables by identity. The first query pays the load cost; subsequent queries on the same source skip it, making the "load once, query many" pattern automatic.

## When to Use DuckDB vs ArrayExecutor

| Operation (1M rows) | DuckDB | Array  | Winner        |
| ------------------- | ------ | ------ | ------------- |
| groupBy + sum + avg | 4.0ms  | 11.5ms | DuckDB (3x)   |
| sort + limit 10     | 1.4ms  | 215ms  | DuckDB (154x) |
| distinct            | 3.5ms  | 6.7ms  | DuckDB (2x)   |
| filter (return 50%) | 190ms  | 9.1ms  | Array (21x)   |

DuckDB wins on operations that **reduce** data (groupBy, sort+limit, distinct). Array wins on operations that **return most rows** (filter, map).

## Documentation

Full docs and examples: [packages/duckdb](https://github.com/spilne/promin/tree/main/packages/duckdb)
