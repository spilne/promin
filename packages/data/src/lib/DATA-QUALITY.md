# Data Quality

Tools for validating, profiling, comparing, and governing datasets. All imports from `@promin/data`.

## ExpectationSuite

Declarative chain of data quality checks on a DataFrame. Chain expectations fluently, then call `validate()` to run them all.

```ts
import { DataFrame } from "@promin/data";

const df = DataFrame.fromArray(rows);
const result = await df
  .expect()
  .expectNotNull("email")
  .expectUnique("id")
  .expectBetween("age", { min: 0, max: 150 })
  .expectMatch("email", { pattern: /@/ })
  .expectRowCount({ min: 1 })
  .validate();

console.log(result.passed); // true/false
console.log(result.summary); // { total, passed, failed, warnings }
console.log(result.results); // per-expectation details
```

**Built-in expectations:**

| Method                                                                    | Description              |
| ------------------------------------------------------------------------- | ------------------------ |
| `expectNotNull(column)`                                                   | No null/undefined values |
| `expectUnique(column)`                                                    | All values distinct      |
| `expectBetween(column, { min, max })`                                     | Numeric range check      |
| `expectMatch(column, { pattern })`                                        | Regex pattern match      |
| `expectIn(column, { values })`                                            | Value in allowed set     |
| `expectRowCount({ min?, max? })`                                          | Row count bounds         |
| `expectFreshness(column, { maxAgeMs })`                                   | Timestamp recency        |
| `expectReferentialIntegrity(column, { referenceTable, referenceColumn })` | Foreign key check        |
| `expect(name, checkFn)`                                                   | Custom expectation       |

All expectations accept an optional `severity: "error" | "warning"` parameter. Warnings don't cause `validate()` to fail.

---

## Data Profiling

Comprehensive dataset analysis in one call. Profiles every column, detects types, computes statistics, and flags warnings.

```ts
import { profileData } from "@promin/data";

const report = await profileData(rows);

console.log(report.rowCount);
console.log(report.duplicateRows);
console.log(report.columns.age); // NumericProfile: mean, median, std, percentiles
console.log(report.columns.name); // StringProfile: avgLength, topValues
console.log(report.correlations); // strong/moderate correlations between numeric columns
console.log(report.warnings); // high nulls, constant columns, high cardinality
```

**Column types detected:** `numeric`, `string`, `boolean`, `date`

**Numeric profile:** mean, median, std, min, max, percentiles (p5/p25/p50/p75/p95), zeros, negatives

**String profile:** avgLength, minLength, maxLength, emptyStrings, uniqueCount, topValues

**Options:** `correlations?` (default `true`), `topValuesLimit?` (default `10`)

---

## Data Diff

Compare two datasets row by row. Identifies added, removed, and modified rows.

```ts
import { dataDiff, schemaDiff } from "@promin/data";

const diff = dataDiff(beforeRows, afterRows, { key: "id" });

console.log(diff.summary);
// { added: 5, removed: 2, modified: 10, unchanged: 983, total: 1000 }
console.log(diff.addedRows);
console.log(diff.removedRows);
console.log(diff.modifications);
// [{ key: "42", column: "status", before: "active", after: "inactive" }]
```

**Options:** `key` (primary key column), `tolerance?` (numeric comparison tolerance, default `0`), `columns?` (columns to compare, default all), `sampleModifications?` (max modifications to return, default `100`)

### schemaDiff

Compare column sets between two dataset versions.

```ts
const diff = schemaDiff(["id", "name", "email"], ["id", "name", "email", "phone"]);
console.log(diff.addedColumns); // ["phone"]
console.log(diff.removedColumns); // []
console.log(diff.compatible); // true (no removals)
```

---

## Data Contracts

Formalize agreements between data producers and consumers. Combines schema validation (Zod) with SLA checks.

```ts
import { defineContract } from "@promin/data";
import { z } from "zod";

const contract = defineContract({
  name: "user-events",
  version: "1.0.0",
  owner: "platform-team",
  schema: z.object({
    userId: z.string(),
    event: z.string(),
    timestamp: z.string().datetime(),
  }),
  sla: {
    freshness: { maxAgeMs: 300_000, column: "timestamp" },
    completeness: { minRowCount: 100, maxNullPct: { userId: 0 } },
    uniqueness: { columns: ["userId"] },
  },
});

const result = await contract.validate(dataframe);
console.log(result.valid);
console.log(result.schemaViolations);
console.log(result.slaViolations);
```

**SLA checks:**

| Check                      | Description                                     |
| -------------------------- | ----------------------------------------------- |
| `freshness`                | Most recent timestamp must be within `maxAgeMs` |
| `completeness.minRowCount` | Minimum number of rows                          |
| `completeness.maxNullPct`  | Maximum null percentage per column              |
| `uniqueness.columns`       | Columns that must have all unique values        |
