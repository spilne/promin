/**
 * Validate data quality before loading to production.
 * Profile the dataset and check for anomalies.
 */

import { profileData } from "@promin/core";

const transactions = [
  { id: "t1", amount: 99.99, currency: "USD", timestamp: "2026-04-06T10:00:00Z" },
  { id: "t2", amount: -5, currency: "USD", timestamp: "2026-04-06T10:01:00Z" },
  { id: "t3", amount: 150, currency: "EUR", timestamp: "2026-04-06T10:02:00Z" },
];

const report = await profileData(transactions as Record<string, unknown>[]);

console.log(`${report.rowCount} rows, ${report.columnCount} columns`);

for (const [name, col] of Object.entries(report.columns)) {
  console.log(`${name}: ${col.type}, ${(col.nullPct * 100).toFixed(0)}% nulls`);
  if (col.type === "numeric") {
    console.log(`  range: [${col.min}, ${col.max}], mean: ${col.mean.toFixed(1)}`);
  }
}

for (const w of report.warnings) {
  console.log(`Warning: ${w.message}`);
}
