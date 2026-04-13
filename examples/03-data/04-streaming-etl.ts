/**
 * Streaming ETL — process large datasets in constant memory.
 * DataFrame.stream() returns a StreamPipeline, enabling chunked processing.
 */

import { DataFrame, col, CsvSink, JsonlSink } from "@promin/data";

// Simulate a large dataset (in practice, use DataFrame.fromFile(CsvFile("huge.csv")))
const orders = Array.from({ length: 100_000 }, (_, i) => ({
  orderId: `ORD-${i}`,
  customerId: `CUST-${i % 500}`,
  region: ["US", "EU", "APAC", "LATAM"][i % 4]!,
  amount: Math.round(Math.random() * 1000),
  status: i % 10 === 0 ? "cancelled" : "completed",
  createdAt: new Date(2024, 0, 1 + (i % 365)).toISOString(),
}));

// 1. Stream filter → transform → write to file (constant memory)
await DataFrame.fromArray(orders)
  .filter(col("status").eq("completed"))
  .filter(col("amount").gt(100))
  .withColumn("tier", (r: any) => (r.amount > 500 ? "premium" : "standard"))
  .select("orderId", "customerId", "region", "amount", "tier")
  .stream({ chunkSize: 10_000 }) // process 10K rows at a time
  .forEach(async (row) => {
    // Each row flows through without buffering the full dataset
  });

// 2. Streaming aggregation — groupBy works in streaming mode too
const summary = await DataFrame.fromArray(orders)
  .filter(col("status").eq("completed"))
  .groupBy("region")
  .agg({ amount: "sum", orderId: "count" })
  .stream({ chunkSize: 20_000 })
  .collect();

console.log("Revenue by region (streamed):", summary);

// 3. Stream to CSV file
await DataFrame.fromArray(orders)
  .filter((r: any) => r.region === "US" && r.amount > 200)
  .to(CsvSink("/tmp/us-high-value.csv"));

// 4. Stream to JSONL (newline-delimited JSON)
await DataFrame.fromArray(orders)
  .filter((r: any) => r.status === "cancelled")
  .to(JsonlSink("/tmp/cancelled-orders.jsonl"));

console.log("ETL complete");
