// Generate test CSV for cross-language benchmarks
import { writeFileSync } from "fs";

const N = 1_000_000;
const regions = ["north", "south", "east", "west", "central"];
const products = ["widget", "gadget", "doohickey", "thingamajig", "whatchamacallit"];
const statuses = ["active", "inactive", "pending"];

const header = "id,name,region,product,status,revenue,quantity,score,created_at\n";
const rows: string[] = [];

for (let i = 0; i < N; i++) {
  rows.push(
    `${i},name_${i % 1000},${regions[i % regions.length]},${products[i % products.length]},${statuses[i % statuses.length]},${Math.round(Math.random() * 10000)},${1 + (i % 100)},${Math.round(Math.random() * 100)},2024-${String(1 + (i % 12)).padStart(2, "0")}-${String(1 + (i % 28)).padStart(2, "0")}`,
  );
}

const csv = header + rows.join("\n") + "\n";
writeFileSync("/tmp/benchmark_1m.csv", csv);
console.log(
  `Generated ${N} rows → /tmp/benchmark_1m.csv (${(csv.length / 1024 / 1024).toFixed(1)} MB)`,
);
