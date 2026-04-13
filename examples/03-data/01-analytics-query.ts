/**
 * Analyze sales data — filter, group, aggregate, sort.
 * DataFrame builds a lazy plan; .collect() executes it.
 */

import { DataFrame, col, lit } from "@promin/data";

interface Sale {
  region: string;
  product: string;
  amount: number;
  quantity: number;
}

const sales: Sale[] = [
  { region: "US", product: "Widget", amount: 150, quantity: 3 },
  { region: "EU", product: "Gadget", amount: 200, quantity: 1 },
  { region: "US", product: "Gadget", amount: 80, quantity: 2 },
  { region: "EU", product: "Widget", amount: 300, quantity: 5 },
  { region: "US", product: "Widget", amount: 50, quantity: 1 },
];

// Revenue by region, only orders > $100
const topRegions = await DataFrame.fromArray(sales)
  .filter(col("amount").gt(lit(100)))
  .groupBy("region")
  .agg({ amount: "sum", quantity: "sum" })
  .sort("amount", "desc")
  .collect();

console.log(topRegions);
// [{ region: "EU", amount: 500, quantity: 6 }, { region: "US", amount: 150, quantity: 3 }]
