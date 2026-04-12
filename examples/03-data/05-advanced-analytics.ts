/**
 * Advanced analytics — custom aggregations, multi-column sort,
 * composite joins, time-series resampling, and forward fill.
 */

import { DataFrame, col, percentile, reduce } from "@promin/core";

// --- Sample data ---

const trades = Array.from({ length: 1000 }, (_, i) => ({
  timestamp: new Date(2024, 0, 1, Math.floor(i / 60), i % 60).getTime(),
  symbol: ["AAPL", "GOOG", "MSFT"][i % 3]!,
  price: 100 + Math.random() * 50,
  volume: Math.floor(Math.random() * 10000),
}));

const users = [
  { userId: 1, name: "Alice", region: "US" },
  { userId: 2, name: "Bob", region: "EU" },
  { userId: 3, name: "Carol", region: "US" },
];

const purchases = [
  { userId: 1, date: "2024-01-15", amount: 150 },
  { userId: 1, date: "2024-02-20", amount: 200 },
  { userId: 2, date: "2024-01-10", amount: 300 },
  { userId: 3, date: "2024-03-01", amount: 50 },
  { userId: 3, date: "2024-03-01", amount: 75 },
];

// --- 1. Multi-column sort ---

const sorted = await DataFrame.fromArray(purchases)
  .sort([
    { column: "userId", order: "asc" },
    { column: "date", order: "desc" },
  ])
  .collect();
console.log("Multi-column sort:", sorted);

// --- 2. Composite join ---

const enriched = await DataFrame.fromArray(purchases)
  .join(DataFrame.fromArray(users), { on: "userId" })
  .select("name", "region", "date", "amount")
  .sort("amount", "desc")
  .collect();
console.log("Enriched purchases:", enriched);

// --- 3. Custom aggregations ---

const stats = await DataFrame.fromArray(trades)
  .groupBy("symbol")
  .agg({
    price: "median",
    volume: "stddev",
  })
  .collect();
console.log("Per-symbol stats:", stats);

// p95 price per symbol using percentile
const p95 = await DataFrame.fromArray(trades)
  .groupBy("symbol")
  .agg({ price: percentile(0.95) })
  .collect();
console.log("P95 price:", p95);

// Custom reducer: VWAP (volume-weighted average price)
const vwap = reduce(
  { totalValue: 0, totalVolume: 0 },
  (acc: { totalValue: number; totalVolume: number }, val: any) => ({
    totalValue: acc.totalValue + val,
    totalVolume: acc.totalVolume + 1,
  }),
  (acc: { totalValue: number; totalVolume: number }) =>
    acc.totalVolume > 0 ? acc.totalValue / acc.totalVolume : 0,
);

// --- 4. Time-series resampling ---

const hourly = await DataFrame.fromArray(trades)
  .resample("timestamp", "1h", { price: "avg", volume: "sum" })
  .collect();
console.log("Hourly OHLC:", hourly.slice(0, 3));

// --- 5. Forward fill ---

const sensor = [
  { ts: 1, temp: 20 },
  { ts: 2, temp: null },
  { ts: 3, temp: null },
  { ts: 4, temp: 25 },
  { ts: 5, temp: null },
];

const filled = await DataFrame.fromArray(sensor).fillNull("temp", { method: "forward" }).collect();
console.log("Forward filled:", filled);
// [{ ts: 1, temp: 20 }, { ts: 2, temp: 20 }, { ts: 3, temp: 20 }, { ts: 4, temp: 25 }, { ts: 5, temp: 25 }]

// --- 6. withColumns (batch) ---

const computed = await DataFrame.fromArray(trades)
  .withColumns({
    priceRounded: (r: any) => Math.round(r.price),
    isHighVolume: (r: any) => r.volume > 5000,
    symbolLower: (r: any) => r.symbol.toLowerCase(),
  })
  .head(3);
console.log("Computed columns:", computed);
