/**
 * Pipeline testing — deterministic fixtures, property assertions, contract validation
 *
 * Business flow:
 * 1. Generate 1000 realistic order records with deterministic seed (reproducible tests)
 * 2. Run the ETL pipeline — filter, transform, aggregate
 * 3. Validate output invariants: filter never adds rows, sort preserves count, etc.
 * 4. Check the output against a data contract (schema + SLA)
 * 5. Run the same test with a different seed to catch edge cases
 *
 * Same seed always produces the same data — tests are reproducible across CI runs.
 */

import {
  DataFrame,
  StreamPipeline,
  generators,
  generateRows,
  dataframeProperties,
  streamProperties,
  defineContract,
} from "@promin/core";
import { z } from "zod";

// ---------------------------------------------------------------------------
// 1. Generate realistic test fixtures
// ---------------------------------------------------------------------------

function createOrderFixture(count: number, seed = 42) {
  return generateRows(
    {
      orderId: generators.sequence(1000),
      userId: generators.pick("u_1", "u_2", "u_3", "u_4", "u_5"),
      amount: generators.float(10, 500),
      status: generators.pick("pending", "completed", "cancelled"),
      region: generators.pick("US", "EU", "APAC"),
      createdAt: generators.date(new Date("2026-01-01"), new Date("2026-04-01")),
      discount: generators.nullable(generators.float(0, 50), 0.3),
    },
    count,
    seed,
  );
}

async function fixtureExample() {
  // Same seed = same data every time
  const orders1 = createOrderFixture(100, 42);
  const orders2 = createOrderFixture(100, 42);
  console.log("Deterministic:", JSON.stringify(orders1[0]) === JSON.stringify(orders2[0])); // true

  // Different seed = different data
  const orders3 = createOrderFixture(100, 99);
  console.log("Different seed:", JSON.stringify(orders1[0]) !== JSON.stringify(orders3[0])); // true

  const df = DataFrame.fromArray(orders1);
  console.log(`Generated ${await df.count()} orders`);
}

// ---------------------------------------------------------------------------
// 2. Test a DataFrame transformation with property assertions
// ---------------------------------------------------------------------------

async function dataframePropertyTests() {
  const orders = DataFrame.fromArray(createOrderFixture(500));

  // These should ALWAYS hold, regardless of the data
  console.log("Filter reduces count:",
    await dataframeProperties.filterReducesOrMaintains(orders, (r) => (r as any).status === "completed"));

  console.log("Sort preserves count:",
    await dataframeProperties.sortPreservesCount(orders, "amount" as any));

  console.log("Distinct reduces count:",
    await dataframeProperties.distinctReducesOrMaintains(orders));

  console.log("Limit bounded:",
    await dataframeProperties.limitBounded(orders, 50));

  console.log("Select preserves count:",
    await dataframeProperties.selectPreservesCount(orders, ["orderId", "amount"] as any));
}

// ---------------------------------------------------------------------------
// 3. Test a StreamPipeline with property assertions
// ---------------------------------------------------------------------------

async function streamPropertyTests() {
  const items = [1, 2, 3, 4, 5, 5, 3, 2, 1];
  const create = (data: number[]) => StreamPipeline.fromIterable(data);

  console.log("Stream filter reduces:",
    await streamProperties.filterReducesOrMaintains(items, (n) => n > 3, create));

  console.log("Stream take bounded:",
    await streamProperties.takeBounded(items, 3, create));

  console.log("Stream map preserves count:",
    await streamProperties.mapPreservesCount(items, (n) => n * 2, create));

  console.log("Stream dedupe reduces:",
    await streamProperties.dedupeReducesOrMaintains(items, create));

  console.log("Stream collect matches source:",
    await streamProperties.collectMatchesSource(items, create));
}

// ---------------------------------------------------------------------------
// 4. Validate output against a data contract
// ---------------------------------------------------------------------------

async function contractValidation() {
  const ordersContract = defineContract({
    name: "processed-orders",
    version: "1.0",
    owner: "data-team",
    schema: z.object({
      orderId: z.number(),
      userId: z.string(),
      amount: z.number().positive(),
      status: z.enum(["pending", "completed", "cancelled"]),
      region: z.string(),
      createdAt: z.string(),
    }),
    sla: {
      completeness: {
        minRowCount: 10,
        maxNullPct: { orderId: 0, userId: 0, amount: 0 },
      },
      uniqueness: { columns: ["orderId"] },
    },
  });

  const orders = DataFrame.fromArray(createOrderFixture(100));
  const result = await ordersContract.validate(orders as any);

  console.log(`Contract valid: ${result.valid}`);
  if (!result.valid) {
    console.log("Schema violations:", result.schemaViolations.length);
    console.log("SLA violations:", result.slaViolations.length);
  }
}

// ---------------------------------------------------------------------------
// 5. Full pipeline test pattern
// ---------------------------------------------------------------------------

async function fullPipelineTest() {
  // Generate fixture
  const raw = createOrderFixture(1000, 42);
  const orders = DataFrame.fromArray(raw);

  // Run "ETL pipeline"
  const result = await orders
    .filter((r) => (r as any).status === "completed")
    .withColumn("netAmount", (r) => ((r as any).amount - ((r as any).discount ?? 0)) as number)
    .sort("netAmount" as any, "desc")
    .limit(100)
    .collect();

  // Assertions
  console.log(`Top 100 completed orders: ${result.length} rows`);
  console.log("All completed:", result.every((r) => (r as any).status === "completed"));
  console.log("Has netAmount:", result.every((r) => "netAmount" in (r as any)));
  console.log("Sorted desc:", (result[0] as any).netAmount >= (result[1] as any).netAmount);
}

export { fixtureExample, dataframePropertyTests, streamPropertyTests, contractValidation, fullPipelineTest };
