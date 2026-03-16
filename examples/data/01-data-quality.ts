/**
 * Data quality — validate datasets before they reach production
 *
 * Run after ETL, before dashboards see the data. Catch nulls,
 * duplicates, stale data, broken foreign keys, and distribution drift.
 */

import { DataFrame, InMemoryWorkflowStorage, workflow, Pipeline } from "@promin/core";

const storage = new InMemoryWorkflowStorage();

// ---------------------------------------------------------------------------
// Daily orders validation
// ---------------------------------------------------------------------------

async function validateOrders() {
  const orders = DataFrame.fromArray([
    { orderId: "o1", userId: "u1", amount: 99.99, status: "completed", email: "alice@co.com", createdAt: new Date().toISOString() },
    { orderId: "o2", userId: "u2", amount: 250, status: "pending", email: "bob@co.com", createdAt: new Date().toISOString() },
    { orderId: "o3", userId: "u1", amount: 0, status: "completed", email: "alice@co.com", createdAt: new Date(Date.now() - 7200_000).toISOString() },
    { orderId: "o4", userId: "u3", amount: 1500, status: "cancelled", email: "bad-email", createdAt: new Date().toISOString() },
  ]);

  const users = DataFrame.fromArray([
    { id: "u1", name: "Alice" },
    { id: "u2", name: "Bob" },
    { id: "u3", name: "Charlie" },
  ]);

  const result = await orders
    .expect()
    .expectNotNull("orderId")
    .expectNotNull("userId")
    .expectUnique("orderId")
    .expectBetween("amount", { min: 0, max: 100_000 })
    .expectIn("status", { values: ["pending", "completed", "cancelled"] })
    .expectMatch("email", { pattern: /^[^@]+@[^@]+\.[^@]+$/ })
    .expectRowCount({ min: 1, max: 50_000 })
    .expectFreshness("createdAt", { maxAgeMs: 24 * 60 * 60 * 1000 })
    .expectReferentialIntegrity("userId", {
      referenceTable: users,
      referenceColumn: "id",
    })
    .validate();

  console.log(`Validation: ${result.summary.passed}/${result.summary.total} passed`);
  for (const r of result.results.filter((r) => !r.passed)) {
    console.log(`  FAIL: ${r.expectation} — ${JSON.stringify(r.details.observed)}`);
  }

  return result;
}

// ---------------------------------------------------------------------------
// Scheduled quality check as a workflow
// ---------------------------------------------------------------------------

const qualityCheck = workflow<{ table: string }>({
  name: "data-quality-check",
  storage,
})
  .stepAsync("validate", async ({ input }) => {
    // In production: DataFrame.fromQuery({ sql: `SELECT * FROM ${input.table}`, db })
    const data = DataFrame.fromArray([
      { id: 1, value: 100, updated_at: new Date().toISOString() },
      { id: 2, value: 200, updated_at: new Date().toISOString() },
    ]);

    return data
      .expect()
      .expectNotNull("id")
      .expectUnique("id")
      .expectRowCount({ min: 1 })
      .expectFreshness("updated_at", { maxAgeMs: 24 * 60 * 60 * 1000 })
      .validate();
  })
  .step("alert", ({ prev }) => {
    if (!prev.passed) {
      console.log(`Quality check FAILED: ${prev.summary.failed} issues`);
      // await slack.send("#data-alerts", formatFailures(prev.results));
    }
    return Pipeline.succeed({ alerted: !prev.passed, summary: prev.summary });
  })
  .build();

// ---------------------------------------------------------------------------
// Warnings vs errors — soft checks that don't fail the suite
// ---------------------------------------------------------------------------

async function softChecks() {
  const data = DataFrame.fromArray([
    { id: 1, score: 95, tag: "premium" },
    { id: 2, score: null as any, tag: "basic" },
  ]);

  const result = await data
    .expect()
    .expectNotNull("id")                                    // error — must pass
    .expectNotNull("score", { severity: "warning" })        // warning — nice to know
    .expectBetween("score", { min: 0, max: 100 })          // error
    .validate();

  console.log(`Passed: ${result.passed}`); // true — warnings don't fail
  console.log(`Warnings: ${result.summary.warnings}`); // 1
}

export { validateOrders, qualityCheck, softChecks };
