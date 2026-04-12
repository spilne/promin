import { describe, it, expect } from "bun:test";
import { DataFrame } from "../dataframe/dataframe.ts";

// ---------------------------------------------------------------------------
// Test data
// ---------------------------------------------------------------------------

const orders = [
  {
    id: "o1",
    userId: "u1",
    amount: 100,
    status: "completed",
    email: "a@test.com",
    createdAt: new Date().toISOString(),
  },
  {
    id: "o2",
    userId: "u2",
    amount: 250,
    status: "pending",
    email: "b@test.com",
    createdAt: new Date().toISOString(),
  },
  {
    id: "o3",
    userId: "u1",
    amount: 50,
    status: "completed",
    email: "invalid-email",
    createdAt: new Date(Date.now() - 3600_000).toISOString(),
  },
  {
    id: "o4",
    userId: "u3",
    amount: -10,
    status: "cancelled",
    email: "d@test.com",
    createdAt: new Date().toISOString(),
  },
  {
    id: "o5",
    userId: null as any,
    amount: 500,
    status: "completed",
    email: null as any,
    createdAt: new Date().toISOString(),
  },
];

const users = [
  { id: "u1", name: "Alice" },
  { id: "u2", name: "Bob" },
  { id: "u3", name: "Charlie" },
];

// ---------------------------------------------------------------------------
// expectNotNull
// ---------------------------------------------------------------------------

describe("Not-null validation — catch missing required fields", () => {
  it("approves a dataset where all required fields are present", async () => {
    const result = await DataFrame.fromArray(users).expect().expectNotNull("id").validate();

    expect(result.passed).toBe(true);
    expect(result.summary.passed).toBe(1);
  });

  it("flags rows with missing customer IDs before they reach the dashboard", async () => {
    const result = await DataFrame.fromArray(orders).expect().expectNotNull("userId").validate();

    expect(result.passed).toBe(false);
    expect(result.results[0]!.details.failingRows).toBe(1);
  });
});

// ---------------------------------------------------------------------------
// expectUnique
// ---------------------------------------------------------------------------

describe("Uniqueness validation — detect duplicate records", () => {
  it("confirms every order has a distinct ID", async () => {
    const result = await DataFrame.fromArray(orders).expect().expectUnique("id").validate();
    expect(result.passed).toBe(true);
  });

  it("catches the same customer appearing twice — possible data duplication", async () => {
    const result = await DataFrame.fromArray(orders).expect().expectUnique("userId").validate();
    expect(result.passed).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// expectBetween
// ---------------------------------------------------------------------------

describe("Range validation — ensure numeric values stay within business limits", () => {
  it("accepts order amounts within the allowed billing range", async () => {
    const result = await DataFrame.fromArray(orders)
      .expect()
      .expectBetween("amount", { min: -100, max: 1000 })
      .validate();

    expect(result.passed).toBe(true);
  });

  it("rejects a negative order amount — likely a refund logged incorrectly", async () => {
    const result = await DataFrame.fromArray(orders)
      .expect()
      .expectBetween("amount", { min: 0, max: 1000 })
      .validate();

    expect(result.passed).toBe(false);
    expect(result.results[0]!.details.failingRows).toBe(1); // -10
  });
});

// ---------------------------------------------------------------------------
// expectMatch
// ---------------------------------------------------------------------------

describe("Pattern validation — enforce format rules on text fields", () => {
  it("confirms all customer names start with an uppercase letter", async () => {
    const result = await DataFrame.fromArray(users)
      .expect()
      .expectMatch("name", { pattern: /^[A-Z]/ })
      .validate();

    expect(result.passed).toBe(true);
  });

  it("flags malformed email addresses before sending campaign", async () => {
    const result = await DataFrame.fromArray(orders)
      .expect()
      .expectMatch("email", { pattern: /^[^@]+@[^@]+\.[^@]+$/ })
      .validate();

    expect(result.passed).toBe(false);
    expect(result.results[0]!.details.failingRows).toBe(1); // "invalid-email"
  });
});

// ---------------------------------------------------------------------------
// expectIn
// ---------------------------------------------------------------------------

describe("Allowed-values validation — restrict fields to known categories", () => {
  it("all order statuses belong to the known set", async () => {
    const result = await DataFrame.fromArray(orders)
      .expect()
      .expectIn("status", { values: ["pending", "completed", "cancelled"] })
      .validate();

    expect(result.passed).toBe(true);
  });

  it("detects an unexpected 'cancelled' status that the downstream system cannot handle", async () => {
    const result = await DataFrame.fromArray(orders)
      .expect()
      .expectIn("status", { values: ["pending", "completed"] })
      .validate();

    expect(result.passed).toBe(false); // "cancelled" not allowed
  });
});

// ---------------------------------------------------------------------------
// expectRowCount
// ---------------------------------------------------------------------------

describe("Row-count validation — verify data volume is within expectations", () => {
  it("daily order feed has a reasonable number of rows", async () => {
    const result = await DataFrame.fromArray(orders)
      .expect()
      .expectRowCount({ min: 1, max: 100 })
      .validate();

    expect(result.passed).toBe(true);
  });

  it("alerts when the pipeline delivers suspiciously few orders", async () => {
    const result = await DataFrame.fromArray(orders)
      .expect()
      .expectRowCount({ min: 100 })
      .validate();

    expect(result.passed).toBe(false);
  });

  it("alerts when an unexpected data spike exceeds the row limit", async () => {
    const result = await DataFrame.fromArray(orders).expect().expectRowCount({ max: 2 }).validate();

    expect(result.passed).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// expectFreshness
// ---------------------------------------------------------------------------

describe("Freshness validation — ensure data is not stale", () => {
  it("order timestamps are within 24 hours — data pipeline is current", async () => {
    const result = await DataFrame.fromArray(orders)
      .expect()
      .expectFreshness("createdAt", { maxAgeMs: 24 * 60 * 60 * 1000 }) // 24 hours
      .validate();

    expect(result.passed).toBe(true);
  });

  it("flags a 2-day-old dataset — pipeline may have stalled", async () => {
    const staleData = [{ ts: new Date(Date.now() - 48 * 60 * 60 * 1000).toISOString() }];
    const result = await DataFrame.fromArray(staleData)
      .expect()
      .expectFreshness("ts", { maxAgeMs: 24 * 60 * 60 * 1000 })
      .validate();

    expect(result.passed).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// expectReferentialIntegrity
// ---------------------------------------------------------------------------

describe("Referential integrity — verify foreign keys point to real records", () => {
  it("every order references an existing customer", async () => {
    const validOrders = orders.filter((o) => o.userId != null);
    const result = await DataFrame.fromArray(validOrders)
      .expect()
      .expectReferentialIntegrity("userId", {
        referenceTable: DataFrame.fromArray(users),
        referenceColumn: "id",
      })
      .validate();

    expect(result.passed).toBe(true);
  });

  it("catches an order referencing a deleted customer — orphan record", async () => {
    const ordersWithOrphan = [
      ...orders.filter((o) => o.userId != null),
      {
        id: "o99",
        userId: "u_nonexistent",
        amount: 10,
        status: "pending",
        email: "x@y.com",
        createdAt: new Date().toISOString(),
      },
    ];
    const result = await DataFrame.fromArray(ordersWithOrphan)
      .expect()
      .expectReferentialIntegrity("userId", {
        referenceTable: DataFrame.fromArray(users),
        referenceColumn: "id",
      })
      .validate();

    expect(result.passed).toBe(false);
    expect(result.results[0]!.details.failingRows).toBe(1);
  });
});

// ---------------------------------------------------------------------------
// Custom expectation
// ---------------------------------------------------------------------------

describe("Custom business rules — domain-specific quality checks", () => {
  it("total revenue is positive — basic sanity check on the daily batch", async () => {
    const result = await DataFrame.fromArray(orders)
      .expect()
      .expect("revenue_positive", async (df) => {
        const total = await df.sum("amount");
        return total > 0;
      })
      .validate();

    expect(result.passed).toBe(true);
  });

  it("a deliberately impossible rule fails — verifying that failures are reported", async () => {
    const result = await DataFrame.fromArray(orders)
      .expect()
      .expect("impossible", async () => false)
      .validate();

    expect(result.passed).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// Chained expectations
// ---------------------------------------------------------------------------

describe("Multi-rule quality suite — run all checks before publishing data", () => {
  it("order feed passes all five quality gates at once", async () => {
    const result = await DataFrame.fromArray(orders)
      .expect()
      .expectNotNull("id")
      .expectUnique("id")
      .expectBetween("amount", { min: -100, max: 100_000 })
      .expectIn("status", { values: ["pending", "completed", "cancelled"] })
      .expectRowCount({ min: 1 })
      .validate();

    expect(result.summary.total).toBe(5);
    expect(result.summary.passed).toBe(5);
    expect(result.passed).toBe(true);
  });

  it("summarizes every broken rule so the team can triage all issues at once", async () => {
    const result = await DataFrame.fromArray(orders)
      .expect()
      .expectNotNull("userId") // fails — 1 null
      .expectNotNull("email") // fails — 1 null
      .expectUnique("userId") // fails — duplicates
      .expectBetween("amount", { min: 0, max: 1000 }) // fails — -10
      .expectRowCount({ min: 1 }) // passes
      .validate();

    expect(result.passed).toBe(false);
    expect(result.summary.total).toBe(5);
    expect(result.summary.failed).toBe(4);
    expect(result.summary.passed).toBe(1);
  });

  it("nullable email is a warning, not a blocker — suite still passes", async () => {
    const result = await DataFrame.fromArray(orders)
      .expect()
      .expectNotNull("id")
      .expectNotNull("email", { severity: "warning" }) // warning, not error
      .validate();

    // email has null but it's a warning — suite still passes
    expect(result.passed).toBe(true);
    expect(result.summary.warnings).toBe(1);
  });
});

// ---------------------------------------------------------------------------
// ValidationResult metadata
// ---------------------------------------------------------------------------

describe("Validation metadata — audit trail for compliance", () => {
  it("records when the check ran and how long it took", async () => {
    const result = await DataFrame.fromArray(orders).expect().expectRowCount({ min: 1 }).validate();

    expect(result.timestamp).toBeInstanceOf(Date);
    expect(result.durationMs).toBeGreaterThanOrEqual(0);
  });
});
