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

describe("expectNotNull", () => {
  it("passes when no nulls", async () => {
    const result = await DataFrame.fromArray(users).expect().expectNotNull("id").validate();

    expect(result.passed).toBe(true);
    expect(result.summary.passed).toBe(1);
  });

  it("fails when nulls exist", async () => {
    const result = await DataFrame.fromArray(orders).expect().expectNotNull("userId").validate();

    expect(result.passed).toBe(false);
    expect(result.results[0]!.details.failingRows).toBe(1);
  });
});

// ---------------------------------------------------------------------------
// expectUnique
// ---------------------------------------------------------------------------

describe("expectUnique", () => {
  it("passes when all unique", async () => {
    const result = await DataFrame.fromArray(orders).expect().expectUnique("id").validate();
    expect(result.passed).toBe(true);
  });

  it("fails when duplicates exist", async () => {
    const result = await DataFrame.fromArray(orders).expect().expectUnique("userId").validate();
    expect(result.passed).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// expectBetween
// ---------------------------------------------------------------------------

describe("expectBetween", () => {
  it("passes when all in range", async () => {
    const result = await DataFrame.fromArray(orders)
      .expect()
      .expectBetween("amount", { min: -100, max: 1000 })
      .validate();

    expect(result.passed).toBe(true);
  });

  it("fails when out of range", async () => {
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

describe("expectMatch", () => {
  it("passes when all match", async () => {
    const result = await DataFrame.fromArray(users)
      .expect()
      .expectMatch("name", { pattern: /^[A-Z]/ })
      .validate();

    expect(result.passed).toBe(true);
  });

  it("fails when some don't match", async () => {
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

describe("expectIn", () => {
  it("passes when all values allowed", async () => {
    const result = await DataFrame.fromArray(orders)
      .expect()
      .expectIn("status", { values: ["pending", "completed", "cancelled"] })
      .validate();

    expect(result.passed).toBe(true);
  });

  it("fails when unexpected values", async () => {
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

describe("expectRowCount", () => {
  it("passes when count in range", async () => {
    const result = await DataFrame.fromArray(orders)
      .expect()
      .expectRowCount({ min: 1, max: 100 })
      .validate();

    expect(result.passed).toBe(true);
  });

  it("fails when too few", async () => {
    const result = await DataFrame.fromArray(orders)
      .expect()
      .expectRowCount({ min: 100 })
      .validate();

    expect(result.passed).toBe(false);
  });

  it("fails when too many", async () => {
    const result = await DataFrame.fromArray(orders).expect().expectRowCount({ max: 2 }).validate();

    expect(result.passed).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// expectFreshness
// ---------------------------------------------------------------------------

describe("expectFreshness", () => {
  it("passes when data is fresh", async () => {
    const result = await DataFrame.fromArray(orders)
      .expect()
      .expectFreshness("createdAt", { maxAgeMs: 24 * 60 * 60 * 1000 }) // 24 hours
      .validate();

    expect(result.passed).toBe(true);
  });

  it("fails when data is stale", async () => {
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

describe("expectReferentialIntegrity", () => {
  it("passes when all references valid", async () => {
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

  it("fails when orphan references exist", async () => {
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

describe("custom expect", () => {
  it("runs custom check", async () => {
    const result = await DataFrame.fromArray(orders)
      .expect()
      .expect("revenue_positive", async (df) => {
        const total = await df.sum("amount");
        return total > 0;
      })
      .validate();

    expect(result.passed).toBe(true);
  });

  it("custom check can fail", async () => {
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

describe("chained expectations", () => {
  it("runs multiple expectations", async () => {
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

  it("reports all failures", async () => {
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

  it("warnings don't fail the suite", async () => {
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

describe("ValidationResult", () => {
  it("includes timestamp and duration", async () => {
    const result = await DataFrame.fromArray(orders).expect().expectRowCount({ min: 1 }).validate();

    expect(result.timestamp).toBeInstanceOf(Date);
    expect(result.durationMs).toBeGreaterThanOrEqual(0);
  });
});
