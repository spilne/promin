import { describe, it, expect } from "bun:test";
import { z } from "zod";
import { DataFrame } from "../../dataframe/dataframe.ts";
import { defineContract } from "../contract.ts";

describe("Data contracts — enforce agreements between producers and consumers", () => {
  it("valid data passes contract validation", async () => {
    const contract = defineContract({
      name: "orders",
      version: "1.0",
      owner: "payments-team",
      schema: z.object({
        id: z.number(),
        amount: z.number().positive(),
        status: z.enum(["pending", "completed"]),
      }),
    });

    const data = DataFrame.fromArray([
      { id: 1, amount: 100, status: "completed" },
      { id: 2, amount: 50, status: "pending" },
    ]);

    const result = await contract.validate(data);
    expect(result.valid).toBe(true);
    expect(result.schemaViolations).toHaveLength(0);
  });

  it("detects schema violations — wrong types or missing fields", async () => {
    const contract = defineContract({
      name: "users",
      version: "1.0",
      owner: "identity-team",
      schema: z.object({
        id: z.string().uuid(),
        email: z.string().email(),
      }),
    });

    const data = DataFrame.fromArray([
      { id: "not-a-uuid", email: "bad-email" },
      { id: 123, email: "valid@test.com" },
    ]);

    const result = await contract.validate(data);
    expect(result.valid).toBe(false);
    expect(result.schemaViolations.length).toBeGreaterThan(0);
  });

  it("enforces freshness SLA — data must be recent", async () => {
    const contract = defineContract({
      name: "events",
      version: "1.0",
      owner: "analytics",
      schema: z.object({ ts: z.string(), value: z.number() }),
      sla: {
        freshness: { maxAgeMs: 60_000, column: "ts" }, // 1 minute
      },
    });

    const staleData = DataFrame.fromArray([
      { ts: new Date(Date.now() - 120_000).toISOString(), value: 1 }, // 2 min old
    ]);

    const result = await contract.validate(staleData);
    expect(result.valid).toBe(false);
    expect(result.slaViolations.some((v) => v.type === "freshness")).toBe(true);
  });

  it("enforces completeness — minimum row count", async () => {
    const contract = defineContract({
      name: "daily-report",
      version: "1.0",
      owner: "data-team",
      schema: z.object({ metric: z.number() }),
      sla: {
        completeness: { minRowCount: 100 },
      },
    });

    const tooFew = DataFrame.fromArray([{ metric: 1 }, { metric: 2 }]);
    const result = await contract.validate(tooFew);

    expect(result.valid).toBe(false);
    expect(result.slaViolations.some((v) => v.type === "completeness")).toBe(true);
  });

  it("enforces completeness — null percentage limits", async () => {
    const contract = defineContract({
      name: "profiles",
      version: "1.0",
      owner: "identity-team",
      schema: z.object({ id: z.number(), email: z.string().nullable() }),
      sla: {
        completeness: { maxNullPct: { email: 10 } }, // max 10% nulls
      },
    });

    const data = DataFrame.fromArray([
      { id: 1, email: "a@b.com" },
      { id: 2, email: null },
      { id: 3, email: null },
      { id: 4, email: null }, // 75% null
    ]);

    const result = await contract.validate(data);
    expect(result.valid).toBe(false);
    expect(result.slaViolations.some((v) => v.details.includes("email"))).toBe(true);
  });

  it("enforces uniqueness — no duplicate keys", async () => {
    const contract = defineContract({
      name: "orders",
      version: "1.0",
      owner: "payments-team",
      schema: z.object({ orderId: z.string(), amount: z.number() }),
      sla: {
        uniqueness: { columns: ["orderId"] },
      },
    });

    const dupes = DataFrame.fromArray([
      { orderId: "o1", amount: 100 },
      { orderId: "o1", amount: 200 }, // duplicate
      { orderId: "o2", amount: 50 },
    ]);

    const result = await contract.validate(dupes);
    expect(result.valid).toBe(false);
    expect(result.slaViolations.some((v) => v.type === "uniqueness")).toBe(true);
  });

  it("all SLAs pass — contract is valid", async () => {
    const contract = defineContract({
      name: "metrics",
      version: "1.0",
      owner: "analytics",
      schema: z.object({ id: z.number(), value: z.number(), ts: z.string() }),
      sla: {
        freshness: { maxAgeMs: 3600_000, column: "ts" },
        completeness: { minRowCount: 1, maxNullPct: { value: 0 } },
        uniqueness: { columns: ["id"] },
      },
    });

    const data = DataFrame.fromArray([
      { id: 1, value: 100, ts: new Date().toISOString() },
      { id: 2, value: 200, ts: new Date().toISOString() },
    ]);

    const result = await contract.validate(data);
    expect(result.valid).toBe(true);
  });
});
