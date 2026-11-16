import { describe, expect, test } from "bun:test";
import { FakeWallClock } from "@promin/workflow";
import { SecurityAuditOutboxWorker } from "../security-audit-outbox.ts";
import { DurableSecurityAudit } from "../security-audit.ts";
import { InMemorySecurityStore } from "../in-memory-security-store.ts";

const KEY = "a".repeat(32);

describe("SecurityAuditOutboxWorker", () => {
  test("concurrent replicas lease exclusively and delivered rows are not resent", async () => {
    const clock = FakeWallClock.create(1_000);
    const store = new InMemorySecurityStore({ clock });
    await appendOutboxEvent(store, clock, "event-1");
    const deliveries: string[] = [];
    const destination = {
      deliver: async ({ idempotencyKey }: { readonly idempotencyKey: string }) => {
        deliveries.push(idempotencyKey);
      },
    };
    const workers = ["one", "two"].map(
      (owner) => new SecurityAuditOutboxWorker({ store, destination, owner, clock, leaseMs: 100 }),
    );

    const reports = await Promise.all(workers.map((worker) => worker.runOnce()));
    expect(reports.reduce((sum, report) => sum + report.claimed, 0)).toBe(1);
    expect(reports.reduce((sum, report) => sum + report.delivered, 0)).toBe(1);
    expect(deliveries).toEqual(["event-1"]);
    expect(await store.getAuditOutbox("event-1")).toMatchObject({ status: "delivered" });
    expect((await workers[0]!.runOnce()).claimed).toBe(0);
    expect(deliveries).toEqual(["event-1"]);
  });

  test("retries with bounded backoff and persists only payload-free failure codes", async () => {
    const clock = FakeWallClock.create(1_000);
    const store = new InMemorySecurityStore({ clock });
    await appendOutboxEvent(store, clock, "event-retry");
    let attempts = 0;
    const worker = new SecurityAuditOutboxWorker({
      store,
      owner: "retry-worker",
      clock,
      initialRetryMs: 100,
      maxRetryMs: 250,
      destination: {
        deliver: async () => {
          attempts += 1;
          if (attempts === 1) throw new Error("upstream body contained secret-token");
        },
      },
    });

    expect(await worker.runOnce()).toMatchObject({ claimed: 1, failed: 1, delivered: 0 });
    expect(await store.getAuditOutbox("event-retry")).toMatchObject({
      status: "pending",
      nextAttemptAt: 1_100,
      lastError: "delivery_failed",
    });
    expect(JSON.stringify(await store.getAuditOutbox("event-retry"))).not.toContain("secret-token");
    expect((await worker.runOnce()).claimed).toBe(0);

    clock.advance(100);
    expect(await worker.runOnce()).toMatchObject({ claimed: 1, delivered: 1, failed: 0 });
    expect(attempts).toBe(2);
    expect(worker.metrics()).toMatchObject({
      runs: 3,
      claimed: 2,
      delivered: 1,
      failed: 1,
      lastFailureAt: 1_000,
      lastSuccessAt: 1_100,
    });
  });

  test("fences stale acknowledgements and reuses the idempotency key after crash-like expiry", async () => {
    const clock = FakeWallClock.create(1_000);
    const store = new InMemorySecurityStore({ clock });
    await appendOutboxEvent(store, clock, "event-fenced");
    const calls: string[] = [];
    const sideEffects = new Set<string>();
    const slow = new SecurityAuditOutboxWorker({
      store,
      owner: "slow",
      clock,
      leaseMs: 10,
      destination: {
        deliver: async ({ idempotencyKey }) => {
          calls.push(idempotencyKey);
          sideEffects.add(idempotencyKey);
          clock.advance(10);
        },
      },
    });
    expect(await slow.runOnce()).toMatchObject({ claimed: 1, delivered: 0, fenced: 1 });

    const recovery = new SecurityAuditOutboxWorker({
      store,
      owner: "recovery",
      clock,
      leaseMs: 10,
      destination: {
        deliver: async ({ idempotencyKey }) => {
          calls.push(idempotencyKey);
          sideEffects.add(idempotencyKey);
        },
      },
    });
    expect(await recovery.runOnce()).toMatchObject({ claimed: 1, delivered: 1, fenced: 0 });
    expect(calls).toEqual(["event-fenced", "event-fenced"]);
    expect(sideEffects.size).toBe(1);
  });
});

async function appendOutboxEvent(
  store: InMemorySecurityStore,
  clock: FakeWallClock,
  id: string,
): Promise<void> {
  const audit = new DurableSecurityAudit({
    store,
    activeKeyId: "key",
    keys: { key: KEY },
    clock,
    idFactory: () => id,
    outbox: {},
  });
  await audit.append({
    namespaceId: "acme",
    occurredAt: clock.currentTimeMs(),
    event: "authorization",
  });
}
