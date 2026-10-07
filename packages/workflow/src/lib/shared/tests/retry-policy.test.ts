// ---------------------------------------------------------------------------
// RetryPolicy adapter — persisted retry policy applied to an Eff, with
// backoff sleeps on a WallClock.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { TaggedError, die, fail, runExit, succeed, suspend } from "@spilne/perfect-core";
import { RETRY_POLICY_DEFAULTS, retryDelayMs, retryWithPolicy } from "../retry-policy.ts";
import { FakeWallClock } from "../wall-clock.ts";

class Flaky extends TaggedError("Flaky")<{ readonly message: string }>() {}
class Fatal extends TaggedError("Fatal")<{ readonly message: string }>() {}

/** Let pending microtasks and engine hops settle. */
const settle = () => new Promise((r) => setTimeout(r, 0));

describe("retryDelayMs", () => {
  it("defaults to a 250ms base doubling per retry", () => {
    expect(RETRY_POLICY_DEFAULTS.maxRetries).toBe(3);
    expect(RETRY_POLICY_DEFAULTS.baseDelayMs).toBe(250);
    expect([0, 1, 2, 3].map((retry) => retryDelayMs({ policy: {}, retry }))).toEqual([
      250, 500, 1000, 2000,
    ]);
  });

  it("caps each delay at maxDelayMs", () => {
    const policy = { baseDelayMs: 100, maxDelayMs: 300 };
    expect([0, 1, 2, 3].map((retry) => retryDelayMs({ policy, retry }))).toEqual([
      100, 200, 300, 300,
    ]);
  });

  it("jitter stays within ±25% of the un-jittered delay", () => {
    const policy = { baseDelayMs: 1000, jitter: true };
    expect(retryDelayMs({ policy, retry: 0, random: () => 0 })).toBe(750);
    expect(retryDelayMs({ policy, retry: 0, random: () => 0.5 })).toBe(1000);
    const high = retryDelayMs({ policy, retry: 0, random: () => 0.999999 });
    expect(high).toBeLessThan(1250);
    expect(high).toBeGreaterThan(1249);
    for (let i = 0; i < 200; i++) {
      const d = retryDelayMs({ policy, retry: 1 });
      expect(d).toBeGreaterThanOrEqual(1500);
      expect(d).toBeLessThan(2500);
    }
  });
});

describe("retryWithPolicy", () => {
  it("retries typed failures maxRetries times (default 3) with backoff on the clock", async () => {
    const clock = FakeWallClock.create(0);
    let attempts = 0;
    const eff = suspend(() => {
      attempts++;
      return fail(new Flaky({ message: `attempt ${attempts}` }));
    });

    const done = runExit(retryWithPolicy({ eff, policy: {}, clock }));
    await settle();
    expect(attempts).toBe(1);

    for (const [delay, expected] of [
      [250, 2],
      [500, 3],
      [1000, 4],
    ] as const) {
      clock.advance(delay - 1);
      await settle();
      expect(attempts).toBe(expected - 1);
      clock.advance(1);
      await settle();
      expect(attempts).toBe(expected);
    }

    const exit = await done;
    expect(exit._tag).toBe("Failure");
    expect(attempts).toBe(4);
    expect(clock.pendingCount()).toBe(0);
  });

  it("succeeds as soon as an attempt succeeds", async () => {
    const clock = FakeWallClock.create(0);
    let attempts = 0;
    const eff = suspend(() => (++attempts < 2 ? fail(new Flaky({ message: "x" })) : succeed("ok")));
    const done = runExit(retryWithPolicy({ eff, policy: { baseDelayMs: 10 }, clock }));
    await settle();
    clock.advance(10);
    const exit = await done;
    expect(exit).toEqual({ _tag: "Success", value: "ok" });
    expect(attempts).toBe(2);
  });

  it("does not retry errors rejected by `when`", async () => {
    let attempts = 0;
    const eff = suspend(() => {
      attempts++;
      return fail(new Fatal({ message: "no" }) as Flaky | Fatal);
    });
    const exit = await runExit(
      retryWithPolicy({ eff, policy: { when: (e) => e._tag === "Flaky" } }),
    );
    expect(exit._tag).toBe("Failure");
    expect(attempts).toBe(1);
  });

  it("never retries defects", async () => {
    let attempts = 0;
    const eff = suspend(() => {
      attempts++;
      return die(new Error("bug"));
    });
    const exit = await runExit(retryWithPolicy({ eff, policy: { maxRetries: 5 } }));
    expect(exit._tag).toBe("Failure");
    expect(attempts).toBe(1);
  });

  it("stops retrying once timeBudgetMs has elapsed since the first failure", async () => {
    const clock = FakeWallClock.create(0);
    let attempts = 0;
    const eff = suspend(() => {
      attempts++;
      return fail(new Flaky({ message: "x" }));
    });
    const done = runExit(
      retryWithPolicy({
        eff,
        policy: { maxRetries: 10, baseDelayMs: 100, timeBudgetMs: 250 },
        clock,
      }),
    );
    // Delays 100 then 200: the third failure lands at t=300 >= 250.
    await settle();
    clock.advance(100);
    await settle();
    clock.advance(200);
    const exit = await done;
    expect(exit._tag).toBe("Failure");
    expect(attempts).toBe(3);
  });
});
