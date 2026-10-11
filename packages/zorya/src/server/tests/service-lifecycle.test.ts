import { describe, expect, it } from "bun:test";
import { FakeWallClock } from "@promin/workflow";
import { ServiceLifecycle, type ManagedService } from "../service-lifecycle.ts";

function flaky(params: { name: string; failures: number }): ManagedService & {
  starts: number;
  stops: number;
} {
  const svc = {
    name: params.name,
    starts: 0,
    stops: 0,
    async start() {
      svc.starts += 1;
      if (svc.starts <= params.failures) throw new Error(`boom ${svc.starts}`);
    },
    async stop() {
      svc.stops += 1;
    },
  };
  return svc;
}

async function settle(): Promise<void> {
  for (let i = 0; i < 10; i++) await Promise.resolve();
}

describe("ServiceLifecycle", () => {
  it("retries a failing start with doubling backoff on the clock", async () => {
    const clock = FakeWallClock.create(0);
    const svc = flaky({ name: "workflows", failures: 2 });
    const errors: string[] = [];
    const lifecycle = new ServiceLifecycle({
      services: [svc],
      clock,
      initialRetryDelayMs: 100,
      onError: (m) => errors.push(m),
    });
    lifecycle.start();
    await settle();
    expect(lifecycle.statuses()[0]).toMatchObject({
      state: "retrying",
      attempts: 1,
      lastError: "boom 1",
    });

    clock.advance(99);
    expect(svc.starts).toBe(1);
    clock.advance(1);
    await settle();
    expect(svc.starts).toBe(2);

    clock.advance(199);
    expect(svc.starts).toBe(2);
    clock.advance(1);
    await lifecycle.ready();
    expect(svc.starts).toBe(3);
    expect(lifecycle.isReady).toBe(true);
    expect(lifecycle.statuses()[0]).toEqual({ name: "workflows", state: "started", attempts: 3 });
    expect(errors).toHaveLength(2);
    expect(clock.pendingCount()).toBe(0);
  });

  it("caps the backoff", async () => {
    const clock = FakeWallClock.create(0);
    const svc = flaky({ name: "s", failures: 100 });
    const lifecycle = new ServiceLifecycle({
      services: [svc],
      clock,
      initialRetryDelayMs: 100,
      maxRetryDelayMs: 250,
      onError: () => {},
    });
    lifecycle.start();
    await settle();
    clock.advance(100);
    await settle();
    clock.advance(200);
    await settle();
    expect(svc.starts).toBe(3);
    clock.advance(250);
    await settle();
    expect(svc.starts).toBe(4);
    await lifecycle.stop();
  });

  it("stop cancels pending retries, stops every service and rejects ready()", async () => {
    const clock = FakeWallClock.create(0);
    const good = flaky({ name: "good", failures: 0 });
    const bad = flaky({ name: "bad", failures: 100 });
    const lifecycle = new ServiceLifecycle({ services: [good, bad], clock, onError: () => {} });
    lifecycle.start();
    const ready = lifecycle.ready();
    await settle();
    expect(clock.pendingCount()).toBe(1);

    await lifecycle.stop();
    await expect(ready).rejects.toThrow(/stopped/);
    expect(clock.pendingCount()).toBe(0);
    expect(good.stops).toBe(1);
    expect(bad.stops).toBe(1);
    expect(lifecycle.statuses().map((s) => s.state)).toEqual(["stopped", "stopped"]);
    clock.advance(60_000);
    expect(bad.starts).toBe(1);
  });

  it("waits for a start in flight before stopping", async () => {
    let finishStart!: () => void;
    const order: string[] = [];
    const svc: ManagedService = {
      name: "slow",
      start: () =>
        new Promise<void>((r) => {
          finishStart = () => {
            order.push("started");
            r();
          };
        }),
      stop: async () => {
        order.push("stopped");
      },
    };
    const lifecycle = new ServiceLifecycle({ services: [svc], onError: () => {} });
    lifecycle.start();
    const stopping = lifecycle.stop();
    await settle();
    expect(order).toEqual([]);
    finishStart();
    await stopping;
    expect(order).toEqual(["started", "stopped"]);
  });

  it("reports a failing stop without throwing", async () => {
    const errors: string[] = [];
    const lifecycle = new ServiceLifecycle({
      services: [
        {
          name: "x",
          start: async () => {},
          stop: async () => {
            throw new Error("nope");
          },
        },
      ],
      onError: (m) => errors.push(m),
    });
    lifecycle.start();
    await lifecycle.ready();
    await lifecycle.stop();
    expect(errors).toEqual(["[zorya] service x failed to stop"]);
  });
});
