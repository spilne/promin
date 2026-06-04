import { describe, it, expect } from "bun:test";
import { createGracefulShutdown } from "../graceful-shutdown.ts";

describe("createGracefulShutdown", () => {
  it("signal is not aborted until run()", () => {
    const s = createGracefulShutdown();
    expect(s.signal.aborted).toBe(false);
    expect(s.isShuttingDown).toBe(false);
  });

  it("run() aborts the signal and awaits every registered teardown", async () => {
    const s = createGracefulShutdown();
    const order: string[] = [];
    s.onShutdown(async () => {
      await new Promise((r) => setTimeout(r, 5));
      order.push("a");
    });
    s.onShutdown(async () => {
      order.push("b");
    });

    await s.run();

    expect(s.signal.aborted).toBe(true);
    expect(s.isShuttingDown).toBe(true);
    expect(order.sort()).toEqual(["a", "b"]);
  });

  it("a failing or synchronously-throwing teardown doesn't block the others", async () => {
    const s = createGracefulShutdown();
    let bRan = false;
    s.onShutdown(async () => {
      throw new Error("async boom");
    });
    s.onShutdown(() => {
      throw new Error("sync boom"); // throws synchronously, not async
    });
    s.onShutdown(async () => {
      bRan = true;
    });
    await expect(s.run()).resolves.toBeUndefined();
    expect(bRan).toBe(true);
  });

  it("run() is idempotent — repeated calls collapse onto the first", async () => {
    const s = createGracefulShutdown();
    let calls = 0;
    s.onShutdown(async () => {
      calls += 1;
    });
    await Promise.all([s.run(), s.run(), s.run()]);
    expect(calls).toBe(1);
  });
});
