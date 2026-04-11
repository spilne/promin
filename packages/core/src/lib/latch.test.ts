import { describe, it, expect } from "bun:test";
import { Effect } from "effect";
import { latchTestSuite } from "./latch-test-suite.ts";
import { PipelineLatch } from "./latch.ts";

latchTestSuite(() => PipelineLatch.make({ count: 3 }));

describe("PipelineLatch Effect API", () => {
  it("countDown decrements remaining", async () => {
    const l = PipelineLatch.make({ count: 2 });
    await Effect.runPromise(l.countDown);
    expect(await Effect.runPromise(l.remaining)).toBe(1);
  });

  it("countDownBy decrements by n", async () => {
    const l = PipelineLatch.make({ count: 5 });
    await Effect.runPromise(l.countDownBy(3));
    expect(await Effect.runPromise(l.remaining)).toBe(2);
  });

  it("countDownBy to zero resolves await", async () => {
    const l = PipelineLatch.make({ count: 4 });
    let resolved = false;
    void Effect.runPromise(l.await).then(() => {
      resolved = true;
    });
    await Effect.runPromise(l.countDownBy(4));
    await new Promise((r) => setTimeout(r, 10));
    expect(resolved).toBe(true);
  });

  it("await resolves when countDown reaches zero", async () => {
    const l = PipelineLatch.make({ count: 1 });
    await Effect.runPromise(l.countDown);
    await Effect.runPromise(l.await);
  });

  it("remaining returns initial count", async () => {
    const l = PipelineLatch.make({ count: 7 });
    expect(await Effect.runPromise(l.remaining)).toBe(7);
  });
});
