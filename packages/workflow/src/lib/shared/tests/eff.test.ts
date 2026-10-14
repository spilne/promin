import { describe, it, expect } from "bun:test";
import { TaggedError, fail, succeed } from "@spilne/perfect-core";
import { promiseOrEff, runEffSafe, runHookValue } from "../eff.ts";

class PaymentError extends TaggedError("PaymentError")<{ readonly message: string }>() {}

describe("runHookValue", () => {
  it("runs an Eff", async () => {
    expect(await runHookValue(succeed(7))).toBe(7);
  });

  it("runs an Eff that a Promise resolves to", async () => {
    expect(await runHookValue(Promise.resolve(succeed(7)))).toBe(7);
  });

  it("rejects with the typed failure of an Eff that a Promise resolves to", async () => {
    const err = new PaymentError({ message: "declined" });
    await expect(runHookValue(Promise.resolve(fail(err)))).rejects.toBe(err);
  });

  it("passes plain values and resolved plain values through", async () => {
    expect(await runHookValue(3)).toBe(3);
    expect(await runHookValue(Promise.resolve("x"))).toBe("x");
  });
});

describe("promiseOrEff", () => {
  it("keeps the typed failure of an Eff the Promise resolves to", async () => {
    const err = new PaymentError({ message: "declined" });
    const { error } = await runEffSafe(promiseOrEff(async () => fail(err)));
    expect(error).toBe(err);
  });

  it("treats a rejection as a defect", async () => {
    const boom = new Error("boom");
    await expect(
      runEffSafe(
        promiseOrEff(async () => {
          throw boom;
        }),
      ),
    ).rejects.toBe(boom);
  });

  it("yields a resolved plain value", async () => {
    const { data } = await runEffSafe(promiseOrEff(async () => 5));
    expect(data).toBe(5);
  });
});
