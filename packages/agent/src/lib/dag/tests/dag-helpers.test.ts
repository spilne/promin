import { describe, it, expect } from "bun:test";
import { checkBlockers, pickPath, resolveNodeInputs } from "../dag-helpers.ts";

describe("pickPath", () => {
  it("empty path returns the value", () => {
    expect(pickPath("hi", "")).toBe("hi");
    expect(pickPath({ a: 1 }, "")).toEqual({ a: 1 });
  });
  it("walks dotted paths through objects + arrays", () => {
    expect(pickPath({ a: { b: [1, 2, 3] } }, "a.b.1")).toBe(2);
    expect(pickPath({ a: { b: 5 } }, "a.b")).toBe(5);
    expect(pickPath({ a: 1 }, "a.b.c")).toBeUndefined();
  });
});

describe("DAG routing helpers", () => {
  const incomingEdges = [
    { from: "a", to: "merge" },
    { from: "b", to: "merge" },
  ];

  it("join any is unblocked by one source output", () => {
    expect(
      checkBlockers({
        incomingEdges,
        join: "any",
        outputs: { a: "yes" },
        errors: {},
        skipped: new Set(["b"]),
      }),
    ).toBeNull();
  });

  it("join all remains blocked by a skipped source", () => {
    expect(
      checkBlockers({
        incomingEdges,
        join: "all",
        outputs: { a: "yes" },
        errors: {},
        skipped: new Set(["b"]),
      }),
    ).toMatch(/upstream node b was skipped/);
  });

  it("resolves a missing upstream output as null", () => {
    expect(
      resolveNodeInputs({
        node: {
          id: "merge",
          kind: "agent",
          agentId: "merge",
          inputs: { fromB: { kind: "node", nodeId: "b", path: "" } },
        },
        initial: {},
        outputs: {},
      }),
    ).toEqual({ fromB: null });
  });
});
