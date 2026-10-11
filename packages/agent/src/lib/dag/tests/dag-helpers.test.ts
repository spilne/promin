import { describe, it, expect } from "bun:test";
import { pickPath } from "../dag-helpers.ts";

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
