import { describe, it, expect } from "bun:test";
import { createHash } from "node:crypto";
import { sha256Hex } from "../sha256.ts";

describe("sha256Hex", () => {
  it("matches node:crypto across padding boundaries and non-ASCII input", () => {
    const inputs = [
      "",
      "abc",
      "a".repeat(55),
      "a".repeat(56),
      "a".repeat(64),
      "a".repeat(1000),
      "héllo ✓ 日本",
    ];
    for (const input of inputs) {
      expect(sha256Hex(input)).toBe(createHash("sha256").update(input).digest("hex"));
    }
  });

  it("negative control: different inputs hash differently", () => {
    expect(sha256Hex("a")).not.toBe(sha256Hex("b"));
  });
});
