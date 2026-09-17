import { describe, expect, it } from "bun:test";
import type { EvalOutput } from "../../types.ts";
import { exactMatch } from "../exact-match.ts";
import { jsonMatch } from "../json-match.ts";
import { regexMatch } from "../regex-match.ts";

function out(value: unknown): EvalOutput {
  return { output: value, metrics: { latencyMs: 0 } };
}

describe("exactMatch", () => {
  it("scores 1 on a case-insensitive trimmed match", async () => {
    const score = await exactMatch.score({
      input: "q",
      expected: "Paris",
      output: out("  paris "),
    });
    expect(score.value).toBe(1);
  });

  it("scores 0 on a mismatch and explains why", async () => {
    const score = await exactMatch.score({ input: "q", expected: "Paris", output: out("London") });
    expect(score.value).toBe(0);
    expect(score.reason).toBeDefined();
  });

  it("scores 0 when the case has no expected value", async () => {
    const score = await exactMatch.score({ input: "q", output: out("anything") });
    expect(score.value).toBe(0);
  });
});

describe("regexMatch", () => {
  it("scores 1 when a RegExp pattern matches", async () => {
    const score = await regexMatch({ pattern: /\d{3}/ }).score({
      input: "q",
      output: out("order 250 ok"),
    });
    expect(score.value).toBe(1);
  });

  it("accepts a string pattern with flags", async () => {
    const score = await regexMatch({ pattern: "hello", flags: "i" }).score({
      input: "q",
      output: out("HELLO there"),
    });
    expect(score.value).toBe(1);
  });

  it("scores 0 when the pattern misses", async () => {
    const score = await regexMatch({ pattern: /zzz/ }).score({ input: "q", output: out("abc") });
    expect(score.value).toBe(0);
  });
});

describe("jsonMatch", () => {
  it("scores 1 on a deep-equal nested object", async () => {
    const score = await jsonMatch().score({
      input: "q",
      expected: { a: 1, b: { c: 2 } },
      output: out({ a: 1, b: { c: 2 } }),
    });
    expect(score.value).toBe(1);
  });

  it("gives smooth partial credit for field overlap", async () => {
    const score = await jsonMatch().score({
      input: "q",
      expected: { a: 1, b: 2, c: 3, d: 4 },
      output: out({ a: 1, b: 2, c: 99, d: 99 }),
    });
    expect(score.value).toBe(0.5);
  });

  it("exact mode collapses partial overlap to 0", async () => {
    const score = await jsonMatch({ exact: true }).score({
      input: "q",
      expected: { a: 1, b: 2 },
      output: out({ a: 1, b: 99 }),
    });
    expect(score.value).toBe(0);
  });

  it("scores 0 when the case has no expected value", async () => {
    const score = await jsonMatch().score({ input: "q", output: out({ a: 1 }) });
    expect(score.value).toBe(0);
  });
});
