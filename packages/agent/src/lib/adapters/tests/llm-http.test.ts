import { describe, expect, it } from "bun:test";
import { FakeWallClock } from "@promin/workflow";
import {
  LLMHttpError,
  LLMTimeoutError,
  parseRetryAfterMs,
  parseToolArguments,
  startLLMRequest,
} from "../llm-http.ts";

describe("parseRetryAfterMs", () => {
  const parse = (headers: Record<string, string>, nowMs = 0) =>
    parseRetryAfterMs({ headers: new Headers(headers), nowMs });

  it("prefers retry-after-ms, then Retry-After seconds, then an HTTP date", () => {
    expect(parse({ "retry-after-ms": "250", "retry-after": "9" })).toBe(250);
    expect(parse({ "retry-after": "3" })).toBe(3_000);
    const now = Date.parse("2026-01-01T00:00:00Z");
    expect(parse({ "retry-after": "Thu, 01 Jan 2026 00:00:10 GMT" }, now)).toBe(10_000);
  });

  it("ignores missing and unparseable values", () => {
    expect(parse({})).toBeUndefined();
    expect(parse({ "retry-after": "soon" })).toBeUndefined();
    expect(parse({ "retry-after-ms": "", "retry-after": "-1" })).toBeUndefined();
  });
});

describe("LLMHttpError", () => {
  it("keeps a bounded body excerpt", () => {
    const err = new LLMHttpError({ status: 500, provider: "x", body: "a".repeat(5_000) });
    expect(err.body.length).toBeLessThan(2_100);
    expect(err.message.startsWith("x API error 500: aaa")).toBe(true);
  });
});

describe("startLLMRequest", () => {
  it("aborts on timeout with an LLMTimeoutError reason and maps late errors to it", () => {
    const clock = FakeWallClock.create(0);
    const request = startLLMRequest({ provider: "p", timeoutMs: 100, clock });
    clock.advance(100);
    expect(request.signal.aborted).toBe(true);
    expect(request.signal.reason).toBeInstanceOf(LLMTimeoutError);
    expect(request.mapError(new Error("socket closed"))).toBeInstanceOf(LLMTimeoutError);
    request.dispose();
  });

  it("follows the caller's signal, including one already aborted", () => {
    const clock = FakeWallClock.create(0);
    const caller = new AbortController();
    const request = startLLMRequest({
      provider: "p",
      timeoutMs: 100,
      clock,
      signal: caller.signal,
    });
    caller.abort("stop");
    expect(request.signal.aborted).toBe(true);
    expect(request.signal.reason).toBe("stop");
    const original = new Error("x");
    expect(request.mapError(original)).toBe(original);
    request.dispose();
    expect(clock.pendingCount()).toBe(0);

    const pre = startLLMRequest({ provider: "p", timeoutMs: 100, clock, signal: caller.signal });
    expect(pre.signal.aborted).toBe(true);
    pre.dispose();
  });
});

describe("parseToolArguments", () => {
  it("parses valid JSON and treats empty arguments as {}", () => {
    expect(parseToolArguments({ json: '{"a":1}' })).toEqual({ input: { a: 1 } });
    expect(parseToolArguments({ json: "" })).toEqual({ input: {} });
  });

  it("returns {} plus an inputError for malformed JSON", () => {
    const res = parseToolArguments({ json: '{"a":' });
    expect(res.input).toEqual({});
    expect(res.inputError).toContain("not valid JSON");
  });
});
