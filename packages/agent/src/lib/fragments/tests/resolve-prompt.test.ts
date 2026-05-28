// ---------------------------------------------------------------------------
// resolveSystemPrompt — turning a recipe's `systemPrompt` (string | layered
// | null) into the concatenated string the agent loop sees. Pins:
//   - null/undefined → undefined
//   - plain string passes through
//   - layered form joins `base` + each layer with blank-line separators
//   - layered form without a registry returns only `base`
//   - missing layer: warn (default) skips, skip is silent, throw throws
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { resolveSystemPrompt } from "../resolve-prompt.ts";
import { InMemoryFragmentRegistry } from "../in-memory-fragment-registry.ts";

const fragments = new InMemoryFragmentRegistry({
  "review-checklist": "## Review checklist\n- correctness\n- edges",
  "explorer-budget": "## Time budget\n- 5m / 15m / 60m tiers",
});

describe("resolveSystemPrompt", () => {
  it("returns undefined for null / undefined", () => {
    expect(resolveSystemPrompt({ systemPrompt: null })).toBeUndefined();
    expect(resolveSystemPrompt({ systemPrompt: undefined as never })).toBeUndefined();
  });

  it("passes a plain string through verbatim", () => {
    expect(resolveSystemPrompt({ systemPrompt: "You are X." })).toBe("You are X.");
  });

  it("concatenates base + resolved layers in declaration order", () => {
    const out = resolveSystemPrompt({
      systemPrompt: {
        base: "You are a reviewer.",
        layers: ["review-checklist", "explorer-budget"],
      },
      fragments,
    });
    expect(out).toBe(
      "You are a reviewer.\n\n## Review checklist\n- correctness\n- edges\n\n## Time budget\n- 5m / 15m / 60m tiers",
    );
  });

  it("returns only base when layers is omitted or empty", () => {
    expect(resolveSystemPrompt({ systemPrompt: { base: "B" }, fragments })).toBe("B");
    expect(resolveSystemPrompt({ systemPrompt: { base: "B", layers: [] }, fragments })).toBe("B");
  });

  it("returns base when layered recipe has no registry wired", () => {
    expect(resolveSystemPrompt({ systemPrompt: { base: "B", layers: ["review-checklist"] } })).toBe(
      "B",
    );
  });

  it("missing layer: default 'warn' skips it but keeps resolving the rest", () => {
    const warns: string[] = [];
    const origWarn = console.warn;
    console.warn = (...args) => warns.push(args.join(" "));
    try {
      const out = resolveSystemPrompt({
        systemPrompt: { base: "B", layers: ["nope", "review-checklist"] },
        fragments,
      });
      expect(out).toBe("B\n\n## Review checklist\n- correctness\n- edges");
      expect(warns.some((w) => w.includes('"nope"'))).toBe(true);
    } finally {
      console.warn = origWarn;
    }
  });

  it("missing layer with onMissingLayer: 'skip' is silent", () => {
    const warns: string[] = [];
    const origWarn = console.warn;
    console.warn = (...args) => warns.push(args.join(" "));
    try {
      const out = resolveSystemPrompt({
        systemPrompt: { base: "B", layers: ["nope"] },
        fragments,
        onMissingLayer: "skip",
      });
      expect(out).toBe("B");
      expect(warns).toEqual([]);
    } finally {
      console.warn = origWarn;
    }
  });

  it("missing layer with onMissingLayer: 'throw' throws", () => {
    expect(() =>
      resolveSystemPrompt({
        systemPrompt: { base: "B", layers: ["nope"] },
        fragments,
        onMissingLayer: "throw",
      }),
    ).toThrow(/nope/);
  });
});

describe("InMemoryFragmentRegistry", () => {
  it("get + list round-trip", () => {
    const r = new InMemoryFragmentRegistry({ a: "A body", b: "B body" });
    expect(r.get("a")).toBe("A body");
    expect(r.get("missing")).toBeUndefined();
    expect(
      r
        .list()
        .map((f) => f.key)
        .sort(),
    ).toEqual(["a", "b"]);
  });
});
