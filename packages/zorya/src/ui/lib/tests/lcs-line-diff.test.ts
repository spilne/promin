// ---------------------------------------------------------------------------
// lcsLineDiff — line-level LCS diff used by AgentVersionsModal.
// Pinned cases:
//   1. Identical inputs → all "ctx"
//   2. Pure addition → original lines as "ctx", new lines as "add"
//   3. Pure deletion → kept lines as "ctx", removed as "del"
//   4. Mixed change in the middle of unchanged context
//   5. Empty inputs round-trip cleanly
//   6. Output preserves order on both sides (reading "ctx"+"del" gives
//      back left; "ctx"+"add" gives back right)
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { lcsLineDiff } from "../../components/agents/agent-versions-modal.tsx";

describe("lcsLineDiff", () => {
  it("identical inputs → all ctx", () => {
    const out = lcsLineDiff("a\nb\nc", "a\nb\nc");
    expect(out.every((d) => d.kind === "ctx")).toBe(true);
    expect(out.map((d) => d.text)).toEqual(["a", "b", "c"]);
  });

  it("pure addition appends as add", () => {
    const out = lcsLineDiff("a\nb", "a\nb\nc");
    expect(out.map((d) => `${d.kind}:${d.text}`)).toEqual(["ctx:a", "ctx:b", "add:c"]);
  });

  it("pure deletion marks dropped lines as del", () => {
    const out = lcsLineDiff("a\nb\nc", "a\nc");
    expect(out.map((d) => `${d.kind}:${d.text}`)).toEqual(["ctx:a", "del:b", "ctx:c"]);
  });

  it("mixed change inside unchanged context", () => {
    const out = lcsLineDiff("a\nb\nc\nd", "a\nB\nc\nd");
    // Naive LCS picks one alignment; the important thing is that both
    // "left only" and "right only" lines appear — order between del/add
    // is implementation detail.
    const dels = out.filter((d) => d.kind === "del").map((d) => d.text);
    const adds = out.filter((d) => d.kind === "add").map((d) => d.text);
    const ctx = out.filter((d) => d.kind === "ctx").map((d) => d.text);
    expect(dels).toEqual(["b"]);
    expect(adds).toEqual(["B"]);
    expect(ctx).toEqual(["a", "c", "d"]);
  });

  it("empty inputs round-trip cleanly", () => {
    expect(lcsLineDiff("", "")).toEqual([{ kind: "ctx", text: "" }]);
    expect(lcsLineDiff("", "x")).toEqual([
      { kind: "del", text: "" },
      { kind: "add", text: "x" },
    ]);
  });

  it("reading ctx+del reconstructs left, ctx+add reconstructs right", () => {
    const left = "alpha\nbeta\ngamma";
    const right = "alpha\nBETA\ngamma\ndelta";
    const out = lcsLineDiff(left, right);
    const reconstructedLeft = out
      .filter((d) => d.kind !== "add")
      .map((d) => d.text)
      .join("\n");
    const reconstructedRight = out
      .filter((d) => d.kind !== "del")
      .map((d) => d.text)
      .join("\n");
    expect(reconstructedLeft).toBe(left);
    expect(reconstructedRight).toBe(right);
  });
});
