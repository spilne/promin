// ---------------------------------------------------------------------------
// recipe-memory-form — form-state ⇄ recipe round-trips for the edit-drawer
// memory/compaction section.
// Pins: inherit/off/on tri-state; blank stays unset (not 0); the off
// toggle persists `false`; contextBudget needs maxMessageTokens.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import {
  buildAutoCompact,
  buildAutoDistill,
  buildContextBudget,
  initAutoCompact,
  initAutoDistill,
  initContextBudget,
  parsePositiveInt,
} from "../recipe-memory-form.ts";

describe("parsePositiveInt", () => {
  it("blank → undefined; 0 / negative / junk → undefined; positive → number", () => {
    expect(parsePositiveInt("")).toBeUndefined();
    expect(parsePositiveInt("   ")).toBeUndefined();
    expect(parsePositiveInt("0")).toBeUndefined();
    expect(parsePositiveInt("-3")).toBeUndefined();
    expect(parsePositiveInt("abc")).toBeUndefined();
    expect(parsePositiveInt("30")).toBe(30);
  });
});

describe("autoCompact form mapping", () => {
  it("undefined recipe → inherit mode → builds back to undefined", () => {
    const form = initAutoCompact(undefined);
    expect(form.mode).toBe("inherit");
    expect(buildAutoCompact(form)).toBeUndefined();
  });

  it("false recipe → off mode → builds back to false", () => {
    const form = initAutoCompact(false);
    expect(form.mode).toBe("off");
    expect(buildAutoCompact(form)).toBe(false);
  });

  it("round-trips a configured recipe", () => {
    const form = initAutoCompact({ messageThreshold: 30, keepRecent: 5, mode: "blocking" });
    expect(form.mode).toBe("on");
    expect(form.messageThreshold).toBe("30");
    expect(buildAutoCompact(form)).toEqual({
      mode: "blocking",
      messageThreshold: 30,
      keepRecent: 5,
    });
  });

  it("blank numeric fields stay unset — never written as 0", () => {
    const built = buildAutoCompact({
      mode: "on",
      messageThreshold: "",
      tokenThreshold: "",
      contextLimit: "",
      compressAt: "",
      keepRecent: "",
      runMode: "background",
    });
    // Only `mode` — no zero-valued thresholds leaked in.
    expect(built).toEqual({ mode: "background" });
  });
});

describe("autoDistill form mapping", () => {
  it("tri-state round-trips (inherit / off)", () => {
    expect(buildAutoDistill(initAutoDistill(undefined))).toBeUndefined();
    expect(buildAutoDistill(initAutoDistill(false))).toBe(false);
  });

  it("round-trips force + thresholds; omits force when false", () => {
    expect(buildAutoDistill(initAutoDistill({ messageThreshold: 6, force: true }))).toEqual({
      mode: "background",
      messageThreshold: 6,
      force: true,
    });
    // force defaults false → not emitted.
    expect(buildAutoDistill(initAutoDistill({ intervalMs: 600_000 }))).toEqual({
      mode: "background",
      intervalMs: 600_000,
    });
  });
});

describe("contextBudget form mapping", () => {
  it("round-trips both fields", () => {
    expect(
      buildContextBudget(initContextBudget({ maxMessageTokens: 4000, maxEpisodeTokens: 1000 })),
    ).toEqual({ maxMessageTokens: 4000, maxEpisodeTokens: 1000 });
  });

  it("omits maxEpisodeTokens when blank", () => {
    expect(buildContextBudget(initContextBudget({ maxMessageTokens: 4000 }))).toEqual({
      maxMessageTokens: 4000,
    });
  });

  it("without maxMessageTokens the whole budget stays unset", () => {
    expect(buildContextBudget({ maxMessageTokens: "", maxEpisodeTokens: "1000" })).toBeUndefined();
    expect(buildContextBudget(initContextBudget(undefined))).toBeUndefined();
  });
});
