import { describe, it, expect } from "bun:test";
import type { Interface } from "node:readline";
import { Terminal } from "../../lib/terminal/terminal.ts";

function fakeRl(): Interface {
  return {
    rawListeners: () => [],
    removeListener: () => {},
    on: () => {},
  } as unknown as Interface;
}

describe("Terminal.showInlineMenu()", () => {
  it("returns null for empty items list", async () => {
    const term = new Terminal(fakeRl());
    const result = await term.showInlineMenu([]);
    expect(result).toBeNull();
  });

  it("returns the single item immediately without reading a key", async () => {
    const term = new Terminal(fakeRl());
    // Single-item fast path: no key read, returns immediately
    const result = await term.showInlineMenu(["/history"]);
    expect(result).toBe("/history");
  });
});
