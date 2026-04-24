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

function captureStdout(fn: () => void): string[] {
  const chunks: string[] = [];
  const orig = process.stdout.write.bind(process.stdout);
  (process.stdout as any).write = (chunk: string | Buffer) => {
    chunks.push(typeof chunk === "string" ? chunk : chunk.toString());
    return true;
  };
  try {
    fn();
  } finally {
    (process.stdout as any).write = orig;
  }
  return chunks;
}

describe("Terminal.suppress — blocks spinner writes", () => {
  it("startSpinner writes spinner content when suppress = false", () => {
    const term = new Terminal(fakeRl());
    const writes = captureStdout(() => {
      term.suppress = false;
      term.startSpinner("visible-label");
      term.stopSpinner();
    });
    expect(writes.some((s) => s.includes("visible-label"))).toBe(true);
  });

  it("startSpinner does not write when suppress = true", () => {
    const term = new Terminal(fakeRl());
    const writes = captureStdout(() => {
      term.suppress = true;
      term.startSpinner("sentinel-label");
      term.stopSpinner();
    });
    expect(writes.some((s) => s.includes("sentinel-label"))).toBe(false);
  });

  it("suppress = false restores normal rendering after suppression", () => {
    const term = new Terminal(fakeRl());

    captureStdout(() => {
      term.suppress = true;
      term.startSpinner("suppressed");
      term.stopSpinner();
    });

    const writes = captureStdout(() => {
      term.suppress = false;
      term.startSpinner("restored");
      term.stopSpinner();
    });
    expect(writes.some((s) => s.includes("restored"))).toBe(true);
  });

  it("simulates ask() scenario: liveRefresh startSpinner calls are suppressed while waiting for input", () => {
    // Regression: SpinnerTracker keeps a liveRefresh interval running for the duration
    // of each tool call. While ask() waits for the user to type a secret, that interval
    // fires every 1s calling startSpinner() → _render() → \r\x1b[K + spinner text,
    // which wipes the question prompt. Setting suppress = true before rl.question()
    // (and false in the callback) prevents any _render() from reaching stdout.
    const term = new Terminal(fakeRl());

    // Simulate ask(): stop spinner, set suppress = true, then show prompt
    term.stopSpinner();
    term.suppress = true;

    // Simulate liveRefresh firing → refreshSpinner() → startSpinner() while suppressed
    const writes = captureStdout(() => {
      term.startSpinner("→ chatGemini  gemini-2.0-flash");
    });

    expect(writes.some((s) => s.includes("chatGemini"))).toBe(false);

    // Simulate ask() callback: user typed the key, suppress restored
    term.suppress = false;
    term.stopSpinner();
  });
});
