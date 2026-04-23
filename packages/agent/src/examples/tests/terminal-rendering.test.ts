import { describe, it, expect } from "bun:test";
import type { Interface } from "node:readline";
import { Terminal } from "../terminal.ts";

// Visible spinner line = `${frame}(1) ${label_visible} (${elapsed}s)` ≈ label_visible + 9
// chars when elapsed = "0.0" (t ≈ 0). At cols=80: label ≥ 72 → 2 lines, label ≥ 152 → 3 lines.

function fakeRl(): Interface {
  return {
    rawListeners: () => [],
    removeListener: () => {},
    on: () => {},
  } as unknown as Interface;
}

function captureStdout(fn: () => void): string {
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
  return chunks.join("");
}

function withColumns(cols: number, fn: () => void): void {
  const orig = (process.stdout as any).columns;
  (process.stdout as any).columns = cols;
  try {
    fn();
  } finally {
    (process.stdout as any).columns = orig;
  }
}

describe("Terminal — multi-line spinner erase", () => {
  it("single-line label: no cursor-up on stopSpinner", () => {
    withColumns(80, () => {
      const term = new Terminal(fakeRl());
      // visible = 9 + 10 = 19 → 1 line
      const out = captureStdout(() => {
        term.startSpinner("short-lbl!");
        term.stopSpinner();
      });
      expect(out).not.toContain("\x1b[1A");
    });
  });

  it("2-line label: stopSpinner emits one cursor-up", () => {
    withColumns(80, () => {
      const term = new Terminal(fakeRl());
      // visible = 9 + 72 = 81 → ceil(81/80) = 2 lines
      const out = captureStdout(() => {
        term.startSpinner("A".repeat(72));
        term.stopSpinner();
      });
      expect(out).toContain("\x1b[1A");
    });
  });

  it("3-line label: stopSpinner emits two cursor-ups", () => {
    withColumns(80, () => {
      const term = new Terminal(fakeRl());
      // visible = 9 + 152 = 161 → ceil(161/80) = 3 lines
      const out = captureStdout(() => {
        term.startSpinner("A".repeat(152));
        term.stopSpinner();
      });
      expect(out).toContain("\x1b[2A");
    });
  });

  it("uses \\x1b[J (erase to end of screen) not \\x1b[K (erase to end of line)", () => {
    withColumns(80, () => {
      const term = new Terminal(fakeRl());
      const out = captureStdout(() => {
        term.startSpinner("x");
        term.stopSpinner();
      });
      expect(out).toContain("\x1b[J");
    });
  });

  it("ANSI codes in label are stripped before computing line count", () => {
    withColumns(80, () => {
      const term = new Terminal(fakeRl());

      // Label: 15 × \x1b[2m (60 raw bytes) + 10 visible chars.
      // Without stripping: raw line ≈ 87 chars → ceil(87/80) = 2 → would emit \x1b[1A on erase.
      // With _visibleLen stripping: visible line = 19 chars → 1 line → no cursor-up.
      const label = `${"\x1b[2m".repeat(15)}${"A".repeat(10)}`;
      const out = captureStdout(() => {
        term.startSpinner(label);
        term.stopSpinner();
      });
      expect(out).not.toContain("\x1b[1A");
    });
  });

  it("relabel to shorter text moves up to erase previously wrapped lines", () => {
    withColumns(80, () => {
      const term = new Terminal(fakeRl());

      // First render: 2-line label — sets _renderedLines = 2.
      captureStdout(() => term.startSpinner("A".repeat(72)));

      // Relabel with a short label while spinning — _render sees _renderedLines = 2
      // and must emit \x1b[1A to clear the stale second line before writing.
      const out = captureStdout(() => term.startSpinner("short"));
      expect(out).toContain("\x1b[1A");

      captureStdout(() => term.stopSpinner());
    });
  });

  it("cursor-up count matches rendered-line count on erase", () => {
    withColumns(80, () => {
      const term = new Terminal(fakeRl());
      // 3 lines → expect \x1b[2A, not \x1b[1A or \x1b[3A
      const out = captureStdout(() => {
        term.startSpinner("A".repeat(152));
        term.stopSpinner();
      });
      expect(out).toContain("\x1b[2A");
      expect(out).not.toContain("\x1b[3A");
      // \x1b[1A should NOT appear independently (only [2A which encompasses it lexically)
      // Check by splitting on [2A and verifying [1A isn't in either fragment
      const [before, after] = out.split("\x1b[2A");
      expect(before).not.toContain("\x1b[1A");
      expect(after ?? "").not.toContain("\x1b[1A");
    });
  });
});

describe("Terminal — wide-character column width", () => {
  // CJK: "字".length === 1 (single UTF-16 unit) but occupies 2 terminal columns.
  // Without _charWidth, _visibleLen counts 1 per char → wrong line count.
  // With _charWidth, each CJK char counts as 2 → correct line count.

  it("CJK characters count as 2 columns — spinner wraps to 2 lines", () => {
    withColumns(80, () => {
      const term = new Terminal(fakeRl());
      // 36 CJK × 2 cols = 72 visible cols; total = 9 + 72 = 81 → ceil(81/80) = 2 lines.
      // Without fix: _visibleLen = 9 + 36 = 45 → 1 line → no cursor-up (wrong).
      const out = captureStdout(() => {
        term.startSpinner("字".repeat(36));
        term.stopSpinner();
      });
      expect(out).toContain("\x1b[1A");
    });
  });

  it("fullwidth Latin counts as 2 columns", () => {
    withColumns(80, () => {
      const term = new Terminal(fakeRl());
      // "Ａ" U+FF21 fullwidth Latin A: .length === 1, visual === 2.
      // 36 fullwidth chars = 72 cols; total = 81 → 2 lines.
      const out = captureStdout(() => {
        term.startSpinner("Ａ".repeat(36));
        term.stopSpinner();
      });
      expect(out).toContain("\x1b[1A");
    });
  });

  it("plain ASCII is unaffected by the refactor", () => {
    withColumns(80, () => {
      const term = new Terminal(fakeRl());
      // 10 ASCII chars → visible = 19 → 1 line → no cursor-up.
      const out = captureStdout(() => {
        term.startSpinner("A".repeat(10));
        term.stopSpinner();
      });
      expect(out).not.toContain("\x1b[1A");
    });
  });

  it("mixed ASCII + CJK computes correct column total", () => {
    withColumns(80, () => {
      const term = new Terminal(fakeRl());
      // 20 ASCII (20 cols) + 26 CJK (52 cols) = 72 cols; total = 81 → 2 lines.
      // Without fix: 20 + 26 + 9 = 55 → 1 line (wrong).
      const out = captureStdout(() => {
        term.startSpinner("A".repeat(20) + "字".repeat(26));
        term.stopSpinner();
      });
      expect(out).toContain("\x1b[1A");
    });
  });
});
