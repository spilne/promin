import { describe, it, expect } from "bun:test";
import {
  JOURNAL_FORMAT_CURRENT,
  JOURNAL_FORMAT_LEGACY,
  branchPrefix,
  detectJournalFormat,
  pathInBranch,
  resolveUndecidedJournalFormat,
} from "../journal-format.ts";

const paths = (...branchPaths: string[]) =>
  branchPaths.map((branchPath, activityIndex) => ({ branchPath, activityIndex }));

describe("detectJournalFormat", () => {
  it("an empty journal is written in the current format", () => {
    expect(detectJournalFormat([])).toBe(JOURNAL_FORMAT_CURRENT);
  });

  it("a slash-prefixed path marks the current format", () => {
    expect(detectJournalFormat(paths("", "/0.0", "/1.0/0.0"))).toBe(JOURNAL_FORMAT_CURRENT);
  });

  it("a dotted path without the leading slash marks the legacy format", () => {
    expect(detectJournalFormat(paths("", "0", "1.1"))).toBe(JOURNAL_FORMAT_LEGACY);
  });

  it("only top-level entries leave the format undecided", () => {
    expect(detectJournalFormat(paths("", ""))).toBeUndefined();
  });

  it("a journal mixing both grammars is rejected", () => {
    expect(() => detectJournalFormat(paths("0", "/0.0"))).toThrow(/mixes branch-path formats/);
  });
});

describe("resolveUndecidedJournalFormat", () => {
  it("entries at or past the parallel's slot mean a legacy run got past it", () => {
    const journal = [{ activityIndex: 0 }, { activityIndex: 2 }];
    expect(resolveUndecidedJournalFormat({ journal, parallelIndex: 1 })).toBe(
      JOURNAL_FORMAT_LEGACY,
    );
  });

  it("no entry at or past the parallel's slot settles on the current format", () => {
    const journal = [{ activityIndex: 0 }, { activityIndex: 1 }];
    expect(resolveUndecidedJournalFormat({ journal, parallelIndex: 2 })).toBe(
      JOURNAL_FORMAT_CURRENT,
    );
  });
});

describe("branch-path grammar", () => {
  it("current format: one '/branch.seq' segment per enclosing parallel", () => {
    const format = JOURNAL_FORMAT_CURRENT;
    const outer1 = branchPrefix({ format, parallelPath: "", branch: 1 });
    expect(outer1).toBe("/1");
    expect(pathInBranch({ format, prefix: outer1, seq: 0 })).toBe("/1.0");
    const nestedAt = pathInBranch({ format, prefix: outer1, seq: 1 });
    expect(nestedAt).toBe("/1.1");
    const inner0 = branchPrefix({ format, parallelPath: nestedAt, branch: 0 });
    expect(pathInBranch({ format, prefix: inner0, seq: 0 })).toBe("/1.1/0.0");
  });

  it("current format: a nested parallel never collides with a sibling yield", () => {
    // Every (branch, seq) path reachable through three nesting levels is
    // generated and must be unique.
    const format = JOURNAL_FORMAT_CURRENT;
    const seen = new Set<string>();
    const walk = (parallelPath: string, depth: number): void => {
      for (let branch = 0; branch < 3; branch++) {
        const prefix = branchPrefix({ format, parallelPath, branch });
        for (let seq = 0; seq < 3; seq++) {
          const path = pathInBranch({ format, prefix, seq });
          expect(seen.has(path)).toBe(false);
          seen.add(path);
          if (depth < 2) walk(path, depth + 1);
        }
      }
    };
    walk("", 0);
    expect(seen.size).toBe(9 + 81 + 729);
  });

  it("legacy format reproduces the old paths verbatim", () => {
    const format = JOURNAL_FORMAT_LEGACY;
    const b0 = branchPrefix({ format, parallelPath: "", branch: 0 });
    expect(pathInBranch({ format, prefix: b0, seq: 0 })).toBe("0");
    expect(pathInBranch({ format, prefix: b0, seq: 1 })).toBe("0.1");
    const nested = branchPrefix({ format, parallelPath: "0", branch: 1 });
    // The collision the current format exists to avoid.
    expect(pathInBranch({ format, prefix: nested, seq: 0 })).toBe("0.1");
  });
});
