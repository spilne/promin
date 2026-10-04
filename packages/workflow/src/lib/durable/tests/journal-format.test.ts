import { describe, it, expect } from "bun:test";
import { branchPrefix, pathInBranch } from "../journal-format.ts";

describe("branch-path grammar", () => {
  it("one '/branch.seq' segment per enclosing parallel", () => {
    const outer1 = branchPrefix({ parallelPath: "", branch: 1 });
    expect(outer1).toBe("/1");
    expect(pathInBranch({ prefix: outer1, seq: 0 })).toBe("/1.0");
    const nestedAt = pathInBranch({ prefix: outer1, seq: 1 });
    expect(nestedAt).toBe("/1.1");
    const inner0 = branchPrefix({ parallelPath: nestedAt, branch: 0 });
    expect(pathInBranch({ prefix: inner0, seq: 0 })).toBe("/1.1/0.0");
  });

  it("a nested parallel never collides with a sibling yield", () => {
    // Every (branch, seq) path reachable through three nesting levels is
    // generated and must be unique.
    const seen = new Set<string>();
    const walk = (parallelPath: string, depth: number): void => {
      for (let branch = 0; branch < 3; branch++) {
        const prefix = branchPrefix({ parallelPath, branch });
        for (let seq = 0; seq < 3; seq++) {
          const path = pathInBranch({ prefix, seq });
          expect(seen.has(path)).toBe(false);
          seen.add(path);
          if (depth < 2) walk(path, depth + 1);
        }
      }
    };
    walk("", 0);
    expect(seen.size).toBe(9 + 81 + 729);
  });
});
