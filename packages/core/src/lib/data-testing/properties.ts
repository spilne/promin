// ---------------------------------------------------------------------------
// Property-based assertions for DataFrame operations
// ---------------------------------------------------------------------------

import type { DataFrame } from "../dataframe/dataframe.ts";

export const dataframeProperties = {
  /** filter should never increase row count. */
  async filterReducesOrMaintains<T>(
    df: DataFrame<T>,
    predicate: (r: T) => boolean,
  ): Promise<boolean> {
    const originalCount = await df.count();
    const filteredCount = await df.filter(predicate).count();
    return filteredCount <= originalCount;
  },

  /** sort should produce same row count. */
  async sortPreservesCount<T>(df: DataFrame<T>, col: keyof T & string): Promise<boolean> {
    const originalCount = await df.count();
    const sortedCount = await df.sort(col).count();
    return sortedCount === originalCount;
  },

  /** distinct should produce fewer or equal rows. */
  async distinctReducesOrMaintains<T>(df: DataFrame<T>): Promise<boolean> {
    const originalCount = await df.count();
    const distinctCount = await df.distinct().count();
    return distinctCount <= originalCount;
  },

  /** limit should return at most N rows. */
  async limitBounded<T>(df: DataFrame<T>, n: number): Promise<boolean> {
    const count = await df.limit(n).count();
    return count <= n;
  },

  /** select should not change row count. */
  async selectPreservesCount<T>(df: DataFrame<T>, cols: (keyof T & string)[]): Promise<boolean> {
    const originalCount = await df.count();
    const selectedCount = await df.select(...cols).count();
    return selectedCount === originalCount;
  },
};
