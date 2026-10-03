// ---------------------------------------------------------------------------
// Property-based assertions for DataFrame operations
// ---------------------------------------------------------------------------

import type { DataFrame } from "../dataframe/dataframe.ts";
import type { Stream } from "@spilne/perfect-core";

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

// ---------------------------------------------------------------------------
// Stream property assertions (perfect `Stream`)
// ---------------------------------------------------------------------------

export const streamProperties = {
  /** filter should never add items. */
  async filterReducesOrMaintains<T>(
    items: T[],
    predicate: (r: T) => boolean,
    createStream: (items: T[]) => Stream<T>,
  ): Promise<boolean> {
    const result = await createStream(items).filter(predicate).toArray().run();
    return result.length <= items.length;
  },

  /** take(n) should return at most n items. */
  async takeBounded<T>(
    items: T[],
    n: number,
    createStream: (items: T[]) => Stream<T>,
  ): Promise<boolean> {
    const result = await createStream(items).take(n).toArray().run();
    return result.length <= n;
  },

  /** map should preserve item count. */
  async mapPreservesCount<T, U>(
    items: T[],
    fn: (r: T) => U,
    createStream: (items: T[]) => Stream<T>,
  ): Promise<boolean> {
    const result = await createStream(items).map(fn).toArray().run();
    return result.length === items.length;
  },

  /** dedupe should reduce or maintain count. */
  async dedupeReducesOrMaintains<T>(
    items: T[],
    createStream: (items: T[]) => Stream<T>,
  ): Promise<boolean> {
    const result = await createStream(items).dedupe().toArray().run();
    return result.length <= items.length;
  },

  /** collect should return all items from the source. */
  async collectMatchesSource<T>(
    items: T[],
    createStream: (items: T[]) => Stream<T>,
  ): Promise<boolean> {
    const result = await createStream(items).toArray().run();
    return result.length === items.length;
  },
};
