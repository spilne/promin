// ---------------------------------------------------------------------------
// JoinBuffer — time-windowed join buffer for stream-stream joins
//
// Buffers items from two keyed streams. When a match is found within the
// join window, emits the joined pair. Expired items are evicted.
// ---------------------------------------------------------------------------

export interface JoinedPair<L, R> {
  left: L;
  right: R;
}

interface BufferedItem<T> {
  value: T;
  timestamp: number;
}

export class JoinBuffer<L, R> {
  private leftBuffer = new Map<string, BufferedItem<L>[]>();
  private rightBuffer = new Map<string, BufferedItem<R>[]>();

  constructor(private readonly windowMs: number) {}

  /** Add a left item. Returns any matches with buffered right items. */
  addLeft(key: string, value: L, timestamp: number): JoinedPair<L, R>[] {
    this.evict(key, timestamp);

    const entry: BufferedItem<L> = { value, timestamp };
    const existing = this.leftBuffer.get(key) ?? [];
    existing.push(entry);
    this.leftBuffer.set(key, existing);

    // Check for matches in right buffer
    const rightItems = this.rightBuffer.get(key) ?? [];
    return rightItems
      .filter((r) => Math.abs(r.timestamp - timestamp) <= this.windowMs)
      .map((r) => ({ left: value, right: r.value }));
  }

  /** Add a right item. Returns any matches with buffered left items. */
  addRight(key: string, value: R, timestamp: number): JoinedPair<L, R>[] {
    this.evict(key, timestamp);

    const entry: BufferedItem<R> = { value, timestamp };
    const existing = this.rightBuffer.get(key) ?? [];
    existing.push(entry);
    this.rightBuffer.set(key, existing);

    // Check for matches in left buffer
    const leftItems = this.leftBuffer.get(key) ?? [];
    return leftItems
      .filter((l) => Math.abs(l.timestamp - timestamp) <= this.windowMs)
      .map((l) => ({ left: l.value, right: value }));
  }

  /** Evict expired items from both buffers for a given key. */
  private evict(key: string, currentTime: number): void {
    const cutoff = currentTime - this.windowMs;

    const leftItems = this.leftBuffer.get(key);
    if (leftItems) {
      const filtered = leftItems.filter((item) => item.timestamp > cutoff);
      if (filtered.length === 0) this.leftBuffer.delete(key);
      else this.leftBuffer.set(key, filtered);
    }

    const rightItems = this.rightBuffer.get(key);
    if (rightItems) {
      const filtered = rightItems.filter((item) => item.timestamp > cutoff);
      if (filtered.length === 0) this.rightBuffer.delete(key);
      else this.rightBuffer.set(key, filtered);
    }
  }

  /** Current buffer sizes (for monitoring). */
  stats(): { leftKeys: number; rightKeys: number; leftItems: number; rightItems: number } {
    let leftItems = 0;
    for (const items of this.leftBuffer.values()) leftItems += items.length;
    let rightItems = 0;
    for (const items of this.rightBuffer.values()) rightItems += items.length;
    return {
      leftKeys: this.leftBuffer.size,
      rightKeys: this.rightBuffer.size,
      leftItems,
      rightItems,
    };
  }

  /** Clear all buffered state. */
  clear(): void {
    this.leftBuffer.clear();
    this.rightBuffer.clear();
  }

  /** Snapshot current state for checkpointing. */
  snapshot(): {
    left: [string, { value: L; timestamp: number }[]][];
    right: [string, { value: R; timestamp: number }[]][];
  } {
    return {
      left: [...this.leftBuffer.entries()],
      right: [...this.rightBuffer.entries()],
    };
  }

  /** Restore state from a checkpoint. */
  restore(data: {
    left: [string, { value: L; timestamp: number }[]][];
    right: [string, { value: R; timestamp: number }[]][];
  }): void {
    this.leftBuffer.clear();
    this.rightBuffer.clear();
    for (const [key, items] of data.left) {
      this.leftBuffer.set(key, items);
    }
    for (const [key, items] of data.right) {
      this.rightBuffer.set(key, items);
    }
  }
}
