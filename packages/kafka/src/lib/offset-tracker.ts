// ---------------------------------------------------------------------------
// OffsetTracker — tracks completed offsets per partition for safe parallel commit
//
// Problem: if you process messages in parallel and commit out of order,
// a crash can lose uncommitted messages with lower offsets.
//
// Solution: track completion per offset, only commit the highest contiguous
// offset (the "high-water mark"). Messages after a gap are held until
// the gap is filled.
//
// Example:
//   Completed: [1, _, 3, 4, _]  → committable: 2 (only offset 1 is contiguous)
//   Completed: [1, 2, 3, 4, _]  → committable: 5 (offsets 1-4 are contiguous)
//   Completed: [1, 2, 3, 4, 5]  → committable: 6 (all done)
// ---------------------------------------------------------------------------

export class OffsetTracker {
  // partition → set of completed offsets
  private completed = new Map<number, Set<number>>();
  // partition → lowest uncommitted offset (the frontier)
  private frontier = new Map<number, number>();

  /**
   * Record that a message at the given partition + offset was processed.
   */
  /**
   * Set the starting frontier for a partition.
   * Call this when the consumer starts from a known offset (e.g., after commit recovery).
   */
  setFrontier(partition: number, offset: number): void {
    this.frontier.set(partition, offset);
  }

  complete(partition: number, offset: number): void {
    if (!this.completed.has(partition)) {
      this.completed.set(partition, new Set());
    }
    this.completed.get(partition)!.add(offset);

    // Initialize frontier at 0 for new partitions — Kafka offsets start at 0
    if (!this.frontier.has(partition)) {
      this.frontier.set(partition, 0);
    }
  }

  /**
   * Get the highest contiguous offset that is safe to commit per partition.
   * Returns null for a partition if nothing new is committable.
   *
   * After calling this, the returned offsets are "consumed" — calling again
   * without new completions returns empty.
   */
  committable(): Map<number, number> {
    const result = new Map<number, number>();

    for (const [partition, completedSet] of this.completed) {
      let cursor = this.frontier.get(partition) ?? 0;
      let advanced = false;

      while (completedSet.has(cursor)) {
        completedSet.delete(cursor);
        cursor++;
        advanced = true;
      }

      if (advanced) {
        // Commit offset is cursor (next offset to read = one past the last completed)
        result.set(partition, cursor);
        this.frontier.set(partition, cursor);
      }
    }

    return result;
  }

  /**
   * Get pending count — how many completed offsets are waiting behind a gap.
   * Useful for monitoring: if this grows, something is stuck.
   */
  pendingCount(): number {
    let total = 0;
    for (const completedSet of this.completed.values()) {
      total += completedSet.size;
    }
    return total;
  }
}
