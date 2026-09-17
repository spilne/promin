/**
 * Coordination point for `rotatingLLM` slot exhaustion. When one process
 * trips a 429 (or sees `remainingTokens` near zero), it marks the slot
 * exhausted with the provider-supplied reset timestamp; every other
 * process picks up the cooldown the next time it consults the store and
 * skips that slot until the window resets.
 *
 * Only the cooldown window is shared. Per-slot remaining counts stay in
 * each process — they're best-effort load-distribution hints, and
 * cross-process correctness (no 429 storm after the first hit) comes from
 * the shared cooldown alone. Cheap path: read once per routing decision.
 * Write path: only on 429 / near-exhaustion (rare).
 *
 * Default implementation: `InMemoryCapacityStore`. Plug in a Redis- or
 * Postgres-backed store for multi-instance deployments.
 */
export interface CapacityStore {
  /**
   * Mark `slotId` as exhausted until `resetsAt` (unix-ms). Subsequent
   * reads include this slot in the returned map until the timestamp
   * passes. Idempotent — re-marking the same slot updates the reset
   * timestamp; the higher / later value wins so a fresh 429 with a
   * longer reset extends the cooldown rather than shrinking it.
   */
  markExhausted(slotId: string, resetsAt: number): Promise<void>;

  /**
   * Snapshot of currently-exhausted slots. The implementation may filter
   * out entries whose `resetsAt` is in the past. Callers MUST also
   * tolerate stale entries — clock skew across hosts can leave a slot
   * "exhausted" briefly past its reset; the rotatingLLM will simply skip
   * it until the next read.
   */
  getExhausted(): Promise<Map<string, number>>;
}

/**
 * Single-process `CapacityStore` for development and single-pod
 * deployments. Auto-evicts entries whose `resetsAt` has passed at read
 * time so the map doesn't grow unbounded.
 */
export class InMemoryCapacityStore implements CapacityStore {
  private readonly cooldowns = new Map<string, number>();

  async markExhausted(slotId: string, resetsAt: number): Promise<void> {
    const existing = this.cooldowns.get(slotId);
    if (existing === undefined || resetsAt > existing) {
      this.cooldowns.set(slotId, resetsAt);
    }
  }

  async getExhausted(): Promise<Map<string, number>> {
    const now = Date.now();
    const out = new Map<string, number>();
    for (const [id, resetsAt] of this.cooldowns) {
      if (resetsAt > now) {
        out.set(id, resetsAt);
      } else {
        this.cooldowns.delete(id);
      }
    }
    return out;
  }
}
