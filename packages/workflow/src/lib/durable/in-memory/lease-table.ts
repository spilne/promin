// ---------------------------------------------------------------------------
// InMemoryLeaseTable — run locks with monotonic fence tokens.
//
// Every method is synchronous: a caller checks a fence and writes with no
// `await` in between, so the check and the write are one step on the event
// loop.
// ---------------------------------------------------------------------------

import { FenceTokenMismatchError } from "../durable-pipeline-error.ts";
import type { FenceGuard, FenceToken } from "../storage/fencing.ts";
import type { WallClock } from "../../shared/wall-clock.ts";

interface Lease {
  readonly expiresAt: number;
  readonly lockedBy: string;
  readonly token: FenceToken;
}

export class InMemoryLeaseTable {
  private readonly leases = new Map<string, Lease>();
  /**
   * Monotonic fence-token counter. Each successful acquire bumps it so the
   * token a new holder gets is strictly greater than any prior one, even
   * when a stale holder's entry was already cleared.
   */
  private nextFenceToken = 1;

  constructor(
    private readonly config: {
      /** Owner stamped on every lease this table hands out. */
      readonly instanceId: string;
      readonly clock: WallClock;
    },
  ) {}

  /** Acquire the lease when it is free or expired. */
  tryAcquire(params: { workflowId: string; lockDurationMs: number }): {
    acquired: boolean;
    token?: FenceToken;
  } {
    const lease = this.leases.get(params.workflowId);
    const now = this.config.clock.currentTimeMs();
    if (lease !== undefined && lease.expiresAt > now) return { acquired: false };
    const token = String(this.nextFenceToken++);
    this.leases.set(params.workflowId, {
      expiresAt: now + params.lockDurationMs,
      lockedBy: this.config.instanceId,
      token,
    });
    return { acquired: true, token };
  }

  /**
   * Release the lease. With a token only its holder releases (a stale token
   * no-ops); without one, only a lease this instance took.
   */
  release(params: { workflowId: string; guard?: FenceGuard }): void {
    const lease = this.leases.get(params.workflowId);
    if (!lease) return;
    if (params.guard?.fenceToken) {
      if (lease.token !== params.guard.fenceToken) return;
    } else if (lease.lockedBy !== this.config.instanceId) {
      return;
    }
    this.leases.delete(params.workflowId);
  }

  /**
   * Extend the lease. A token holder whose lease is gone or re-taken learns
   * it lost the run (`checkFence` rejects); without a token, a lease this
   * instance doesn't hold is left alone.
   */
  extend(params: { workflowId: string; lockDurationMs: number; guard?: FenceGuard }): void {
    const { workflowId, guard } = params;
    this.checkFence({ workflowId, guard });
    const lease = this.leases.get(workflowId);
    if (!lease) return;
    if (!guard?.fenceToken && lease.lockedBy !== this.config.instanceId) return;
    this.leases.set(workflowId, {
      expiresAt: this.config.clock.currentTimeMs() + params.lockDurationMs,
      lockedBy: lease.lockedBy,
      token: lease.token,
    });
  }

  /** True while an unexpired lease is held at `nowMs`. */
  isHeld(params: { workflowId: string; nowMs: number }): boolean {
    const lease = this.leases.get(params.workflowId);
    return lease !== undefined && lease.expiresAt > params.nowMs;
  }

  /**
   * Reject a fenced write unless `guard.fenceToken` is the workflow's
   * current, unexpired lease token. Without a token the write is unfenced.
   */
  checkFence(params: { workflowId: string; guard?: FenceGuard }): void {
    const { workflowId, guard } = params;
    if (!guard?.fenceToken) return;
    const lease = this.leases.get(workflowId);
    // No lease at all — the new holder already released, or never held.
    // Either way, the stale write must be rejected.
    if (!lease) {
      throw new FenceTokenMismatchError({
        workflowId,
        expected: "(no lock)",
        provided: guard.fenceToken,
        message: `Fenced write for "${workflowId}" rejected — no active lock`,
      });
    }
    if (lease.token !== guard.fenceToken) {
      throw new FenceTokenMismatchError({
        workflowId,
        expected: lease.token,
        provided: guard.fenceToken,
        message: `Fenced write for "${workflowId}" rejected — token mismatch (expected "${lease.token}", got "${guard.fenceToken}")`,
      });
    }
    if (lease.expiresAt <= this.config.clock.currentTimeMs()) {
      throw new FenceTokenMismatchError({
        workflowId,
        expected: "(expired)",
        provided: guard.fenceToken,
        message: `Fenced write for "${workflowId}" rejected — the lock for token "${guard.fenceToken}" expired`,
      });
    }
  }

  /** Drop a workflow's lease (purge). */
  forget(workflowId: string): void {
    this.leases.delete(workflowId);
  }

  clear(): void {
    this.leases.clear();
  }
}
