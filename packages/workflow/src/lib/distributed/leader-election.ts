// ---------------------------------------------------------------------------
// LeaderElection — ensures only one coordinator (or scanner) acts at a time
// ---------------------------------------------------------------------------

import type { LeaderLease } from "../scheduler/leader-lease.ts";

export interface LeaderElection {
  /**
   * Try to become (or stay) the leader. Returns true if this instance is
   * the leader now. Called once per loop iteration, so a lease-based
   * election refreshes its lease here.
   */
  tryAcquire(): Promise<boolean>;
  /** Release leadership. */
  release(): Promise<void>;
  /**
   * The lease behind the current leadership, for fencing writes made under
   * it (`LeaseLeaderElection` provides one). Elections without a fencing
   * token leave it undefined, and those writes go unfenced.
   */
  readonly lease?: LeaderLease | null;
}

/**
 * Single-instance leader — always wins. For single-coordinator setups and
 * testing. Writes made under it are not fenced; use `LeaseLeaderElection`
 * when more than one instance runs.
 */
export class SingleLeader implements LeaderElection {
  async tryAcquire(): Promise<boolean> {
    return true;
  }
  async release(): Promise<void> {}
}

/**
 * Lease key for the distributed runner's sweep in a namespace, for
 * `LeaseLeaderElection`. Scanners use their own keys
 * (`scannerLeaderKey`) so each loop has its own leader.
 */
export function coordinatorLeaderKey(params: { namespace?: string } = {}): string {
  return `coordinator/${leaderKeyNamespace(params.namespace)}`;
}

/** Lease key for a sleep or signal scanner in a namespace. */
export function scannerLeaderKey(params: {
  scanner: "sleep" | "signal";
  namespace?: string;
}): string {
  return `scanner/${params.scanner}/${leaderKeyNamespace(params.namespace)}`;
}

function leaderKeyNamespace(namespace: string | undefined): string {
  return namespace === undefined ? "-" : `ns:${encodeURIComponent(namespace)}`;
}
