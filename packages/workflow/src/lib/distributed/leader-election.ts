// ---------------------------------------------------------------------------
// LeaderElection — ensures only one coordinator runs at a time
// ---------------------------------------------------------------------------

export interface LeaderElection {
  /** Try to become the leader. Returns true if this instance is now the leader. */
  tryAcquire(): Promise<boolean>;
  /** Release leadership. */
  release(): Promise<void>;
}

/**
 * Single-instance leader — always wins. For single-coordinator setups and testing.
 */
export class SingleLeader implements LeaderElection {
  async tryAcquire(): Promise<boolean> {
    return true;
  }
  async release(): Promise<void> {}
}
