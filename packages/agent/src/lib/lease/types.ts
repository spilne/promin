// ---------------------------------------------------------------------------
// Per-thread lease — coordination primitive for horizontally-scaled agent
// runtimes. When N replicas share one MemoryStore (via PgMemoryStore),
// two inbound messages on the same thread can land on different workers
// and run in parallel, racing on every layer: message append order,
// memory writes, auto-compact / auto-distill triggers, working-memory
// upserts.
//
// Before running a turn, the agent runtime claims a lease on the
// (namespace, thread) tuple. One owner at a time. Two policies on top
// of this primitive (see AgentTurnGate):
//
//   strict — try-acquire; held → reject (HTTP 409 with current owner).
//            Matches OpenAI Assistants semantics. Implemented in Phase 1.
//   queued — wait or async-dispatch until free. Tracked separately and
//            returns NotImplementedError today.
//
// The lease is purely advisory — the underlying tables don't reject
// writes from non-leaseholders. Discipline is enforced at the agent-
// action layer (turn = "claim → run → release"). Storage stays simple
// and writes from misbehaving callers don't corrupt structure, only
// linearization.
//
// Implementations:
//   - InMemoryLeaseStore — single-process tests + dev
//   - PgLeaseStore       — production, colocated with PgMemoryStore
//
// Both must pass `leaseStoreTestSuite`.
// ---------------------------------------------------------------------------

/**
 * Identifies which thread to lease. Same shape as `ThreadKey` in the
 * memory store except `resourceId` is dropped — leases are scoped to
 * `(namespaceId, threadId)`, matching how `agent_thread` is keyed.
 */
export interface ThreadLeaseKey {
  readonly namespaceId: string;
  readonly threadId: string;
}

/**
 * A live lease on a thread. `leaseId` is the unique handle returned to
 * the holder; `release()` and `extend()` require it. `expiresAt` is
 * when the lease becomes claimable again by anyone.
 */
export interface ThreadLease {
  readonly key: ThreadLeaseKey;
  /** Worker / process / actor that holds the lease. */
  readonly ownerId: string;
  /** Unique handle for release / extend. Random per acquire(). */
  readonly leaseId: string;
  /** Millisecond unix epoch — when the lease was first granted. */
  readonly acquiredAt: number;
  /** Millisecond unix epoch — when the lease expires absent extension. */
  readonly expiresAt: number;
}

/**
 * Outcome of `acquire()`. `acquired: true` means the caller now holds
 * the lease and must `release()` it. `acquired: false` means another
 * owner held a non-expired lease; the caller MUST NOT proceed.
 */
export type AcquireResult =
  | { readonly acquired: true; readonly lease: ThreadLease }
  | { readonly acquired: false; readonly currentLease: ThreadLease };

/**
 * Outcome of `extend()`. Extension fails when the caller's lease has
 * already expired (so a different owner may have claimed) or the
 * lease was never registered. The store reports the current state so
 * the caller can react (give up, retry the whole turn, etc.).
 */
export type ExtendResult =
  | { readonly extended: true; readonly lease: ThreadLease }
  | { readonly extended: false; readonly currentLease: ThreadLease | null };

export interface LeaseStore {
  /**
   * Try to claim a lease on `key` for `ownerId`. Atomically:
   *   - if no row exists, create one with the requested TTL
   *   - if a row exists and has expired (expiresAt <= now), steal it
   *   - if a row exists and has not expired, leave it alone
   *
   * Returns `acquired: true` only on the first or stolen path. Stealing
   * an expired lease is intentional — a worker that crashed mid-turn
   * loses its lease after TTL and the next worker resumes the thread
   * from the journal (caller is responsible for journal-driven replay).
   */
  acquire(params: {
    readonly key: ThreadLeaseKey;
    readonly ownerId: string;
    /** TTL from now, in milliseconds. Recommended: 5 minutes default. */
    readonly ttlMs: number;
  }): Promise<AcquireResult>;

  /**
   * Extend a lease the caller already holds. The store verifies
   * `leaseId` matches the current lease AND the current lease has
   * not expired; on success, `expiresAt` advances by `additionalMs`.
   */
  extend(params: {
    readonly leaseId: string;
    readonly additionalMs: number;
  }): Promise<ExtendResult>;

  /**
   * Release a lease the caller holds. No-op when `leaseId` does not
   * match the current lease (already released, expired and stolen,
   * or never existed).
   */
  release(params: { readonly leaseId: string }): Promise<void>;

  /**
   * Inspect the current lease for a key (for tests + observability).
   * Returns `null` when no row exists. Returns the row even when
   * expired — callers that care about liveness must check `expiresAt`.
   */
  get(key: ThreadLeaseKey): Promise<ThreadLease | null>;
}
