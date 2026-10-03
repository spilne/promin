// ---------------------------------------------------------------------------
// Leader leases — TTL'd, fenced leadership over a string key.
//
// A lease names its key, its holder and an `epoch` fencing token. The epoch
// goes up every time a new lease starts on the key: a different instance
// takes it over, or anyone (the old holder included) takes it again after it
// expired or was released. Refreshing a live lease keeps the epoch. Writers
// that must only happen under leadership carry their lease and the store
// rejects the write, in the same transaction as the write itself, when the
// key's epoch has moved on (`StaleLeaseError`). A leader that paused past
// its TTL can therefore keep running for a while but can no longer commit.
//
// Backends: `InMemoryLeaderLeases` here (also the reference semantics), and
// Postgres / Redis / SQLite stores next to their scheduler storages. Every
// `SchedulerStorage` is a `LeaderLeaseStore`.
// ---------------------------------------------------------------------------

import { SystemWallClock, type WallClock } from "../shared/wall-clock.ts";

/** Leadership of one key, held until it expires or is released. */
export interface LeaderLease {
  /** The lock key, e.g. from `schedulerLeaderKey`. */
  readonly key: string;
  /** The holder's instance id. */
  readonly instanceId: string;
  /**
   * Fencing token. Strictly increases each time a new lease starts on the
   * key; a refresh by the live holder keeps it.
   */
  readonly epoch: number;
}

/** Acquire, refresh and release leader leases. */
export interface LeaderLeaseStore {
  /**
   * Acquire the lease on `key`, or refresh it when `instanceId` already holds
   * it. Returns the lease, or `null` while another instance holds a live
   * lease. The lease expires `ttlMs` after this call (Postgres and Redis
   * measure expiry on the server clock). Acquiring and refreshing are one
   * atomic step, so two callers never both get a live lease.
   */
  tryAcquireLeader(params: {
    key: string;
    instanceId: string;
    ttlMs: number;
  }): Promise<LeaderLease | null>;

  /**
   * Give the lease up so another instance can take over right away instead
   * of waiting out the TTL. A no-op when the lease is no longer current.
   */
  releaseLeader(params: { lease: LeaderLease }): Promise<void>;
}

/** A fenced write was attempted with a lease that is no longer current. */
export class StaleLeaseError extends Error {
  override readonly name = "StaleLeaseError";
  readonly lease: LeaderLease;
  /** The key's current epoch, or `null` when it has no lease record. */
  readonly currentEpoch: number | null;

  constructor(params: { lease: LeaderLease; currentEpoch: number | null }) {
    super(
      `Lease "${params.lease.key}" epoch ${params.lease.epoch} held by "${params.lease.instanceId}" ` +
        `is stale (current epoch: ${params.currentEpoch ?? "none"}); the write was rejected`,
    );
    this.lease = params.lease;
    this.currentEpoch = params.currentEpoch;
  }
}

/** True for a `StaleLeaseError`, including one rebuilt across a module boundary. */
export function isStaleLeaseError(error: unknown): error is StaleLeaseError {
  return (
    error instanceof StaleLeaseError || (error instanceof Error && error.name === "StaleLeaseError")
  );
}

/**
 * Lease key for a scheduler poll loop. Each namespace, and each partition of
 * a partitioned scheduler, gets its own key and so its own leader. The
 * namespace is URI-encoded so no two (namespace, partition) pairs collide.
 */
export function schedulerLeaderKey(params: {
  namespace?: string;
  partition?: { index: number; count: number };
}): string {
  const ns = params.namespace === undefined ? "-" : `ns:${encodeURIComponent(params.namespace)}`;
  const part = params.partition ? `/p${params.partition.index}of${params.partition.count}` : "";
  return `scheduler/${ns}${part}`;
}

interface LeaseRecord {
  /** `null` once released. */
  instanceId: string | null;
  epoch: number;
  expiresAt: number;
}

export interface InMemoryLeaderLeasesConfig {
  /** Time source for lease expiry. Default: `SystemWallClock`. */
  clock?: WallClock;
}

/**
 * In-process `LeaderLeaseStore`. Single-process only: it coordinates the
 * instances that share this object. `assertCurrent` is the fence check, for
 * in-memory stores to run inside their own (synchronous) write.
 */
export class InMemoryLeaderLeases implements LeaderLeaseStore {
  private readonly records = new Map<string, LeaseRecord>();
  private readonly clock: WallClock;

  constructor(config?: InMemoryLeaderLeasesConfig) {
    this.clock = config?.clock ?? SystemWallClock;
  }

  async tryAcquireLeader(params: {
    key: string;
    instanceId: string;
    ttlMs: number;
  }): Promise<LeaderLease | null> {
    return this.acquire(params);
  }

  /** Synchronous `tryAcquireLeader`. */
  acquire(params: { key: string; instanceId: string; ttlMs: number }): LeaderLease | null {
    const now = this.clock.currentTimeMs();
    const expiresAt = now + Math.max(0, params.ttlMs);
    const record = this.records.get(params.key);
    if (!record) {
      this.records.set(params.key, { instanceId: params.instanceId, epoch: 1, expiresAt });
      return { key: params.key, instanceId: params.instanceId, epoch: 1 };
    }
    const live = record.instanceId !== null && record.expiresAt > now;
    if (live && record.instanceId !== params.instanceId) return null;
    if (!live) record.epoch += 1;
    record.instanceId = params.instanceId;
    record.expiresAt = expiresAt;
    return { key: params.key, instanceId: params.instanceId, epoch: record.epoch };
  }

  async releaseLeader(params: { lease: LeaderLease }): Promise<void> {
    const record = this.records.get(params.lease.key);
    if (!record) return;
    if (record.epoch !== params.lease.epoch) return;
    if (record.instanceId !== params.lease.instanceId) return;
    record.instanceId = null;
    record.expiresAt = this.clock.currentTimeMs();
  }

  /** The key's current epoch, or `null` when it has never been leased. */
  currentEpoch(key: string): number | null {
    return this.records.get(key)?.epoch ?? null;
  }

  /** Throw `StaleLeaseError` unless `lease` is the key's current lease. */
  assertCurrent(lease: LeaderLease): void {
    const current = this.currentEpoch(lease.key);
    if (current !== lease.epoch) throw new StaleLeaseError({ lease, currentEpoch: current });
  }
}

/**
 * `LeaderLeaseStore` as a boolean leader election for one key: `tryAcquire`
 * acquires or refreshes, `release` gives the lease up, and `lease` is the
 * lease from the last successful `tryAcquire` (for fencing writes). Has the
 * same shape as the distributed coordinator's `LeaderElection`.
 */
export class LeaseLeaderElection {
  private readonly store: LeaderLeaseStore;
  private readonly key: string;
  private readonly instanceId: string;
  private readonly ttlMs: number;
  private current: LeaderLease | null = null;

  constructor(params: { store: LeaderLeaseStore; key: string; instanceId: string; ttlMs: number }) {
    this.store = params.store;
    this.key = params.key;
    this.instanceId = params.instanceId;
    this.ttlMs = params.ttlMs;
  }

  /** The lease from the last successful `tryAcquire`, or `null`. */
  get lease(): LeaderLease | null {
    return this.current;
  }

  async tryAcquire(): Promise<boolean> {
    this.current = await this.store.tryAcquireLeader({
      key: this.key,
      instanceId: this.instanceId,
      ttlMs: this.ttlMs,
    });
    return this.current !== null;
  }

  async release(): Promise<void> {
    const lease = this.current;
    this.current = null;
    if (lease) await this.store.releaseLeader({ lease });
  }
}
