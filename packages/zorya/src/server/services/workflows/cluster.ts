// ---------------------------------------------------------------------------
// Running several Zorya instances against the same stores.
//
// Every background loop that must run once per deployment — sleep and
// signal wake-ups, orphan recovery, advertisement pruning — takes its
// leadership from a `ZoryaCluster`: a shared `LeaderLeaseStore` plus this
// instance's id. Without one, the process is the only instance and every
// loop runs here.
//
// A clustered service only works when every store it uses is shared, so
// `assertSharedStores` refuses in-memory stores up front instead of letting
// instances silently disagree (a start enqueued on one instance never
// reaching a worker attached to another).
// ---------------------------------------------------------------------------

import { InMemoryWorkflowStorage, InMemoryWorkflowVersionRegistry } from "@promin/workflow";
import {
  InMemoryWorkerRegistry,
  InMemoryWorkflowAdvertisementRegistry,
  InMemoryWorkflowStartQueue,
  SingleLeader,
  type LeaderElection,
} from "@promin/workflow/distributed";
import {
  InMemoryLeaderLeases,
  InMemorySchedulerStorage,
  LeaseLeaderElection,
  type LeaderLeaseStore,
} from "@promin/workflow/scheduler";

/** What a Zorya instance needs to share background work with its peers. */
export interface ZoryaCluster {
  /** This instance's id. Leader leases are held under it; unique per instance. */
  readonly instanceId: string;
  /**
   * The leader-lease store every instance shares (`PgLeaderLeaseStore`,
   * `RedisLeaderLeaseStore`, `SqliteLeaderLeaseStore` on one host).
   */
  readonly leases: LeaderLeaseStore;
}

/** Thrown when a clustered service is configured with a store other instances can't see. */
export class ZoryaClusterConfigError extends Error {
  readonly _tag = "ZoryaClusterConfigError";
  constructor(message: string) {
    super(message);
    this.name = "ZoryaClusterConfigError";
  }
}

/** Leader election for one background loop: a lease in the cluster, else always this process. */
export function leaderElectionFor(params: {
  readonly cluster: ZoryaCluster | undefined;
  readonly key: string;
  readonly intervalMs: number;
}): LeaderElection {
  const { cluster, key, intervalMs } = params;
  if (!cluster) return new SingleLeader();
  return new LeaseLeaderElection({
    store: cluster.leases,
    key: `zorya/${key}`,
    instanceId: cluster.instanceId,
    // Re-acquired every iteration: outlive a few slow iterations so leadership
    // doesn't flap, yet move to a peer soon after this instance dies.
    ttlMs: Math.max(5_000, intervalMs * 3),
  });
}

const IN_MEMORY_STORES: ReadonlyArray<
  readonly [string, abstract new (...args: never[]) => unknown]
> = [
  ["InMemoryWorkflowStorage", InMemoryWorkflowStorage],
  ["InMemoryWorkflowStartQueue", InMemoryWorkflowStartQueue],
  ["InMemoryWorkflowAdvertisementRegistry", InMemoryWorkflowAdvertisementRegistry],
  ["InMemoryWorkerRegistry", InMemoryWorkerRegistry],
  ["InMemoryWorkflowVersionRegistry", InMemoryWorkflowVersionRegistry],
  ["InMemoryLeaderLeases", InMemoryLeaderLeases],
  ["InMemorySchedulerStorage", InMemorySchedulerStorage],
];

/**
 * Throw `ZoryaClusterConfigError` when any of `stores` is an in-memory
 * store: with several instances each would hold its own copy.
 */
export function assertSharedStores(params: {
  readonly owner: string;
  readonly stores: Readonly<Record<string, unknown>>;
}): void {
  const local: string[] = [];
  for (const [field, store] of Object.entries(params.stores)) {
    if (store === undefined) continue;
    for (const [name, cls] of IN_MEMORY_STORES) {
      if (store instanceof cls) local.push(`${field} (${name})`);
    }
  }
  if (local.length === 0) return;
  throw new ZoryaClusterConfigError(
    `${params.owner}: \`cluster\` is set, but these stores are in-memory and would not be ` +
      `shared between instances: ${local.join(", ")}. Pass shared stores (Postgres, Redis, ` +
      `or SQLite for instances on one host) or drop \`cluster\` for a single instance.`,
  );
}
