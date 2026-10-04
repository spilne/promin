// ---------------------------------------------------------------------------
// RedisLeaderLeaseStore — `LeaderLeaseStore` on two keys per lease:
//
//   <prefix>:lease:{<key>}:holder  — STRING, the holder's instance id, PX ttl
//   <prefix>:lease:{<key>}:epoch   — STRING counter, no TTL (fencing token)
//
// The `{<key>}` hash tag keeps a lease's two keys in one cluster slot. A
// prefix that carries a hash tag itself (`{sq}`) wins over it, since Redis
// hashes the first tag in a key: every lease of the store then sits in the
// prefix's slot, next to the keys of the store using that tag, which is
// how a fenced write checks the epoch in its own script.
//
// Acquire, refresh and release are each one Lua script, so the check and
// the write can't interleave with another instance (the old SET NX + GET +
// PEXPIRE could extend a lock someone else had just taken). Expiry is the
// holder key's PX TTL, i.e. the Redis server clock. The epoch counter is
// never expired, so it keeps increasing across expiries and releases.
//
// Fencing: a write is current when `GET <epoch key>` still equals the
// lease's epoch. Check it inside the same script as the write (see
// `RedisSchedulerStorage.commitPoll` and `RedisStepQueue.requeueStuck`).
// ---------------------------------------------------------------------------

import type { LeaderLease, LeaderLeaseStore } from "@promin/workflow/scheduler";
import type { RedisStoreClient } from "./redis-client.ts";

/**
 * KEYS: [holder_key, epoch_key]  ARGV: [instance_id, ttl_ms]
 * Returns the lease epoch, or 0 while another instance holds the lease.
 */
const ACQUIRE_LUA = `
local holder = redis.call('GET', KEYS[1])
if holder == ARGV[1] then
  local epoch = redis.call('GET', KEYS[2])
  if epoch then
    redis.call('PEXPIRE', KEYS[1], ARGV[2])
    return tonumber(epoch)
  end
elseif holder then
  return 0
end
local epoch = redis.call('INCR', KEYS[2])
redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
return epoch
`;

/** KEYS: [holder_key, epoch_key]  ARGV: [instance_id, epoch] */
const RELEASE_LUA = `
if redis.call('GET', KEYS[1]) == ARGV[1] and redis.call('GET', KEYS[2]) == ARGV[2] then
  redis.call('DEL', KEYS[1])
  return 1
end
return 0
`;

export interface RedisLeaderLeaseStoreConfig {
  redis: RedisStoreClient;
  /** Key prefix. Default: "lease". */
  prefix?: string;
}

export class RedisLeaderLeaseStore implements LeaderLeaseStore {
  private readonly redis: RedisStoreClient;
  private readonly prefix: string;

  constructor(config: RedisLeaderLeaseStoreConfig) {
    this.redis = config.redis;
    this.prefix = config.prefix ?? "lease";
  }

  /** The holder and epoch keys of a lease key. Fenced writers check `epoch`. */
  keysFor(key: string): { holder: string; epoch: string } {
    const base = `${this.prefix}:lease:{${key}}`;
    return { holder: `${base}:holder`, epoch: `${base}:epoch` };
  }

  async tryAcquireLeader(params: {
    key: string;
    instanceId: string;
    ttlMs: number;
  }): Promise<LeaderLease | null> {
    const keys = this.keysFor(params.key);
    // PX needs a positive TTL.
    const ttlMs = Math.max(1, Math.trunc(params.ttlMs));
    const epoch = Number(
      await this.redis.eval(ACQUIRE_LUA, 2, keys.holder, keys.epoch, params.instanceId, ttlMs),
    );
    if (!epoch) return null;
    return { key: params.key, instanceId: params.instanceId, epoch };
  }

  async releaseLeader(params: { lease: LeaderLease }): Promise<void> {
    const keys = this.keysFor(params.lease.key);
    await this.redis.eval(
      RELEASE_LUA,
      2,
      keys.holder,
      keys.epoch,
      params.lease.instanceId,
      String(params.lease.epoch),
    );
  }
}
