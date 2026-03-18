// ---------------------------------------------------------------------------
// CacheStore<K, V> — key-value cache with TTL, layering, and eviction
// ---------------------------------------------------------------------------

import { Effect } from "effect";

// ---------------------------------------------------------------------------
// Interface
// ---------------------------------------------------------------------------

export interface CacheStore<K, V> {
  get(key: K): Promise<V | undefined>;
  set(key: K, value: V, ttlMs?: number): Promise<void>;
  delete(key: K): Promise<void>;
  has(key: K): Promise<boolean>;
  clear(): Promise<void>;
  size(): Promise<number>;
}

// ---------------------------------------------------------------------------
// In-memory cache with TTL + LRU eviction
// ---------------------------------------------------------------------------

interface MemoryCacheEntry<V> {
  value: V;
  expiresAt: number;
}

export interface MemoryCacheConfig {
  /** Default TTL in ms. */
  ttlMs: number;
  /** Max entries before LRU eviction. Default: Infinity. */
  maxSize?: number;
}

export class MemoryCache<K, V> implements CacheStore<K, V> {
  private readonly entries = new Map<K, MemoryCacheEntry<V>>();
  private readonly ttlMs: number;
  private readonly maxSize: number;

  constructor(config: MemoryCacheConfig) {
    this.ttlMs = config.ttlMs;
    this.maxSize = config.maxSize ?? Infinity;
  }

  async get(key: K): Promise<V | undefined> {
    const entry = this.entries.get(key);
    if (!entry) return undefined;
    if (Date.now() > entry.expiresAt) {
      this.entries.delete(key);
      return undefined;
    }
    // Move to end (most recently used)
    this.entries.delete(key);
    this.entries.set(key, entry);
    return entry.value;
  }

  async set(key: K, value: V, ttlMs?: number): Promise<void> {
    // Evict if at capacity
    if (this.entries.size >= this.maxSize && !this.entries.has(key)) {
      const oldest = this.entries.keys().next().value;
      if (oldest !== undefined) this.entries.delete(oldest);
    }
    this.entries.set(key, {
      value,
      expiresAt: Date.now() + (ttlMs ?? this.ttlMs),
    });
  }

  async delete(key: K): Promise<void> {
    this.entries.delete(key);
  }

  async has(key: K): Promise<boolean> {
    const entry = this.entries.get(key);
    if (!entry) return false;
    if (Date.now() > entry.expiresAt) {
      this.entries.delete(key);
      return false;
    }
    return true;
  }

  async clear(): Promise<void> {
    this.entries.clear();
  }

  async size(): Promise<number> {
    // Purge expired before counting
    const now = Date.now();
    for (const [key, entry] of this.entries) {
      if (now > entry.expiresAt) this.entries.delete(key);
    }
    return this.entries.size;
  }
}

// ---------------------------------------------------------------------------
// Layered cache — L1 → L2 → L3
// ---------------------------------------------------------------------------

/**
 * Chain multiple cache stores. Reads cascade down until a hit.
 * On hit, the value is written back to all layers above the hit.
 * On compute, the value is written to all layers.
 *
 * @example
 * ```ts
 * const cache = layered(
 *   new MemoryCache({ ttlMs: 10_000, maxSize: 1000 }),  // L1: fast, small
 *   redisCacheStore,                                      // L2: shared, medium
 *   postgresCacheStore,                                   // L3: durable, large
 * );
 *
 * // Miss all → compute → write to L1, L2, L3
 * const user = await cache.getOrCompute("user:42", () => fetchUser("42"));
 *
 * // Next call: hit L1 → instant
 * const same = await cache.getOrCompute("user:42", () => fetchUser("42"));
 * ```
 */
export class LayeredCache<K, V> implements CacheStore<K, V> {
  private readonly layers: CacheStore<K, V>[];

  constructor(...layers: CacheStore<K, V>[]) {
    if (layers.length === 0) throw new Error("LayeredCache needs at least one layer");
    this.layers = layers;
  }

  async get(key: K): Promise<V | undefined> {
    for (let i = 0; i < this.layers.length; i++) {
      const value = await this.layers[i]!.get(key);
      if (value !== undefined) {
        // Write back to all layers above the hit
        for (let j = 0; j < i; j++) {
          await this.layers[j]!.set(key, value);
        }
        return value;
      }
    }
    return undefined;
  }

  async set(key: K, value: V, ttlMs?: number): Promise<void> {
    // Write to all layers
    await Promise.all(this.layers.map((layer) => layer.set(key, value, ttlMs)));
  }

  async delete(key: K): Promise<void> {
    await Promise.all(this.layers.map((layer) => layer.delete(key)));
  }

  async has(key: K): Promise<boolean> {
    for (const layer of this.layers) {
      if (await layer.has(key)) return true;
    }
    return false;
  }

  async clear(): Promise<void> {
    await Promise.all(this.layers.map((layer) => layer.clear()));
  }

  async size(): Promise<number> {
    // Return L1 size (most relevant)
    return this.layers[0]!.size();
  }

  /**
   * Get from cache or compute. On miss, the value is written to all layers.
   */
  async getOrCompute(key: K, compute: () => Promise<V>): Promise<V> {
    const cached = await this.get(key);
    if (cached !== undefined) return cached;
    const value = await compute();
    await this.set(key, value);
    return value;
  }
}

export function layered<K, V>(...layers: CacheStore<K, V>[]): LayeredCache<K, V> {
  return new LayeredCache(...layers);
}

// ---------------------------------------------------------------------------
// Pipeline integration — .cachedBy(store, key)
// ---------------------------------------------------------------------------

/**
 * Wrap an Effect with a keyed cache lookup.
 * Used internally by Pipeline.cachedBy().
 *
 * @param ttl — static TTL in ms, or a function that computes TTL from the result.
 *              Useful for entities with their own expiry (OAuth tokens, session data).
 */
export function withCacheStore<T, E, K>(
  effect: Effect.Effect<T, E>,
  store: CacheStore<K, T>,
  key: K,
  ttl?: number | ((value: T) => number),
): Effect.Effect<T, E> {
  return Effect.suspend(() =>
    Effect.promise(() => store.get(key)).pipe(
      Effect.flatMap((cached) => {
        if (cached !== undefined) return Effect.succeed(cached);
        return effect.pipe(
          Effect.tap((value) =>
            Effect.promise(() => {
              const ttlMs = typeof ttl === "function" ? ttl(value) : ttl;
              return store.set(key, value, ttlMs);
            }),
          ),
        );
      }),
    ),
  ) as Effect.Effect<T, E>;
}
