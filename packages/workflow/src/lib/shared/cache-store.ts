// ---------------------------------------------------------------------------
// CacheStore<K, V> — key-value cache with TTL and eviction
// ---------------------------------------------------------------------------

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
