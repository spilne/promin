/**
 * Caching — single-value, keyed, layered, and dynamic TTL
 *
 * Business flow:
 * 1. API gateway receives a request for user profile data
 * 2. Check in-memory L1 cache (10s TTL, 1000 entries max) — instant if fresh
 * 3. L1 miss → check shared Redis L2 cache (5 min TTL) — shared across instances
 * 4. L2 miss → fetch from database, write result back to both L1 and L2
 * 5. Next request for same user → served from L1, no network call
 * 6. After 10s, L1 expires → next request hits L2, refills L1 automatically
 * 7. OAuth tokens cached with dynamic TTL derived from the token's own expires_in field
 *
 * LRU eviction prevents memory growth — oldest entries removed when cache is full.
 */

import { Pipeline, PipelineCache, MemoryCache, layered } from "@promin/core";

// ---------------------------------------------------------------------------
// Single-value cache — for singletons like auth tokens or config
// ---------------------------------------------------------------------------

async function singletonCache() {
  const configCache = new PipelineCache<{ apiUrl: string; features: string[] }>(60_000);

  const getConfig = Pipeline.fromPromise(async () => {
    console.log("Fetching config from remote...");
    return { apiUrl: "https://api.example.com", features: ["v2", "beta"] };
  }).cached(configCache);

  await getConfig.runPromise(); // fetches
  await getConfig.runPromise(); // cached — no fetch
  await getConfig.runPromise(); // cached

  configCache.invalidate(); // force refresh
  await getConfig.runPromise(); // fetches again
}

// ---------------------------------------------------------------------------
// Keyed cache — different values by key
// ---------------------------------------------------------------------------

async function keyedCache() {
  const userCache = new MemoryCache<string, { id: string; name: string }>({
    ttlMs: 60_000,
    maxSize: 1000,
  });

  let fetchCount = 0;
  const getUser = (id: string) =>
    Pipeline.fromPromise(async () => {
      fetchCount++;
      return { id, name: `User ${id}` };
    }).cachedBy(userCache, `user:${id}`);

  await getUser("42").runPromise(); // fetches
  await getUser("42").runPromise(); // cached
  await getUser("99").runPromise(); // fetches (different key)
  await getUser("42").runPromise(); // still cached

  console.log(`Fetch count: ${fetchCount}`); // 2 (not 4)
}

// ---------------------------------------------------------------------------
// LRU eviction — oldest entries removed when full
// ---------------------------------------------------------------------------

async function lruEviction() {
  const cache = new MemoryCache<string, string>({ ttlMs: 60_000, maxSize: 3 });

  await cache.set("a", "first");
  await cache.set("b", "second");
  await cache.set("c", "third");

  // Cache is full (3 entries). Adding "d" evicts "a" (oldest)
  await cache.set("d", "fourth");

  console.log(await cache.get("a")); // undefined — evicted
  console.log(await cache.get("b")); // "second" — still there

  // Accessing "b" refreshes its position — now "c" is oldest
  await cache.get("b");
  await cache.set("e", "fifth"); // evicts "c"

  console.log(await cache.get("c")); // undefined — evicted
  console.log(await cache.get("b")); // "second" — refreshed, survives
}

// ---------------------------------------------------------------------------
// Layered cache — L1 (memory) → L2 (Redis) with automatic backfill
// ---------------------------------------------------------------------------

async function layeredCaching() {
  // L1: fast, small, short-lived
  const l1 = new MemoryCache<string, { id: string; score: number }>({
    ttlMs: 10_000, // 10 seconds
    maxSize: 100,
  });

  // L2: shared across instances, longer TTL
  // In production: new RedisCacheStore({ redis, prefix: "scores:", ttlMs: 300_000 })
  const l2 = new MemoryCache<string, { id: string; score: number }>({
    ttlMs: 300_000, // 5 minutes
  });

  const cache = layered(l1, l2);

  // First call: miss L1, miss L2 → compute → write to both
  let computeCalls = 0;
  const score = await cache.getOrCompute("user:42", async () => {
    computeCalls++;
    return { id: "42", score: 95 };
  });
  console.log(score); // { id: "42", score: 95 }

  // Second call: hit L1 → instant
  await cache.getOrCompute("user:42", async () => {
    computeCalls++; // not called
    return { id: "42", score: 0 };
  });

  console.log(`Compute calls: ${computeCalls}`); // 1

  // With Pipeline
  const getScore = (userId: string) =>
    Pipeline.fromPromise(async () => {
      return { id: userId, score: Math.random() * 100 };
    }).cachedBy(cache, `score:${userId}`);

  await getScore("42").runPromise(); // L1 or L2 hit, or compute
}

// ---------------------------------------------------------------------------
// Dynamic TTL — cache duration derived from the entity itself
// ---------------------------------------------------------------------------

async function dynamicTtl() {
  interface OAuthToken {
    accessToken: string;
    expiresIn: number; // seconds until expiry
    scope: string;
  }

  const tokenCache = new MemoryCache<string, OAuthToken>({ ttlMs: 3600_000 });

  const getToken = (clientId: string) =>
    Pipeline.fromPromise(async (): Promise<OAuthToken> => {
      console.log(`Fetching token for ${clientId}...`);
      return {
        accessToken: `tok_${Date.now()}`,
        expiresIn: 3600, // 1 hour
        scope: "read write",
      };
    }).cachedBy(tokenCache, `token:${clientId}`, {
      // TTL computed from the token's own expiry — cache exactly as long as it's valid
      ttl: (token) => (token.expiresIn - 60) * 1000, // expire 60s early for safety margin
    });

  const token = await getToken("client_1").runPromise();
  console.log(`Token cached for ${token.expiresIn - 60}s`);

  // Cached — no fetch
  await getToken("client_1").runPromise();
}

// ---------------------------------------------------------------------------
// Per-entry TTL override — mix short and long TTL in the same cache
// ---------------------------------------------------------------------------

async function perEntryTtl() {
  const cache = new MemoryCache<string, string>({ ttlMs: 60_000 }); // default: 1 min

  // Volatile data — short TTL
  await cache.set("stock:AAPL", "150.25", 5_000); // 5 seconds

  // Stable data — long TTL
  await cache.set("company:AAPL", "Apple Inc.", 3600_000); // 1 hour

  // After 10 seconds...
  // stock price expired, company name still cached
}

export { singletonCache, keyedCache, lruEviction, layeredCaching, dynamicTtl, perEntryTtl };
