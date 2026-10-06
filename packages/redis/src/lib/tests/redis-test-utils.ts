// ---------------------------------------------------------------------------
// Test utilities — one Redis testcontainer per describe block
// ---------------------------------------------------------------------------

import { afterAll, beforeAll, describe, setDefaultTimeout } from "bun:test";
import { Redis as IoRedis } from "ioredis";
import { GenericContainer, Wait, type StartedTestContainer } from "testcontainers";
import type { RedisStoreClient } from "../redis-client.ts";

const REDIS_IMAGE = "redis:7-alpine";
const STARTUP_TIMEOUT_MS = 180_000;

setDefaultTimeout(30_000);

export interface RedisTestContext {
  /** Open a new connection. Closed automatically after the describe block. */
  client(): RedisStoreClient;
}

/**
 * `describe()` wrapper that starts a Redis container before the block and
 * stops it (and every connection handed out by `ctx.client()`) afterwards.
 */
export function redisDescribe(name: string, fn: (ctx: RedisTestContext) => void): void {
  describe(name, () => {
    let container: StartedTestContainer | undefined;
    const clients: IoRedis[] = [];

    beforeAll(async () => {
      container = await new GenericContainer(REDIS_IMAGE)
        .withExposedPorts(6379)
        .withWaitStrategy(Wait.forLogMessage("Ready to accept connections"))
        .withStartupTimeout(STARTUP_TIMEOUT_MS)
        .start();
    }, STARTUP_TIMEOUT_MS);

    afterAll(async () => {
      for (const c of clients) c.disconnect();
      await container?.stop();
    });

    fn({
      client() {
        if (!container) throw new Error("Redis container not started");
        const c = new IoRedis(container.getMappedPort(6379), container.getHost());
        clients.push(c);
        // ioredis overloads (optional callbacks) don't line up with the
        // variadic driver-agnostic signatures, though it implements them.
        return c as unknown as RedisStoreClient;
      },
    });
  });
}

/** Unique key prefix so tests sharing a container don't see each other's keys. */
export function uniquePrefix(name: string): string {
  return `${name}-${crypto.randomUUID().slice(0, 8)}`;
}
