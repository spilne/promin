// ---------------------------------------------------------------------------
// Integration test infrastructure — testcontainer lifecycle helpers
//
// Usage:
//   withRedis("cache tests", (ctx) => {
//     it("stores and reads", async () => { ... });
//   });
// ---------------------------------------------------------------------------

import { describe, beforeAll, afterAll, setDefaultTimeout } from "bun:test";

// Integration tests need longer timeouts for container startup
setDefaultTimeout(300_000);
import { GenericContainer, Wait, type StartedTestContainer } from "testcontainers";

// ---------------------------------------------------------------------------
// Container configs
// ---------------------------------------------------------------------------

const REDIS_IMAGE = "redis:7-alpine";

const TIMEOUT = 180_000; // container startup timeout

// ---------------------------------------------------------------------------
// Context — what tests receive
// ---------------------------------------------------------------------------

export interface RedisCtx {
  url: string;
  host: string;
  port: number;
}

// ---------------------------------------------------------------------------
// Container launchers
// ---------------------------------------------------------------------------

async function startRedis(): Promise<{ container: StartedTestContainer; ctx: RedisCtx }> {
  const container = await new GenericContainer(REDIS_IMAGE)
    .withExposedPorts(6379)
    .withWaitStrategy(Wait.forLogMessage("Ready to accept connections"))
    .withStartupTimeout(TIMEOUT)
    .start();

  const host = container.getHost();
  const port = container.getMappedPort(6379);

  return { container, ctx: { host, port, url: `redis://${host}:${port}` } };
}

// ---------------------------------------------------------------------------
// Describe wrappers — one per infra combo
// ---------------------------------------------------------------------------

type TestFn<T> = (ctx: T) => void;

export function withRedis(name: string, fn: TestFn<RedisCtx>) {
  describe(name, () => {
    let container: StartedTestContainer;
    const ctx: RedisCtx = { url: "", host: "", port: 0 };

    beforeAll(async () => {
      const result = await startRedis();
      container = result.container;
      Object.assign(ctx, result.ctx);
    }, TIMEOUT);

    afterAll(async () => {
      await container?.stop();
    });

    fn(ctx);
  });
}

// ---------------------------------------------------------------------------
// Test utilities
// ---------------------------------------------------------------------------

/**
 * Retry an assertion until it passes or timeout.
 * Use for eventually-consistent distributed assertions.
 *
 * @example
 * ```ts
 * await eventually(() => expect(sink.items.length).toBe(5));
 * await eventually(() => expect(metrics.count).toBeGreaterThan(0), { timeoutMs: 10_000 });
 * ```
 */
export async function eventually(
  assertion: () => void | Promise<void>,
  opts?: { timeoutMs?: number; intervalMs?: number },
): Promise<void> {
  const { timeoutMs = 5_000, intervalMs = 100 } = opts ?? {};
  const deadline = Date.now() + timeoutMs;
  let lastError: unknown;

  while (Date.now() < deadline) {
    try {
      await assertion();
      return;
    } catch (e) {
      lastError = e;
      await new Promise((r) => setTimeout(r, intervalMs));
    }
  }

  throw lastError;
}

/**
 * Generate a unique stream/key/table name for test isolation.
 *
 * @example
 * ```ts
 * const name = uniqueName("orders"); // "orders-a1b2c3"
 * ```
 */
export function uniqueName(prefix: string): string {
  return `${prefix}-${crypto.randomUUID().slice(0, 8)}`;
}
