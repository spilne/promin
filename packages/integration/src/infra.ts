// ---------------------------------------------------------------------------
// Integration test infrastructure — testcontainer lifecycle helpers
//
// Usage:
//   withRedis("cache tests", (ctx) => {
//     it("stores and reads", async () => { ... });
//   });
//
//   withPostgres("queue tests", (ctx) => { ... });
//
//   withAll("e2e pipeline", (ctx) => { ... });
// ---------------------------------------------------------------------------

import { describe, beforeAll, afterAll, setDefaultTimeout } from "bun:test";

// Integration tests need longer timeouts for container startup
setDefaultTimeout(300_000);
import { GenericContainer, Wait, type StartedTestContainer } from "testcontainers";

// ---------------------------------------------------------------------------
// Container configs
// ---------------------------------------------------------------------------

const REDIS_IMAGE = "redis:7-alpine";
const POSTGRES_IMAGE = "postgres:17-alpine";

const TIMEOUT = 180_000; // container startup timeout

// ---------------------------------------------------------------------------
// Context — what tests receive
// ---------------------------------------------------------------------------

export interface RedisCtx {
  url: string;
  host: string;
  port: number;
}

export interface PostgresCtx {
  url: string;
  host: string;
  port: number;
}

export interface InfraCtx {
  redis?: RedisCtx;
  postgres?: PostgresCtx;
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

async function startPostgres(): Promise<{ container: StartedTestContainer; ctx: PostgresCtx }> {
  const container = await new GenericContainer(POSTGRES_IMAGE)
    .withExposedPorts(5432)
    .withEnvironment({
      POSTGRES_USER: "test",
      POSTGRES_PASSWORD: "test",
      POSTGRES_DB: "test",
    })
    .withCommand(["postgres", "-c", "fsync=off", "-c", "synchronous_commit=off"])
    .withWaitStrategy(Wait.forLogMessage("database system is ready to accept connections", 2))
    .withStartupTimeout(TIMEOUT)
    .start();

  const host = container.getHost();
  const port = container.getMappedPort(5432);

  return { container, ctx: { host, port, url: `postgres://test:test@${host}:${port}/test` } };
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

export function withPostgres(name: string, fn: TestFn<PostgresCtx>) {
  describe(name, () => {
    let container: StartedTestContainer;
    const ctx: PostgresCtx = { url: "", host: "", port: 0 };

    beforeAll(async () => {
      const result = await startPostgres();
      container = result.container;
      Object.assign(ctx, result.ctx);
    }, TIMEOUT);

    afterAll(async () => {
      await container?.stop();
    });

    fn(ctx);
  });
}

export function withAll(name: string, fn: TestFn<Required<InfraCtx>>) {
  describe(name, () => {
    const containers: StartedTestContainer[] = [];
    const ctx = {} as Required<InfraCtx>;

    beforeAll(async () => {
      const [r, p] = await Promise.all([startRedis(), startPostgres()]);
      containers.push(r.container, p.container);
      ctx.redis = r.ctx;
      ctx.postgres = p.ctx;
    }, TIMEOUT);

    afterAll(async () => {
      await Promise.all(containers.map((c) => c.stop()));
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
