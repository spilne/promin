// ---------------------------------------------------------------------------
// Integration test infrastructure — testcontainer lifecycle helpers
//
// Usage:
//   withKafka("topic tests", (ctx) => {
//     it("publishes and consumes", async () => { ... });
//   });
//
//   withRedis("cache tests", (ctx) => { ... });
//
//   withAll("e2e pipeline", (ctx) => { ... });
// ---------------------------------------------------------------------------

import { describe, beforeAll, afterAll } from "bun:test";
import { GenericContainer, Wait, type StartedTestContainer } from "testcontainers";

// ---------------------------------------------------------------------------
// Container configs
// ---------------------------------------------------------------------------

const REDPANDA_IMAGE = "redpandadata/redpanda:v24.3.7";
const KAFKA_IMAGE = "apache/kafka:3.9.0";
const REDIS_IMAGE = "redis:7-alpine";
const POSTGRES_IMAGE = "postgres:17-alpine";

const TIMEOUT = 180_000; // container startup timeout
const KAFKA_TIMEOUT = 180_000; // Kafka (JVM) needs more time

// ---------------------------------------------------------------------------
// Context — what tests receive
// ---------------------------------------------------------------------------

export interface KafkaCtx {
  broker: string;
}

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
  kafka?: KafkaCtx;
  redis?: RedisCtx;
  postgres?: PostgresCtx;
}

// ---------------------------------------------------------------------------
// Container launchers
// ---------------------------------------------------------------------------

async function startKafka(): Promise<{ container: StartedTestContainer; ctx: KafkaCtx }> {
  // Redpanda — Kafka-compatible, starts in seconds (no JVM).
  // Use a fixed host port so the advertised listener matches what clients connect to.
  const hostPort = 29092 + Math.floor(Math.random() * 1000);

  const container = await new GenericContainer(KAFKA_IMAGE)
    .withExposedPorts({ container: 29092, host: hostPort })
    .withCommand([
      "redpanda",
      "start",
      "--smp",
      "1",
      "--memory",
      "256M",
      "--mode",
      "dev-container",
      "--kafka-addr",
      "PLAINTEXT://0.0.0.0:29092",
      "--advertise-kafka-addr",
      `PLAINTEXT://localhost:${hostPort}`,
    ])
    .withWaitStrategy(Wait.forLogMessage(/Successfully started Redpanda/))
    .withStartupTimeout(TIMEOUT)
    .start();

  return { container, ctx: { broker: `localhost:${hostPort}` } };
}

async function startApacheKafka(): Promise<{ container: StartedTestContainer; ctx: KafkaCtx }> {
  const hostPort = 19092 + Math.floor(Math.random() * 1000);

  const container = await new GenericContainer(KAFKA_IMAGE)
    .withExposedPorts({ container: 9092, host: hostPort })
    .withEnvironment({
      KAFKA_NODE_ID: "1",
      KAFKA_PROCESS_ROLES: "broker,controller",
      KAFKA_LISTENERS: "PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093",
      KAFKA_ADVERTISED_LISTENERS: `PLAINTEXT://localhost:${hostPort}`,
      KAFKA_CONTROLLER_QUORUM_VOTERS: "1@localhost:9093",
      KAFKA_CONTROLLER_LISTENER_NAMES: "CONTROLLER",
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: "1",
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: "0",
    })
    .withWaitStrategy(Wait.forLogMessage(/Kafka Server started/))
    .withStartupTimeout(KAFKA_TIMEOUT)
    .start();

  return { container, ctx: { broker: `localhost:${hostPort}` } };
}

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

export function withKafka(name: string, fn: TestFn<KafkaCtx>) {
  describe(name, () => {
    let container: StartedTestContainer;
    const ctx: KafkaCtx = { broker: "" };

    beforeAll(async () => {
      const result = await startKafka();
      container = result.container;
      Object.assign(ctx, result.ctx);
    }, TIMEOUT);

    afterAll(async () => {
      await container?.stop();
    });

    fn(ctx);
  });
}

export function withApacheKafka(name: string, fn: TestFn<KafkaCtx>) {
  describe(name, () => {
    let container: StartedTestContainer;
    const ctx: KafkaCtx = { broker: "" };

    beforeAll(async () => {
      const result = await startApacheKafka();
      container = result.container;
      Object.assign(ctx, result.ctx);
    }, KAFKA_TIMEOUT);

    afterAll(async () => {
      await container?.stop();
    });

    fn(ctx);
  });
}

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
      const [k, r, p] = await Promise.all([startKafka(), startRedis(), startPostgres()]);
      containers.push(k.container, r.container, p.container);
      ctx.kafka = k.ctx;
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
 * Generate a unique topic/stream/key name for test isolation.
 *
 * @example
 * ```ts
 * const topic = uniqueName("orders"); // "orders-a1b2c3"
 * ```
 */
export function uniqueName(prefix: string): string {
  return `${prefix}-${crypto.randomUUID().slice(0, 8)}`;
}
