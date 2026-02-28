// ---------------------------------------------------------------------------
// Test utilities — Postgres container for integration tests
// ---------------------------------------------------------------------------

import { GenericContainer, Wait, type StartedTestContainer } from "testcontainers";
import { drizzle } from "drizzle-orm/postgres-js";
import postgres from "postgres";

const POSTGRES_IMAGE = "postgres:17-alpine";

export interface PostgresContainerConfig {
  user?: string;
  password?: string;
  database?: string;
  startupTimeout?: number;
}

export class PostgresTestContainer {
  private container?: StartedTestContainer;
  private _sql?: ReturnType<typeof postgres>;
  private _db?: ReturnType<typeof drizzle>;

  private readonly user: string;
  private readonly password: string;
  private readonly database: string;
  private readonly startupTimeout: number;

  constructor(config?: PostgresContainerConfig) {
    this.user = config?.user ?? "test";
    this.password = config?.password ?? "test";
    this.database = config?.database ?? "test";
    this.startupTimeout = config?.startupTimeout ?? 30_000;
  }

  async start(): Promise<void> {
    this.container = await new GenericContainer(POSTGRES_IMAGE)
      .withExposedPorts(5432)
      .withEnvironment({
        POSTGRES_USER: this.user,
        POSTGRES_PASSWORD: this.password,
        POSTGRES_DB: this.database,
      })
      .withCommand(["postgres", "-c", "fsync=off", "-c", "synchronous_commit=off"])
      .withWaitStrategy(Wait.forLogMessage("database system is ready to accept connections", 2))
      .withStartupTimeout(this.startupTimeout)
      .start();

    const connectionString = `postgres://${this.user}:${this.password}@${this.container.getHost()}:${this.container.getMappedPort(5432)}/${this.database}`;
    this._sql = postgres(connectionString);
    this._db = drizzle(this._sql);
  }

  async stop(): Promise<void> {
    await this._sql?.end();
    await this.container?.stop();
    this.container = undefined;
    this._sql = undefined;
    this._db = undefined;
  }

  get db(): ReturnType<typeof drizzle> {
    if (!this._db) throw new Error("Container not started. Call start() first.");
    return this._db;
  }

  get sql(): ReturnType<typeof postgres> {
    if (!this._sql) throw new Error("Container not started. Call start() first.");
    return this._sql;
  }
}
