// ---------------------------------------------------------------------------
// PgWakeups — Postgres LISTEN/NOTIFY as a `Wakeups` backend.
//
// `notify` sends `pg_notify(channel, "topic[:key]")` on the caller's
// database handle. `onWake` subscribers are served by one dedicated LISTEN
// connection, opened with the first subscriber and closed with the last. A
// lost connection is reopened with capped exponential backoff, and after a
// reconnect every subscriber is woken once (a notification sent while
// disconnected is gone for good).
//
// A hint only. NOTIFY is neither durable nor ordered across a reconnect, so
// every loop keeps polling; a wake just shortens the wait.
// ---------------------------------------------------------------------------

import { sql } from "drizzle-orm";
import postgres, { type Options as PostgresOptions } from "postgres";
import { SystemWallClock, type WallClock } from "@promin/workflow";
import {
  InMemoryWakeups,
  type WakeHandler,
  type WakeTopic,
  type Wakeups,
} from "@promin/workflow/distributed";
import type { DrizzleDb } from "@spilne/perfect-postgres";
import { execRaw } from "./exec-raw.ts";

/** An open LISTEN connection. */
export interface PgListenConnection {
  close(): Promise<void>;
}

/**
 * Opens a dedicated connection that LISTENs on `channel`. It calls
 * `onNotify` with each payload and `onClose` once if the connection is lost
 * (not after `close()`). Rejects if the connection cannot be opened.
 */
export type PgListenConnect = (params: {
  channel: string;
  onNotify: (payload: string) => void;
  onClose: (error?: unknown) => void;
}) => Promise<PgListenConnection>;

/**
 * A `PgListenConnect` over postgres.js: its own single connection per
 * `connect`, never pooled and never recycled by lifetime.
 */
export function postgresJsListen(params: {
  url: string;
  options?: PostgresOptions<Record<string, never>>;
}): PgListenConnect {
  return async ({ channel, onNotify, onClose }) => {
    let closing = false;
    const client = postgres(params.url, {
      ...params.options,
      max: 1,
      idle_timeout: 0,
      max_lifetime: null,
      onnotify: (_channel: string, payload: string) => onNotify(payload),
      onclose: () => {
        if (!closing) onClose();
      },
    } as PostgresOptions<Record<string, never>>);
    try {
      await client.unsafe(`LISTEN "${channel.replace(/"/g, '""')}"`);
    } catch (error) {
      closing = true;
      await client.end({ timeout: 0 }).catch(() => {});
      throw error;
    }
    return {
      close: async () => {
        closing = true;
        await client.end({ timeout: 1 }).catch(() => {});
      },
    };
  };
}

export interface PgWakeupsConfig {
  /** Handle `notify` sends on. */
  db: DrizzleDb;
  /** Opens the dedicated LISTEN connection (see `postgresJsListen`). */
  connect: PgListenConnect;
  /** Channel name. Default `"promin_wake"`. */
  channel?: string;
  /** First reconnect delay (ms), doubled per failure in a row. Default 250. */
  minReconnectMs?: number;
  /** Cap of the reconnect delay (ms). Default 30 000. */
  maxReconnectMs?: number;
  /** Time source for the reconnect delay. Default `SystemWallClock`. */
  clock?: WallClock;
  /** Random source in [0, 1) for reconnect jitter. Default `Math.random`. */
  random?: () => number;
  /** A failed notify, connect or dropped connection. Default: ignored (polling covers it). */
  onError?: (error: unknown) => void;
}

const TOPICS: readonly WakeTopic[] = ["steps", "results", "signals"];
/** NOTIFY payloads are limited to 8000 bytes; a longer key is dropped, the topic still goes out. */
const MAX_KEY_BYTES = 4000;

export class PgWakeups implements Wakeups {
  private readonly local = new InMemoryWakeups();
  private readonly db: DrizzleDb;
  private readonly connect: PgListenConnect;
  private readonly channel: string;
  private readonly minReconnectMs: number;
  private readonly maxReconnectMs: number;
  private readonly clock: WallClock;
  private readonly random: () => number;
  private readonly onError: (error: unknown) => void;

  private subscribers = 0;
  /** Bumped whenever listening starts or stops, so a stale connect attempt discards itself. */
  private epoch = 0;
  private connection: PgListenConnection | undefined;
  private retryTimer: { clear(): void } | undefined;
  private failures = 0;
  private everConnected = false;

  constructor(config: PgWakeupsConfig) {
    this.db = config.db;
    this.connect = config.connect;
    this.channel = config.channel ?? "promin_wake";
    this.minReconnectMs = config.minReconnectMs ?? 250;
    this.maxReconnectMs = config.maxReconnectMs ?? 30_000;
    this.clock = config.clock ?? SystemWallClock;
    this.random = config.random ?? Math.random;
    this.onError = config.onError ?? (() => {});
  }

  async notify(params: { topic: WakeTopic; key?: string }): Promise<void> {
    const key =
      params.key !== undefined && Buffer.byteLength(params.key) <= MAX_KEY_BYTES
        ? `:${params.key}`
        : "";
    try {
      await execRaw(this.db, sql`SELECT pg_notify(${this.channel}, ${params.topic + key})`);
    } catch (error) {
      this.onError(error);
    }
  }

  onWake(params: { topic: WakeTopic; handler: WakeHandler }): () => void {
    const unsubscribeLocal = this.local.onWake(params);
    this.subscribers++;
    if (this.subscribers === 1) this.startListening();
    let done = false;
    return () => {
      if (done) return;
      done = true;
      unsubscribeLocal();
      this.subscribers--;
      if (this.subscribers === 0) void this.stopListening();
    };
  }

  /** Close the LISTEN connection and stop reconnecting; subscribers stay registered but hear nothing more. */
  async close(): Promise<void> {
    this.subscribers = 0;
    await this.stopListening();
  }

  private startListening(): void {
    this.epoch++;
    this.failures = 0;
    void this.attempt(this.epoch);
  }

  private async stopListening(): Promise<void> {
    this.epoch++;
    this.retryTimer?.clear();
    this.retryTimer = undefined;
    const connection = this.connection;
    this.connection = undefined;
    this.everConnected = false;
    await connection?.close().catch(() => {});
  }

  private async attempt(epoch: number): Promise<void> {
    let connection: PgListenConnection;
    try {
      connection = await this.connect({
        channel: this.channel,
        onNotify: (payload) => this.deliver(payload),
        onClose: (error) => {
          if (epoch !== this.epoch) return;
          this.connection = undefined;
          this.onError(error ?? new Error("LISTEN connection closed"));
          this.scheduleRetry(epoch);
        },
      });
    } catch (error) {
      if (epoch !== this.epoch) return;
      this.onError(error);
      this.scheduleRetry(epoch);
      return;
    }
    if (epoch !== this.epoch) {
      await connection.close().catch(() => {});
      return;
    }
    this.connection = connection;
    this.failures = 0;
    // Notifications sent while we were not listening are gone: wake everyone once.
    if (this.everConnected) for (const topic of TOPICS) this.local.notify({ topic });
    this.everConnected = true;
  }

  private scheduleRetry(epoch: number): void {
    const base = Math.min(this.maxReconnectMs, this.minReconnectMs * 2 ** this.failures);
    this.failures++;
    // Jitter in [0.5, 1) of the delay, so a fleet does not reconnect in lockstep.
    const delay = Math.round(base * (0.5 + this.random() / 2));
    this.retryTimer = this.clock.setTimeout(() => {
      this.retryTimer = undefined;
      if (epoch === this.epoch) void this.attempt(epoch);
    }, delay);
  }

  private deliver(payload: string): void {
    const colon = payload.indexOf(":");
    const topic = (colon === -1 ? payload : payload.slice(0, colon)) as WakeTopic;
    if (!TOPICS.includes(topic)) return;
    this.local.notify({ topic, ...(colon !== -1 && { key: payload.slice(colon + 1) }) });
  }
}
